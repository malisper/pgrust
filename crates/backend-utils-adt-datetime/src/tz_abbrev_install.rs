//! `InstallTimeZoneAbbrevs` (datetime.c) + the production [`TimezoneResolver`]
//! over the installed runtime abbreviation table.
//!
//! C keeps the active abbreviation table in the file-static `zoneabbrevtbl` and
//! looks abbreviations up with `datebsearch` over `zoneabbrevtbl->abbrevs`,
//! resolving `DYNTZ` rows through `FetchDynamicTimeZone` (which calls
//! `pg_tzset`).  `InstallTimeZoneAbbrevs(tbl)` simply stores the table and
//! clears `tzabbrevcache`.
//!
//! Here the table is the owned [`TimeZoneAbbrevTable`] (a sorted-by-abbrev
//! `Vec<TzEntry>`) produced by `ConvertTimeZoneAbbrevs`.  We park it in a
//! thread-local (the analogue of the C static) and install a zero-sized
//! [`AbbrevTableResolver`] into the crate's resolver hook so
//! `DecodeTimezoneAbbrev`'s `zoneabbrevtbl` leg works for real, including the
//! `DYNTZ` -> `pg_tzset` path.

use std::cell::RefCell;
use std::rc::Rc;

use types_misc_more2::TimeZoneAbbrevTable;
use types_pgtime::pg_tz;

use crate::tz_resolver::{set_timezone_resolver, TimezoneResolver, TzAbbrev};

thread_local! {
    /// C analogue of the file-static `zoneabbrevtbl` (datetime.c).  `None` is
    /// the C `zoneabbrevtbl == NULL` state (no table installed yet).
    static ZONEABBREVTBL: RefCell<Option<TimeZoneAbbrevTable>> = const { RefCell::new(None) };
}

/// The production resolver: a zero-sized type whose `resolve_abbrev` searches
/// the thread-local [`ZONEABBREVTBL`] (the analogue of `datebsearch` over
/// `zoneabbrevtbl->abbrevs`), resolving `DYNTZ` rows through `pg_tzset`
/// (C: `FetchDynamicTimeZone`).
struct AbbrevTableResolver;

impl TimezoneResolver for AbbrevTableResolver {
    fn resolve_abbrev(&self, abbrev: &str) -> Option<TzAbbrev> {
        ZONEABBREVTBL.with(|cell| {
            let borrow = cell.borrow();
            let tbl = borrow.as_ref()?;

            // C: datebsearch(lowtoken, zoneabbrevtbl->abbrevs, numabbrevs).
            // `abbrev` is already lowercased and the entries are stored lowercased
            // and kept sorted by abbrev (tzparser::add_to_array), so a binary
            // search reproduces datebsearch.
            let idx = tbl
                .abbrevs
                .binary_search_by(|e| e.abbrev.as_str().cmp(abbrev))
                .ok()?;
            let entry = &tbl.abbrevs[idx];

            match &entry.zone {
                // C: dtoken->type == DYNTZ -> FetchDynamicTimeZone -> pg_tzset.
                Some(zone) => match fetch_dynamic_time_zone(zone) {
                    Some(tz) => Some(TzAbbrev::dyntz(tz)),
                    // C: pg_tzset returned NULL -> DTERR_BAD_ZONE_ABBREV; carry
                    // the underlying zone name out for the error message.
                    None => Some(TzAbbrev::dyntz_failed(zone.clone())),
                },
                // C: fixed abbreviation (TZ/DTZ), *offset = tp->value.
                None => Some(TzAbbrev::fixed(entry.offset, entry.is_dst)),
            }
        })
    }
}

/// `FetchDynamicTimeZone` (datetime.c): resolve a `DYNTZ` abbreviation's
/// underlying zone via `pg_tzset`.  C caches the resolved `pg_tz *` inside the
/// `DynamicZoneAbbrev`; `pg_tzset` itself memoizes by name (the timezone hash
/// table), so re-resolving on each lookup is observably identical.  Returns
/// `None` on a bogus zone name (C: `pg_tzset` returns NULL).
fn fetch_dynamic_time_zone(zone: &str) -> Option<Rc<pg_tz>> {
    // C: dtza->tz = pg_tzset(dtza->zone);
    backend_timezone_pgtz::pg_tzset(zone).ok().flatten()
}

/// `InstallTimeZoneAbbrevs(tbl)` (datetime.c): make `tbl` the active
/// abbreviation table and reset the lookup cache.  Here we also ensure the
/// production [`AbbrevTableResolver`] is installed into the crate's resolver hook
/// (idempotent), so `DecodeTimezoneAbbrev`'s `zoneabbrevtbl` leg consults the
/// new table.
pub fn install_time_zone_abbrevs(tbl: TimeZoneAbbrevTable) {
    // zoneabbrevtbl = tbl;
    ZONEABBREVTBL.with(|cell| *cell.borrow_mut() = Some(tbl));

    // reset tzabbrevcache (memset 0).  The crate's abbrev lookups are not
    // memoized in a separate cache structure here; the per-call resolver read
    // already reflects the new table, so there is no stale cache to clear.

    // Install the production resolver (replacing the default "no resolver",
    // i.e. C's zoneabbrevtbl == NULL behavior).  Idempotent: re-installing the
    // same ZST is a no-op.
    static RESOLVER: AbbrevTableResolver = AbbrevTableResolver;
    set_timezone_resolver(Some(&RESOLVER));
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_datetime::{DTZ, DYNTZ, TZ};
    use types_misc_more2::TzEntry;

    fn entry(abbrev: &str, zone: Option<&str>, offset: i32, is_dst: bool) -> TzEntry {
        TzEntry {
            abbrev: abbrev.to_string(),
            zone: zone.map(|z| z.to_string()),
            offset,
            is_dst,
            lineno: 0,
            filename: String::new(),
        }
    }

    #[test]
    fn install_resolves_fixed_and_unknown() {
        let _g = crate::tz_resolver::TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Sorted-by-abbrev table (tzparser keeps it sorted).
        let tbl = TimeZoneAbbrevTable {
            abbrevs: vec![
                entry("abc", None, 3600, false),
                entry("xyz", None, -7200, true),
            ],
        };
        install_time_zone_abbrevs(tbl);

        let r = crate::tz_resolver::timezone_resolver().expect("resolver installed");
        let a = r.resolve_abbrev("abc").expect("abc known");
        assert_eq!((a.ftype, a.gmtoff, a.isdst), (TZ, 3600, false));
        let x = r.resolve_abbrev("xyz").expect("xyz known");
        assert_eq!((x.ftype, x.gmtoff, x.isdst), (DTZ, -7200, true));
        assert!(r.resolve_abbrev("nope").is_none());

        // Clear for other tests on this worker thread.
        ZONEABBREVTBL.with(|c| *c.borrow_mut() = None);
        crate::tz_resolver::set_timezone_resolver(None);
    }

    #[test]
    fn install_resolves_dyntz_bad_zone() {
        let _g = crate::tz_resolver::TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let tbl = TimeZoneAbbrevTable {
            abbrevs: vec![entry("zzz", Some("Nowhere/Land"), 0, false)],
        };
        install_time_zone_abbrevs(tbl);

        let r = crate::tz_resolver::timezone_resolver().unwrap();
        let bad = r.resolve_abbrev("zzz").expect("zzz known");
        assert_eq!(bad.ftype, DYNTZ);
        assert!(bad.tz.is_none());
        assert_eq!(bad.dyntz_zone.as_deref(), Some("Nowhere/Land"));

        ZONEABBREVTBL.with(|c| *c.borrow_mut() = None);
        crate::tz_resolver::set_timezone_resolver(None);
    }
}
