//! Timezone-resolution interface hook for the decode engine, ported as a
//! crate-local deferred-provider trait + thread-local `Cell` (the idiomatic
//! analogue of `backend-regex-core`'s `RegexCollationResolver`).
//!
//! # Why a hook?
//!
//! Full tzdb zone names (e.g. `America/New_York`) are resolved by calling
//! [`types_pgtime::pg_tzset`] directly -- a fully-implemented,
//! safe-Rust tzdb loader that the rest of this crate already uses (see
//! `DecodeTimezoneName`'s "full zone name" leg and the `UNKNOWN_FIELD` legs of
//! `DecodeDateTime`/`DecodeTimeOnly`, all of which call `pg_tzset`).  No
//! resolver is needed for that.
//!
//! What *does* still need a hook is the runtime-loaded **timezone abbreviation
//! table** (`zoneabbrevtbl`, populated from the `timezone_abbreviations` GUC
//! file).  This is the table consulted by `DecodeTimezoneAbbrev` *after*
//! `session_timezone`; a hit may be a fixed offset (`TZ`/`DTZ`) or a dynamic
//! abbreviation (`DYNTZ`) that references an underlying zone via
//! `FetchDynamicTimeZone`.  This table depends on backend GUC machinery the
//! rest of this crate deliberately does not pull in, so we route that one
//! lookup through an installable [`TimezoneResolver`].  When no resolver is
//! installed (the default) it acts as `zoneabbrevtbl == NULL`: unknown
//! abbreviations return `UNKNOWN_FIELD` (not an error), just as in C.
//!
//! The `session_timezone` leg of `DecodeTimezoneAbbrev` (IANA-known abbrevs
//! such as `EST`) needs *no* resolver either: it already works through the
//! reused `backend_timezone_localtime` engine.  The resolver only fills the
//! `zoneabbrevtbl` leg.
//!
//! This is a plain trait + thread-local `Cell` (NOT a `seams::seam!`/`Runtime`),
//! exactly as the C-ABI reference models it; it is allowed as a crate-local
//! module.

use core::cell::Cell;
use std::rc::Rc;

use types_pgtime::pg_tz;
use types_datetime::{DTZ, DYNTZ, TZ};

/// The result of resolving a timezone abbreviation against the runtime
/// abbreviation table (C: a `datetkn` row of `zoneabbrevtbl`).
///
/// C `DecodeTimezoneAbbrev` distinguishes two outcomes for a table hit:
///
///  * a fixed-offset abbreviation (`tp->type == TZ` or `DTZ`): `*offset` is the
///    abbreviation's GMT offset (zoneabbrevtbl sign convention) and `*tz` is
///    `NULL`.
///  * a dynamic abbreviation (`tp->type == DYNTZ`): `*offset` is `0` and `*tz`
///    is the underlying zone returned by `FetchDynamicTimeZone`.
///
/// This struct carries both the `(gmtoff, isdst)` pair and the resolved DTK
/// type so the caller can reproduce that branch faithfully.
#[derive(Clone)]
pub struct TzAbbrev {
    /// The DTK field type for this abbreviation: [`TZ`], [`DTZ`], or [`DYNTZ`].
    pub ftype: i32,
    /// GMT offset in seconds, in the **zoneabbrevtbl sign convention** (the
    /// same convention `datetkn.value` uses: positive is *west* of Greenwich,
    /// i.e. already sign-flipped relative to `DetermineTimeZoneOffset`).  Only
    /// meaningful for the fixed (`TZ`/`DTZ`) case; `0` for `DYNTZ`.
    pub gmtoff: i32,
    /// Whether this is a daylight-saving abbreviation (selects `DTZ` over `TZ`
    /// for the fixed case).
    pub isdst: bool,
    /// For [`DYNTZ`], the underlying zone the abbreviation references
    /// (C: `FetchDynamicTimeZone`'s result).  `None` for the fixed case, and
    /// also `None` when a `DYNTZ` abbreviation's configured underlying zone
    /// failed to load (C: `FetchDynamicTimeZone` returned `NULL`); in that
    /// failure case [`dyntz_zone`](Self::dyntz_zone) carries the underlying
    /// zone name for the error report.
    pub tz: Option<Rc<pg_tz>>,
    /// For a [`DYNTZ`] abbreviation whose underlying zone failed to load, the
    /// name of that underlying zone (C: `DynamicZoneAbbrev.zone`, surfaced as
    /// `DateTimeErrorExtra.dtee_timezone`).  Used to build the
    /// `DTERR_BAD_ZONE_ABBREV` error message, which names the underlying zone,
    /// not the abbreviation.  `None` in every non-failure case.
    pub dyntz_zone: Option<String>,
}

impl TzAbbrev {
    /// Construct a fixed-offset abbreviation result (C: `tp->type` is `TZ`/`DTZ`,
    /// `*offset = tp->value`, `*tz = NULL`).  `gmtoff` is in the zoneabbrevtbl
    /// sign convention.
    pub fn fixed(gmtoff: i32, isdst: bool) -> Self {
        TzAbbrev {
            ftype: if isdst { DTZ } else { TZ },
            gmtoff,
            isdst,
            tz: None,
            dyntz_zone: None,
        }
    }

    /// Construct a dynamic-abbreviation result (C: `tp->type == DYNTZ`,
    /// `*offset = 0`, `*tz = FetchDynamicTimeZone(...)`).
    pub fn dyntz(tz: Rc<pg_tz>) -> Self {
        TzAbbrev {
            ftype: DYNTZ,
            gmtoff: 0,
            isdst: false,
            tz: Some(tz),
            dyntz_zone: None,
        }
    }

    /// Construct a *failed* dynamic-abbreviation result: the abbreviation is a
    /// `DYNTZ` whose configured underlying zone `zone_name` could not be loaded
    /// (C: `FetchDynamicTimeZone` returned `NULL`, leaving
    /// `extra->dtee_timezone = dtza->zone`).  The crate reports this as
    /// `DTERR_BAD_ZONE_ABBREV`, naming `zone_name` in the principal message and
    /// the abbreviation in the detail.
    pub fn dyntz_failed(zone_name: impl Into<String>) -> Self {
        TzAbbrev {
            ftype: DYNTZ,
            gmtoff: 0,
            isdst: false,
            tz: None,
            dyntz_zone: Some(zone_name.into()),
        }
    }
}

/// Installable resolver for the one timezone lookup that the rest of this
/// crate cannot perform on its own: the runtime abbreviation table.  (Full
/// tzdb zone names are resolved directly via
/// [`types_pgtime::pg_tzset`] and need no resolver.)  Mirrors the
/// role of `backend_regex_core::RegexCollationResolver`.
pub trait TimezoneResolver: Sync {
    /// Resolve a timezone abbreviation against the runtime abbreviation table
    /// (C: the `zoneabbrevtbl` leg of `DecodeTimezoneAbbrev`, including
    /// `FetchDynamicTimeZone` for the `DYNTZ` case).  `abbrev` is the
    /// already-lowercased token.  Returns `None` if the abbreviation is not in
    /// the table (C: a `NULL` `datebsearch` result -> `UNKNOWN_FIELD`).
    fn resolve_abbrev(&self, abbrev: &str) -> Option<TzAbbrev>;
}

thread_local! {
    /// C analogue: there is no single global here -- the lookup lives in
    /// `zoneabbrevtbl`.  We park the installable resolver in a thread-local
    /// `Cell`, exactly as `regc_pg_locale.rs` parks `REGEX_COLLATION_RESOLVER`.
    static TIMEZONE_RESOLVER: Cell<Option<&'static dyn TimezoneResolver>> =
        const { Cell::new(None) };
}

/// Install (or clear) the timezone resolver, returning the previous one.
///
/// This is the hook through which a ported abbreviation-table loader plugs in,
/// analogous to `backend_regex_core::set_regex_collation_resolver`.
pub fn set_timezone_resolver(
    resolver: Option<&'static dyn TimezoneResolver>,
) -> Option<&'static dyn TimezoneResolver> {
    TIMEZONE_RESOLVER.with(|slot| slot.replace(resolver))
}

/// The currently-installed resolver, or `None` (the default: no resolver).
pub(crate) fn timezone_resolver() -> Option<&'static dyn TimezoneResolver> {
    TIMEZONE_RESOLVER.with(|slot| slot.get())
}

// ===========================================================================
// Test support
// ===========================================================================

/// Serialization lock for tests that install a [`TimezoneResolver`].  The slot
/// is thread-local, but the cargo test runner reuses worker threads, so two
/// tests can land on the same thread; holding this lock (and always restoring
/// the previous resolver) keeps each test observing only its own install.
#[cfg(test)]
pub(crate) static TZ_RESOLVER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A small test resolver for the abbreviation-table hook:
///  * `resolve_abbrev` maps the custom abbreviation "xyz" to a fixed +3600s,
///    non-DST offset (zoneabbrevtbl sign convention: `value` is +east, so a
///    +1:00 zone has `gmtoff == 3600`).
///  * "badzone" is a `DYNTZ` abbreviation whose configured underlying zone
///    ("Nowhere/Land") fails to load (C: `FetchDynamicTimeZone` -> `NULL`).
#[cfg(test)]
pub(crate) struct TestTimezoneResolver;

#[cfg(test)]
impl TimezoneResolver for TestTimezoneResolver {
    fn resolve_abbrev(&self, abbrev: &str) -> Option<TzAbbrev> {
        // `abbrev` arrives already lowercased.
        match abbrev {
            // A +01:00 fixed abbreviation.  zoneabbrevtbl `value` uses the ISO
            // sign convention (positive == east of Greenwich), so +1:00 == 3600.
            "xyz" => Some(TzAbbrev::fixed(3600, false)),
            // A DYNTZ abbreviation whose underlying zone cannot be loaded.
            "badzone" => Some(TzAbbrev::dyntz_failed("Nowhere/Land")),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_accessor_is_none() {
        let _g = TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // No resolver installed on this thread by default.
        let prev = set_timezone_resolver(None);
        assert!(timezone_resolver().is_none());
        // Restore whatever was there.
        set_timezone_resolver(prev);
    }

    #[test]
    fn install_and_restore_round_trips() {
        let _g = TZ_RESOLVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        static R: TestTimezoneResolver = TestTimezoneResolver;

        let prev = set_timezone_resolver(Some(&R));
        assert!(timezone_resolver().is_some());

        // resolve_abbrev behaves as configured.
        let r = timezone_resolver().unwrap();
        let a = r.resolve_abbrev("xyz").expect("xyz known");
        assert_eq!((a.gmtoff, a.isdst), (3600, false));
        assert_eq!(a.ftype, TZ);
        assert!(r.resolve_abbrev("nope").is_none());

        // A DYNTZ abbreviation whose underlying zone fails to load carries the
        // underlying zone name for the error report.
        let bad = r.resolve_abbrev("badzone").expect("badzone known");
        assert_eq!(bad.ftype, DYNTZ);
        assert!(bad.tz.is_none());
        assert_eq!(bad.dyntz_zone.as_deref(), Some("Nowhere/Land"));

        let restored = set_timezone_resolver(prev);
        // We just put our resolver back; restoring returns it.
        assert!(restored.is_some());
        assert!(timezone_resolver().is_none());
    }

    #[test]
    fn dyntz_constructor_carries_zone() {
        let z = state_pgtz::session_timezone();
        let a = TzAbbrev::dyntz(z);
        assert_eq!(a.ftype, DYNTZ);
        assert_eq!(a.gmtoff, 0);
        assert!(a.tz.is_some());
    }
}
