//! Port of PostgreSQL's `src/backend/utils/adt/amutils.c` — SQL-level APIs
//! related to index access methods.
//!
//! # Scope (every C function ported in full; no in-crate deferrals)
//!
//! * `lookup_prop_name` — string property name → [`IndexAmProperty`] enum.
//! * `test_indoption` — inspect an `indoption` array element (the
//!   `INDOPTION_DESC` / `INDOPTION_NULLS_FIRST` bits).
//! * `indexam_property` — the shared worker behind all four SQL functions.
//! * `pg_indexam_has_property` — `SELECT pg_indexam_has_property(amoid, prop)`.
//! * `pg_index_has_property` — `SELECT pg_index_has_property(indexoid, prop)`.
//! * `pg_index_column_has_property` —
//!   `SELECT pg_index_column_has_property(indexoid, colno, prop)`.
//! * `pg_indexam_progress_phasename` —
//!   `SELECT pg_indexam_progress_phasename(amoid, phasenum)`.
//!
//! # Seams
//!
//! amutils.c is almost entirely a thin SQL wrapper over the index AM layer and
//! the catalog. The genuinely-external call-outs — the syscache/relcache
//! lookups (`SearchSysCache1` of `pg_class` / `pg_index`, the `indoption`
//! array), the index AM API (`GetIndexAmRoutineByAmId` and the AM's
//! `amproperty` / `ambuildphasename` callbacks), and the generic `index_open` /
//! `index_can_return` / `index_close` fallback — are funneled through the
//! `backend-utils-adt-amutils-seams` crate (one fn-pointer slot per call-out,
//! each defaulting to a loud panic until installed). The catalog projections
//! (`index_relation`, `index_form`) are installed by the syscache owner; the
//! index-AM seams (`am_routine`, `am_property`, `index_can_return`,
//! `am_buildphasename`) by the amapi owner. The seam returns *raw* AM-routine
//! flags and *raw* catalog rows, so the entire `indexam_property` decision
//! tree, `test_indoption` and `lookup_prop_name` are ported 1:1 and live in
//! this crate.
//!
//! The SQL-level argument unmarshalling the C `PG_FUNCTION_ARGS` body performs
//! (`PG_GETARG_OID`, `text_to_cstring(PG_GETARG_TEXT_PP(...))`,
//! `CStringGetTextDatum`) is the bare-word `PGFunction` registry boundary,
//! which is deferred; these functions take/return the already-unmarshalled
//! scalar values.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid};
use types_error::PgResult;

use backend_utils_adt_amutils_seams as seam;
use backend_utils_adt_amutils_seams::{AmPropertyRequest, IndexAmProperty};

/// `pg_class.h`: `RELKIND_INDEX` — a secondary (non-partitioned) index.
const RELKIND_INDEX: u8 = b'i';
/// `pg_class.h`: `RELKIND_PARTITIONED_INDEX` — a partitioned index.
const RELKIND_PARTITIONED_INDEX: u8 = b'I';

/// `pg_index.h`: `INDOPTION_DESC` — column values are in reverse (DESC) order.
pub const INDOPTION_DESC: i16 = 0x0001;
/// `pg_index.h`: `INDOPTION_NULLS_FIRST` — NULLs sort first instead of last.
pub const INDOPTION_NULLS_FIRST: i16 = 0x0002;

/// C macro `OidIsValid(objectId)` — `(objectId) != InvalidOid`.
#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != InvalidOid
}

/// C: `struct am_propname` — one entry of the property-name lookup table.
struct AmPropName {
    name: &'static str,
    prop: IndexAmProperty,
}

/// C: `static const struct am_propname am_propnames[]`.
static AM_PROPNAMES: &[AmPropName] = &[
    AmPropName { name: "asc", prop: IndexAmProperty::Asc },
    AmPropName { name: "desc", prop: IndexAmProperty::Desc },
    AmPropName { name: "nulls_first", prop: IndexAmProperty::NullsFirst },
    AmPropName { name: "nulls_last", prop: IndexAmProperty::NullsLast },
    AmPropName { name: "orderable", prop: IndexAmProperty::Orderable },
    AmPropName { name: "distance_orderable", prop: IndexAmProperty::DistanceOrderable },
    AmPropName { name: "returnable", prop: IndexAmProperty::Returnable },
    AmPropName { name: "search_array", prop: IndexAmProperty::SearchArray },
    AmPropName { name: "search_nulls", prop: IndexAmProperty::SearchNulls },
    AmPropName { name: "clusterable", prop: IndexAmProperty::Clusterable },
    AmPropName { name: "index_scan", prop: IndexAmProperty::IndexScan },
    AmPropName { name: "bitmap_scan", prop: IndexAmProperty::BitmapScan },
    AmPropName { name: "backward_scan", prop: IndexAmProperty::BackwardScan },
    AmPropName { name: "can_order", prop: IndexAmProperty::CanOrder },
    AmPropName { name: "can_unique", prop: IndexAmProperty::CanUnique },
    AmPropName { name: "can_multi_col", prop: IndexAmProperty::CanMultiCol },
    AmPropName { name: "can_exclude", prop: IndexAmProperty::CanExclude },
    AmPropName { name: "can_include", prop: IndexAmProperty::CanInclude },
];

/// `pg_strcasecmp(s1, s2)` (`src/port/pgstrcasecmp.c`) over NUL-free byte
/// slices — an ASCII-case-insensitive compare of the property-name strings.
/// This is a pure leaf helper (no cross-subsystem dependency), so it is ported
/// inline. Returns the C-style sign of the first differing byte after each is
/// folded; the shorter prefix sorts first (its implicit terminating NUL is byte
/// 0).
///
/// The property names in `am_propnames` and the SQL-supplied name are ASCII, so
/// the high-bit/locale `tolower` path C uses is not exercised; only the ASCII
/// `'A'..='Z'` fold is modeled.
fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let mut i = 0usize;
    loop {
        // C reads through the terminating NUL; model "at or past end" as byte 0.
        let mut ch1: u8 = if i < s1.len() { s1[i] } else { 0 };
        let mut ch2: u8 = if i < s2.len() { s2[i] } else { 0 };
        if ch1 != ch2 {
            if ch1.is_ascii_uppercase() {
                ch1 += b'a' - b'A';
            }
            if ch2.is_ascii_uppercase() {
                ch2 += b'a' - b'A';
            }
            if ch1 != ch2 {
                // return (int) ch1 - (int) ch2;
                return ch1 as i32 - ch2 as i32;
            }
        }
        if ch1 == 0 {
            break;
        }
        i += 1;
    }
    0
}

/// C: `lookup_prop_name` — map a property name string to [`IndexAmProperty`]
/// using a case-insensitive scan over `am_propnames`.
///
/// > We do not throw an error, so that AMs can define their own properties.
///
/// so an unrecognized name yields [`IndexAmProperty::Unknown`].
pub fn lookup_prop_name(name: &str) -> IndexAmProperty {
    for entry in AM_PROPNAMES {
        // C: pg_strcasecmp(am_propnames[i].name, name) == 0
        if pg_strcasecmp(entry.name.as_bytes(), name.as_bytes()) == 0 {
            return entry.prop;
        }
    }

    // We do not throw an error, so that AMs can define their own properties.
    IndexAmProperty::Unknown
}

/// C: `test_indoption` — common code for properties that are just bit tests of
/// `indoptions`.
///
/// * `indoption`: the index's `pg_index.indoption` array (the seam-fetched
///   [`IndexFormInfo::indoption`](seam::IndexFormInfo)).
/// * `attno`: identify the index column to test the `indoptions` of.
/// * `guard`: if false, a boolean false result is forced (saves code in caller).
/// * `iopt_mask`: mask for interesting `indoption` bit.
/// * `iopt_expect`: value for a "true" result (should be 0 or `iopt_mask`).
///
/// Returns `None` to indicate a NULL result (for "unknown/inapplicable"),
/// otherwise `Some(res)` with the boolean value to return.
fn test_indoption(
    indoption: &[i16],
    attno: i32,
    guard: bool,
    iopt_mask: i16,
    iopt_expect: i16,
) -> Option<bool> {
    if !guard {
        // C: *res = false; return true;
        return Some(false);
    }

    // C: indoption->values[attno - 1]
    let indoption_val = indoption[(attno - 1) as usize];

    // C: *res = (indoption_val & iopt_mask) == iopt_expect; return true;
    Some((indoption_val & iopt_mask) == iopt_expect)
}

/// C: `indexam_property` — the shared worker behind all four SQL functions.
///
/// Tests a property of an index AM, index, or index column. The `amoid` and
/// `index_oid` parameters are mutually exclusive: with an index OID we look up
/// the AM (and column count) from the index; with no index OID we test AM-wide
/// properties.
///
/// Returns `Ok(None)` for a SQL NULL result (`PG_RETURN_NULL`) and
/// `Ok(Some(bool))` for a boolean result (`PG_RETURN_BOOL`).
fn indexam_property<'mcx>(
    mcx: Mcx<'mcx>,
    propname: &str,
    mut amoid: Oid,
    index_oid: Oid,
    attno: i32,
) -> PgResult<Option<bool>> {
    let mut res = false;
    let mut natts: i32 = 0;

    // Try to convert property name to enum (no error if not known).
    let prop = lookup_prop_name(propname);

    // If we have an index OID, look up the AM, and get # of columns too.
    if OidIsValid(index_oid) {
        debug_assert!(!OidIsValid(amoid));
        let rd_rel = match seam::index_relation::call(index_oid)? {
            Some(rel) => rel,
            None => return Ok(None),
        };
        if rd_rel.relkind != RELKIND_INDEX && rd_rel.relkind != RELKIND_PARTITIONED_INDEX {
            return Ok(None);
        }
        amoid = rd_rel.relam;
        natts = rd_rel.relnatts as i32;
    }

    // At this point, either index_oid == InvalidOid or it's a valid index OID.
    // Also, after this test and the one below, either attno == 0 for index-wide
    // or AM-wide tests, or it's a valid column number in a valid index.
    if attno < 0 || attno > natts {
        return Ok(None);
    }

    // Get AM information.  If we don't have a valid AM OID, return NULL.
    let routine = match seam::am_routine::call(amoid)? {
        Some(routine) => routine,
        None => return Ok(None),
    };

    // If there's an AM property routine, give it a chance to override the
    // generic logic.  Proceed if it returns false.
    if routine.has_amproperty {
        if let Some((cb_res, cb_isnull)) = seam::am_property::call(mcx, AmPropertyRequest {
            amoid,
            index_oid,
            attno,
            prop,
            propname: String::from(propname),
        })? {
            if cb_isnull {
                return Ok(None);
            }
            return Ok(Some(cb_res));
        }
    }

    if attno > 0 {
        // Handle column-level properties. Many of these need the pg_index row
        // (which we also need to use to check for nonkey atts) so we fetch that
        // first.
        let rd_index = match seam::index_form::call(index_oid)? {
            Some(form) => form,
            None => return Ok(None),
        };

        debug_assert_eq!(index_oid, rd_index.indexrelid);
        debug_assert!(attno > 0 && attno <= rd_index.indnatts as i32);

        let mut isnull = true;

        // If amcaninclude, we might be looking at an attno for a nonkey column,
        // for which we (generically) assume that most properties are null.
        let mut iskey = true;
        if routine.amcaninclude && attno > rd_index.indnkeyatts as i32 {
            iskey = false;
        }

        match prop {
            IndexAmProperty::Asc => {
                if iskey {
                    if let Some(r) = test_indoption(
                        &rd_index.indoption,
                        attno,
                        routine.amcanorder,
                        INDOPTION_DESC,
                        0,
                    ) {
                        res = r;
                        isnull = false;
                    }
                }
            }

            IndexAmProperty::Desc => {
                if iskey {
                    if let Some(r) = test_indoption(
                        &rd_index.indoption,
                        attno,
                        routine.amcanorder,
                        INDOPTION_DESC,
                        INDOPTION_DESC,
                    ) {
                        res = r;
                        isnull = false;
                    }
                }
            }

            IndexAmProperty::NullsFirst => {
                if iskey {
                    if let Some(r) = test_indoption(
                        &rd_index.indoption,
                        attno,
                        routine.amcanorder,
                        INDOPTION_NULLS_FIRST,
                        INDOPTION_NULLS_FIRST,
                    ) {
                        res = r;
                        isnull = false;
                    }
                }
            }

            IndexAmProperty::NullsLast => {
                if iskey {
                    if let Some(r) = test_indoption(
                        &rd_index.indoption,
                        attno,
                        routine.amcanorder,
                        INDOPTION_NULLS_FIRST,
                        0,
                    ) {
                        res = r;
                        isnull = false;
                    }
                }
            }

            IndexAmProperty::Orderable => {
                // generic assumption is that nonkey columns are not orderable
                res = if iskey { routine.amcanorder } else { false };
                isnull = false;
            }

            IndexAmProperty::DistanceOrderable => {
                // The conditions for whether a column is distance-orderable are
                // really up to the AM (at time of writing, only GiST supports it
                // at all). The planner has its own idea based on whether it
                // finds an operator with amoppurpose 'o', but getting there from
                // just the index column type seems like a lot of work. So instead
                // we expect the AM to handle this in its amproperty routine. The
                // generic result is to return false if the AM says it never
                // supports this, or if this is a nonkey column, and null
                // otherwise (meaning we don't know).
                if !iskey || !routine.amcanorderbyop {
                    res = false;
                    isnull = false;
                }
            }

            IndexAmProperty::Returnable => {
                // note that we ignore iskey for this property
                isnull = false;
                res = false;

                if routine.has_amcanreturn {
                    // If possible, the AM should handle this test in its
                    // amproperty function without opening the rel. But this is
                    // the generic fallback if it does not.
                    res = seam::index_can_return::call(mcx, index_oid, attno)?;
                }
            }

            IndexAmProperty::SearchArray => {
                if iskey {
                    res = routine.amsearcharray;
                    isnull = false;
                }
            }

            IndexAmProperty::SearchNulls => {
                if iskey {
                    res = routine.amsearchnulls;
                    isnull = false;
                }
            }

            _ => {}
        }

        if !isnull {
            return Ok(Some(res));
        }
        return Ok(None);
    }

    if OidIsValid(index_oid) {
        // Handle index-level properties.  Currently, these only depend on the
        // AM, but that might not be true forever, so we make users name an
        // index not just an AM.
        return Ok(match prop {
            IndexAmProperty::Clusterable => Some(routine.amclusterable),
            IndexAmProperty::IndexScan => Some(routine.has_amgettuple),
            IndexAmProperty::BitmapScan => Some(routine.has_amgetbitmap),
            IndexAmProperty::BackwardScan => Some(routine.amcanbackward),
            _ => None,
        });
    }

    // Handle AM-level properties (those that control what you can say in CREATE
    // INDEX).
    Ok(match prop {
        IndexAmProperty::CanOrder => Some(routine.amcanorder),
        IndexAmProperty::CanUnique => Some(routine.amcanunique),
        IndexAmProperty::CanMultiCol => Some(routine.amcanmulticol),
        IndexAmProperty::CanExclude => Some(routine.has_amgettuple),
        IndexAmProperty::CanInclude => Some(routine.amcaninclude),
        _ => None,
    })
}

/// C: `pg_indexam_has_property(amoid oid, prop text)` — test property of an AM
/// specified by AM OID.
pub fn pg_indexam_has_property<'mcx>(
    mcx: Mcx<'mcx>,
    amoid: Oid,
    prop: &str,
) -> PgResult<Option<bool>> {
    indexam_property(mcx, prop, amoid, InvalidOid, 0)
}

/// C: `pg_index_has_property(indexoid oid, prop text)` — test property of an
/// index specified by index OID.
pub fn pg_index_has_property<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    prop: &str,
) -> PgResult<Option<bool>> {
    indexam_property(mcx, prop, InvalidOid, indexoid, 0)
}

/// C: `pg_index_column_has_property(indexoid oid, attno int4, prop text)` —
/// test property of an index column specified by index OID and column number.
pub fn pg_index_column_has_property<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    attno: i32,
    prop: &str,
) -> PgResult<Option<bool>> {
    // Reject attno 0 immediately, so that attno > 0 identifies this case.
    if attno <= 0 {
        return Ok(None);
    }

    indexam_property(mcx, prop, InvalidOid, indexoid, attno)
}

/// C: `pg_indexam_progress_phasename(amoid oid, phasenum int8)` — return the
/// name of the given phase, as used for progress reporting by the given AM.
///
/// Returns `Ok(None)` for a SQL NULL (the C `PG_RETURN_NULL` paths); a returned
/// `String` corresponds to `CStringGetTextDatum(name)`.
///
/// The SQL signature is `(oid, int8)` (per `pg_proc.dat`), but the C body reads
/// the second argument with `int32 phasenum = PG_GETARG_INT32(1)`. That is
/// `DatumGetInt32(...)`, i.e. `(int32) X`, which *truncates* the 64-bit int8
/// datum to its low 32 bits (sign-extended) before the value is widened back to
/// `int64` for the `ambuildphasename(int64)` callback. We reproduce that exactly
/// with `(phasenum as i32) as i64` so e.g. `0x1_0000_0001` calls the callback
/// with `1`, matching C, rather than `4294967297`.
pub fn pg_indexam_progress_phasename(amoid: Oid, phasenum: i64) -> PgResult<Option<String>> {
    // C: int32 phasenum = PG_GETARG_INT32(1);  (DatumGetInt32 truncates int8)
    // then the callback is invoked as ambuildphasename(phasenum as int64).
    let phasenum = phasenum as i32 as i64;

    let routine = match seam::am_routine::call(amoid)? {
        Some(routine) => routine,
        None => return Ok(None),
    };
    if !routine.has_ambuildphasename {
        return Ok(None);
    }

    // name = routine->ambuildphasename(phasenum); if (!name) PG_RETURN_NULL();
    seam::am_buildphasename::call(amoid, phasenum)
}

#[cfg(test)]
mod tests;
