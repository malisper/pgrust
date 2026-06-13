//! Family `range-fmgr-boundary`: the ~49 `PG_FUNCTION_ARGS` entry points.
//!
//! Each `*_v1` mirrors one `Datum fn(PG_FUNCTION_ARGS)` from `rangetypes.c`,
//! marshalling `Datum` <-> typed args (and caching the resolved element/range
//! support in `fcinfo->flinfo->fn_extra`), then delegating to the kernel in the
//! relevant family. This layer is deliberately thin: no range logic lives here.

use types_datum::Datum;
use types_fmgr::FunctionCallInfoBaseData;

/// A `PGFunction` body in this crate.
pub type RangeFn = fn(&mut FunctionCallInfoBaseData) -> Datum;

macro_rules! entry {
    ($(#[$attr:meta])* $name:ident) => {
        $(#[$attr])*
        pub fn $name(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            todo!(concat!(stringify!($name), ": marshal Datum args and delegate to the kernel"))
        }
    };
}

// --- I/O (range-io) -------------------------------------------------------
entry!(/// `range_in(PG_FUNCTION_ARGS)` (rangetypes.c:90).
    range_in);
entry!(/// `range_out(PG_FUNCTION_ARGS)` (rangetypes.c:139).
    range_out);
entry!(/// `range_recv(PG_FUNCTION_ARGS)` (rangetypes.c:179).
    range_recv);
entry!(/// `range_send(PG_FUNCTION_ARGS)` (rangetypes.c:263).
    range_send);

// --- constructors / accessors (range-repr-serialize) ----------------------
entry!(/// `range_constructor2(PG_FUNCTION_ARGS)` (rangetypes.c:379).
    range_constructor2);
entry!(/// `range_constructor3(PG_FUNCTION_ARGS)` (rangetypes.c).
    range_constructor3);
entry!(/// `range_lower(PG_FUNCTION_ARGS)` (rangetypes.c:448).
    range_lower);
entry!(/// `range_upper(PG_FUNCTION_ARGS)` (rangetypes.c:469).
    range_upper);
entry!(/// `range_empty(PG_FUNCTION_ARGS)` (rangetypes.c:493).
    range_empty);
entry!(/// `range_lower_inc(PG_FUNCTION_ARGS)` (rangetypes.c:503).
    range_lower_inc);
entry!(/// `range_upper_inc(PG_FUNCTION_ARGS)` (rangetypes.c:513).
    range_upper_inc);
entry!(/// `range_lower_inf(PG_FUNCTION_ARGS)` (rangetypes.c:523).
    range_lower_inf);
entry!(/// `range_upper_inf(PG_FUNCTION_ARGS)` (rangetypes.c:533).
    range_upper_inf);

// --- element / predicate operators (range-bounds-compare) -----------------
entry!(/// `range_contains_elem(PG_FUNCTION_ARGS)` (rangetypes.c:546).
    range_contains_elem);
entry!(/// `elem_contained_by_range(PG_FUNCTION_ARGS)` (rangetypes.c:559).
    elem_contained_by_range);
entry!(/// `range_eq(PG_FUNCTION_ARGS)` (rangetypes.c:607).
    range_eq);
entry!(/// `range_ne(PG_FUNCTION_ARGS)` (rangetypes.c:627).
    range_ne);
entry!(/// `range_contains(PG_FUNCTION_ARGS)` (rangetypes.c:640).
    range_contains);
entry!(/// `range_contained_by(PG_FUNCTION_ARGS)` (rangetypes.c:653).
    range_contained_by);
entry!(/// `range_before(PG_FUNCTION_ARGS)` (rangetypes.c:691).
    range_before);
entry!(/// `range_after(PG_FUNCTION_ARGS)` (rangetypes.c:729).
    range_after);
entry!(/// `range_adjacent(PG_FUNCTION_ARGS)` (rangetypes.c:830).
    range_adjacent);
entry!(/// `range_overlaps(PG_FUNCTION_ARGS)` (rangetypes.c:876).
    range_overlaps);
entry!(/// `range_overleft(PG_FUNCTION_ARGS)` (rangetypes.c:917).
    range_overleft);
entry!(/// `range_overright(PG_FUNCTION_ARGS)` (rangetypes.c:958).
    range_overright);

// --- set operations (range-setops) ----------------------------------------
entry!(/// `range_minus(PG_FUNCTION_ARGS)` (rangetypes.c:974).
    range_minus);
entry!(/// `range_union(PG_FUNCTION_ARGS)` (rangetypes.c:1100).
    range_union);
entry!(/// `range_merge(PG_FUNCTION_ARGS)` (rangetypes.c:1116).
    range_merge);
entry!(/// `range_intersect(PG_FUNCTION_ARGS)` (rangetypes.c:1129).
    range_intersect);
entry!(/// `range_intersect_agg_transfn(PG_FUNCTION_ARGS)` (rangetypes.c:1221).
    range_intersect_agg_transfn);

// --- ordering / hash / sortsupport (range-canonical-subdiff-hash) ---------
entry!(/// `range_cmp(PG_FUNCTION_ARGS)` (rangetypes.c:1251).
    range_cmp);
entry!(/// `range_lt(PG_FUNCTION_ARGS)` (rangetypes.c:1359).
    range_lt);
entry!(/// `range_le(PG_FUNCTION_ARGS)` (rangetypes.c:1367).
    range_le);
entry!(/// `range_ge(PG_FUNCTION_ARGS)` (rangetypes.c:1375).
    range_ge);
entry!(/// `range_gt(PG_FUNCTION_ARGS)` (rangetypes.c:1383).
    range_gt);
entry!(/// `hash_range(PG_FUNCTION_ARGS)` (rangetypes.c:1394).
    hash_range);
entry!(/// `hash_range_extended(PG_FUNCTION_ARGS)` (rangetypes.c:1460).
    hash_range_extended);
entry!(/// `range_sortsupport(PG_FUNCTION_ARGS)` (rangetypes.c:1297).
    range_sortsupport);

// --- canonical / subdiff (range-canonical-subdiff-hash) -------------------
entry!(/// `int4range_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1572).
    int4range_canonical);
entry!(/// `int8range_canonical(PG_FUNCTION_ARGS)` (rangetypes.c).
    int8range_canonical);
entry!(/// `daterange_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1622).
    daterange_canonical);
entry!(/// `int4range_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c).
    int4range_subdiff);
entry!(/// `int8range_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c).
    int8range_subdiff);
entry!(/// `numrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1703).
    numrange_subdiff);
entry!(/// `daterange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1719).
    daterange_subdiff);
entry!(/// `tsrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1728).
    tsrange_subdiff);
entry!(/// `tstzrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1739).
    tstzrange_subdiff);

// --- planner support (range-planner-support) ------------------------------
entry!(/// `elem_contained_by_range_support(PG_FUNCTION_ARGS)` (rangetypes.c:2251).
    elem_contained_by_range_support);
entry!(/// `range_contains_elem_support(PG_FUNCTION_ARGS)` (rangetypes.c:2277).
    range_contains_elem_support);
