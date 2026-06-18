//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `rangetypes.c`
//! SQL-callable functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching `PG_FUNCTION_ARGS`-shaped kernel in
//! [`crate::range_fmgr_boundary`], and writes back the result word /
//! by-reference payload. [`register_rangetypes_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `anyrange` convention
//!
//! `range` (`anyrange`) is a pass-by-reference (varlena) type. Unlike `numeric`
//! — a self-describing varlena whose codec reads bytes straight off the by-ref
//! lane — a serialized `RangeType` is a real in-memory image whose header
//! carries the range type's OID, and the range ADT's boundary kernels
//! (`range_fmgr_boundary`) read it as a `*const RangeType` POINTER taken from
//! the by-VALUE arg word (`DatumGetRangeTypeP(PG_GETARG_DATUM(n)) =
//! (RangeType *) PG_DETOAST_DATUM(X)`, i.e. it dereferences the word as a
//! varlena address). So the by-ref bridge's `Varlena(bytes)` image must first be
//! materialized as a real varlena in a memory context, and the by-value arg word
//! set to its address, before the kernel runs. Symmetrically, a `range` RESULT
//! comes back as a pointer word (`PG_RETURN_RANGE_P` = `PointerGetDatum`); the
//! wrapper reads the full varlena image at that address (`VARSIZE`) and copies
//! it onto the by-ref result lane as `Varlena`. The bytes carried on the by-ref
//! lane are therefore the COMPLETE `RangeType` varlena image INCLUDING its
//! 4-byte `VARHDRSZ` header (the canonical `ByRef` image for a disk-stored
//! type), symmetric on the arg and result lanes.
//!
//! Scalar-typed args/results (`cstring` for `_in`/`_out`, `bytea` for `_send`,
//! `int4`/`int8`/`bool` for the comparators/hashes, `int4`/`int8`/`oid` for the
//! I/O `typioparam`/`typmod`) cross on their natural lanes exactly as the
//! `numeric` precedent does: `cstring`/`bytea` on the by-ref `Cstring`/`Varlena`
//! lane (the kernels already read/set those), scalars in the by-value word.

use mcx::{Mcx, MemoryContext};
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument / result marshalling between the by-ref bridge and the boundary
// kernels' `DatumGetRangeTypeP` pointer convention.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` (`c.h`) — `sizeof(int32)`.
const VARHDRSZ: usize = 4;

/// Read `VARSIZE_4B(ptr)` from a plain (4-byte-header, uncompressed) varlena: the
/// little-endian length word's high 30 bits. A serialized `RangeType` always has
/// a plain 4-byte header (`range_serialize` writes `SET_VARSIZE`), so this is the
/// exact total image length.
///
/// # Safety
/// `ptr` must point at a valid plain 4B varlena header.
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    let word = (ptr as *const u32).read_unaligned();
    ((word >> 2) & 0x3FFF_FFFF) as usize
}

/// Materialize a `RangeType` varlena image (the full bytes carried on the by-ref
/// lane, header and all) into `mcx` as an 8-byte-aligned (MAXALIGN) copy, and
/// return the `Datum` pointer word the boundary kernels' `DatumGetRangeTypeP`
/// dereferences. MAXALIGN matches the alignment `range_serialize` produces (the
/// relative-offset payload accounting only matches absolute-address reads when
/// the image base is `MAXALIGN(8)`-aligned).
fn range_bytes_to_arg_word<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum> {
    use allocator_api2::alloc::Allocator;
    use core::alloc::Layout;
    mcx::check_alloc_size(image.len())?;
    let layout = Layout::from_size_align(image.len().max(1), 8)
        .expect("valid RangeType image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(image.len()))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: `dst` heads a freshly allocated image.len()-byte region.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), dst, image.len());
    }
    Ok(Datum::from_usize(dst as usize))
}

/// Read the complete `RangeType` varlena image (header and all) at the pointer
/// word a boundary kernel returned via `PG_RETURN_RANGE_P`, copying it out to an
/// owned `Vec<u8>` for the by-ref result lane. A zero word means no result (the
/// caller handles SQL NULL separately).
///
/// # Safety
/// `word` must be the address of a plain 4B `RangeType` varlena living for the
/// duration of this read (it does, until the result arena drops at wrapper end).
unsafe fn range_word_to_result_bytes(word: Datum) -> Vec<u8> {
    let ptr = word.as_usize() as *const u8;
    debug_assert!(!ptr.is_null());
    let len = varsize_4b(ptr);
    debug_assert!(len >= VARHDRSZ);
    core::slice::from_raw_parts(ptr, len).to_vec()
}

/// A scratch / result context for the range ADT's `Mcx`-allocating kernels (the
/// serialized result range, the element output cstring, etc.). The result bytes
/// are copied off the by-ref lane before it is dropped (C: the palloc'd result
/// lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("rangetypes fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

/// Move arg `i`'s by-ref `Varlena` image (a serialized `RangeType`) onto the
/// by-value word as a pointer into `mcx`, the form `DatumGetRangeTypeP` reads.
/// No-op if the arg has no by-ref `Varlena` payload (e.g. it is SQL NULL — the
/// strict dispatcher never calls a strict builtin with a NULL arg, but the
/// agg/non-strict kernels guard with `PG_ARGISNULL`).
fn stage_range_arg(fcinfo: &mut FunctionCallInfoBaseData, mcx: Mcx<'_>, i: usize) {
    let image: Option<Vec<u8>> = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .map(|b| b.to_vec());
    if let Some(image) = image {
        let word = ok(range_bytes_to_arg_word(mcx, &image));
        if let Some(nd) = fcinfo.args.get_mut(i) {
            nd.value = word;
            nd.isnull = false;
        }
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
//
// Each delegates to the `PG_FUNCTION_ARGS`-shaped kernel in
// `range_fmgr_boundary`, after staging any `range` args from the by-ref lane to
// the by-value word, and (for `range`-returning kernels) moving the pointer-word
// result back onto the by-ref lane.
// ---------------------------------------------------------------------------

/// A `range`-arg, scalar-result builtin (comparators returning `bool`/`int4`,
/// hashes returning `int4`/`int8`). Stages `nrange` leading `range` args, runs
/// the kernel, and returns its by-value result word verbatim.
macro_rules! fc_range_scalar {
    ($fc:ident, $kernel:path, $nrange:expr) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            for i in 0..$nrange {
                stage_range_arg(fcinfo, m.mcx(), i);
            }
            ok($kernel(m.mcx(), fcinfo))
        }
    };
}

/// A `range`-arg, `range`-result builtin (`range_in`, `range_recv`). Stages the
/// `range` args (none for `_in`/`_recv`, which build from cstring/wire), runs the
/// kernel, then moves the returned `RangeType` pointer-word image onto the by-ref
/// result lane. A SQL-NULL result (the kernel set `fcinfo->isnull`) is left as a
/// null word with no by-ref payload.
macro_rules! fc_range_result {
    ($fc:ident, $kernel:path, $nrange:expr) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            for i in 0..$nrange {
                stage_range_arg(fcinfo, m.mcx(), i);
            }
            let word = ok($kernel(m.mcx(), fcinfo));
            if fcinfo.result_is_null() || word.as_usize() == 0 {
                return Datum::null();
            }
            // SAFETY: `word` is the address of a plain `RangeType` varlena that
            // the kernel allocated in `m` and that lives until `m` drops below.
            let bytes = unsafe { range_word_to_result_bytes(word) };
            fcinfo.set_ref_result(types_fmgr::RefPayload::Varlena(bytes));
            Datum::null()
        }
    };
}

// --- I/O -------------------------------------------------------------------

/// `range_in(cstring, oid, int4) -> anyrange` (oid 3834). cstring on the by-ref
/// lane, `typioparam`/`typmod` scalars in the by-value words; result range on the
/// by-ref lane.
fc_range_result!(fc_range_in, crate::range_fmgr_boundary::range_in, 0usize);

/// `range_recv(internal, oid, int4) -> anyrange` (oid 3836). The wire buffer is
/// the by-ref `Varlena` arg 0 (a `StringInfo`'s message bytes — the kernel reads
/// it as the cursor, NOT a `RangeType`, so it is not staged as a range arg).
fc_range_result!(fc_range_recv, crate::range_fmgr_boundary::range_recv, 0usize);

/// `range_constructor2(anyelement, anyelement) -> anyrange` (oid 3840 + per-type
/// dups). Element args ride the by-value words; the result range type is read
/// off `flinfo->fn_expr` (`get_fn_expr_rettype`), so a real planned call frame is
/// required. Result range on the by-ref lane.
fc_range_result!(
    fc_range_constructor2,
    crate::range_fmgr_boundary::range_constructor2,
    0usize
);

/// `range_constructor3(anyelement, anyelement, text) -> anyrange` (oid 3841 +
/// per-type dups). Like `_constructor2` plus a `text` flags arg (read off its
/// by-value Datum word through the varlena seam).
fc_range_result!(
    fc_range_constructor3,
    crate::range_fmgr_boundary::range_constructor3,
    0usize
);

/// `range_out(anyrange) -> cstring` (oid 3835). Stages arg 0; the kernel sets the
/// cstring result on the by-ref lane.
fc_range_scalar!(fc_range_out, crate::range_fmgr_boundary::range_out, 1usize);

/// `range_send(anyrange) -> bytea` (oid 3837). Stages arg 0; the kernel sets the
/// bytea result on the by-ref lane.
fc_range_scalar!(fc_range_send, crate::range_fmgr_boundary::range_send, 1usize);

// --- comparison / predicate operators -> bool ------------------------------

fc_range_scalar!(fc_range_eq, crate::range_fmgr_boundary::range_eq, 2usize);
fc_range_scalar!(fc_range_ne, crate::range_fmgr_boundary::range_ne, 2usize);
fc_range_scalar!(fc_range_lt, crate::range_fmgr_boundary::range_lt, 2usize);
fc_range_scalar!(fc_range_le, crate::range_fmgr_boundary::range_le, 2usize);
fc_range_scalar!(fc_range_gt, crate::range_fmgr_boundary::range_gt, 2usize);
fc_range_scalar!(fc_range_ge, crate::range_fmgr_boundary::range_ge, 2usize);
fc_range_scalar!(fc_range_overlaps, crate::range_fmgr_boundary::range_overlaps, 2usize);
fc_range_scalar!(fc_range_contains, crate::range_fmgr_boundary::range_contains, 2usize);
fc_range_scalar!(
    fc_range_contained_by,
    crate::range_fmgr_boundary::range_contained_by,
    2usize
);
fc_range_scalar!(fc_range_adjacent, crate::range_fmgr_boundary::range_adjacent, 2usize);
fc_range_scalar!(fc_range_before, crate::range_fmgr_boundary::range_before, 2usize);
fc_range_scalar!(fc_range_after, crate::range_fmgr_boundary::range_after, 2usize);
fc_range_scalar!(fc_range_overleft, crate::range_fmgr_boundary::range_overleft, 2usize);
fc_range_scalar!(fc_range_overright, crate::range_fmgr_boundary::range_overright, 2usize);

/// `range_contains_elem(anyrange, anyelement) -> bool` (oid 3858). Stages arg 0
/// (the range); arg 1 (the element) stays in its natural by-value/by-ref form the
/// element compare proc reads.
fc_range_scalar!(
    fc_range_contains_elem,
    crate::range_fmgr_boundary::range_contains_elem,
    1usize
);

/// `elem_contained_by_range(anyelement, anyrange) -> bool` (oid 3860). The range
/// is arg 1, so stage only that one (arg 0 the element keeps its lane).
fn fc_elem_contained_by_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    stage_range_arg(fcinfo, m.mcx(), 1);
    ok(crate::range_fmgr_boundary::elem_contained_by_range(m.mcx(), fcinfo))
}

// --- 3-way comparison -> int4 ----------------------------------------------

fc_range_scalar!(fc_range_cmp, crate::range_fmgr_boundary::range_cmp, 2usize);

// --- set operations -> range -----------------------------------------------

fc_range_result!(fc_range_minus, crate::range_fmgr_boundary::range_minus, 2usize);
fc_range_result!(fc_range_union, crate::range_fmgr_boundary::range_union, 2usize);
fc_range_result!(fc_range_merge, crate::range_fmgr_boundary::range_merge, 2usize);
fc_range_result!(
    fc_range_intersect,
    crate::range_fmgr_boundary::range_intersect,
    2usize
);

// --- canonicalization (anyrange) -> anyrange -------------------------------
// The result range type is the SAME as the input's (read off the arg range's
// own header OID, not `flinfo->fn_expr`), so these need no planned call frame.

fc_range_result!(
    fc_int4range_canonical,
    crate::range_fmgr_boundary::int4range_canonical_v1,
    1usize
);
fc_range_result!(
    fc_int8range_canonical,
    crate::range_fmgr_boundary::int8range_canonical_v1,
    1usize
);
fc_range_result!(
    fc_daterange_canonical,
    crate::range_fmgr_boundary::daterange_canonical_v1,
    1usize
);

// --- hash -> int4 / int8 ----------------------------------------------------

fc_range_scalar!(fc_hash_range, crate::range_fmgr_boundary::hash_range, 1usize);
fc_range_scalar!(
    fc_hash_range_extended,
    crate::range_fmgr_boundary::hash_range_extended,
    1usize
);

// --- accessors / constructors ----------------------------------------------

/// `range_lower(anyrange) -> anyelement` (oid 3848).
fc_range_scalar!(fc_range_lower, crate::range_fmgr_boundary::range_lower, 1usize);
/// `range_upper(anyrange) -> anyelement` (oid 3849).
fc_range_scalar!(fc_range_upper, crate::range_fmgr_boundary::range_upper, 1usize);
/// `range_empty(anyrange) -> bool` (oid 3850).
fc_range_scalar!(fc_range_empty, crate::range_fmgr_boundary::range_empty, 1usize);
/// `range_lower_inc(anyrange) -> bool` (oid 3851).
fc_range_scalar!(fc_range_lower_inc, crate::range_fmgr_boundary::range_lower_inc, 1usize);
/// `range_upper_inc(anyrange) -> bool` (oid 3852).
fc_range_scalar!(fc_range_upper_inc, crate::range_fmgr_boundary::range_upper_inc, 1usize);
// `range_lower_inf(anyrange) -> bool` (oid 3853).
fc_range_scalar!(fc_range_lower_inf, crate::range_fmgr_boundary::range_lower_inf, 1usize);
/// `range_upper_inf(anyrange) -> bool` (oid 3854).
fc_range_scalar!(fc_range_upper_inf, crate::range_fmgr_boundary::range_upper_inf, 1usize);

// --- subdiff support fns (scalar args, float8 result) ----------------------

/// `int4range_subdiff(int4, int4) -> float8` (oid 3922).
fn fc_int4range_subdiff(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ok(crate::range_fmgr_boundary::int4range_subdiff(m.mcx(), fcinfo))
}

/// `int8range_subdiff(int8, int8) -> float8` (oid 3923).
fn fc_int8range_subdiff(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    ok(crate::range_fmgr_boundary::int8range_subdiff(m.mcx(), fcinfo))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the expressible `rangetypes.c` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat`; all of these are `proisstrict => 't'`
/// default and none `proretset`.
///
/// NOT registered here (genuinely keystone-gated, not this lever):
/// - `range_intersect_agg_transfn` (3978): aggregate transition fn — needs the
///   `AggCheckCallContext` call-context channel (#324/#335), absent from the
///   `types_fmgr` frame.
/// - `range_sortsupport` (6391) / `range_typanalyze` (3916): take an `internal`
///   (`SortSupport` / `VacAttrStats`) executor-owned scratch struct, not
///   expressible on the by-ref boundary.
/// - the `numrange`/`daterange`/`tsrange`/`tstzrange` subdiffs: ride the
///   Datum-seam arg surface of their element types (numeric/date/timestamp), not
///   the generic by-ref range boundary.
pub fn register_rangetypes_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // I/O: cstring/internal/bytea <-> anyrange.
        builtin(3834, "range_in", 3, true, false, fc_range_in),
        builtin(3835, "range_out", 1, true, false, fc_range_out),
        builtin(3836, "range_recv", 3, true, false, fc_range_recv),
        builtin(3837, "range_send", 1, true, false, fc_range_send),
        // accessors / predicates over a single range.
        builtin(3848, "range_lower", 1, true, false, fc_range_lower),
        builtin(3849, "range_upper", 1, true, false, fc_range_upper),
        builtin(3850, "range_empty", 1, true, false, fc_range_empty),
        builtin(3851, "range_lower_inc", 1, true, false, fc_range_lower_inc),
        builtin(3852, "range_upper_inc", 1, true, false, fc_range_upper_inc),
        builtin(3853, "range_lower_inf", 1, true, false, fc_range_lower_inf),
        builtin(3854, "range_upper_inf", 1, true, false, fc_range_upper_inf),
        // element / predicate operators -> bool.
        builtin(3858, "range_contains_elem", 2, true, false, fc_range_contains_elem),
        builtin(3860, "elem_contained_by_range", 2, true, false, fc_elem_contained_by_range),
        // (range, range) -> bool.
        builtin(3855, "range_eq", 2, true, false, fc_range_eq),
        builtin(3856, "range_ne", 2, true, false, fc_range_ne),
        builtin(3857, "range_overlaps", 2, true, false, fc_range_overlaps),
        builtin(3859, "range_contains", 2, true, false, fc_range_contains),
        builtin(3861, "range_contained_by", 2, true, false, fc_range_contained_by),
        builtin(3862, "range_adjacent", 2, true, false, fc_range_adjacent),
        builtin(3863, "range_before", 2, true, false, fc_range_before),
        builtin(3864, "range_after", 2, true, false, fc_range_after),
        builtin(3865, "range_overleft", 2, true, false, fc_range_overleft),
        builtin(3866, "range_overright", 2, true, false, fc_range_overright),
        builtin(3871, "range_lt", 2, true, false, fc_range_lt),
        builtin(3872, "range_le", 2, true, false, fc_range_le),
        builtin(3873, "range_ge", 2, true, false, fc_range_ge),
        builtin(3874, "range_gt", 2, true, false, fc_range_gt),
        // (range, range) -> int4 (3-way compare).
        builtin(3870, "range_cmp", 2, true, false, fc_range_cmp),
        // constructors (anyelement[, anyelement[, text]]) -> anyrange. Non-strict
        // (NULL bound => infinite); one (oid) row per built-in range type, all
        // sharing the same kernel (C: identical prosrc across the six range types).
        builtin(3840, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3841, "range_constructor3", 3, false, false, fc_range_constructor3),
        builtin(3844, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3845, "range_constructor3", 3, false, false, fc_range_constructor3),
        builtin(3933, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3934, "range_constructor3", 3, false, false, fc_range_constructor3),
        builtin(3937, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3938, "range_constructor3", 3, false, false, fc_range_constructor3),
        builtin(3941, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3942, "range_constructor3", 3, false, false, fc_range_constructor3),
        builtin(3945, "range_constructor2", 2, false, false, fc_range_constructor2),
        builtin(3946, "range_constructor3", 3, false, false, fc_range_constructor3),
        // canonicalize (anyrange) -> anyrange (the discrete-type rngcanonical procs).
        builtin(3914, "int4range_canonical", 1, true, false, fc_int4range_canonical),
        builtin(3928, "int8range_canonical", 1, true, false, fc_int8range_canonical),
        builtin(3915, "daterange_canonical", 1, true, false, fc_daterange_canonical),
        // set operations (range, range) -> range.
        builtin(3867, "range_union", 2, true, false, fc_range_union),
        builtin(3868, "range_intersect", 2, true, false, fc_range_intersect),
        builtin(3869, "range_minus", 2, true, false, fc_range_minus),
        builtin(4057, "range_merge", 2, true, false, fc_range_merge),
        // hash -> int4 / int8.
        builtin(3902, "hash_range", 1, true, false, fc_hash_range),
        builtin(3417, "hash_range_extended", 2, true, false, fc_hash_range_extended),
        // subdiff opclass support (scalar args -> float8).
        builtin(3922, "int4range_subdiff", 2, true, false, fc_int4range_subdiff),
        builtin(3923, "int8range_subdiff", 2, true, false, fc_int8range_subdiff),
    ]);
}

// ===========================================================================
// End-to-end proof: a by-reference `anyrange` builtin is genuinely callable
// through the fmgr registry. The comparison/equality kernels need the range
// type's typcache (`lookup_type_cache_entry`) to deserialize and order bounds;
// in this unit (no backend, no typcache owner installed) we install a synthetic
// `int4`-range typcache seam and build empty/finite-bound ranges via the real
// `range_serialize` kernel, then drive the registered builtins BY OID through
// `fmgr_isbuiltin`, passing `range` images on `fcinfo.ref_args` and reading the
// `bool`/`int4` result off the returned word. Empty-vs-empty and empty-vs-finite
// comparisons short-circuit before any subtype compare proc, so no element fmgr
// dispatch is required.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_cache::typcache::TypeCacheEntry;
    use types_datum::NullableDatum;
    use types_fmgr::RefPayload;
    use types_rangetypes::{RangeBound, RangeTypeP};

    /// OID we use for the synthetic `int4range` type in the test typcache.
    const TEST_RANGE_OID: u32 = 3904; // pg_type int4range
    /// OID of the element type `int4`.
    const TEST_ELEM_OID: u32 = 23;

    /// Build a synthetic `int4`-range `TypeCacheEntry`: a range type whose
    /// element type is `int4` (4-byte, by-value, int-aligned). This is the
    /// `lookup_type_cache(rngtypoid, TYPECACHE_RANGE_INFO)` projection the range
    /// I/O / compare kernels read. The `rng_cmp_proc_finfo` is left at its
    /// default (fn_oid 0) — our tests only exercise paths that short-circuit on
    /// the EMPTY flag before any element compare, so the subtype proc is never
    /// invoked.
    fn int4_range_typcache() -> TypeCacheEntry {
        let mut elem = TypeCacheEntry::default();
        elem.type_id = TEST_ELEM_OID;
        elem.typlen = 4;
        elem.typbyval = true;
        elem.typalign = b'i' as i8;
        elem.typstorage = b'p' as i8;

        let mut entry = TypeCacheEntry::default();
        entry.type_id = TEST_RANGE_OID;
        entry.typlen = -1; // varlena
        entry.typbyval = false;
        entry.typalign = b'd' as i8;
        entry.typstorage = b'x' as i8;
        entry.rngelemtype = Some(Box::new(elem));
        entry
    }

    /// Install the typcache seam (once) so the range kernels can resolve our
    /// synthetic `int4`-range. `set` panics if installed twice, so guard on
    /// `is_installed` — in this crate's unit-test binary nobody else installs it.
    fn install_test_typcache() {
        use backend_utils_cache_typcache_seams as ts;
        use backend_utils_fmgr_fmgr_seams as fs;
        if !ts::lookup_type_cache_entry::is_installed() {
            ts::lookup_type_cache_entry::set(|_type_id, _flags| Ok(int4_range_typcache()));
        }
        // The element (int4) btree comparison support fn: `range_serialize` of a
        // FINITE range calls `range_cmp_bound_values` -> `function_call2_coll` to
        // validate lower <= upper. The synthetic typcache leaves `fn_oid` at 0, so
        // this stand-in just three-way-compares the two int4 words (btint4cmp).
        if !fs::function_call2_coll::is_installed() {
            fs::function_call2_coll::set(|_fid, _coll, a, b| {
                Ok(Datum::from_i32((a.as_i32()).cmp(&b.as_i32()) as i32))
            });
        }
        register_rangetypes_builtins();
    }

    /// Serialize an empty `int4`-range to its full varlena image (the by-ref lane
    /// form), via the real `range_serialize` kernel.
    fn empty_range_image() -> Vec<u8> {
        install_test_typcache();
        let tc = int4_range_typcache();
        let m = MemoryContext::new("test empty range");
        // `make_empty_range` builds (empty=true) bounds and serializes.
        let r: RangeTypeP =
            crate::range_repr_serialize::make_empty_range(m.mcx(), &tc).expect("empty range");
        // SAFETY: r.ptr is a plain RangeType varlena in `m`.
        unsafe { range_word_to_result_bytes(Datum::from_usize(r.ptr as usize)) }
    }

    /// Serialize the finite `int4`-range `[lo, hi)` to its full varlena image.
    fn finite_range_image(lo: i32, hi: i32) -> Vec<u8> {
        install_test_typcache();
        let tc = int4_range_typcache();
        let m = MemoryContext::new("test finite range");
        let lower = RangeBound {
            val: Datum::from_i32(lo),
            infinite: false,
            inclusive: true,
            lower: true,
        };
        let upper = RangeBound {
            val: Datum::from_i32(hi),
            infinite: false,
            inclusive: false,
            lower: false,
        };
        let r: RangeTypeP =
            crate::range_repr_serialize::range_serialize(m.mcx(), &tc, &lower, &upper, false)
                .expect("finite range");
        unsafe { range_word_to_result_bytes(Datum::from_usize(r.ptr as usize)) }
    }

    /// Invoke a registered `(range, range) -> bool` builtin by OID through the
    /// fmgr registry, passing the two `range` images on the by-ref lane.
    fn call_range_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        install_test_typcache();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_bool()
    }

    /// Invoke a registered `(range, range) -> int4` builtin (`range_cmp`).
    fn call_range_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        install_test_typcache();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_i32()
    }

    /// Invoke a registered single-range `-> bool` builtin (`range_empty`).
    fn call_range_pred(oid: u32, a: &[u8]) -> bool {
        install_test_typcache();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_bool()
    }

    #[test]
    fn builtins_are_registered() {
        register_rangetypes_builtins();
        // Spot-check a representative set across every result-shape family.
        for oid in [
            3834u32, 3835, 3836, 3837, 3855, 3856, 3870, 3868, 3902, 3417, 3850, 3922,
        ] {
            assert!(
                backend_utils_fmgr_core::fmgr_isbuiltin(oid).is_some(),
                "range builtin {oid} should be registered"
            );
        }
    }

    /// THE PROOF: `range_eq`/`range_ne`/`range_cmp` over empty ranges, computed
    /// entirely through the fmgr registry by OID with `anyrange` args crossing on
    /// the by-reference lane.
    #[test]
    fn byref_range_empty_eq_through_registry() {
        let e1 = empty_range_image();
        let e2 = empty_range_image();
        // range_eq (3855): empty == empty -> true; range_ne (3856) -> false.
        assert!(call_range_cmp_bool(3855, &e1, &e2));
        assert!(!call_range_cmp_bool(3856, &e1, &e2));
        // range_cmp (3870): empty <=> empty == 0.
        assert_eq!(call_range_cmp_i32(3870, &e1, &e2), 0);
        // range_empty (3850): an empty range is empty.
        assert!(call_range_pred(3850, &e1));
    }

    /// Empty vs. finite: `range_eq` false, `range_cmp` orders empty first (C:
    /// an empty range sorts before any non-empty range, -1), `range_contains`
    /// (finite contains empty) true. All short-circuit on the EMPTY flag.
    #[test]
    fn byref_range_empty_vs_finite_through_registry() {
        let empty = empty_range_image();
        let finite = finite_range_image(1, 10);
        // range_eq (3855): empty != [1,10).
        assert!(!call_range_cmp_bool(3855, &empty, &finite));
        // range_cmp (3870): empty sorts before non-empty -> -1; reverse -> 1.
        assert_eq!(call_range_cmp_i32(3870, &empty, &finite), -1);
        assert_eq!(call_range_cmp_i32(3870, &finite, &empty), 1);
        // range_contains (3859): [1,10) contains the empty range -> true.
        assert!(call_range_cmp_bool(3859, &finite, &empty));
        // range_contained_by (3861): empty contained by [1,10) -> true.
        assert!(call_range_cmp_bool(3861, &empty, &finite));
        // range_empty (3850): the finite range is not empty.
        assert!(!call_range_pred(3850, &finite));
    }
}
