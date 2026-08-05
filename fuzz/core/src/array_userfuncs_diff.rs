//! array_userfuncs_diff: differential fuzz driver — shipped Rust
//! `array_userfuncs` (crates/backend/utils/adt/array_userfuncs) vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_array_userfuncs_io.c).
//!
//! Comparison planes: result array/bytea byte image (or i32 position),
//! Ok/Err verdict, errcode/sqlstate class. Message text out of scope. Any
//! mismatch panics — libFuzzer minimizes it into the divergence reproducer.
//!
//! ELEMENT-TYPE PIN (the crate's carve of record): typcache/fmgr element
//! dispatch is OUT of the crate's scope, so the environment is pinned to
//! CONCRETE element types int4 (typlen 4, byval, 'i') and text (varlena,
//! 'i') on BOTH sides — Rust via the sanctioned seam installs below
//! (syscache/typcache/fmgr/detoast pins, the ws_tests.rs recipe), C via the
//! oracle's pinned catalog shims. The eq comparators are the SHIPPED
//! fc_int4eq / fc_texteq wrappers on the Rust side and verbatim
//! int4eq/texteq cores on the C side. Nothing is claimed about other
//! element types or about typcache lookup internals.
//!
//! FLAT-ARRAY FENCE: images are plain in-line varlenas (no TOAST, no
//! expanded arrays), the same fence the shipped crate operates under;
//! detoast is pinned to identity on both sides.
//!
//! PRNG (array_shuffle/array_sample): both sides run xoroshiro128** seeded
//! IDENTICALLY per exec from the fuzz input (Rust: pg_prng global seed; C:
//! verbatim pg_prng.c) — outputs are deterministic-diffable, no property
//! fallback needed.
//!
//! Input layout: [sel][flags][payload]; sel % 12 picks the arm:
//!   0 array_append   (oid 378)  flags: bit0 elemsel, bit1 arr-null, bit2 elem-null
//!   1 array_prepend  (oid 379)  same flags
//!   2 array_cat      (oid 383)  flags: bit0 elemsel(arr1), bit1/bit2 null args,
//!                               bit3 = force-different-elemtype (mismatch arm)
//!   3 array_position (oid 3277) flags: bit0 elemsel, bit1 elem-null
//!   4 array_position_start (oid 3278) + bit2 start-null
//!   5 array_positions (oid 3279)
//!   6 trim_array     (oid 6172) payload leads with i32 n
//!   7 array_reverse  (oid 6381)
//!   8 array_shuffle  (oid 6215) payload leads with u64 seed
//!   9 array_sample   (oid 6216) payload leads with u64 seed + i32 n
//!  10 array_agg_array pipeline (oids 4051/4052/6296/6297/6298): transfn
//!     accumulation split into two states, serialize(state2) byte-law,
//!     deserialize, combine, finalfn — full wire round-trip both sides
//!  11 array_agg_array_deserialize on RAW wire bytes (error plane), result
//!     re-serialized canonically for comparison
//!
//! Wrapper grain: the Rust side calls the SHIPPED builtins.rs fc_* wrappers
//! on native LocalFcinfo frames (agg arms with a fabricated AggStateNode),
//! so the wrappers' arg decode / fn_extra memo / result encode execute every
//! iteration; C parity rides the same-input oracle comparison.
//!
//! SKIPPED rows (with reasons; exceptions per the fuzzuproof-crate skill):
//!  - array_larger/array_smaller (515/516): fmgr dispatch to btarraycmp
//!    (owned by adt/arrayfuncs, lane p1-lanex) — fmgr-dispatch carve; the
//!    2-line pick_first mapping is unit-tested in-crate.
//!  - array_sort family (6388-6390): tuplesort seam carve (executor dep).
//!  - array_append_support/array_prepend_support (6378/6379): constant
//!    NULL-pointer return; proved trivially (routes row), nothing to fuzz.
//!  - arm 10's value plane on the DIV-2 shape (state1 has a null bitmap,
//!    state2 has none): C's array_agg_array_combine leaves the appended
//!    items' null bits unwritten (upstream bug; pgrust fixes it), so those
//!    bytes are UNDEFINED in the oracle. Verdict + errcode still compared.
//!    See fuzz/divergences/array_userfuncs_diff/DIV-2-*.md.
//!  - array_agg_array_transfn's non-aggregate-context panic arm: C elogs,
//!    Rust panics — flagged to the lane as a potential error-plane
//!    divergence (never reachable through SQL; the executor always arms the
//!    context).

use std::cell::Cell;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_core::Oid;
use types_error::PgError;
use types_fmgr::{AggStateNode, FmgrInfo, LocalFcinfo, PGFunction};

use array_userfuncs::builtins as ab;

extern "C" {
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_array_append(
        elemsel: i32,
        argmode: i32,
        arr: *const u8,
        elem_null: i32,
        elem: *const u8,
        out: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_array_prepend(
        elemsel: i32,
        argmode: i32,
        arr: *const u8,
        elem_null: i32,
        elem: *const u8,
        out: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_array_support(is_append: i32) -> i32;
    fn pg_diff_array_cat(a1: *const u8, a2: *const u8, out: *mut u8, outcap: i32) -> i32;
    fn pg_diff_array_position(
        elemsel: i32,
        arr: *const u8,
        elem_null: i32,
        elem: *const u8,
        has_start: i32,
        start_null: i32,
        start: i32,
        collation: u32,
        pos_out: *mut i32,
    ) -> i32;
    fn pg_diff_array_positions(
        elemsel: i32,
        arr: *const u8,
        elem_null: i32,
        elem: *const u8,
        collation: u32,
        out: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_trim_array(arr: *const u8, n: i32, out: *mut u8, outcap: i32) -> i32;
    fn pg_diff_array_reverse(arr: *const u8, out: *mut u8, outcap: i32) -> i32;
    fn pg_diff_array_shuffle(arr: *const u8, seed: u64, out: *mut u8, outcap: i32) -> i32;
    fn pg_diff_array_sample(arr: *const u8, n: i32, seed: u64, out: *mut u8, outcap: i32) -> i32;
    fn pg_diff_array_agg_pipeline(
        elemsel: i32,
        argmode: i32,
        nimgs: i32,
        imgs: *const u8,
        offs: *const i32,
        nullflags: *const u8,
        split: i32,
        ser_out: *mut u8,
        ser_cap: i32,
        ser_len: *mut i32,
        out: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_array_agg_deserialize_raw(
        elemsel: i32,
        bytes: *const u8,
        len: i32,
        ser_out: *mut u8,
        ser_cap: i32,
    ) -> i32;
}

const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const INT4ARRAYOID: Oid = 1007;
const TEXTARRAYOID: Oid = 1009;
// int8/int8[] are not element-pin choices; they exist on BOTH sides so the
// two type universes match exactly (the oracle's pinned catalog knows them,
// so a wire-supplied oid of 20/1016 must resolve identically here — an
// asymmetric universe showed up as a bogus cache-lookup divergence).
const INT8OID: Oid = 20;
const INT8ARRAYOID: Oid = 1016;
const C_COLLATION: Oid = types_core::C_COLLATION_OID;
const OUTCAP: usize = 1 << 18;

/// Oracle error classes (csrc/pg_array_userfuncs_io.c header).
fn rust_err_class(e: &PgError) -> i32 {
    use types_error::*;
    if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        1
    } else if e.sqlstate == ERRCODE_DATA_EXCEPTION {
        2
    } else if e.sqlstate == ERRCODE_DATATYPE_MISMATCH {
        3
    } else if e.sqlstate == ERRCODE_ARRAY_SUBSCRIPT_ERROR {
        4
    } else if e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        5
    } else if e.sqlstate == ERRCODE_NULL_VALUE_NOT_ALLOWED {
        6
    } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
        7
    } else if e.sqlstate == ERRCODE_UNDEFINED_FUNCTION {
        8
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        9
    } else if e.sqlstate == ERRCODE_PROTOCOL_VIOLATION {
        11
    } else {
        99
    }
}

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

// ---------------------------------------------------------------------------
// Seam pins (environment only, never computation): the sanctioned typcache
// mock recipe. get_fn_expr_argtype reads a TLS pin the arms set per call.
// ---------------------------------------------------------------------------

std::thread_local! {
    static ARGTYPE_PIN: Cell<Oid> = const { Cell::new(0) };
}

/// Seams are process-global set-once and other diff modules (arrayfuncs_diff,
/// rowtypes_diff) pin the same ones with THEIR fixtures: exactly one diff
/// module can own the environment per process. Fuzz binaries are
/// one-target-per-process so ownership is always ours there; under
/// `cargo test` whichever module installs first owns it and our drivers
/// become no-ops (run `cargo test array_userfuncs_diff` when the full suite
/// raced the seams) — same convention as rowtypes_diff.
static OWNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn setup() -> bool {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use syscache_seams as sc;
        if sc::lookup_pg_type_typcache_shape::is_installed()
            || sc::lookup_pg_type_shape::is_installed()
            || fmgr_seams::fmgr_info::is_installed()
        {
            return; // another diff module owns the environment
        }
        OWNED.store(true, std::sync::atomic::Ordering::Relaxed);
        // TupleDescInitEntry-shape reads (lsyscache get_typlenbyvalalign).
        sc::lookup_pg_type_shape::set(|typid| {
            Ok(type_lba(typid).map(|(l, bv, al)| types_tuple::PgTypeShape {
                typlen: l,
                typbyval: bv,
                typalign: al,
                typstorage: if l == -1 { b'x' as i8 } else { b'p' as i8 },
                typcollation: if typid == TEXTOID { 100 } else { 0 },
            }))
        });
        // get_element_type.
        sc::pg_type_element_shape::set(|typid| {
            let (typelem, typsubscript) = match typid {
                INT4ARRAYOID => (INT4OID, F_ARRAY_SUBSCRIPT_HANDLER),
                TEXTARRAYOID => (TEXTOID, F_ARRAY_SUBSCRIPT_HANDLER),
                INT8ARRAYOID => (INT8OID, F_ARRAY_SUBSCRIPT_HANDLER),
                INT4OID | TEXTOID | INT8OID => (0, 0),
                _ => return Ok(None),
            };
            Ok(Some(sc::PgTypeElementShape { typelem, typsubscript }))
        });
        // typcache slow-path fill + format_type_be.
        sc::lookup_pg_type_typcache_shape::set(|typid| Ok(typcache_shape(typid)));
        // typcache inval registration reads a syscache hash of the type oid;
        // any stable value works (no invalidation traffic in the harness).
        sc::syscache_hash_value_typeoid::set(|typid| Ok(typid));
        // format_type_be's unqualified-name arm (error-message plane only).
        namespace_seams::type_is_visible::set(|_typid| Ok(true));
        // btree opclass resolution for TYPECACHE_EQ_OPR_FINFO (literal pg_catalog
        // values: int4_ops/integer_ops, text_ops/text_ops).
        indexcmds_seams::get_default_opclass::set(|typid, am| {
            Ok(match (typid, am) {
                (INT4OID, 403) => 1978,
                (TEXTOID, 403) => 3126,
                _ => 0,
            })
        });
        sc::lookup_pg_opclass_shape::set(|opclass| {
            Ok(match opclass {
                1978 => Some(sc::PgOpclassShape {
                    opcmethod: 403,
                    opcfamily: 1976,
                    opcintype: INT4OID,
                    opckeytype: 0,
                }),
                3126 => Some(sc::PgOpclassShape {
                    opcmethod: 403,
                    opcfamily: 1994,
                    opcintype: TEXTOID,
                    opckeytype: 0,
                }),
                _ => None,
            })
        });
        sc::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
            Ok(match (opfamily, left, right, strategy) {
                (1976, INT4OID, INT4OID, 3) => 96,  /* int4 = */
                (1994, TEXTOID, TEXTOID, 3) => 98,  /* text = */
                _ => 0,
            })
        });
        sc::lookup_pg_operator_shape::set(|opno| {
            let oprcode = match opno {
                96 => 65,  /* int4eq */
                98 => 67,  /* texteq */
                _ => return Ok(None),
            };
            Ok(Some(sc::PgOperatorShape {
                oprnamespace: 11,
                oprleft: if opno == 96 { INT4OID } else { TEXTOID },
                oprright: if opno == 96 { INT4OID } else { TEXTOID },
                oprresult: 16,
                oprcom: opno,
                oprnegate: 0,
                oprcode,
                oprrest: 0,
                oprjoin: 0,
                oprcanmerge: true,
                oprcanhash: true,
            }))
        });
        // Pinned map for THIS harness's oids; every other oid delegates to
        // the real fmgr so sibling modules sharing the test binary (e.g.
        // arrayfuncs_diff's width_bucket arm resolving btint4cmp 351 /
        // bttextcmp 360) still work when this module owns the environment.
        fmgr_seams::fmgr_info::set(|oid| match oid {
            65 => Ok(FmgrInfo::new(adt_int::builtins::fc_int4eq, 65, 2, true, false)),
            67 => Ok(FmgrInfo::new(varlena::builtins::fc_texteq, 67, 2, true, false)),
            _ => fmgr_core::fmgr_info(oid),
        });
        // fmgr_core::init_seams's second install (arrayfuncs_diff runs it
        // under catch_unwind) aborts at the fmgr_info panic before reaching
        // this seam — install it here so the environment stays complete.
        fmgr_seams::fmgr_info_not_ported_name::set(fmgr_core::fmgr_info_not_ported_name);
        fmgr_seams::get_fn_expr_argtype::set(|_flinfo, _argnum| ARGTYPE_PIN.with(|c| c.get()));
        // catch_unwind tolerates another lane's harness installing the
        // detoast seam first (double-install panics; all lanes share one
        // test binary). All images here are inline, for which every
        // installed impl is the identity copy.
        let _ = std::panic::catch_unwind(|| {
            detoast_seams::detoast_attr::set(|mcx, raw| {
                let mut v = mcx::vec_with_capacity_in(mcx, raw.len())?;
                mcx::vec_append_bytes(&mut v, raw)?;
                Ok(v)
            })
        });
    });
    OWNED.load(std::sync::atomic::Ordering::Relaxed)
}

const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

fn type_lba(typid: Oid) -> Option<(i16, bool, i8)> {
    match typid {
        INT4OID => Some((4, true, b'i' as i8)),
        TEXTOID => Some((-1, false, b'i' as i8)),
        INT8OID => Some((8, true, b'd' as i8)),
        INT4ARRAYOID | TEXTARRAYOID | INT8ARRAYOID => Some((-1, false, b'i' as i8)),
        _ => None,
    }
}

fn typcache_shape(typid: Oid) -> Option<syscache_seams::PgTypeTypcacheShape> {
    let (l, bv, al) = type_lba(typid)?;
    let mut typname = types_tuple::NameData::default();
    typname.namestrcpy(match typid {
        INT4OID => "int4",
        TEXTOID => "text",
        INT8OID => "int8",
        INT4ARRAYOID => "_int4",
        TEXTARRAYOID => "_text",
        INT8ARRAYOID => "_int8",
        _ => "?",
    });
    let (typelem, typarray) = match typid {
        INT4OID => (0, INT4ARRAYOID),
        TEXTOID => (0, TEXTARRAYOID),
        INT8OID => (0, INT8ARRAYOID),
        INT4ARRAYOID => (INT4OID, 0),
        TEXTARRAYOID => (TEXTOID, 0),
        INT8ARRAYOID => (INT8OID, 0),
        _ => (0, 0),
    };
    Some(syscache_seams::PgTypeTypcacheShape {
        typname,
        typlen: l,
        typbyval: bv,
        typalign: al,
        typstorage: if l == -1 { b'x' as i8 } else { b'p' as i8 },
        typtype: b'b' as i8,
        typisdefined: true,
        typrelid: 0,
        typsubscript: if typelem != 0 { F_ARRAY_SUBSCRIPT_HANDLER } else { 0 },
        typelem,
        typarray,
        typcollation: if typid == TEXTOID { 100 } else { 0 },
    })
}

// ---------------------------------------------------------------------------
// Input decoding + array image builder (structurally valid images: the data
// area always matches nitems(dims), so element walks stay in-bounds on both
// sides; dims/lbs stay free enough to reach every validation arm).
// ---------------------------------------------------------------------------

struct Rd<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Rd<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.b.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    fn i32(&mut self) -> i32 {
        let mut a = [0u8; 4];
        for x in a.iter_mut() {
            *x = self.u8();
        }
        i32::from_le_bytes(a)
    }
    fn u64(&mut self) -> u64 {
        let mut a = [0u8; 8];
        for x in a.iter_mut() {
            *x = self.u8();
        }
        u64::from_le_bytes(a)
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(self.u8());
        }
        v
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Elem {
    Int4,
    Text,
    /// int8 is IN the pinned type universe (both sides) but has NO eq
    /// operator in either pinned catalog — it exists to drive the
    /// "could not identify an equality operator" arm of the position
    /// family, plus 8-byte ('d'-align) element walks everywhere else.
    Int8,
}

impl Elem {
    fn oid(self) -> Oid {
        match self {
            Elem::Int4 => INT4OID,
            Elem::Text => TEXTOID,
            Elem::Int8 => INT8OID,
        }
    }
    fn arr_oid(self) -> Oid {
        match self {
            Elem::Int4 => INT4ARRAYOID,
            Elem::Text => TEXTARRAYOID,
            Elem::Int8 => INT8ARRAYOID,
        }
    }
    fn sel(self) -> i32 {
        match self {
            Elem::Int4 => 0,
            Elem::Text => 1,
            Elem::Int8 => 2,
        }
    }
}

/// A logical element: int4 value bytes or a full 4B-header text varlena.
/// MAXALIGN-aligned (see `Aligned`): a by-ref text datum is read through
/// VARSIZE, a 4-byte load at the datum pointer.
fn read_elem(r: &mut Rd<'_>, e: Elem) -> Aligned {
    match e {
        Elem::Int4 => Aligned::from_bytes(&r.bytes(4)),
        Elem::Int8 => Aligned::from_bytes(&r.bytes(8)),
        Elem::Text => {
            let len = (r.u8() as usize) % 12;
            let body = r.bytes(len);
            Aligned::from_bytes(&text_image(&body))
        }
    }
}

fn text_image(body: &[u8]) -> Vec<u8> {
    let word: u32 = ((body.len() as u32 + 4) << 2).to_le();
    let mut v = word.to_ne_bytes().to_vec();
    v.extend_from_slice(body);
    v
}

/// A byte buffer whose start address is MAXALIGN (8-byte) aligned, which is
/// what `palloc` guarantees for every varlena PostgreSQL ever hands these
/// functions.
///
/// This is load-bearing, not hygiene: the element walk aligns on ABSOLUTE
/// addresses (`att_align_pointer(ptr, typalign, ...)` in array_seek /
/// array_nelems_size / array_slice_size), while an image builder naturally
/// pads relative to the image start. A `Vec<u8>` has alignment 1, so an
/// odd-addressed image makes the reader skip a different number of pad bytes
/// than the writer wrote and walk off the end of the allocation — which is
/// exactly what the vendored C did here before this type existed. Aligning
/// the base makes relative and absolute alignment agree, as they always do in
/// a real backend.
struct Aligned {
    words: Vec<u64>,
    len: usize,
}

impl Aligned {
    fn from_bytes(b: &[u8]) -> Self {
        let mut words = vec![0u64; (b.len() + 7) / 8 + 1];
        // SAFETY: `words` owns at least b.len() bytes of writable storage.
        unsafe {
            core::ptr::copy_nonoverlapping(b.as_ptr(), words.as_mut_ptr().cast::<u8>(), b.len());
        }
        Aligned { words, len: b.len() }
    }

    fn as_bytes(&self) -> &[u8] {
        // SAFETY: u64 storage is valid to read as bytes; len <= capacity.
        unsafe { core::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }
}

/// STORED-ARRAY CONTRACT (not a behaviour carve): every array that reaches
/// these functions in a real server passed `ArrayCheckBounds`, i.e.
/// `lb[i] + dim[i] - 1` does not overflow int32 — arrayfuncs.c states the
/// assumption explicitly ("we assume the existing subscripts passed
/// ArrayCheckBounds") and both implementations index on it, so a violating
/// image is out of contract on BOTH sides (it read past the image in the
/// vendored C before this clamp). Lower bounds stay fully fuzzed otherwise,
/// including values adjacent to the int32 extremes.
///
/// Consequence worth recording: array_append's `pg_add_s32_overflow(lb[0],
/// dims[0])` arm and array_prepend's `pg_sub_s32_overflow(lb[0], 1)` arm need
/// `lb + dim > INT32_MAX` / `lb == INT32_MIN`, which a contract-satisfying
/// array cannot have — they are defensive-C-parity arms unreachable from
/// valid input, not fuzz gaps. (They are reachable through the NULL-array
/// leg only via construct_empty_array, which pins lb = 1.)
/// DIV-1 (FIXED, see fuzz/divergences/array_userfuncs_diff/): a lower
/// bound of exactly i32::MIN is a valid stored array and real
/// PostgreSQL 18.3 wraps `lb - 1` under -fwrapv; the shipped
/// array_position/array_positions formerly did a checked subtraction, so every
/// overflow-checked (debug/test/fuzz) build panics where release agrees with
/// C. Fixed at lib.rs (wrapping_sub/wrapping_add); the exclusion that lived
/// here is deleted — every campaign exec on an i32::MIN-lb array is now the
/// regression test.
fn clamp_lb(raw: i32, dim: i32) -> i32 {
    // ArrayCheckBoundsSafe (arrayutils.c) rejects an array whose `dims[i] +
    // lb[i]` overflows int32 — note it is the SUM, not `sum - 1`, that must
    // fit, so `lb == INT32_MAX - dim + 1` is already unstorable even though
    // the highest subscript would fit. Anything looser feeds both
    // implementations an array no backend can hold (the C `dim[i] + lb[i]`
    // comparisons in array_get_slice then rely on -fwrapv while the Rust
    // checked add traps).
    match raw.checked_add(dim) {
        Some(_) => raw,
        None => raw.wrapping_sub(dim),
    }
}

/// Build a flat ArrayType image from fuzzed planes. ndims 0..=3; dims from
/// the element count (first dim absorbs the remainder so products match);
/// lbs fully fuzzed i32 (bounds-check arms); per-element null flags.
fn read_array(r: &mut Rd<'_>, e: Elem) -> Aligned {
    let mode = r.u8();
    // 0 = empty array; up to MAXDIM(6) dims so the ndims+1 > MAXDIM
    // accumulation arm is reachable.
    let ndims = (mode % 7) as i32;
    if ndims == 0 {
        return build_array(e, 0, &[], &[], &[], &[]);
    }
    let nelems_want = (r.u8() as usize) % 17;
    let mut dims = [0i32; 6];
    let mut lbs = [0i32; 6];
    let mut raw_lbs = [0i32; 6];
    for d in 0..ndims as usize {
        dims[d] = (r.u8() as i32) % 4 + 1;
        raw_lbs[d] = r.i32();
    }
    // first dimension count comes straight from nelems so 1-D shapes are rich
    if ndims == 1 {
        dims[0] = nelems_want as i32;
        if dims[0] == 0 {
            dims[0] = 1;
        }
    }
    // Clamp AFTER dims are final: clamping against a provisional dim leaves
    // `dims[0] + lb` overflowing once dims[0] grows, i.e. still out of the
    // ArrayCheckBounds contract.
    for d in 0..ndims as usize {
        lbs[d] = clamp_lb(raw_lbs[d], dims[d]);
    }
    let mut nitems = 1i64;
    for d in 0..ndims as usize {
        nitems *= dims[d] as i64;
    }
    let nitems = nitems as usize;
    let mut elems: Vec<Vec<u8>> = Vec::with_capacity(nitems);
    let mut nulls = Vec::with_capacity(nitems);
    let with_nulls = r.u8() % 4 == 0;
    for _ in 0..nitems {
        let isnull = with_nulls && r.u8() % 3 == 0;
        nulls.push(isnull);
        elems.push(if isnull { Vec::new() } else { read_elem(r, e).as_bytes().to_vec() });
    }
    build_array(e, ndims, &dims[..ndims as usize], &lbs[..ndims as usize], &elems, &nulls)
}

/// Raw flat-image builder (matches array.h layout; LE 4B varlena header).
fn build_array(
    e: Elem,
    ndims: i32,
    dims: &[i32],
    lbs: &[i32],
    elems: &[Vec<u8>],
    nulls: &[bool],
) -> Aligned {
    let hdrsz = 16 + 8 * ndims as usize;
    let hasnulls = nulls.iter().any(|&n| n);
    let nitems = elems.len();
    let dataoffset = if hasnulls {
        (hdrsz + (nitems + 7) / 8 + 7) & !7
    } else {
        0
    };
    let datastart = if hasnulls { dataoffset } else { (hdrsz + 7) & !7 };
    let mut data: Vec<u8> = Vec::new();
    for (i, el) in elems.iter().enumerate() {
        if nulls[i] {
            continue;
        }
        match e {
            Elem::Int4 => data.extend_from_slice(el),
            Elem::Int8 => {
                // 'd' align; datastart is 8-aligned so relative == absolute.
                while data.len() % 8 != 0 {
                    data.push(0);
                }
                data.extend_from_slice(el);
            }
            Elem::Text => {
                // 'i' align each varlena element
                while data.len() % 4 != 0 {
                    data.push(0);
                }
                data.extend_from_slice(el);
            }
        }
    }
    // STORED-ARRAY CONTRACT: PostgreSQL pads after EVERY variable-length
    // element, the last one included — construct_md_array sizes the image with
    // `att_align_nominal` applied per element, and the element walk
    // (array_seek) aligns after each element too, so a real stored text array
    // with one 2-byte element is 32 bytes, not 30 (ground-truthed:
    // `pg_column_size('{ab}'::text[])` = 32 on postgres:18.3). Omitting the
    // trailing pad here produced images no backend can hold, and the two
    // implementations disagreed about the size of a slice of one.
    if matches!(e, Elem::Text) {
        while data.len() % 4 != 0 {
            data.push(0);
        }
    }
    let total = datastart + data.len();
    let mut img = vec![0u8; total];
    img[0..4].copy_from_slice(&(((total as u32) << 2).to_ne_bytes()));
    img[4..8].copy_from_slice(&ndims.to_ne_bytes());
    img[8..12].copy_from_slice(&(dataoffset as i32).to_ne_bytes());
    img[12..16].copy_from_slice(&(e.oid()).to_ne_bytes());
    for d in 0..ndims as usize {
        img[16 + 4 * d..20 + 4 * d].copy_from_slice(&dims[d].to_ne_bytes());
        let off = 16 + 4 * ndims as usize + 4 * d;
        img[off..off + 4].copy_from_slice(&lbs[d].to_ne_bytes());
    }
    if hasnulls {
        for (i, &n) in nulls.iter().enumerate() {
            if !n {
                img[hdrsz + i / 8] |= 1 << (i % 8);
            }
        }
    }
    img[datastart..].copy_from_slice(&data);
    Aligned::from_bytes(&img)
}

// ---------------------------------------------------------------------------
// fc plumbing
// ---------------------------------------------------------------------------

/// One native fmgr call with per-arg null flags, fresh flinfo, optional agg
/// context. Returns (result, isnull).
#[allow(clippy::too_many_arguments)]
fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: &mut FmgrInfo,
    coll: Oid,
    m: mcx::Mcx<'_>,
    agg: Option<&mut AggStateNode>,
    args: [(Datum, bool); N],
) -> (Result<Datum, Box<PgError>>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(coll);
    // SAFETY: the arming context outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    if let Some(node) = agg {
        fcinfo.context = node.fm_node_ptr();
    }
    for (i, (d, isnull)) in args.into_iter().enumerate() {
        fcinfo.args[i] = if isnull {
            NullableDatum { value: Datum::null(), isnull: true }
        } else {
            NullableDatum { value: d, isnull: false }
        };
    }
    let r = f(Some(flinfo), &mut fcinfo);
    let isnull = fcinfo.isnull;
    (r, isnull)
}

/// Read a returned by-ref array image (varsize_any length).
fn read_arr<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: fc results are live flat varlena images in the armed arena.
    unsafe { core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p)) }
}

fn ptr_or_null(img: Option<&[u8]>) -> *const u8 {
    img.map_or(core::ptr::null(), |s| s.as_ptr())
}

/// Compare a C entry outcome against a Rust fc outcome.
/// cst: C status (-1 err, -2 SQL NULL, >=0 image length).
fn compare_imgres(
    name: &str,
    cst: i32,
    cout: &[u8],
    r: (Result<Datum, Box<PgError>>, bool),
) {
    match r {
        (Ok(d), isnull) => {
            if isnull {
                assert!(cst == -2, "{name} DIVERGENCE: C status {cst} (err {}), Rust SQL NULL", c_errcode());
            } else {
                assert!(cst >= 0, "{name} DIVERGENCE: C status {cst} (err {}), Rust Ok", c_errcode());
                let rimg = read_arr(d);
                assert!(
                    rimg == &cout[..cst as usize],
                    "{name} VALUE DIVERGENCE: C={:02x?} Rust={:02x?}",
                    &cout[..cst as usize],
                    rimg
                );
            }
        }
        (Err(e), _) => {
            let rc = rust_err_class(&e);
            assert!(
                cst == -1 && c_errcode() == rc,
                "{name} ERROR DIVERGENCE: C status {cst} err {}, Rust Err class {rc} ({})",
                c_errcode(),
                e.message
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn array_userfuncs_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if !setup() {
        return; // another diff module owns the seam environment (see OWNED)
    }
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&flags, payload)) = rest.split_first() else {
        return;
    };
    let mut r = Rd { b: payload, i: 0 };
    let e = if flags & 16 != 0 {
        Elem::Int8
    } else if flags & 1 == 0 {
        Elem::Int4
    } else {
        Elem::Text
    };
    match sel % 13 {
        0 => append_prepend_arm(true, flags, e, &mut r),
        1 => append_prepend_arm(false, flags, e, &mut r),
        2 => cat_arm(flags, e, &mut r),
        3 => position_arm(false, flags, e, &mut r),
        4 => position_arm(true, flags, e, &mut r),
        5 => positions_arm(flags, e, &mut r),
        6 => trim_arm(e, &mut r),
        7 => reverse_arm(e, &mut r),
        8 => shuffle_arm(e, &mut r),
        9 => sample_arm(e, &mut r),
        10 => agg_pipeline_arm(flags, e, &mut r),
        11 => deserialize_raw_arm(e, &mut r),
        _ => support_arm(flags),
    }
}

/// array_append_support / array_prepend_support (oids 6378/6379): C falls
/// through to PG_RETURN_POINTER(NULL) for any request that is not a
/// SupportRequestModifyInPlace; the Rust wrappers return the same NULL
/// pointer datum unconditionally (the support-node vocabulary carve,
/// documented at builtins.rs). The diffable domain is therefore exactly the
/// non-SupportRequestModifyInPlace request space, pinned here via a
/// fabricated plain Node on the C side.
fn support_arm(flags: u8) {
    let is_append = flags & 2 == 0;
    let cst = unsafe { pg_diff_array_support(is_append as i32) };
    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let (f, oid): (PGFunction, Oid) = if is_append {
        (ab::fc_array_append_support, 6378)
    } else {
        (ab::fc_array_prepend_support, 6379)
    };
    let mut fl = FmgrInfo::new(f, oid, 1, true, false);
    // Arg 0 is an `internal` Node*; the Rust wrapper ignores it entirely.
    let (res, _) = fc_call(f, &mut fl, C_COLLATION, m, None, [(Datum::from_usize(8), false)]);
    let name = if is_append { "array_append_support" } else { "array_prepend_support" };
    match res {
        Ok(d) => assert!(
            cst == 0 && d.as_usize() == 0,
            "{name} DIVERGENCE: C status {cst}, Rust {:#x}",
            d.as_usize()
        ),
        Err(e2) => panic!("{name} DIVERGENCE: C status {cst}, Rust Err {}", e2.message),
    }
}

/// flags: bit1 arr-null, bit2 elem-null, bit3 argmode=1 (argtype pin
/// InvalidOid: the "could not determine input data type" arm), bit5
/// argmode=2 (pin = the SCALAR element oid: the "input data type is not an
/// array" arm; both only observable on the NULL-array leg), bit6 = arm the
/// Rust call with a fabricated agg context (C array_append has no
/// agg-context read; the Rust mcx-selection branch is context-only, value
/// plane identical), bit7 = a SECOND call reusing the same FmgrInfo on an
/// OTHER-element-type array (drives the fn_extra memo stale-type leg).
fn append_prepend_arm(is_append: bool, flags: u8, e: Elem, r: &mut Rd<'_>) {
    let arr_null = flags & 2 != 0;
    let elem_null = flags & 4 != 0;
    let argmode: i32 = if flags & 8 != 0 {
        1
    } else if flags & 32 != 0 {
        2
    } else {
        0
    };
    let with_agg = flags & 64 != 0;
    let second = flags & 128 != 0 && !arr_null;

    let f: PGFunction = if is_append { ab::fc_array_append } else { ab::fc_array_prepend };
    let mut fl = FmgrInfo::new(f, if is_append { 378 } else { 379 }, 2, false, false);
    let name = if is_append { "array_append" } else { "array_prepend" };

    let aggcx = mcx::MemoryContext::new("aufuzz_agg");
    let mut node = AggStateNode::new(aggcx);

    let mut pass = 0u8;
    loop {
        // Pass 1 (bit7): same flinfo, other element type — memo goes stale.
        let ep = if pass == 0 {
            e
        } else if e == Elem::Int4 {
            Elem::Text
        } else {
            Elem::Int4
        };
        let arr = if arr_null { None } else { Some(read_array(r, ep)) };
        let elem = if elem_null { Aligned::from_bytes(&[0u8; 8]) } else { read_elem(r, ep) };
        let elem_bytes = elem.as_bytes();
        ARGTYPE_PIN.with(|c| {
            c.set(match argmode {
                1 => 0,
                2 => ep.oid(),
                _ => ep.arr_oid(),
            })
        });

        let mut cout = vec![0u8; OUTCAP];
        let cst = unsafe {
            if is_append {
                pg_diff_array_append(
                    ep.sel(),
                    argmode,
                    ptr_or_null(arr.as_ref().map(|a| a.as_bytes())),
                    elem_null as i32,
                    elem_bytes.as_ptr(),
                    cout.as_mut_ptr(),
                    OUTCAP as i32,
                )
            } else {
                pg_diff_array_prepend(
                    ep.sel(),
                    argmode,
                    ptr_or_null(arr.as_ref().map(|a| a.as_bytes())),
                    elem_null as i32,
                    elem_bytes.as_ptr(),
                    cout.as_mut_ptr(),
                    OUTCAP as i32,
                )
            }
        };

        let cx = mcx::MemoryContext::new("aufuzz");
        let m = cx.mcx();
        let elem_datum = if elem_null {
            Datum::null()
        } else {
            match ep {
                Elem::Int4 => {
                    Datum::from_i32(i32::from_le_bytes(elem_bytes[..4].try_into().unwrap()))
                }
                Elem::Int8 => {
                    Datum::from_i64(i64::from_le_bytes(elem_bytes[..8].try_into().unwrap()))
                }
                Elem::Text => Datum::from_usize(elem_bytes.as_ptr() as usize),
            }
        };
        let arr_datum = arr
            .as_ref()
            .map(|a| Datum::from_usize(a.as_bytes().as_ptr() as usize))
            .unwrap_or(Datum::null());
        // C arg order: append(arr, elem), prepend(elem, arr).
        let args = if is_append {
            [(arr_datum, arr_null), (elem_datum, elem_null)]
        } else {
            [(elem_datum, elem_null), (arr_datum, arr_null)]
        };
        let res = fc_call(
            f,
            &mut fl,
            C_COLLATION,
            m,
            if with_agg { Some(&mut node) } else { None },
            args,
        );
        compare_imgres(name, cst, &cout, res);
        core::hint::black_box((&arr, &elem));
        if pass == 1 || !second {
            break;
        }
        pass = 1;
    }
}

fn cat_arm(flags: u8, e: Elem, r: &mut Rd<'_>) {
    let a1_null = flags & 2 != 0;
    let a2_null = flags & 4 != 0;
    let e2 = if flags & 8 != 0 {
        if e == Elem::Int4 { Elem::Text } else { Elem::Int4 }
    } else {
        e
    };
    let a1 = if a1_null { None } else { Some(read_array(r, e)) };
    let a2 = if a2_null { None } else { Some(read_array(r, e2)) };

    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe {
        pg_diff_array_cat(
            ptr_or_null(a1.as_ref().map(|a| a.as_bytes())),
            ptr_or_null(a2.as_ref().map(|a| a.as_bytes())),
            cout.as_mut_ptr(),
            OUTCAP as i32,
        )
    };

    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_array_cat, 383, 2, false, false);
    let d1 = a1
        .as_ref()
        .map(|a| Datum::from_usize(a.as_bytes().as_ptr() as usize))
        .unwrap_or(Datum::null());
    let d2 = a2
        .as_ref()
        .map(|a| Datum::from_usize(a.as_bytes().as_ptr() as usize))
        .unwrap_or(Datum::null());
    let res = fc_call(ab::fc_array_cat, &mut fl, C_COLLATION, m, None, [(d1, a1_null), (d2, a2_null)]);
    compare_imgres("array_cat", cst, &cout, res);
    core::hint::black_box((&a1, &a2));
}

fn position_arm(has_start: bool, flags: u8, e: Elem, r: &mut Rd<'_>) {
    let elem_null = flags & 2 != 0;
    let start_null = has_start && flags & 4 != 0;
    let arr_null = flags & 8 != 0;
    let start = r.i32();
    let arr = if arr_null { None } else { Some(read_array(r, e)) };
    let arr_b: &[u8] = arr.as_ref().map(|a| a.as_bytes()).unwrap_or(&[]);
    let elem = if elem_null { Aligned::from_bytes(&[0u8; 8]) } else { read_elem(r, e) };
    let elem_b = elem.as_bytes();

    let mut cpos: i32 = 0;
    let cst = unsafe {
        pg_diff_array_position(
            e.sel(),
            ptr_or_null(arr.as_ref().map(|a| a.as_bytes())),
            elem_null as i32,
            elem_b.as_ptr(),
            has_start as i32,
            start_null as i32,
            start,
            C_COLLATION,
            &mut cpos,
        )
    };

    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let (f, oid): (PGFunction, Oid) = if has_start {
        (ab::fc_array_position_start, 3278)
    } else {
        (ab::fc_array_position, 3277)
    };
    let mut fl = FmgrInfo::new(f, oid, if has_start { 3 } else { 2 }, false, false);
    let da = if arr_null { Datum::null() } else { Datum::from_usize(arr_b.as_ptr() as usize) };
    let de = match e {
        Elem::Int4 if !elem_null => {
            Datum::from_i32(i32::from_le_bytes(elem_b[..4].try_into().unwrap()))
        }
        Elem::Int8 if !elem_null => {
            Datum::from_i64(i64::from_le_bytes(elem_b[..8].try_into().unwrap()))
        }
        Elem::Text if !elem_null => Datum::from_usize(elem_b.as_ptr() as usize),
        _ => Datum::null(),
    };
    let call_rust = |fl: &mut FmgrInfo, m| {
        if has_start {
            fc_call(
                f,
                fl,
                C_COLLATION,
                m,
                None,
                [(da, arr_null), (de, elem_null), (Datum::from_i32(start), start_null)],
            )
        } else {
            fc_call::<2>(f, fl, C_COLLATION, m, None, [(da, arr_null), (de, elem_null)])
        }
    };
    let res = call_rust(&mut fl, m);
    // Second call on the SAME flinfo: the PosMemo hit leg (fn_extra already
    // resolved for this element type). Must agree with the first result.
    if res.0.is_ok() {
        let res2 = call_rust(&mut fl, m);
        match (&res, &res2) {
            ((Ok(d1), n1), (Ok(d2), n2)) => assert!(
                n1 == n2 && (*n1 || d1.as_i32() == d2.as_i32()),
                "array_position memo-hit result drift"
            ),
            _ => panic!("array_position memo-hit verdict drift"),
        }
    }
    let name = if has_start { "array_position_start" } else { "array_position" };
    match res {
        (Ok(d), isnull) => {
            if isnull {
                assert!(cst == -2, "{name} DIVERGENCE: C status {cst}, Rust SQL NULL");
            } else {
                assert!(
                    cst == 1 && cpos == d.as_i32(),
                    "{name} DIVERGENCE: C=({cst},{cpos}) Rust={}",
                    d.as_i32()
                );
            }
        }
        (Err(e2), _) => {
            let rc = rust_err_class(&e2);
            assert!(
                cst == -1 && c_errcode() == rc,
                "{name} ERROR DIVERGENCE: C status {cst} err {}, Rust class {rc} ({})",
                c_errcode(),
                e2.message
            );
        }
    }
    core::hint::black_box((&arr, &elem));
}

fn positions_arm(flags: u8, e: Elem, r: &mut Rd<'_>) {
    let elem_null = flags & 2 != 0;
    let arr_null = flags & 8 != 0;
    let arr = if arr_null { None } else { Some(read_array(r, e)) };
    let arr_b: &[u8] = arr.as_ref().map(|a| a.as_bytes()).unwrap_or(&[]);
    let elem = if elem_null { Aligned::from_bytes(&[0u8; 8]) } else { read_elem(r, e) };
    let elem_b = elem.as_bytes();

    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe {
        pg_diff_array_positions(
            e.sel(),
            ptr_or_null(arr.as_ref().map(|a| a.as_bytes())),
            elem_null as i32,
            elem_b.as_ptr(),
            C_COLLATION,
            cout.as_mut_ptr(),
            OUTCAP as i32,
        )
    };

    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_array_positions, 3279, 2, false, false);
    let da = if arr_null { Datum::null() } else { Datum::from_usize(arr_b.as_ptr() as usize) };
    let de = match e {
        Elem::Int4 if !elem_null => {
            Datum::from_i32(i32::from_le_bytes(elem_b[..4].try_into().unwrap()))
        }
        Elem::Int8 if !elem_null => {
            Datum::from_i64(i64::from_le_bytes(elem_b[..8].try_into().unwrap()))
        }
        Elem::Text if !elem_null => Datum::from_usize(elem_b.as_ptr() as usize),
        _ => Datum::null(),
    };
    let res =
        fc_call(ab::fc_array_positions, &mut fl, C_COLLATION, m, None, [(da, arr_null), (de, elem_null)]);
    compare_imgres("array_positions", cst, &cout, res);
    core::hint::black_box((&arr, &elem));
}

fn trim_arm(e: Elem, r: &mut Rd<'_>) {
    let n = r.i32();
    let arr = read_array(r, e);
    let arr_b = arr.as_bytes();
    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe { pg_diff_trim_array(arr_b.as_ptr(), n, cout.as_mut_ptr(), OUTCAP as i32) };

    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_trim_array, 6172, 2, true, false);
    let da = Datum::from_usize(arr_b.as_ptr() as usize);
    let res = fc_call(ab::fc_trim_array, &mut fl, C_COLLATION, m, None, [(da, false), (Datum::from_i32(n), false)]);
    compare_imgres("trim_array", cst, &cout, res);
    core::hint::black_box(&arr);
}

fn reverse_arm(e: Elem, r: &mut Rd<'_>) {
    let arr = read_array(r, e);
    let arr_b = arr.as_bytes();
    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe { pg_diff_array_reverse(arr_b.as_ptr(), cout.as_mut_ptr(), OUTCAP as i32) };

    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_array_reverse, 6381, 1, true, false);
    let da = Datum::from_usize(arr_b.as_ptr() as usize);
    let res = fc_call(ab::fc_array_reverse, &mut fl, C_COLLATION, m, None, [(da, false)]);
    compare_imgres("array_reverse", cst, &cout, res);
    core::hint::black_box(&arr);
}

fn shuffle_arm(e: Elem, r: &mut Rd<'_>) {
    let seed = r.u64();
    let arr = read_array(r, e);
    let arr_b = arr.as_bytes();
    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe { pg_diff_array_shuffle(arr_b.as_ptr(), seed, cout.as_mut_ptr(), OUTCAP as i32) };

    pg_prng::global_prng(|p| p.seed(seed));
    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_array_shuffle, 6215, 1, true, false);
    let da = Datum::from_usize(arr_b.as_ptr() as usize);
    let res = fc_call(ab::fc_array_shuffle, &mut fl, C_COLLATION, m, None, [(da, false)]);
    compare_imgres("array_shuffle", cst, &cout, res);
    core::hint::black_box(&arr);
}

fn sample_arm(e: Elem, r: &mut Rd<'_>) {
    let seed = r.u64();
    let n = r.i32();
    let arr = read_array(r, e);
    let arr_b = arr.as_bytes();
    let mut cout = vec![0u8; OUTCAP];
    let cst =
        unsafe { pg_diff_array_sample(arr_b.as_ptr(), n, seed, cout.as_mut_ptr(), OUTCAP as i32) };

    pg_prng::global_prng(|p| p.seed(seed));
    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let mut fl = FmgrInfo::new(ab::fc_array_sample, 6216, 2, true, false);
    let da = Datum::from_usize(arr_b.as_ptr() as usize);
    let res = fc_call(ab::fc_array_sample, &mut fl, C_COLLATION, m, None, [(da, false), (Datum::from_i32(n), false)]);
    compare_imgres("array_sample", cst, &cout, res);
    core::hint::black_box(&arr);
}

/// flags bit3 (8): argtype pin = InvalidOid ("could not determine input data
/// type"); bit5 (32): pin = the SCALAR element oid ("data type %s is not an
/// array type" in initArrayResultArr). Mirrored by the C entry's argmode.
fn agg_pipeline_arm(flags: u8, e: Elem, r: &mut Rd<'_>) {
    let argmode: i32 = if flags & 8 != 0 {
        1
    } else if flags & 32 != 0 {
        2
    } else {
        0
    };
    let nimgs = (r.u8() as usize) % 5;
    let split = if nimgs == 0 { 0 } else { (r.u8() as usize) % (nimgs + 1) };
    let mut flat: Vec<u8> = Vec::new();
    let mut offs: Vec<i32> = Vec::new();
    let mut nullflags: Vec<u8> = Vec::new();
    let mut per: Vec<Option<Aligned>> = Vec::new();
    for _ in 0..nimgs {
        // Every image inside the flat buffer starts MAXALIGN-aligned, as
        // palloc'd varlenas always do (see `Aligned`) — the C entry indexes
        // into this buffer, so a misaligned offset would desynchronise its
        // element walk from ours.
        while flat.len() % 8 != 0 {
            flat.push(0);
        }
        offs.push(flat.len() as i32);
        let isnull = r.u8() % 8 == 0;
        nullflags.push(isnull as u8);
        if isnull {
            per.push(None);
        } else {
            let a = read_array(r, e);
            flat.extend_from_slice(a.as_bytes());
            per.push(Some(a));
        }
    }
    offs.push(flat.len() as i32);
    let imgs = Aligned::from_bytes(&flat);
    let imgs_b = imgs.as_bytes();

    ARGTYPE_PIN.with(|c| {
        c.set(match argmode {
            1 => 0,
            2 => e.oid(),
            _ => e.arr_oid(),
        })
    });
    let mut ser_c = vec![0u8; OUTCAP];
    let mut ser_len_c: i32 = -1;
    let mut cout = vec![0u8; OUTCAP];
    let cst = unsafe {
        pg_diff_array_agg_pipeline(
            e.sel(),
            argmode,
            nimgs as i32,
            imgs_b.as_ptr(),
            offs.as_ptr(),
            nullflags.as_ptr(),
            split as i32,
            ser_c.as_mut_ptr(),
            OUTCAP as i32,
            &mut ser_len_c,
            cout.as_mut_ptr(),
            OUTCAP as i32,
        )
    };

    // Rust pipeline through the shipped fc wrappers with a fabricated agg
    // context.
    let aggcx = mcx::MemoryContext::new("aufuzz_agg");
    let mut node = AggStateNode::new(aggcx);
    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();

    let mut fl = FmgrInfo::new(ab::fc_array_agg_array_transfn, 4051, 2, false, false);
    let mut state1: (Datum, bool) = (Datum::null(), true);
    let mut state2: (Datum, bool) = (Datum::null(), true);
    let mut rerr: Option<Box<PgError>> = None;
    for (i, img) in per.iter().enumerate() {
        let st = if i < split { &mut state1 } else { &mut state2 };
        let darg = img
            .as_ref()
            .map(|a| Datum::from_usize(a.as_bytes().as_ptr() as usize))
            .unwrap_or(Datum::null());
        let (res, isnull) = fc_call(
            ab::fc_array_agg_array_transfn,
            &mut fl,
            C_COLLATION,
            m,
            Some(&mut node),
            [(st.0, st.1), (darg, img.is_none())],
        );
        match res {
            Ok(d) => *st = (d, isnull),
            Err(e2) => {
                rerr = Some(e2);
                break;
            }
        }
    }

    let mut ser_r: Option<Vec<u8>> = None;
    if rerr.is_none() && !state2.1 {
        let mut fls = FmgrInfo::new(ab::fc_array_agg_array_serialize, 6297, 1, true, false);
        let (res, _) = fc_call(
            ab::fc_array_agg_array_serialize,
            &mut fls,
            C_COLLATION,
            m,
            Some(&mut node),
            [(state2.0, false)],
        );
        match res {
            Ok(d) => {
                let img = read_arr(d);
                ser_r = Some(img[4..].to_vec());
                // deserialize back
                let mut fld =
                    FmgrInfo::new(ab::fc_array_agg_array_deserialize, 6298, 2, true, false);
                let (res2, isnull2) = fc_call(
                    ab::fc_array_agg_array_deserialize,
                    &mut fld,
                    C_COLLATION,
                    m,
                    Some(&mut node),
                    [(d, false), (Datum::null(), true)],
                );
                match res2 {
                    Ok(d2) => state2 = (d2, isnull2),
                    Err(e2) => rerr = Some(e2),
                }
            }
            Err(e2) => rerr = Some(e2),
        }
    }

    if rerr.is_none() {
        let mut flc = FmgrInfo::new(ab::fc_array_agg_array_combine, 6296, 2, false, false);
        let (res, isnull) = fc_call(
            ab::fc_array_agg_array_combine,
            &mut flc,
            C_COLLATION,
            m,
            Some(&mut node),
            [(state1.0, state1.1), (state2.0, state2.1)],
        );
        match res {
            Ok(d) => state1 = (d, isnull),
            Err(e2) => rerr = Some(e2),
        }
    }

    let final_res: (Result<Datum, Box<PgError>>, bool) = if let Some(e2) = rerr {
        (Err(e2), false)
    } else {
        let mut flf = FmgrInfo::new(ab::fc_array_agg_array_finalfn, 4052, 2, false, false);
        fc_call(
            ab::fc_array_agg_array_finalfn,
            &mut flf,
            C_COLLATION,
            m,
            Some(&mut node),
            [(state1.0, state1.1), (Datum::null(), true)],
        )
    };

    // serialize byte-law plane
    if let Some(sr) = &ser_r {
        assert!(ser_len_c >= 0, "array_agg serialize: Rust produced an image, C did not");
        compare_serialized("array_agg", &ser_c[..ser_len_c as usize], sr);
    }
    // DIV-2 (upstream bug, see fuzz/divergences/array_userfuncs_diff/): when
    // state1 accumulated a null bitmap and state2 contributed none, C's
    // array_agg_array_combine never writes the appended items' null bits, so
    // the result bitmap holds uninitialized heap there — undefined, not
    // divergent. Verdict and errcode still compare; the value plane cannot.
    let has_null = |img: &Aligned| {
        let b = img.as_bytes();
        b.len() >= 12 && i32::from_ne_bytes(b[8..12].try_into().unwrap()) != 0
    };
    let left_nulls = per[..split.min(per.len())].iter().flatten().any(has_null);
    let right = &per[split.min(per.len())..];
    let right_nulls = right.iter().flatten().any(has_null);
    let unwritten_bits_shape = left_nulls && !right_nulls && right.iter().any(|i| i.is_some());
    if unwritten_bits_shape {
        match final_res {
            (Ok(_), isnull) => assert!(
                (isnull && cst == -2) || (!isnull && cst >= 0),
                "array_agg pipeline VERDICT DIVERGENCE (DIV-2 shape): C {cst}, Rust Ok"
            ),
            (Err(e2), _) => {
                let rc = rust_err_class(&e2);
                assert!(
                    cst == -1 && c_errcode() == rc,
                    "array_agg pipeline ERROR DIVERGENCE (DIV-2 shape): C {cst}/{} Rust {rc}",
                    c_errcode()
                );
            }
        }
    } else {
        compare_imgres("array_agg pipeline", cst, &cout, final_res);
    }
    core::hint::black_box((&imgs_b, &per));
}

/// Compare two array_agg_array_serialize wire images.
///
/// RATIFIED NON-SURFACE (carve, documented per the target-header rule): the
/// serialized null bitmap is `(aitems + 7) / 8` bytes long but only the first
/// `nitems` BITS are ever written — C reaches it through
/// `palloc((aitems + 7) / 8)` (accumArrayResultArr / array_agg_array_combine),
/// never palloc0, so every byte past `ceil(nitems / 8)` is uninitialized heap
/// in real PostgreSQL and the wire image is NOT byte-deterministic there.
/// (The fuzz build made this visible immediately: ASan's 0xbe fill in the C
/// image against the shipped Rust's zeros.) The padding is unspecified rather
/// than divergent — deserialize reads it back and consults only bits below
/// nitems — so the plane compares every defined byte and skips exactly that
/// padding run. Everything else, including field order, widths, byte order and
/// the whole data region, is compared exactly.
fn compare_serialized(name: &str, c: &[u8], r: &[u8]) {
    assert!(
        c.len() == r.len(),
        "{name} serialize LENGTH DIVERGENCE: C={} Rust={}",
        c.len(),
        r.len()
    );
    let be = |b: &[u8], o: usize| -> u32 { u32::from_be_bytes(b[o..o + 4].try_into().unwrap()) };
    // Header up to and including nbytes + the data region + abytes + aitems.
    if r.len() < 12 {
        assert!(c == r, "{name} serialize DIVERGENCE (short image)");
        return;
    }
    let nbytes = be(r, 8) as usize;
    let fixed_end = 12 + nbytes + 8; // through abytes + aitems
    if r.len() < fixed_end {
        assert!(c == r, "{name} serialize DIVERGENCE (truncated image)");
        return;
    }
    assert!(
        c[..fixed_end] == r[..fixed_end],
        "{name} serialize DIVERGENCE (header/data/abytes/aitems):\n  C={:02x?}\n  R={:02x?}",
        &c[..fixed_end],
        &r[..fixed_end]
    );
    let aitems = be(r, 12 + nbytes + 4) as usize;
    let bm_len = if aitems > 0 { (aitems + 7) / 8 } else { 0 };
    let bm_start = fixed_end;
    let tail_start = bm_start + bm_len;
    if tail_start + 8 > r.len() {
        // No trailing nitems/ndims: compare the rest verbatim.
        assert!(
            c[bm_start..] == r[bm_start..],
            "{name} serialize DIVERGENCE (trailing bytes)"
        );
        return;
    }
    let nitems = be(r, tail_start) as usize;
    // Defined region is BIT-granular: array_bitmap_copy read-modify-writes
    // each destination byte, so within the last partially-used byte only the
    // bits below nitems are written — the rest keep palloc's uninitialized
    // content on the C side.
    let whole = (nitems / 8).min(bm_len);
    assert!(
        c[bm_start..bm_start + whole] == r[bm_start..bm_start + whole],
        "{name} serialize BITMAP DIVERGENCE (whole bytes):\n  C={:02x?}\n  R={:02x?}",
        &c[bm_start..bm_start + whole],
        &r[bm_start..bm_start + whole]
    );
    let rem = nitems % 8;
    if rem != 0 && whole < bm_len {
        let mask = (1u8 << rem) - 1;
        let (cb, rb) = (c[bm_start + whole] & mask, r[bm_start + whole] & mask);
        assert!(
            cb == rb,
            "{name} serialize BITMAP DIVERGENCE (partial byte, {rem} defined bits): C={cb:02x} Rust={rb:02x}"
        );
    }
    assert!(
        c[tail_start..] == r[tail_start..],
        "{name} serialize DIVERGENCE (nitems/ndims/dims/lbs):\n  C={:02x?}\n  R={:02x?}",
        &c[tail_start..],
        &r[tail_start..]
    );
}

/// Bound the wire image's declared allocation fields.
///
/// HARNESS INPUT BOUND (not a behavior carve): the serialize wire format is
/// internal — only array_agg_array_serialize produces it — and both
/// implementations honour a declared `nbytes`/`aitems` by allocating that
/// many bytes BEFORE discovering the buffer is short. An absurd declared
/// length therefore makes BOTH sides allocate gigabytes and time out, with
/// no behavioural information gained. The fields are clamped in place, so
/// both sides still consume byte-identical input and the whole
/// insufficient-data / extra-data error plane stays reachable.
fn bound_wire_fields(raw: &mut [u8]) {
    // layout: [element_type:4][array_type:4][nbytes:4][data:nbytes][abytes:4]
    //         [aitems:4] ... (network byte order)
    const CAP: u32 = 1 << 16;
    if raw.len() >= 12 {
        let nbytes = u32::from_be_bytes(raw[8..12].try_into().unwrap());
        let bounded = nbytes % CAP;
        raw[8..12].copy_from_slice(&bounded.to_be_bytes());
        let ai_off = 12 + bounded as usize + 4;
        if ai_off + 4 <= raw.len() {
            let aitems = u32::from_be_bytes(raw[ai_off..ai_off + 4].try_into().unwrap());
            raw[ai_off..ai_off + 4].copy_from_slice(&(aitems % CAP).to_be_bytes());
        }
    }
}

fn deserialize_raw_arm(e: Elem, r: &mut Rd<'_>) {
    let mut raw = r.bytes(r.b.len().saturating_sub(r.i).min(4096));
    bound_wire_fields(&mut raw);
    let raw = raw;
    ARGTYPE_PIN.with(|c| c.set(e.arr_oid()));

    let mut ser_c = vec![0u8; OUTCAP];
    let cst = unsafe {
        pg_diff_array_agg_deserialize_raw(
            e.sel(),
            raw.as_ptr(),
            raw.len() as i32,
            ser_c.as_mut_ptr(),
            OUTCAP as i32,
        )
    };

    let aggcx = mcx::MemoryContext::new("aufuzz_agg");
    let mut node = AggStateNode::new(aggcx);
    let cx = mcx::MemoryContext::new("aufuzz");
    let m = cx.mcx();
    let input = {
        let mut v = ((raw.len() as u32 + 4) << 2).to_ne_bytes().to_vec();
        v.extend_from_slice(&raw);
        Aligned::from_bytes(&v)
    };
    let input_b = input.as_bytes();
    let mut fld = FmgrInfo::new(ab::fc_array_agg_array_deserialize, 6298, 2, true, false);
    let (res, _) = fc_call(
        ab::fc_array_agg_array_deserialize,
        &mut fld,
        C_COLLATION,
        m,
        Some(&mut node),
        [(Datum::from_usize(input_b.as_ptr() as usize), false), (Datum::null(), true)],
    );
    match res {
        Ok(d) => {
            let mut fls = FmgrInfo::new(ab::fc_array_agg_array_serialize, 6297, 1, true, false);
            let (res2, _) = fc_call(
                ab::fc_array_agg_array_serialize,
                &mut fls,
                C_COLLATION,
                m,
                Some(&mut node),
                [(d, false)],
            );
            match res2 {
                Ok(d2) => {
                    let img = read_arr(d2);
                    assert!(cst >= 0, "agg_deserialize_raw: Rust Ok, C status {cst}");
                    compare_serialized(
                        "agg_deserialize_raw roundtrip",
                        &ser_c[..cst as usize],
                        &img[4..],
                    );
                }
                Err(e2) => {
                    let rc = rust_err_class(&e2);
                    assert!(
                        cst == -1 && c_errcode() == rc,
                        "agg_deserialize_raw re-serialize ERROR DIVERGENCE: C {cst}/{} Rust {rc}",
                        c_errcode()
                    );
                }
            }
        }
        Err(e2) => {
            let rc = rust_err_class(&e2);
            assert!(
                cst == -1 && c_errcode() == rc,
                "agg_deserialize_raw ERROR DIVERGENCE: C {cst} err {}, Rust class {rc} ({})",
                c_errcode(),
                e2.message
            );
        }
    }
    core::hint::black_box((&raw, &input_b));
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke: replay shaped seeds through every arm.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn drive(sel: u8, flags: u8, payload: &[u8]) {
        let mut d = vec![sel, flags];
        d.extend_from_slice(payload);
        array_userfuncs_diff(&d);
    }

    /// 1-D int4 array payload: [mode=1][nelems][dim0][lb0*4][nullflag][elems...]
    fn p_arr_int4(elems: &[Option<i32>], lb: i32) -> Vec<u8> {
        let mut p = vec![1u8, elems.len() as u8, 0u8];
        p.extend_from_slice(&lb.to_le_bytes());
        p.push(if elems.iter().any(|e| e.is_none()) { 0 } else { 1 }); // with_nulls byte (0 => %4==0 true)
        for e in elems {
            match e {
                Some(v) => {
                    p.push(1); // not-null (only consulted when with_nulls)
                    p.extend_from_slice(&v.to_le_bytes());
                }
                None => p.push(0),
            }
        }
        p
    }

    #[test]
    fn arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // append int4, non-null array + elem
        let mut p = p_arr_int4(&[Some(1), Some(2)], 1);
        p.extend_from_slice(&7i32.to_le_bytes());
        drive(0, 0, &p);
        // prepend
        drive(1, 0, &p);
        // append onto NULL array (argtype-pin path)
        drive(0, 2, &42i32.to_le_bytes());
        // append NULL elem
        drive(0, 4, &p_arr_int4(&[Some(5)], 1));
        // cat: two arrays
        let mut pc = p_arr_int4(&[Some(1)], 1);
        pc.extend_from_slice(&p_arr_int4(&[Some(9), Some(8)], 3));
        drive(2, 0, &pc);
        // cat with elemtype mismatch (error arm)
        drive(2, 8, &pc);
        // position / position_start / positions
        let mut pp = 1i32.to_le_bytes().to_vec();
        pp.extend_from_slice(&p_arr_int4(&[Some(4), Some(5), Some(4)], 1));
        pp.extend_from_slice(&4i32.to_le_bytes());
        drive(3, 0, &pp);
        drive(4, 0, &pp);
        drive(5, 0, &pp[4..].to_vec());
        // null search
        drive(3, 2, &pp);
        // trim
        let mut pt = 1i32.to_le_bytes().to_vec();
        pt.extend_from_slice(&p_arr_int4(&[Some(1), Some(2), Some(3)], 1));
        drive(6, 0, &pt);
        // reverse
        drive(7, 0, &p_arr_int4(&[Some(1), Some(2), Some(3)], -5));
        // shuffle / sample (seeded)
        let mut ps = 0xDEADBEEFu64.to_le_bytes().to_vec();
        ps.extend_from_slice(&p_arr_int4(&[Some(1), Some(2), Some(3), Some(4)], 1));
        drive(8, 0, &ps);
        let mut ps2 = 0xDEADBEEFu64.to_le_bytes().to_vec();
        ps2.extend_from_slice(&2i32.to_le_bytes());
        ps2.extend_from_slice(&p_arr_int4(&[Some(1), Some(2), Some(3), Some(4)], 1));
        drive(9, 0, &ps2);
        // agg pipeline: 3 arrays, split 1
        let mut pa = vec![3u8, 1u8];
        for _ in 0..3 {
            pa.push(1); // not null
            pa.extend_from_slice(&p_arr_int4(&[Some(1), Some(2)], 1));
        }
        drive(10, 0, &pa);
        // deserialize raw garbage (error plane)
        drive(11, 0, &[0x01, 0x02, 0x03]);
        drive(11, 0, &[]);
    }

    #[test]
    fn text_arms_smoke() {
        let _serial = crate::c_oracle_serial();
        // text 1-D array: mode=1, nelems=2, dim0 junk, lb, nulls byte, elems
        let mut p = vec![1u8, 2u8, 0u8];
        p.extend_from_slice(&1i32.to_le_bytes());
        p.push(1); // no nulls
        p.extend_from_slice(&[2, b'h', b'i']); // "hi"
        p.extend_from_slice(&[0]); // ""
        let mut pa = p.clone();
        pa.extend_from_slice(&[3, b'x', b'y', b'z']);
        drive(0, 1, &pa); // append text
        drive(7, 1, &p); // reverse text
        let mut pc = p.clone();
        pc.extend_from_slice(&p);
        drive(2, 1, &pc); // cat text
    }

    /// Fuzz-shaped byte soup through every selector: panic-free on arbitrary
    /// input (asserts fire only on divergence).
    #[test]
    fn selector_soup() {
        let _serial = crate::c_oracle_serial();
        for sel in 0u8..24 {
            for len in [0usize, 1, 2, 5, 16, 40, 120] {
                let payload: Vec<u8> = (0..len)
                    .map(|i| (i as u8).wrapping_mul(31).wrapping_add(sel))
                    .collect();
                let mut d = vec![sel, sel.wrapping_mul(3)];
                d.extend_from_slice(&payload);
                array_userfuncs_diff(&d);
            }
        }
        array_userfuncs_diff(&[]);
        array_userfuncs_diff(&[5]);
    }

    /// MANDATORY single-field-difference witness pairs for the array_cat
    /// dims/lbs merge: 2-D pairs differing in exactly one dims[i]/lbs[i],
    /// both argument orders (the mac/mac8 lesson).
    #[test]
    fn cat_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        // 2-D arrays: mode=2 => ndims=2; dims from bytes, elements follow.
        fn arr2(d0: u8, d1: u8, lb0: i32, lb1: i32) -> Vec<u8> {
            let n = (d0 % 4 + 1) as usize * (d1 % 4 + 1) as usize;
            let mut p = vec![2u8, 0u8, d0];
            p.extend_from_slice(&lb0.to_le_bytes());
            p.push(d1);
            p.extend_from_slice(&lb1.to_le_bytes());
            p.push(1); // no nulls
            for i in 0..n {
                p.push(1);
                p.extend_from_slice(&(i as i32).to_le_bytes());
            }
            p
        }
        let base = arr2(1, 1, 1, 1);
        let variants = [
            arr2(2, 1, 1, 1), // dims[0] differs
            arr2(1, 2, 1, 1), // dims[1] differs
            arr2(1, 1, 2, 1), // lbs[0] differs
            arr2(1, 1, 1, 2), // lbs[1] differs
        ];
        for v in &variants {
            let mut p = base.clone();
            p.extend_from_slice(v);
            drive(2, 0, &p);
            let mut q = v.clone();
            q.extend_from_slice(&base);
            drive(2, 0, &q);
        }
    }
}
