//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `nbtcompare.c` btree three-way comparison functions
//! (`btint2cmp`/`btint4cmp`/.../`btoidcmp`/`btcharcmp`/`btboolcmp`).
//!
//! These are the btree `BTORDER_PROC` (support proc 1) functions for the in-core
//! trivial integer/oid/char/bool opclasses. The relcache nails up several
//! catalog indexes (`pg_class_oid_index`, `pg_attribute_relid_attnum_index`,
//! `pg_amproc_fam_proc_index`, ...) before the syscache exists; scanning those
//! indexes drives `_bt_compare`, which calls the column opclass's `BTORDER_PROC`
//! through fmgr. If that proc is not in the builtin fast-path table,
//! `fmgr_isbuiltin` misses and recurses into `SearchSysCache(PROCOID)` →
//! `catalog_cache_initialize_cache` → boot stack overflow. Registering them here
//! (C: their `fmgr_builtins[]` rows) keeps the fast path complete.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr call
//! frame, calls the matching value core (ported in this crate), and writes the
//! `int4` result word. OIDs / nargs / strict / retset are transcribed exactly
//! from `pg_proc.dat` (all are `proisstrict => 't'`, none retset).
//!
//! `btoidvectorcmp` is NOT registered here: its `oidvector` arguments are a
//! by-reference array carrier not expressible at the current fmgr boundary
//! (mirroring the `oidvector` family deferral in `oid.c`'s fmgr layer). Its value
//! core remains in-crate for that owner to call.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writer.
// ---------------------------------------------------------------------------

#[inline]
fn arg_i16(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i16 {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_i16()
}
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_i32()
}
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_i64()
}
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_oid()
}
/// `PG_GETARG_CHAR(i)`: the `char` (`"char"`) type is a single signed byte.
#[inline]
fn arg_char(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i8 {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_i8()
}
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("nbtcompare fn: missing arg").value.as_bool()
}
/// `PG_RETURN_INT32(x)`: the three-way comparison result word.
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_btint2cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint2cmp(arg_i16(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_btint4cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint4cmp(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_btint8cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint8cmp(arg_i64(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_btint48cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint48cmp(arg_i32(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_btint84cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint84cmp(arg_i64(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_btint24cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint24cmp(arg_i16(fcinfo, 0), arg_i32(fcinfo, 1)))
}
fn fc_btint42cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint42cmp(arg_i32(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_btint28cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint28cmp(arg_i16(fcinfo, 0), arg_i64(fcinfo, 1)))
}
fn fc_btint82cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btint82cmp(arg_i64(fcinfo, 0), arg_i16(fcinfo, 1)))
}
fn fc_btoidcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btoidcmp(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1)))
}
fn fc_btcharcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btcharcmp(arg_char(fcinfo, 0), arg_char(fcinfo, 1)))
}
fn fc_btboolcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::btboolcmp(arg_bool(fcinfo, 0), arg_bool(fcinfo, 1)))
}

/// Decode an `oidvector` on-disk image to its `Oid` element list. An
/// `oidvector` is a PLAIN-storage flat `ArrayType` (no TOAST): a fixed header
/// `int32 ndim; int32 dataoffset; Oid elemtype; int32 dim1; int32 lbound1;`
/// (20 bytes, no null bitmap because `oidvectorin` never produces NULLs)
/// followed by `dim1` native-endian `Oid` words. A 0-dimension vector
/// (`ndim == 0`) has no elements. This mirrors `oidvector_to_oids_bytes`
/// inline, so the fmgr adapter needs no allocating context.
fn decode_oidvector(image: &[u8]) -> Vec<types_core::Oid> {
    if image.len() < 20 {
        return Vec::new();
    }
    let ndim = i32::from_ne_bytes([image[0], image[1], image[2], image[3]]);
    if ndim < 1 {
        return Vec::new();
    }
    // dim1 == ARR_DIMS(vec)[0] (the 4th int32 in the header).
    let dim1 = i32::from_ne_bytes([image[12], image[13], image[14], image[15]]);
    let n = dim1.max(0) as usize;
    // ARR_DATA_PTR: header (5 int32 = 20 bytes), no null bitmap for oidvector.
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = 20 + i * 4;
        match image.get(off..off + 4) {
            Some(b) => out.push(types_core::Oid::from(u32::from_ne_bytes([
                b[0], b[1], b[2], b[3],
            ]))),
            None => break,
        }
    }
    out
}

fn fc_btoidvectorcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a_img = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("btoidvectorcmp: oidvector arg 0 missing from by-ref lane");
    let a = decode_oidvector(a_img);
    let b_img = fcinfo
        .ref_arg(1)
        .and_then(|p| p.as_varlena())
        .expect("btoidvectorcmp: oidvector arg 1 missing from by-ref lane");
    let b = decode_oidvector(b_img);
    ret_i32(crate::btoidvectorcmp(&a, &b))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict: true,
        retset: false,
        func: Some(func),
    }
}

/// Register every scalar `nbtcompare.c` btree comparison builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs/nargs
/// from `pg_proc.dat`; all are `proisstrict => 't'` and not retset.
///
/// `btoidvectorcmp` (oid 404) is deferred with the `oidvector` carrier.
pub fn register_nbtcompare_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(350, "btint2cmp", 2, fc_btint2cmp),
        builtin(351, "btint4cmp", 2, fc_btint4cmp),
        builtin(842, "btint8cmp", 2, fc_btint8cmp),
        builtin(2188, "btint48cmp", 2, fc_btint48cmp),
        builtin(2189, "btint84cmp", 2, fc_btint84cmp),
        builtin(2190, "btint24cmp", 2, fc_btint24cmp),
        builtin(2191, "btint42cmp", 2, fc_btint42cmp),
        builtin(2192, "btint28cmp", 2, fc_btint28cmp),
        builtin(2193, "btint82cmp", 2, fc_btint82cmp),
        builtin(356, "btoidcmp", 2, fc_btoidcmp),
        builtin(358, "btcharcmp", 2, fc_btcharcmp),
        builtin(1693, "btboolcmp", 2, fc_btboolcmp),
        builtin(404, "btoidvectorcmp", 2, fc_btoidvectorcmp),
    ]);
}
