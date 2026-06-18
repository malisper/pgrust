//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `jsonb.c` / `jsonb_op.c` / `jsonb_util.c` functions whose argument/result
//! types are expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_jsonb_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `jsonb` convention
//!
//! `jsonb` is a pass-by-reference (varlena) type. Its values cross the fmgr
//! boundary on the by-reference side channel: a `jsonb` ARG arrives as
//! `fcinfo.ref_arg(i) == Some(RefPayload::Varlena(image))` and a `jsonb`
//! RESULT is set via `fcinfo.set_ref_result(RefPayload::Varlena(image))`. The
//! bare by-value word is the null/dummy word, exactly as the canonical->ABI
//! bridge `datum_to_ref_arg`/`ref_out_to_datum` arranges.
//!
//! The `image` carried on the lane is the COMPLETE `jsonb` varlena byte image
//! INCLUDING its `VARHDRSZ` header — the canonical `ByRef` image of a
//! disk-stored type. The two distinct value-core families consume it
//! differently, and the wrappers reconcile that here:
//!
//! * **I/O** (`jsonb_in`/`jsonb_recv`/`jsonb_out`/`jsonb_send`,
//!   [`crate`]): `jsonb_out`/`jsonb_send` themselves slice `&jsonb[VARHDRSZ..]`,
//!   and `jsonb_in`/`jsonb_recv` (via `JsonbValueToJsonb`) PRODUCE the full
//!   varlena image. So these wrappers pass / receive the full image verbatim.
//! * **operators / B-Tree compare / hash** ([`backend_utils_adt_jsonb_op`]):
//!   these take `&jb->root`, i.e. the on-disk container bytes STARTING AFTER the
//!   varlena header. So these wrappers strip the leading `VARHDRSZ` bytes off
//!   the lane image before calling the core.
//!
//! The `text` arguments of `jsonb_exists` / `jsonb_exists_any` /
//! `jsonb_exists_all` follow the `text`/`bytea` convention
//! (`backend-utils-adt-varlena`): the lane delivers the header-stripped payload
//! (`VARDATA_ANY`), which is exactly what the cores expect.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use backend_utils_adt_jsonb_util::VARHDRSZ;
use backend_utils_fmgr_core as fmgr_core;
/// The unified value type the `to_jsonb` core consumes (`types_tuple::Datum`).
use types_tuple::Datum as ValDatum;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_JSONB_P(i)`: a `jsonb` arg's FULL varlena byte image, read from
/// the by-reference side channel (the boundary carries it un-stripped).
#[inline]
fn arg_jsonb_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonb fn: by-ref `jsonb` arg missing from by-ref lane")
}

/// A `jsonb` arg's root-container bytes (`&jb->root`), i.e. the lane image with
/// its leading `VARHDRSZ` header stripped — the form `jsonb_op.c`'s container
/// engine consumes.
#[inline]
fn arg_jsonb_root<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    &arg_jsonb_image(fcinfo, i)[VARHDRSZ..]
}

/// `PG_GETARG_TEXT_PP(i)` payload bytes (`VARDATA_ANY`): under the header-ful
/// convention the lane carries the full `text` varlena image, so strip its
/// leading `VARHDRSZ` header to recover the payload the cores consume.
#[inline]
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    &arg_varlena_image(fcinfo, i)[VARHDRSZ..]
}

/// The FULL by-reference varlena image of arg `i` (e.g. a detoasted `text[]`
/// array), read off the by-ref lane verbatim (header-ful) — the form
/// `deconstruct_text_array` and other whole-image consumers expect.
#[inline]
fn arg_varlena_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonb fn: by-ref varlena arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("jsonb fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("jsonb fn: missing arg").value.as_i64()
}

/// `get_fn_expr_argtype(fcinfo->flinfo, i)`: the actual type OID of a
/// polymorphic argument resolved from the calling expression tree (the
/// `anyelement` resolution path for `to_jsonb`).
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> types_core::Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
}

/// Materialize argument `i` as the unified `types_tuple::Datum` the `to_jsonb`
/// value core consumes: a by-value scalar word, a by-reference varlena, a
/// `cstring`, or a composite record. Scratch copies live in `mcx`. The arg must
/// be non-NULL (`to_jsonb` is `proisstrict`).
///
/// The `to_jsonb` core routes scalar by-reference types (`text`/numeric/...)
/// through `output_function_call` → the type's output function, which reads its
/// arg off the fmgr by-reference lane in the same header-STRIPPED form the lane
/// already carries; arrays/composites cross as full images and the core's
/// `detoast_attr` consumes them directly. So the lane bytes are forwarded as
/// the `ByRef` image verbatim — no header re-attachment (which would corrupt the
/// scalar output-function path).
fn arg_value<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<ValDatum<'mcx>> {
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => ValDatum::ByRef(mcx::slice_in(mcx, b)?),
        Some(RefPayload::Cstring(s)) => ValDatum::Cstring(s.clone()),
        Some(RefPayload::Composite(image)) => {
            ValDatum::Composite(types_tuple::FormedTuple::from_datum_image(mcx, image)?)
        }
        Some(RefPayload::Expanded(eo)) => {
            ValDatum::ByRef(mcx::slice_in(mcx, &types_datum::flatten_expanded(eo.as_ref()))?)
        }
        // `to_jsonb` does not take an `internal` argument.
        Some(RefPayload::Internal(_)) => {
            panic!("to_jsonb: unexpected `internal` argument on the by-ref lane")
        }
        None => ValDatum::ByVal(
            fcinfo
                .arg(i)
                .expect("jsonb fn: missing by-value arg")
                .value
                .as_usize(),
        ),
    })
}

/// Set a `jsonb` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. The bytes are the full jsonb varlena image (with header).
#[inline]
fn ret_jsonb(fcinfo: &mut FunctionCallInfoBaseData, image: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// Set a `text`/`bytea` (by-reference) result on the by-ref lane. As with the
/// `text`/`bytea` family (`backend-utils-adt-varlena`), `_send` results carry
/// the wire bytes verbatim.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// resulting bytes are copied onto the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("jsonb fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// I/O adapters (jsonb.c).
// ---------------------------------------------------------------------------

/// `jsonb_in(cstring) -> jsonb` (oid 3806).
fn fc_jsonb_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::jsonb_in(m.mcx(), s.as_bytes()));
    ret_jsonb(fcinfo, image.as_slice().to_vec())
}

/// `jsonb_recv(internal) -> jsonb` (oid 3805): a 1-byte version then JSON text.
/// The `internal` (StringInfo) arg is delivered as its message-buffer bytes on
/// the by-ref lane.
fn fc_jsonb_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_jsonb_image(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::jsonb_recv(m.mcx(), buf));
    ret_jsonb(fcinfo, image.as_slice().to_vec())
}

/// `jsonb_out(jsonb) -> cstring` (oid 3804).
fn fc_jsonb_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let image = arg_jsonb_image(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::jsonb_out(m.mcx(), image));
    // `jsonb_out` returns a NUL-terminated cstring byte buffer; decode to a
    // String for the cstring lane (strip a trailing NUL if present).
    let raw = bytes.as_slice();
    let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
    ret_cstring(fcinfo, String::from_utf8_lossy(body).into_owned())
}

/// `jsonb_send(jsonb) -> bytea` (oid 3803). The core returns the wire bytes
/// (version byte + text); we wrap them into a `bytea` varlena image.
fn fc_jsonb_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let image = arg_jsonb_image(fcinfo, 0);
    let m = scratch_mcx();
    let wire = ok(crate::jsonb_send(m.mcx(), image));
    // C `pq_endtypsend` wraps the StringInfo payload into a `bytea` varlena:
    // a 4-byte length header (`VARHDRSZ + len`, `<< 2` native-order) + payload.
    let total = VARHDRSZ + wire.len();
    let mut out = Vec::with_capacity(total);
    let header = (total as u32) << 2;
    out.extend_from_slice(&header.to_ne_bytes());
    out.extend_from_slice(&wire);
    ret_varlena(fcinfo, out)
}

// ---------------------------------------------------------------------------
// Output family (jsonb.c) -> jsonb.
// ---------------------------------------------------------------------------

/// `to_jsonb(anyelement) -> jsonb` (oid 3787). `val_type =
/// get_fn_expr_argtype(flinfo, 0)` resolves the polymorphic input type, then
/// the value is classified + rendered into a `jsonb` image by the core.
fn fc_to_jsonb(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val_type = fn_expr_argtype(fcinfo, 0);
    let m = scratch_mcx();
    let val = ok(arg_value(m.mcx(), fcinfo, 0));
    let image = ok(crate::to_jsonb(m.mcx(), &val, val_type));
    ret_jsonb(fcinfo, image.as_slice().to_vec())
}

// ---------------------------------------------------------------------------
// B-Tree comparison adapters (jsonb_op.c) -> bool / int4.
// ---------------------------------------------------------------------------

/// Body of a binary `(jsonb, jsonb) -> bool` builtin around a
/// `fn(&[u8], &[u8]) -> PgResult<bool>` container core (`&jb->root` args).
macro_rules! fc_jb_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let a = arg_jsonb_root(fcinfo, 0);
            let b = arg_jsonb_root(fcinfo, 1);
            ret_bool(ok($core(a, b)))
        }
    };
}

fc_jb_cmp_bool!(fc_jsonb_eq, backend_utils_adt_jsonb_op::jsonb_eq);
fc_jb_cmp_bool!(fc_jsonb_ne, backend_utils_adt_jsonb_op::jsonb_ne);
fc_jb_cmp_bool!(fc_jsonb_lt, backend_utils_adt_jsonb_op::jsonb_lt);
fc_jb_cmp_bool!(fc_jsonb_le, backend_utils_adt_jsonb_op::jsonb_le);
fc_jb_cmp_bool!(fc_jsonb_gt, backend_utils_adt_jsonb_op::jsonb_gt);
fc_jb_cmp_bool!(fc_jsonb_ge, backend_utils_adt_jsonb_op::jsonb_ge);

/// `jsonb_cmp(jsonb, jsonb) -> int4` (oid 4044): -1/0/1.
fn fc_jsonb_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a = arg_jsonb_root(fcinfo, 0);
    let b = arg_jsonb_root(fcinfo, 1);
    ret_i32(ok(backend_utils_adt_jsonb_op::jsonb_cmp(a, b)))
}

// ---------------------------------------------------------------------------
// Containment / existence operator adapters (jsonb_op.c) -> bool.
// ---------------------------------------------------------------------------

/// `jsonb_contains(jsonb, jsonb) -> bool` (oid 4046).
fn fc_jsonb_contains(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = arg_jsonb_root(fcinfo, 0);
    let tmpl = arg_jsonb_root(fcinfo, 1);
    ret_bool(ok(backend_utils_adt_jsonb_op::jsonb_contains(val, tmpl)))
}

/// `jsonb_contained(jsonb, jsonb) -> bool` (oid 4050): arg 0 = tmpl, arg 1 = val.
fn fc_jsonb_contained(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tmpl = arg_jsonb_root(fcinfo, 0);
    let val = arg_jsonb_root(fcinfo, 1);
    ret_bool(ok(backend_utils_adt_jsonb_op::jsonb_contained(tmpl, val)))
}

/// `jsonb_exists(jsonb, text) -> bool` (oid 4047).
fn fc_jsonb_exists(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jb = arg_jsonb_root(fcinfo, 0);
    let key = arg_text_payload(fcinfo, 1);
    ret_bool(ok(backend_utils_adt_jsonb_op::jsonb_exists(jb, key)))
}

/// `jsonb_exists_any(jsonb, _text) -> bool` (oid 4048). The `text[]` arg arrives
/// as its detoasted array varlena bytes; the core flattens it through the
/// `deconstruct_text_array` seam.
fn fc_jsonb_exists_any(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jb = arg_jsonb_root(fcinfo, 0);
    let keys = arg_varlena_image(fcinfo, 1);
    ret_bool(ok(backend_utils_adt_jsonb_op::jsonb_exists_any(jb, keys)))
}

/// `jsonb_exists_all(jsonb, _text) -> bool` (oid 4049).
fn fc_jsonb_exists_all(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jb = arg_jsonb_root(fcinfo, 0);
    let keys = arg_varlena_image(fcinfo, 1);
    ret_bool(ok(backend_utils_adt_jsonb_op::jsonb_exists_all(jb, keys)))
}

// ---------------------------------------------------------------------------
// Hash adapters (jsonb_op.c).
// ---------------------------------------------------------------------------

/// `jsonb_hash(jsonb) -> int4` (oid 4045).
fn fc_jsonb_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jb = arg_jsonb_root(fcinfo, 0);
    ret_i32(ok(backend_utils_adt_jsonb_op::jsonb_hash(jb)))
}

/// `jsonb_hash_extended(jsonb, int8) -> int8` (oid 3416).
fn fc_jsonb_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let jb = arg_jsonb_root(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    ret_i64(ok(backend_utils_adt_jsonb_op::jsonb_hash_extended(jb, seed)) as i64)
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

/// Register every expressible scalar `jsonb` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`
/// (all of these are `proisstrict => 't'` default and none `proretset`).
pub fn register_jsonb_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // I/O.
        builtin(3806, "jsonb_in", 1, true, false, fc_jsonb_in),
        builtin(3805, "jsonb_recv", 1, true, false, fc_jsonb_recv),
        builtin(3804, "jsonb_out", 1, true, false, fc_jsonb_out),
        builtin(3803, "jsonb_send", 1, true, false, fc_jsonb_send),
        // Output family: resolved-arg-type + arbitrary-type output dispatch.
        builtin(3787, "to_jsonb", 1, true, false, fc_to_jsonb),
        // B-Tree comparison -> bool.
        builtin(4043, "jsonb_eq", 2, true, false, fc_jsonb_eq),
        builtin(4038, "jsonb_ne", 2, true, false, fc_jsonb_ne),
        builtin(4039, "jsonb_lt", 2, true, false, fc_jsonb_lt),
        builtin(4041, "jsonb_le", 2, true, false, fc_jsonb_le),
        builtin(4040, "jsonb_gt", 2, true, false, fc_jsonb_gt),
        builtin(4042, "jsonb_ge", 2, true, false, fc_jsonb_ge),
        // 3-way comparison -> int4.
        builtin(4044, "jsonb_cmp", 2, true, false, fc_jsonb_cmp),
        // Containment / existence -> bool.
        builtin(4046, "jsonb_contains", 2, true, false, fc_jsonb_contains),
        builtin(4050, "jsonb_contained", 2, true, false, fc_jsonb_contained),
        builtin(4047, "jsonb_exists", 2, true, false, fc_jsonb_exists),
        builtin(4048, "jsonb_exists_any", 2, true, false, fc_jsonb_exists_any),
        builtin(4049, "jsonb_exists_all", 2, true, false, fc_jsonb_exists_all),
        // Hash.
        builtin(4045, "jsonb_hash", 1, true, false, fc_jsonb_hash),
        builtin(3416, "jsonb_hash_extended", 2, true, false, fc_jsonb_hash_extended),
    ]);
}

// ===========================================================================
// End-to-end proof: a by-reference `jsonb` builtin is genuinely callable
// through the fmgr registry.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    use backend_utils_adt_jsonb_util::{
        self as jbu, jbvType, JsonbValue, JsonbValueData, JsonbIteratorToken::*,
    };
    use mcx::MemoryContext;
    use types_datum::NullableDatum;

    /// Install the external seams the jsonb serialization / compare / hash cores
    /// reach. Delegates to the crate's single shared installer (`tests.rs`,
    /// one process-global `Once`) so both test modules install the same slots
    /// exactly once without tripping the seam's install-twice panic. The
    /// `jsonb_in`/`jsonb_recv` text-parse path needs the jsonapi lexer provider
    /// (owned by another crate), so these tests assemble `jsonb` images directly
    /// through the sanctioned `JsonbValue` push API — exactly like `tests.rs` —
    /// and prove the by-ref `jsonb_out`/compare/contains wrappers by OID.
    fn install_seams() {
        crate::tests::install_seams();
    }

    fn jstring(s: &str) -> JsonbValue {
        JsonbValue {
            typ: jbvType::jbvString,
            val: JsonbValueData::String(s.as_bytes().to_vec()),
        }
    }

    /// Build an on-disk jsonb object from `(key, value-string)` pairs.
    fn build_object(pairs: &[(&str, &str)]) -> Vec<u8> {
        install_seams();
        register_jsonb_builtins();
        let ctx = MemoryContext::new("jsonb.fmgr.test.object");
        let mut ps: Option<Box<jbu::JsonbParseState>> = None;
        jbu::pushJsonbValue(&mut ps, WJB_BEGIN_OBJECT, None).unwrap();
        for (k, v) in pairs {
            jbu::pushJsonbValue(&mut ps, WJB_KEY, Some(&jstring(k))).unwrap();
            jbu::pushJsonbValue(&mut ps, WJB_VALUE, Some(&jstring(v))).unwrap();
        }
        let res = jbu::pushJsonbValue(&mut ps, WJB_END_OBJECT, None)
            .unwrap()
            .unwrap();
        let buf = jbu::JsonbValueToJsonb(ctx.mcx(), &res).unwrap();
        buf.as_slice().to_vec()
    }

    /// Build a top-level raw-scalar jsonb string image.
    fn build_scalar(s: &str) -> Vec<u8> {
        install_seams();
        register_jsonb_builtins();
        let ctx = MemoryContext::new("jsonb.fmgr.test.scalar");
        let buf = jbu::JsonbValueToJsonb(ctx.mcx(), &jstring(s)).unwrap();
        buf.as_slice().to_vec()
    }

    /// Render a jsonb image back to text through the registered `jsonb_out`.
    fn jsonb_text(image: &[u8]) -> String {
        register_jsonb_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(3804).expect("jsonb_out registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("jsonb_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("jsonb_out: unexpected result lane {other:?}"),
        }
    }

    fn call_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        register_jsonb_builtins();
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
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_bool()
    }

    fn call_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        register_jsonb_builtins();
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
        let d = (entry.func.unwrap())(&mut fcinfo);
        d.as_i32()
    }

    /// THE PROOF (I/O): render a directly-assembled `jsonb` object through the
    /// registered `jsonb_out` by OID, with the `jsonb` value crossing on the
    /// by-reference (full-varlena-image) lane.
    #[test]
    fn byref_jsonb_out_through_registry() {
        let img = build_object(&[("k", "v")]);
        assert_eq!(jsonb_text(&img), r#"{"k": "v"}"#);
    }

    /// `jsonb_send` (oid 3803) wraps the wire bytes into a `bytea` varlena: the
    /// 4-byte length header (`<< 2`) + version byte (1) + the rendered text.
    #[test]
    fn byref_jsonb_send_through_registry() {
        let img = build_object(&[("k", "v")]);
        register_jsonb_builtins();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(img.clone()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(3803).expect("jsonb_send registered");
        (entry.func.unwrap())(&mut fcinfo);
        let bytea = match fcinfo.take_ref_result().expect("jsonb_send produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("jsonb_send: unexpected result lane {other:?}"),
        };
        // Header decodes to the total length; payload is [version=1, text...].
        let header = u32::from_ne_bytes([bytea[0], bytea[1], bytea[2], bytea[3]]) >> 2;
        assert_eq!(header as usize, bytea.len());
        assert_eq!(bytea[VARHDRSZ], 1u8);
        let text = &bytea[VARHDRSZ + 1..];
        assert_eq!(text, br#"{"k": "v"}"#);
    }

    /// Equality / ordering through the registry by OID, `jsonb` args on the
    /// by-ref lane. `"a" < "b"` as jsonb string scalars.
    #[test]
    fn byref_jsonb_compare_through_registry() {
        let a = build_scalar("a");
        let b = build_scalar("b");
        // jsonb_eq 4043, jsonb_ne 4038, jsonb_lt 4039, jsonb_gt 4040.
        assert!(call_cmp_bool(4043, &a, &a)); // a == a
        assert!(!call_cmp_bool(4043, &a, &b)); // a == b -> false
        assert!(call_cmp_bool(4038, &a, &b)); // a != b
        assert!(call_cmp_bool(4039, &a, &b)); // a < b
        assert!(!call_cmp_bool(4040, &a, &b)); // a > b -> false
        // jsonb_cmp 4044.
        assert_eq!(call_cmp_i32(4044, &a, &b), -1);
        assert_eq!(call_cmp_i32(4044, &a, &a), 0);
        assert_eq!(call_cmp_i32(4044, &b, &a), 1);
    }

    /// Containment through the registry (`{"k":"v"}` is contained in
    /// `{"k":"v","x":"y"}`), `jsonb` args on the by-ref lane.
    #[test]
    fn byref_jsonb_contains_through_registry() {
        let big = build_object(&[("k", "v"), ("x", "y")]);
        let small = build_object(&[("k", "v")]);
        // jsonb_contains 4046: big @> small -> true; small @> big -> false.
        assert!(call_cmp_bool(4046, &big, &small));
        assert!(!call_cmp_bool(4046, &small, &big));
        // jsonb_contained 4050 (arg 0 = tmpl, arg 1 = val): small <@ big -> true.
        assert!(call_cmp_bool(4050, &small, &big));
    }
}
