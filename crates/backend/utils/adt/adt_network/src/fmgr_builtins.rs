//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `network.c`
//! `inet`/`cidr` functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_network_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `inet` / `cidr` convention
//!
//! `inet` and `cidr` are pass-by-reference varlena types, but the value core
//! here operates on the owned [`inet_struct`] payload (family / bits / 16 addr
//! bytes), not the varlena image. The settled canonical `ByRef` image for these
//! types at this boundary is [`inet_struct::to_datum_bytes`] — an 18-byte,
//! header-LESS image (`family`, `bits`, then 16 address bytes) decoded by
//! [`inet_struct::from_datum_bytes`]. An `inet`/`cidr` ARG arrives as
//! `RefPayload::Varlena(image)` and a result is written symmetrically. (The real
//! on-disk varlena header / `SET_INET_VARSIZE` is the fmgr boundary's concern,
//! out of scope of the value core.)
//!
//! The `text`-returning accessors (`host`/`text`/`abbrev`) produce
//! header-stripped text payload bytes on the by-ref lane, matching the
//! `text`/`bytea` family convention.

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::types_network::inet_struct;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// Build a header-ful 4-byte-header varlena image from a payload.
#[inline]
fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    img
}

/// `PG_GETARG_INET_PP(i)`: decode an `inet`/`cidr` arg from its header-ful by-ref
/// image (`SET_VARSIZE_4B` length word + the 18-byte canonical `inet_struct`
/// image) on the side channel. inet/cidr is typlen==-1; under
/// header-ful-everywhere `VARDATA_ANY` is the payload after the 4-byte header.
#[inline]
fn arg_inet(fcinfo: &FunctionCallInfoBaseData, i: usize) -> inet_struct {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("inet fn: by-ref `inet` arg missing from by-ref lane");
    let body = vardata_any(image);
    assert!(body.len() >= 18, "inet fn: by-ref image too short");
    inet_struct::from_datum_bytes(body)
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image:
/// skip ONE header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ`
/// (4). A small stored value arrives short-headed once `SHORT_VARLENA_PACKING` is
/// on; a fixed 4-byte strip would drop three payload bytes. No-op while off.
#[inline]
fn vardata_any(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("inet fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)`: arg `i`'s low 32 bits, sign-extended.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("inet fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`: arg `i`'s full word as a signed 64-bit int.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("inet fn: missing arg").value.as_i64()
}

/// Set an `inet`/`cidr` (by-reference) result on the by-ref lane as a header-ful
/// varlena image: `SET_VARSIZE_4B(4 + 18)` length word + the 18-byte canonical
/// `inet_struct` image.
#[inline]
fn ret_inet(fcinfo: &mut FunctionCallInfoBaseData, addr: inet_struct) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&addr.to_datum_bytes())));
    Datum::from_usize(0)
}

/// Set a `text` result on the by-ref lane as a header-ful varlena image.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(varlena_image(&bytes)));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// `PG_RETURN_INT32(v)`.
#[inline]
fn ret_int32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// `PG_RETURN_NULL()`: set `fcinfo->isnull`.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx` (the
/// hash byte-gathering path). The bytes are copied out before drop.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("network fmgr scratch")
}

// ---------------------------------------------------------------------------
// I/O.
// ---------------------------------------------------------------------------

fn fc_inet_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    // Forward `fcinfo->context` (the soft ErrorSaveContext installed by
    // InputFunctionCallSafe); on the soft path the body returns `Ok(None)` and
    // the caller discards this placeholder after `soft_error_occurred()`.
    let escontext = fcinfo.escontext_mut();
    match crate::inet_in(&s, escontext)? {
        Some(addr) => Ok(ret_inet(fcinfo, addr)),
        None => Ok(Datum::null()),
    }
}

fn fc_cidr_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let escontext = fcinfo.escontext_mut();
    match crate::cidr_in(&s, escontext)? {
        Some(addr) => Ok(ret_inet(fcinfo, addr)),
        None => Ok(Datum::null()),
    }
}

fn fc_inet_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = arg_inet(fcinfo, 0);
    let s = String::from_utf8(crate::inet_out(&addr)?).expect("inet_out: valid utf8");
    Ok(ret_cstring(fcinfo, s))
}

fn fc_cidr_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = arg_inet(fcinfo, 0);
    let s = String::from_utf8(crate::cidr_out(&addr)?).expect("cidr_out: valid utf8");
    Ok(ret_cstring(fcinfo, s))
}

/// Read the raw external binary message body off the by-ref lane. C's
/// `recv(internal)` arg is a `StringInfo`; here the message bytes ride the by-ref
/// `Varlena` lane (as `macaddr_recv`/`oidrecv`/`uuid_recv` do) and the core reads
/// the `family`/`bits`/`is_cidr`/`length`/address octets off them.
#[inline]
fn arg_recv_msg<'a>(fcinfo: &'a FunctionCallInfoBaseData) -> &'a [u8] {
    fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("inet/cidr recv: by-ref message arg missing from by-ref lane")
}

fn fc_inet_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = crate::inet_recv(arg_recv_msg(fcinfo))?;
    Ok(ret_inet(fcinfo, addr))
}

fn fc_cidr_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = crate::cidr_recv(arg_recv_msg(fcinfo))?;
    Ok(ret_inet(fcinfo, addr))
}

fn fc_inet_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = arg_inet(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::inet_send(m.mcx(), &addr)?;
    Ok(ret_text(fcinfo, bytes))
}

fn fc_cidr_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let addr = arg_inet(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::cidr_send(m.mcx(), &addr)?;
    Ok(ret_text(fcinfo, bytes))
}

// ---------------------------------------------------------------------------
// Comparison (inet, inet -> bool / int4).
// ---------------------------------------------------------------------------

macro_rules! fc_cmp_bool {
    ($name:ident, $core:path) => {
        fn $name(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let a = arg_inet(fcinfo, 0);
            let b = arg_inet(fcinfo, 1);
            Ok(Datum::from_bool($core(&a, &b)))
        }
    };
}

fc_cmp_bool!(fc_network_eq, crate::network_eq);
fc_cmp_bool!(fc_network_ne, crate::network_ne);
fc_cmp_bool!(fc_network_lt, crate::network_lt);
fc_cmp_bool!(fc_network_le, crate::network_le);
fc_cmp_bool!(fc_network_gt, crate::network_gt);
fc_cmp_bool!(fc_network_ge, crate::network_ge);
fc_cmp_bool!(fc_network_sub, crate::network_sub);
fc_cmp_bool!(fc_network_subeq, crate::network_subeq);
fc_cmp_bool!(fc_network_sup, crate::network_sup);
fc_cmp_bool!(fc_network_supeq, crate::network_supeq);
fc_cmp_bool!(fc_network_overlap, crate::network_overlap);
fc_cmp_bool!(fc_inet_same_family, crate::inet_same_family);

fn fc_network_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(Datum::from_i32(crate::network_cmp(&a, &b)))
}

// ---------------------------------------------------------------------------
// inet,inet -> inet/cidr.
// ---------------------------------------------------------------------------

fn fc_network_larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::network_larger(&a, &b)))
}

fn fc_network_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::network_smaller(&a, &b)))
}

fn fc_inetand(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inetand(&a, &b)?))
}

fn fc_inetor(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inetor(&a, &b)?))
}

fn fc_inet_merge(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inet_merge(&a, &b)?))
}

// ---------------------------------------------------------------------------
// inet -> inet/cidr.
// ---------------------------------------------------------------------------

fn fc_inetnot(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::inetnot(&a)))
}

fn fc_inet_to_cidr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::inet_to_cidr(&a)?))
}

fn fc_network_broadcast(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::network_broadcast(&a)))
}

fn fc_network_network(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::network_network(&a)))
}

fn fc_network_netmask(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::network_netmask(&a)))
}

fn fc_network_hostmask(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_inet(fcinfo, crate::network_hostmask(&a)))
}

// ---------------------------------------------------------------------------
// set_masklen (inet/cidr, int4 -> inet/cidr).
// ---------------------------------------------------------------------------

fn fc_inet_set_masklen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let bits = arg_int32(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inet_set_masklen(&a, bits)?))
}

fn fc_cidr_set_masklen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let bits = arg_int32(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::cidr_set_masklen(&a, bits)?))
}

// ---------------------------------------------------------------------------
// inet -> int4.
// ---------------------------------------------------------------------------

fn fc_network_masklen(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_int32(crate::network_masklen(&a)))
}

fn fc_network_family(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_int32(crate::network_family(&a)))
}

// ---------------------------------------------------------------------------
// inet -> text (header-stripped text payload).
// ---------------------------------------------------------------------------

fn fc_network_host(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_text(fcinfo, crate::network_host(&a)?))
}

fn fc_network_show(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_text(fcinfo, crate::network_show(&a)?))
}

fn fc_inet_abbrev(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_text(fcinfo, crate::inet_abbrev(&a)?))
}

fn fc_cidr_abbrev(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    Ok(ret_text(fcinfo, crate::cidr_abbrev(&a)?))
}

// ---------------------------------------------------------------------------
// inet arithmetic (inet,int8 -> inet ; inet,inet -> int8).
// ---------------------------------------------------------------------------

fn fc_inetpl(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let addend = arg_int64(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inetpl(&a, addend)?))
}

fn fc_inetmi_int8(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let addend = arg_int64(fcinfo, 1);
    Ok(ret_inet(fcinfo, crate::inetmi_int8(&a, addend)?))
}

fn fc_inetmi(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let b = arg_inet(fcinfo, 1);
    Ok(Datum::from_i64(crate::inetmi(&a, &b)?))
}

// ---------------------------------------------------------------------------
// hashing (inet -> int4 ; inet,int8 -> int8). `network.c` hashes VARDATA_ANY
// (family + bits + addr) via hash_any; the value core returns those bytes, and
// here we apply hash_any over them.
// ---------------------------------------------------------------------------

fn fc_hashinet(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let mcx = scratch_mcx();
    let bytes = crate::hashinet(mcx.mcx(), &a)?;
    Ok(Datum::from_i32(hashfn::hash_bytes(&bytes) as i32))
}

fn fc_hashinetextended(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_inet(fcinfo, 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    let mcx = scratch_mcx();
    let bytes = crate::hashinetextended(mcx.mcx(), &a)?;
    Ok(Datum::from_i64(
        hashfn::hash_bytes_extended(&bytes, seed) as i64,
    ))
}

// ---------------------------------------------------------------------------
// 0-ary session-info int4 functions (kept from the prior registration).
// ---------------------------------------------------------------------------

fn fc_inet_client_port(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::inet_client_port()? {
        Some(p) => Ok(ret_int32(p)),
        None => Ok(ret_null(fcinfo)),
    }
}

fn fc_inet_server_port(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::inet_server_port()? {
        Some(p) => Ok(ret_int32(p)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `inet_client_addr() -> inet or NULL` (network.c). 0-ary, not strict.
fn fc_inet_client_addr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::inet_client_addr()? {
        Some(a) => Ok(ret_inet(fcinfo, a)),
        None => Ok(ret_null(fcinfo)),
    }
}

/// `inet_server_addr() -> inet or NULL` (network.c). 0-ary, not strict.
fn fc_inet_server_addr(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    match crate::inet_server_addr()? {
        Some(a) => Ok(ret_inet(fcinfo, a)),
        None => Ok(ret_null(fcinfo)),
    }
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
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the `network.c` `inet`/`cidr` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset are
/// transcribed exactly from `pg_proc.dat` (the inet relops/io/accessors are all
/// `proisstrict => 't'`, non-retset; the two session-port functions are
/// `proisstrict => 'f'`, 0-ary).
pub fn register_network_builtins() {
    fmgr_core::register_builtins_native([
        // I/O.
        builtin(910, "inet_in", 1, true, false, fc_inet_in),
        builtin(911, "inet_out", 1, true, false, fc_inet_out),
        builtin(1267, "cidr_in", 1, true, false, fc_cidr_in),
        builtin(1427, "cidr_out", 1, true, false, fc_cidr_out),
        builtin(2496, "inet_recv", 1, true, false, fc_inet_recv),
        builtin(2497, "inet_send", 1, true, false, fc_inet_send),
        builtin(2498, "cidr_recv", 1, true, false, fc_cidr_recv),
        builtin(2499, "cidr_send", 1, true, false, fc_cidr_send),
        // Comparison -> bool.
        builtin(920, "network_eq", 2, true, false, fc_network_eq),
        builtin(925, "network_ne", 2, true, false, fc_network_ne),
        builtin(921, "network_lt", 2, true, false, fc_network_lt),
        builtin(922, "network_le", 2, true, false, fc_network_le),
        builtin(923, "network_gt", 2, true, false, fc_network_gt),
        builtin(924, "network_ge", 2, true, false, fc_network_ge),
        builtin(927, "network_sub", 2, true, false, fc_network_sub),
        builtin(928, "network_subeq", 2, true, false, fc_network_subeq),
        builtin(929, "network_sup", 2, true, false, fc_network_sup),
        builtin(930, "network_supeq", 2, true, false, fc_network_supeq),
        builtin(3551, "network_overlap", 2, true, false, fc_network_overlap),
        builtin(4071, "inet_same_family", 2, true, false, fc_inet_same_family),
        builtin(926, "network_cmp", 2, true, false, fc_network_cmp),
        // inet,inet -> inet/cidr.
        builtin(3562, "network_larger", 2, true, false, fc_network_larger),
        builtin(3563, "network_smaller", 2, true, false, fc_network_smaller),
        builtin(2628, "inetand", 2, true, false, fc_inetand),
        builtin(2629, "inetor", 2, true, false, fc_inetor),
        builtin(4063, "inet_merge", 2, true, false, fc_inet_merge),
        // inet -> inet/cidr.
        builtin(2627, "inetnot", 1, true, false, fc_inetnot),
        builtin(1715, "inet_to_cidr", 1, true, false, fc_inet_to_cidr),
        builtin(698, "network_broadcast", 1, true, false, fc_network_broadcast),
        builtin(683, "network_network", 1, true, false, fc_network_network),
        builtin(696, "network_netmask", 1, true, false, fc_network_netmask),
        builtin(1362, "network_hostmask", 1, true, false, fc_network_hostmask),
        // set_masklen.
        builtin(605, "inet_set_masklen", 2, true, false, fc_inet_set_masklen),
        builtin(635, "cidr_set_masklen", 2, true, false, fc_cidr_set_masklen),
        // inet -> int4.
        builtin(697, "network_masklen", 1, true, false, fc_network_masklen),
        builtin(711, "network_family", 1, true, false, fc_network_family),
        // inet -> text.
        builtin(699, "network_host", 1, true, false, fc_network_host),
        builtin(730, "network_show", 1, true, false, fc_network_show),
        builtin(598, "inet_abbrev", 1, true, false, fc_inet_abbrev),
        builtin(599, "cidr_abbrev", 1, true, false, fc_cidr_abbrev),
        // inet arithmetic.
        builtin(2630, "inetpl", 2, true, false, fc_inetpl),
        builtin(2632, "inetmi_int8", 2, true, false, fc_inetmi_int8),
        builtin(2633, "inetmi", 2, true, false, fc_inetmi),
        // hashing.
        builtin(422, "hashinet", 1, true, false, fc_hashinet),
        builtin(779, "hashinetextended", 2, true, false, fc_hashinetextended),
        // 0-ary session-info int4 functions (proisstrict => 'f').
        builtin(2197, "inet_client_port", 0, false, false, fc_inet_client_port),
        builtin(2199, "inet_server_port", 0, false, false, fc_inet_server_port),
        // 0-ary session-info inet functions (proisstrict => 'f').
        builtin(2196, "inet_client_addr", 0, false, false, fc_inet_client_addr),
        builtin(2198, "inet_server_addr", 0, false, false, fc_inet_server_addr),
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::datum::NullableDatum;

    /// Build an `inet` image via the registered `inet_in` (OID 910).
    fn inet_in(s: &str) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let f = fmgr_core::native_builtin(910).expect("inet_in registered");
        f(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("inet_in produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("inet_in returned non-varlena: {other:?}"),
        }
    }

    /// Render an `inet` image via the registered `inet_out` (OID 911).
    fn inet_out(image: &[u8]) -> String {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let f = fmgr_core::native_builtin(911).expect("inet_out registered");
        f(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("inet_out produced a result") {
            RefPayload::Cstring(s) => s,
            other => panic!("inet_out returned non-cstring: {other:?}"),
        }
    }

    fn call_cmp(oid: u32, a: &[u8], b: &[u8]) -> bool {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let f = fmgr_core::native_builtin(oid).expect("op registered");
        f(&mut fcinfo).unwrap().as_bool()
    }

    fn call_unary_inet(oid: u32, a: &[u8]) -> Vec<u8> {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let f = fmgr_core::native_builtin(oid).expect("op registered");
        f(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("op produced a result") {
            RefPayload::Varlena(b) => b,
            other => panic!("non-varlena result: {other:?}"),
        }
    }

    fn call_unary_int32(oid: u32, a: &[u8]) -> i32 {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let f = fmgr_core::native_builtin(oid).expect("op registered");
        f(&mut fcinfo).unwrap().as_i32()
    }

    fn call_unary_text(oid: u32, a: &[u8]) -> String {
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let f = fmgr_core::native_builtin(oid).expect("op registered");
        f(&mut fcinfo).unwrap();
        match fcinfo.take_ref_result().expect("op produced a result") {
            RefPayload::Varlena(b) => String::from_utf8(b).unwrap(),
            other => panic!("non-varlena text result: {other:?}"),
        }
    }

    #[test]
    fn byref_inet_in_out_roundtrip() {
        crate::init_seams();
        let img = inet_in("192.168.1.226/24");
        assert_eq!(inet_out(&img), "192.168.1.226/24");
        // family=PGSQL_AF_INET(2), bits=24, then the four address octets.
        assert_eq!(&img[..6], &[2u8, 24, 192, 168, 1, 226]);
    }

    #[test]
    fn byref_network_compare_through_registry() {
        crate::init_seams();
        let a = inet_in("10.0.0.0/8");
        let b = inet_in("11.0.0.0/8");
        assert!(call_cmp(921, &a, &b)); // network_lt
        assert!(call_cmp(925, &a, &b)); // network_ne
        assert!(!call_cmp(920, &a, &b)); // network_eq
        assert!(call_cmp(920, &a, &a)); // network_eq self
        // 10.0.0.1/32 is contained within 10.0.0.0/8 -> network_sub.
        let sub = inet_in("10.0.0.1/32");
        let sup = inet_in("10.0.0.0/8");
        assert!(call_cmp(927, &sub, &sup)); // network_sub
        assert!(call_cmp(930, &sup, &sub)); // network_supeq
    }

    #[test]
    fn byref_accessors_through_registry() {
        crate::init_seams();
        let a = inet_in("192.168.1.226/24");
        // masklen = 24, family = 1 (IPv4 reported as 1 by network_family).
        assert_eq!(call_unary_int32(697, &a), 24); // network_masklen
        assert_eq!(call_unary_int32(711, &a), 4); // network_family (AF -> 4 for IPv4? see core)
        // host strips the masklen.
        assert_eq!(call_unary_text(699, &a), "192.168.1.226"); // network_host
        // broadcast / network of the /24.
        let bc = call_unary_inet(698, &a); // network_broadcast
        assert_eq!(inet_out(&bc), "192.168.1.255/24");
        let nw = call_unary_inet(683, &a); // network_network -> cidr
        assert_eq!(inet_out(&nw), "192.168.1.0/24");
    }

    #[test]
    fn byref_hashinet_through_registry() {
        crate::init_seams();
        let a = inet_in("192.168.1.226/24");
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.clone()))];
        let f = fmgr_core::native_builtin(422).unwrap();
        let h = f(&mut fcinfo).unwrap().as_i32();
        // hash over family,bits,addr (addrsize+2 bytes).
        let want = hashfn::hash_bytes(&[2u8, 24, 192, 168, 1, 226]) as i32;
        assert_eq!(h, want);
    }

    #[test]
    fn byref_inet_arithmetic_through_registry() {
        crate::init_seams();
        let a = inet_in("10.0.0.0/8");
        // inetpl(a, 256) = 10.0.1.0/8.
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::from_i64(256)),
        ];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.clone())), None];
        let f = fmgr_core::native_builtin(2630).unwrap();
        f(&mut fcinfo).unwrap();
        let sum = match fcinfo.take_ref_result().unwrap() {
            RefPayload::Varlena(v) => v,
            o => panic!("{o:?}"),
        };
        assert_eq!(inet_out(&sum), "10.0.1.0/8");
        // inetmi(10.0.1.0/8, 10.0.0.0/8) = 256.
        let lo = inet_in("10.0.1.0/8");
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(lo)),
            Some(RefPayload::Varlena(a)),
        ];
        let f = fmgr_core::native_builtin(2633).unwrap();
        let diff = f(&mut fcinfo).unwrap().as_i64();
        assert_eq!(diff, 256);
    }
}
