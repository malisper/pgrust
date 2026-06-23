//! The type-specific distance support functions
//! (`brin_minmax_multi_distance_*`, brin_minmax_multi.c:1882..2377).
//!
//! Each is a SQL function (`PG_FUNCTION_INFO_V1`) returning `float8`, reached BY
//! OID through fmgr (`PROCNUM_DISTANCE`) during compaction. They are registered
//! as fmgr builtins ([`register_distance_builtins`]) so
//! `function_call2_coll(distance_oid, ...)` resolves them, exactly as C's
//! `fmgr_builtins[]` does. The pure per-type computation is split out (so it is
//! directly unit-testable); the fcinfo wrappers only marshal the arguments
//! (by-value scalars off the bare word, by-reference values off the fmgr
//! by-reference lane) and the `float8` result.

use datum::Datum as WordDatum;
use types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use numeric_seams as numeric;

/// `PGSQL_AF_INET` (inet.h): `AF_INET` (== 2 on the supported platforms). The
/// other family (`PGSQL_AF_INET6`) is `AF_INET + 1`; the distance function only
/// compares families for equality and derives the address size from `AF_INET`.
const PGSQL_AF_INET: u8 = 2;

/// `MaxHeapTuplesPerPage` for the tid distance mapping.
const MAX_HEAP_TUPLES_PER_PAGE: f64 = types_storage::bufpage::MaxHeapTuplesPerPage as f64;

/// `USECS_PER_SEC` (timestamp.h).
const USECS_PER_SEC: i64 = 1_000_000;
/// `USECS_PER_DAY` (timestamp.h).
const USECS_PER_DAY: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// pure distance computations (one per type).
// ---------------------------------------------------------------------------

/// `brin_minmax_multi_distance_float4` (brin_minmax_multi.c:1882).
pub fn dist_float4(a1: f32, a2: f32) -> f64 {
    if a1.is_nan() && a2.is_nan() {
        return 0.0;
    }
    if a1.is_nan() || a2.is_nan() {
        return f64::INFINITY;
    }
    debug_assert!(a1 <= a2);
    (a2 as f64) - (a1 as f64)
}

/// `brin_minmax_multi_distance_float8` (brin_minmax_multi.c:1908).
pub fn dist_float8(a1: f64, a2: f64) -> f64 {
    if a1.is_nan() && a2.is_nan() {
        return 0.0;
    }
    if a1.is_nan() || a2.is_nan() {
        return f64::INFINITY;
    }
    debug_assert!(a1 <= a2);
    a2 - a1
}

/// `brin_minmax_multi_distance_int2` (brin_minmax_multi.c:1934).
pub fn dist_int2(a1: i16, a2: i16) -> f64 {
    debug_assert!(a1 <= a2);
    (a2 as f64) - (a1 as f64)
}

/// `brin_minmax_multi_distance_int4` (brin_minmax_multi.c:1952).
pub fn dist_int4(a1: i32, a2: i32) -> f64 {
    debug_assert!(a1 <= a2);
    (a2 as f64) - (a1 as f64)
}

/// `brin_minmax_multi_distance_int8` (brin_minmax_multi.c:1970).
pub fn dist_int8(a1: i64, a2: i64) -> f64 {
    debug_assert!(a1 <= a2);
    (a2 as f64) - (a1 as f64)
}

/// `brin_minmax_multi_distance_tid` (brin_minmax_multi.c:1989). `(block, off)`
/// from `ItemPointerGet{BlockNumber,OffsetNumber}NoCheck`.
pub fn dist_tid(a1: (u32, u16), a2: (u32, u16)) -> f64 {
    let da1 = (a1.0 as f64) * MAX_HEAP_TUPLES_PER_PAGE + (a1.1 as f64);
    let da2 = (a2.0 as f64) * MAX_HEAP_TUPLES_PER_PAGE + (a2.1 as f64);
    da2 - da1
}

/// `brin_minmax_multi_distance_uuid` (brin_minmax_multi.c:2047). 16 octets.
pub fn dist_uuid(u1: &[u8; 16], u2: &[u8; 16]) -> f64 {
    let mut delta: f64 = 0.0;
    for i in (0..16).rev() {
        delta += (u2[i] as i32 - u1[i] as i32) as f64;
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_date` (brin_minmax_multi.c:2079). `DateADT` i32.
pub fn dist_date(d1: i32, d2: i32) -> f64 {
    let delta = (d2 as f64) - (d1 as f64);
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_time` (brin_minmax_multi.c:2099). `TimeADT` i64.
pub fn dist_time(ta: i64, tb: i64) -> f64 {
    let delta = (tb - ta) as f64;
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_timetz` (brin_minmax_multi.c:2119). `(time, zone)`.
pub fn dist_timetz(ta: (i64, i32), tb: (i64, i32)) -> f64 {
    let delta = ((tb.0 - ta.0) + ((tb.1 - ta.1) as i64) * USECS_PER_SEC) as f64;
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_timestamp` (brin_minmax_multi.c:2137). i64.
pub fn dist_timestamp(dt1: i64, dt2: i64) -> f64 {
    let delta = (dt2 as f64) - (dt1 as f64);
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_interval` (brin_minmax_multi.c:2155).
/// `(time, day, month)`.
pub fn dist_interval(ia: (i64, i32, i32), ib: (i64, i32, i32)) -> f64 {
    let dayfraction = (ib.0 % USECS_PER_DAY) - (ia.0 % USECS_PER_DAY);
    let mut days = (ib.0 / USECS_PER_DAY) - (ia.0 / USECS_PER_DAY);
    days += (ib.1 as i64) - (ia.1 as i64);
    days += ((ib.2 as i64) - (ia.2 as i64)) * 30;
    let delta = (days as f64) + (dayfraction as f64) / (USECS_PER_DAY as f64);
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_pg_lsn` (brin_minmax_multi.c:2191). `XLogRecPtr` u64.
pub fn dist_pg_lsn(lsna: u64, lsnb: u64) -> f64 {
    let delta = (lsnb - lsna) as f64;
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_macaddr` (brin_minmax_multi.c:2212). 6 octets,
/// from high (f) to low (a).
pub fn dist_macaddr(a: &[u8; 6], b: &[u8; 6]) -> f64 {
    let mut delta: f64 = 0.0;
    for i in (0..6).rev() {
        delta += (b[i] as f64) - (a[i] as f64);
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_macaddr8` (brin_minmax_multi.c:2249). 8 octets.
pub fn dist_macaddr8(a: &[u8; 8], b: &[u8; 8]) -> f64 {
    let mut delta: f64 = 0.0;
    for i in (0..8).rev() {
        delta += (b[i] as f64) - (a[i] as f64);
        delta /= 256.0;
    }
    debug_assert!(delta >= 0.0);
    delta
}

/// `brin_minmax_multi_distance_inet` (brin_minmax_multi.c:2297): difference of
/// two inet addresses, masked and normalized to `[0,1]`; different families ->
/// 1.0. `family`/`addr`/`bits` are the `ip_family`/`ip_addr`/`ip_bits` fields;
/// `addr` is `ip_addrsize` bytes.
pub fn dist_inet(family_a: u8, addr_a: &[u8], bits_a: i32, family_b: u8, addr_b: &[u8], bits_b: i32) -> f64 {
    // different families -> maximal distance
    if family_a != family_b {
        return 1.0;
    }

    let len = addr_a.len();
    let mut a = addr_a.to_vec();
    let mut b = addr_b.to_vec();

    // apply the network mask to both addresses
    for i in 0..len {
        let nbits = (bits_a - (i as i32 * 8)).max(0);
        if nbits < 8 {
            // C: `(0xFF << (8 - nbits))` computed in int, truncated to uchar.
            let mask = (0xFFu16 << (8 - nbits)) as u8;
            a[i] &= mask;
        }
        let nbits = (bits_b - (i as i32 * 8)).max(0);
        if nbits < 8 {
            let mask = (0xFFu16 << (8 - nbits)) as u8;
            b[i] &= mask;
        }
    }

    // difference, from high byte to low
    let mut delta: f64 = 0.0;
    for i in (0..len).rev() {
        delta += (b[i] as f64) - (a[i] as f64);
        delta /= 256.0;
    }

    debug_assert!((0.0..=1.0).contains(&delta));
    delta
}

/// `brin_minmax_multi_distance_numeric` (brin_minmax_multi.c:2021): `a2 - a1` as
/// float8, via the numeric unit's `numeric_subdiff_bytes` seam. `a1`/`a2` are
/// the on-disk `numeric` varlena images.
pub fn dist_numeric(a1: &[u8], a2: &[u8]) -> f64 {
    // C: DatumGetFloat8(numeric_float8(numeric_sub(a2, a1))) == subdiff(a2, a1).
    // The seam can ereport (overflow); the distance functions are infallible at
    // the fmgr-result level, so propagate the panic the boundary wrapper turns
    // back into a PgError (matching C's ereport unwinding out of the PGFunction).
    match numeric::numeric_subdiff_bytes::call(a2, a1) {
        Ok(v) => v,
        Err(e) => panic!("brin_minmax_multi_distance_numeric: {e:?}"),
    }
}

// ---------------------------------------------------------------------------
// fcinfo argument marshaling helpers.
// ---------------------------------------------------------------------------

/// Read by-value argument `i` as the raw machine word.
#[inline]
fn arg_word(fcinfo: &FunctionCallInfoBaseData, i: usize) -> usize {
    fcinfo.arg(i).expect("distance fn: missing argument").value.as_usize()
}

/// Read by-reference argument `i`'s byte image off the fmgr by-reference lane.
#[inline]
fn arg_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("distance fn: by-reference argument missing from the by-ref lane")
}

/// Box a `float8` result into the fmgr-ABI result word (`PG_RETURN_FLOAT8`).
#[inline]
fn ret_float8(v: f64) -> WordDatum {
    WordDatum::from_usize(v.to_bits() as usize)
}

/// `VARDATA_ANY` offset of a detoasted 4-byte-header varlena (the codec only
/// produces / detoasts to 4-byte headers).
const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// fcinfo wrappers (PG_FUNCTION_ARGS -> PG_RETURN_FLOAT8).
// ---------------------------------------------------------------------------

fn fc_float4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a1 = f32::from_bits(arg_word(fcinfo, 0) as u32);
    let a2 = f32::from_bits(arg_word(fcinfo, 1) as u32);
    Ok(ret_float8(dist_float4(a1, a2)))
}

fn fc_float8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a1 = f64::from_bits(arg_word(fcinfo, 0) as u64);
    let a2 = f64::from_bits(arg_word(fcinfo, 1) as u64);
    Ok(ret_float8(dist_float8(a1, a2)))
}

fn fc_int2(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_int2(arg_word(fcinfo, 0) as i16, arg_word(fcinfo, 1) as i16)))
}

fn fc_int4(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_int4(arg_word(fcinfo, 0) as i32, arg_word(fcinfo, 1) as i32)))
}

fn fc_int8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_int8(arg_word(fcinfo, 0) as i64, arg_word(fcinfo, 1) as i64)))
}

fn fc_date(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_date(arg_word(fcinfo, 0) as i32, arg_word(fcinfo, 1) as i32)))
}

fn fc_time(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_time(arg_word(fcinfo, 0) as i64, arg_word(fcinfo, 1) as i64)))
}

fn fc_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_timestamp(arg_word(fcinfo, 0) as i64, arg_word(fcinfo, 1) as i64)))
}

fn fc_pg_lsn(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    Ok(ret_float8(dist_pg_lsn(arg_word(fcinfo, 0) as u64, arg_word(fcinfo, 1) as u64)))
}

fn fc_tid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a = parse_tid(arg_bytes(fcinfo, 0));
    let b = parse_tid(arg_bytes(fcinfo, 1));
    Ok(ret_float8(dist_tid(a, b)))
}

fn fc_uuid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let mut u1 = [0u8; 16];
    let mut u2 = [0u8; 16];
    u1.copy_from_slice(&arg_bytes(fcinfo, 0)[..16]);
    u2.copy_from_slice(&arg_bytes(fcinfo, 1)[..16]);
    Ok(ret_float8(dist_uuid(&u1, &u2)))
}

fn fc_macaddr(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let mut a = [0u8; 6];
    let mut b = [0u8; 6];
    a.copy_from_slice(&arg_bytes(fcinfo, 0)[..6]);
    b.copy_from_slice(&arg_bytes(fcinfo, 1)[..6]);
    Ok(ret_float8(dist_macaddr(&a, &b)))
}

fn fc_macaddr8(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    a.copy_from_slice(&arg_bytes(fcinfo, 0)[..8]);
    b.copy_from_slice(&arg_bytes(fcinfo, 1)[..8]);
    Ok(ret_float8(dist_macaddr8(&a, &b)))
}

fn fc_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a = parse_timetz(arg_bytes(fcinfo, 0));
    let b = parse_timetz(arg_bytes(fcinfo, 1));
    Ok(ret_float8(dist_timetz(a, b)))
}

fn fc_interval(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a = parse_interval(arg_bytes(fcinfo, 0));
    let b = parse_interval(arg_bytes(fcinfo, 1));
    Ok(ret_float8(dist_interval(a, b)))
}

fn fc_numeric(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let a1 = arg_bytes(fcinfo, 0);
    let a2 = arg_bytes(fcinfo, 1);
    Ok(ret_float8(dist_numeric(a1, a2)))
}

fn fc_inet(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<WordDatum> {
    let ia = parse_inet(arg_bytes(fcinfo, 0));
    let ib = parse_inet(arg_bytes(fcinfo, 1));
    Ok(ret_float8(dist_inet(ia.0, &ia.1, ia.2, ib.0, &ib.1, ib.2)))
}

// ---------------------------------------------------------------------------
// by-reference struct parsers (off the on-disk image).
// ---------------------------------------------------------------------------

/// `ItemPointerData` (6 bytes): `ip_blkid` { bi_hi: u16, bi_lo: u16 }, `ip_posid:
/// u16`. Returns `(blocknumber, offset)` per the No-Check accessors.
fn parse_tid(b: &[u8]) -> (u32, u16) {
    let bi_hi = u16::from_ne_bytes([b[0], b[1]]);
    let bi_lo = u16::from_ne_bytes([b[2], b[3]]);
    let ip_posid = u16::from_ne_bytes([b[4], b[5]]);
    let block = ((bi_hi as u32) << 16) | (bi_lo as u32);
    (block, ip_posid)
}

/// `TimeTzADT` (12 bytes): `time: TimeADT (i64)`, `zone: int32`.
fn parse_timetz(b: &[u8]) -> (i64, i32) {
    let time = i64::from_ne_bytes(b[0..8].try_into().unwrap());
    let zone = i32::from_ne_bytes(b[8..12].try_into().unwrap());
    (time, zone)
}

/// `Interval` (16 bytes): `time: TimeOffset (i64)`, `day: int32`, `month: int32`.
fn parse_interval(b: &[u8]) -> (i64, i32, i32) {
    let time = i64::from_ne_bytes(b[0..8].try_into().unwrap());
    let day = i32::from_ne_bytes(b[8..12].try_into().unwrap());
    let month = i32::from_ne_bytes(b[12..16].try_into().unwrap());
    (time, day, month)
}

/// `VARDATA_ANY` byte offset of an inline (non-compressed, non-external) varlena
/// image: ONE header byte for a short (1-byte, low-bit-set, non-1-byte-toast)
/// header, else `VARHDRSZ`. C's `brin_minmax_multi_distance_inet` reaches its
/// args via `PG_GETARG_INET_PP` (the packed/`VARDATA_ANY`-aware accessor) because
/// a stored BRIN inet bound can carry a short header once `SHORT_VARLENA_PACKING`
/// is on; a fixed 4-byte strip would read 3 bytes into the inet_struct. No-op
/// while the flag is off (no stored value is short-headed).
#[inline]
fn vardata_any_off(b: &[u8]) -> usize {
    match b.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => 1,
        _ => VARHDRSZ,
    }
}

/// `inet` varlena: varlena header (4-byte OR 1-byte short) then `inet_struct
/// { family: u8, bits: u8, ipaddr: [u8;16] }`. Returns `(family,
/// addr[ip_addrsize], bits)`. The struct offset is computed from the actual
/// header form so a short-packed stored inet is read correctly.
fn parse_inet(b: &[u8]) -> (u8, alloc::vec::Vec<u8>, i32) {
    let off = vardata_any_off(b);
    let family = b[off];
    let bits = b[off + 1] as i32;
    let addrsize = if family == PGSQL_AF_INET { 4 } else { 16 };
    let addr = b[off + 2..off + 2 + addrsize].to_vec();
    (family, addr, bits)
}

// ---------------------------------------------------------------------------
// builtin registration (C: fmgr_builtins[] entries).
// ---------------------------------------------------------------------------

/// pg_proc.dat OIDs of the distance functions.
const F_BRIN_MINMAX_MULTI_DISTANCE_INT2: u32 = 4621;
const F_BRIN_MINMAX_MULTI_DISTANCE_INT4: u32 = 4622;
const F_BRIN_MINMAX_MULTI_DISTANCE_INT8: u32 = 4623;
const F_BRIN_MINMAX_MULTI_DISTANCE_FLOAT4: u32 = 4624;
const F_BRIN_MINMAX_MULTI_DISTANCE_FLOAT8: u32 = 4625;
const F_BRIN_MINMAX_MULTI_DISTANCE_NUMERIC: u32 = 4626;
const F_BRIN_MINMAX_MULTI_DISTANCE_TID: u32 = 4627;
const F_BRIN_MINMAX_MULTI_DISTANCE_UUID: u32 = 4628;
const F_BRIN_MINMAX_MULTI_DISTANCE_DATE: u32 = 4629;
const F_BRIN_MINMAX_MULTI_DISTANCE_TIME: u32 = 4630;
const F_BRIN_MINMAX_MULTI_DISTANCE_INTERVAL: u32 = 4631;
const F_BRIN_MINMAX_MULTI_DISTANCE_TIMETZ: u32 = 4632;
const F_BRIN_MINMAX_MULTI_DISTANCE_PG_LSN: u32 = 4633;
const F_BRIN_MINMAX_MULTI_DISTANCE_MACADDR: u32 = 4634;
const F_BRIN_MINMAX_MULTI_DISTANCE_MACADDR8: u32 = 4635;
const F_BRIN_MINMAX_MULTI_DISTANCE_INET: u32 = 4636;
const F_BRIN_MINMAX_MULTI_DISTANCE_TIMESTAMP: u32 = 4637;

fn builtin(
    foid: u32,
    name: &str,
    func: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: alloc::string::String::from(name),
            nargs: 2,
            strict: true,
            retset: false,
            func: None,
        },
        func,
    )
}

/// Register the 17 minmax-multi distance functions as fmgr builtins, so the
/// compaction `function_call2_coll(distance_oid, ...)` dispatch resolves them
/// (C: their `fmgr_builtins[]` rows). Called from the BRIN dispatch installer's
/// `init_seams()`.
pub fn register_distance_builtins() {
    fmgr_core::register_builtins_native([
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_INT2, "brin_minmax_multi_distance_int2", fc_int2),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_INT4, "brin_minmax_multi_distance_int4", fc_int4),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_INT8, "brin_minmax_multi_distance_int8", fc_int8),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_FLOAT4, "brin_minmax_multi_distance_float4", fc_float4),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_FLOAT8, "brin_minmax_multi_distance_float8", fc_float8),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_NUMERIC, "brin_minmax_multi_distance_numeric", fc_numeric),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_TID, "brin_minmax_multi_distance_tid", fc_tid),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_UUID, "brin_minmax_multi_distance_uuid", fc_uuid),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_DATE, "brin_minmax_multi_distance_date", fc_date),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_TIME, "brin_minmax_multi_distance_time", fc_time),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_INTERVAL, "brin_minmax_multi_distance_interval", fc_interval),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_TIMETZ, "brin_minmax_multi_distance_timetz", fc_timetz),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_PG_LSN, "brin_minmax_multi_distance_pg_lsn", fc_pg_lsn),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_MACADDR, "brin_minmax_multi_distance_macaddr", fc_macaddr),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_MACADDR8, "brin_minmax_multi_distance_macaddr8", fc_macaddr8),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_INET, "brin_minmax_multi_distance_inet", fc_inet),
        builtin(F_BRIN_MINMAX_MULTI_DISTANCE_TIMESTAMP, "brin_minmax_multi_distance_timestamp", fc_timestamp),
    ]);
}
