//! uuid.c value cores. uuid_sortsupport/uuid_skipsupport stay unregistered
//! until the SortSupport/SkipSupport node frame lands (macaddr_sortsupport
//! precedent).

pub mod abbrev;
pub mod builtins;
#[cfg(test)]
mod tests;

use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_INVALID_TEXT_REPRESENTATION,
};
pub use types_fmgr::UUID_LEN;

use adt_datetime::{POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC};

pub type PgUuid = [u8; UUID_LEN];

pub const UUID_OUT_LEN: usize = 2 * UUID_LEN + 4;

const US_PER_MS: i64 = 1_000;
const NS_PER_MS: i64 = 1_000_000;
const NS_PER_US: i64 = 1_000;
const GREGORIAN_EPOCH_JDATE: i64 = 2_299_161;

// upstream c31b0fca059c (18.6): Reject infinite and out-of-range interval shifts in uuidv7().
/// The offset between the PostgreSQL epoch (2000-01-01) and the Unix epoch
/// (1970-01-01) in microseconds. Subtract this from Unix-epoch microseconds
/// to get a TimestampTz.
const PG_UNIX_EPOCH_OFFSET_US: i64 =
    (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64) * SECS_PER_DAY as i64 * USECS_PER_SEC;
/// Valid timestamp range for UUID version 7, in PostgreSQL-epoch microseconds.
/// UUIDv7 uses a 48-bit unsigned millisecond field relative to the Unix
/// epoch, so the representable window is [1970-01-01, ~10889].
const UUIDV7_MIN_TIMESTAMP: i64 = -PG_UNIX_EPOCH_OFFSET_US;
const UUIDV7_MAX_TIMESTAMP: i64 = ((1i64 << 48) - 1) * US_PER_MS - PG_UNIX_EPOCH_OFFSET_US;

// C: 10 sub-ms precision bits on __darwin__/_MSC_VER (µs clocks), 12 elsewhere.
#[cfg(target_os = "macos")]
const SUBMS_MINIMAL_STEP_BITS: i64 = 10;
#[cfg(not(target_os = "macos"))]
const SUBMS_MINIMAL_STEP_BITS: i64 = 12;
const SUBMS_BITS: u32 = 12;
const SUBMS_MINIMAL_STEP_NS: i64 = (NS_PER_MS / (1 << SUBMS_MINIMAL_STEP_BITS)) + 1;

#[inline]
fn uuid_set_version(uuid: &mut PgUuid, version: u8) {
    uuid[6] = (uuid[6] & 0x0f) | (version << 4);
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
}

#[cold]
#[inline(never)]
fn no_random_err() -> PgError {
    PgError::error("could not generate random values")
}

pub fn gen_random_uuid() -> PgResult<PgUuid> {
    let mut uuid = [0u8; UUID_LEN];
    if !pg_strong_random::pg_strong_random(&mut uuid) {
        return Err(no_random_err().into());
    }
    uuid_set_version(&mut uuid, 4);
    Ok(uuid)
}

std::thread_local! {
    // C's static previous_ns in get_real_time_ns_ascending (backend-private).
    static PREVIOUS_NS: core::cell::Cell<i64> = const { core::cell::Cell::new(0) };
}

fn get_real_time_ns_ascending() -> i64 {
    // DST P2 (contract §1.2): clock_gettime -> pg_clock::wall_ns(); the
    // ascending-guard TLS below is unchanged — under sim, correctness rides
    // the wall = base + mono coupling law (§0.3).
    let mut ns = pg_clock::wall_ns();
    let previous_ns = PREVIOUS_NS.get();
    if previous_ns + SUBMS_MINIMAL_STEP_NS >= ns {
        ns = previous_ns + SUBMS_MINIMAL_STEP_NS;
    }
    PREVIOUS_NS.set(ns);
    ns
}

pub fn generate_uuidv7(unix_ts_ms: u64, sub_ms: u32) -> PgResult<PgUuid> {
    let mut uuid = [0u8; UUID_LEN];
    uuid[0] = (unix_ts_ms >> 40) as u8;
    uuid[1] = (unix_ts_ms >> 32) as u8;
    uuid[2] = (unix_ts_ms >> 24) as u8;
    uuid[3] = (unix_ts_ms >> 16) as u8;
    uuid[4] = (unix_ts_ms >> 8) as u8;
    uuid[5] = unix_ts_ms as u8;

    let increased_clock_precision = sub_ms.wrapping_mul(1 << SUBMS_BITS) / NS_PER_MS as u32;
    uuid[6] = (increased_clock_precision >> 8) as u8;
    uuid[7] = increased_clock_precision as u8;

    if !pg_strong_random::pg_strong_random(&mut uuid[8..]) {
        return Err(no_random_err().into());
    }

    if SUBMS_MINIMAL_STEP_BITS == 10 {
        // Lowest 2 sub-ms bits carry no entropy on µs clocks; randomize them
        // (SUBMS_MINIMAL_STEP still guarantees monotonicity).
        uuid[7] ^= uuid[8] >> 6;
    }

    uuid_set_version(&mut uuid, 7);
    Ok(uuid)
}

pub fn uuidv7() -> PgResult<PgUuid> {
    let ns = get_real_time_ns_ascending();
    generate_uuidv7((ns / NS_PER_MS) as u64, (ns % NS_PER_MS) as u32)
}

pub fn uuidv7_interval(shift: &adt_datetime::Interval) -> PgResult<PgUuid> {
    let ns = get_real_time_ns_ascending();

    // upstream c31b0fca059c (18.6): Reject infinite and out-of-range interval shifts in uuidv7().
    // Reject infinite intervals before any arithmetic (18.3 wrapped the epoch
    // re-base under -fwrapv and produced a garbage UUID).
    if shift.not_finite() {
        return Err(uuidv7_infinite_interval_err().into());
    }

    // Shift the current timestamp by the given interval: convert the Unix
    // epoch to TimestampTz and use timestamptz_pl_interval() so sub-second
    // precision is kept.
    let ts: TimestampTz = ns / NS_PER_US - PG_UNIX_EPOCH_OFFSET_US;
    let ts = adt_timestamp::interval::timestamptz_pl_interval_internal(ts, shift, None)?;

    // Reject timestamps outside the range representable by UUID version 7's
    // 48-bit millisecond field. Compared in PostgreSQL-epoch units so that
    // the conversion back to Unix-epoch microseconds cannot overflow.
    if ts < UUIDV7_MIN_TIMESTAMP || ts > UUIDV7_MAX_TIMESTAMP {
        return Err(uuidv7_timestamp_out_of_range_err().into());
    }

    // Convert the TimestampTz value to a Unix-epoch timestamp in usec.
    let us = ts + PG_UNIX_EPOCH_OFFSET_US;

    generate_uuidv7(
        (us / US_PER_MS) as u64,
        ((us % US_PER_MS) * NS_PER_US + ns % NS_PER_US) as u32,
    )
}

// upstream c31b0fca059c (18.6): Reject infinite and out-of-range interval shifts in uuidv7().
#[cold]
#[inline(never)]
fn uuidv7_infinite_interval_err() -> PgError {
    PgError::error("interval out of range for UUID version 7")
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
        .with_detail("UUID version 7 does not support infinite intervals.")
}

#[cold]
#[inline(never)]
fn uuidv7_timestamp_out_of_range_err() -> PgError {
    PgError::error("timestamp out of range for UUID version 7")
        .with_sqlstate(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
        .with_detail(
            "UUID version 7 supports timestamps from 1970-01-01 to approximately year 10889.",
        )
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &[u8]) -> PgError {
    let s = String::from_utf8_lossy(input);
    PgError::error(format!("invalid input syntax for type uuid: \"{s}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

pub fn uuid_in(source: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<PgUuid> {
    let mut uuid = [0u8; UUID_LEN];
    let mut src = source;
    let mut braces = false;

    if src.first() == Some(&b'{') {
        src = &src[1..];
        braces = true;
    }

    for i in 0..UUID_LEN {
        let (hi, lo) = match (src.first().copied(), src.get(1).copied()) {
            (Some(a), Some(b)) => match (hex_nibble(a), hex_nibble(b)) {
                (Some(hi), Some(lo)) => (hi, lo),
                _ => return ereturn(escontext, uuid, invalid_syntax_err(source)),
            },
            _ => return ereturn(escontext, uuid, invalid_syntax_err(source)),
        };
        uuid[i] = (hi << 4) | lo;
        src = &src[2..];
        if src.first() == Some(&b'-') && (i % 2) == 1 && i < UUID_LEN - 1 {
            src = &src[1..];
        }
    }

    if braces {
        if src.first() != Some(&b'}') {
            return ereturn(escontext, uuid, invalid_syntax_err(source));
        }
        src = &src[1..];
    }

    if !src.is_empty() {
        return ereturn(escontext, uuid, invalid_syntax_err(source));
    }

    Ok(uuid)
}

pub fn uuid_out_into(uuid: &PgUuid, buf: &mut [u8; UUID_OUT_LEN]) -> usize {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    // Const-offset group writes: the C loop's data-dependent cursor defeats
    // bounds-check elision here (uuid_out lane measured 1.44x ns).
    #[inline(always)]
    fn put<const N: usize>(dst: &mut [u8], src: &[u8]) {
        let dst: &mut [u8; N] = dst.try_into().unwrap();
        for i in 0..N / 2 {
            dst[2 * i] = HEX_CHARS[(src[i] >> 4) as usize];
            dst[2 * i + 1] = HEX_CHARS[(src[i] & 0x0F) as usize];
        }
    }
    put::<8>(&mut buf[0..8], &uuid[0..4]);
    buf[8] = b'-';
    put::<4>(&mut buf[9..13], &uuid[4..6]);
    buf[13] = b'-';
    put::<4>(&mut buf[14..18], &uuid[6..8]);
    buf[18] = b'-';
    put::<4>(&mut buf[19..23], &uuid[8..10]);
    buf[23] = b'-';
    put::<12>(&mut buf[24..36], &uuid[10..16]);
    UUID_OUT_LEN
}

#[inline]
pub fn uuid_internal_cmp(a: &PgUuid, b: &PgUuid) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

pub fn uuid_lt(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) < 0
}

pub fn uuid_le(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) <= 0
}

pub fn uuid_eq(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) == 0
}

pub fn uuid_ge(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) >= 0
}

pub fn uuid_gt(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) > 0
}

pub fn uuid_ne(a: &PgUuid, b: &PgUuid) -> bool {
    uuid_internal_cmp(a, b) != 0
}

pub fn uuid_hash(key: &PgUuid) -> u32 {
    hashfn::hash_bytes(key)
}

pub fn uuid_hash_extended(key: &PgUuid, seed: u64) -> u64 {
    hashfn::hash_bytes_extended(key, seed)
}

pub fn uuid_recv(buf: &mut StringInfo<'_>) -> PgResult<PgUuid> {
    let bytes = pqformat::pq_getmsgbytes(buf, UUID_LEN)?;
    let mut uuid = [0u8; UUID_LEN];
    uuid.copy_from_slice(bytes);
    Ok(uuid)
}

pub fn uuid_send<'mcx>(mcx: Mcx<'mcx>, uuid: &PgUuid) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendbytes(&mut b, uuid)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub fn uuid_extract_timestamp(uuid: &PgUuid) -> Option<TimestampTz> {
    if (uuid[8] & 0xc0) != 0x80 {
        return None;
    }

    let version = uuid[6] >> 4;

    if version == 1 {
        let tms = ((uuid[0] as u64) << 24)
            + ((uuid[1] as u64) << 16)
            + ((uuid[2] as u64) << 8)
            + (uuid[3] as u64)
            + ((uuid[4] as u64) << 40)
            + ((uuid[5] as u64) << 32)
            + (((uuid[6] as u64) & 0xf) << 56)
            + ((uuid[7] as u64) << 48);

        let ts = (tms / 10) as i64
            - (POSTGRES_EPOCH_JDATE as i64 - GREGORIAN_EPOCH_JDATE)
                * SECS_PER_DAY as i64
                * USECS_PER_SEC;
        return Some(ts);
    }

    if version == 7 {
        let tms = (uuid[5] as u64)
            + ((uuid[4] as u64) << 8)
            + ((uuid[3] as u64) << 16)
            + ((uuid[2] as u64) << 24)
            + ((uuid[1] as u64) << 32)
            + ((uuid[0] as u64) << 40);

        let ts = (tms as i64) * US_PER_MS
            - (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
                * SECS_PER_DAY as i64
                * USECS_PER_SEC;
        return Some(ts);
    }

    None
}

pub fn uuid_extract_version(uuid: &PgUuid) -> Option<u16> {
    if (uuid[8] & 0xc0) != 0x80 {
        return None;
    }
    Some((uuid[6] >> 4) as u16)
}
