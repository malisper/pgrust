//! uuid.c value cores. gen_random_uuid/uuidv7/uuidv7_interval are registered
//! loud (pg_strong_random unexported from tcop; timestamptz_pl_interval in a
//! concurrent lane); uuid_sortsupport/uuid_skipsupport stay unregistered until
//! the SortSupport/SkipSupport node frame lands (macaddr_sortsupport precedent).

pub mod builtins;
#[cfg(test)]
mod tests;

use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_core::TimestampTz;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
pub use types_fmgr::UUID_LEN;

use adt_datetime::{POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC};

pub type PgUuid = [u8; UUID_LEN];

pub const UUID_OUT_LEN: usize = 2 * UUID_LEN + 4;

const US_PER_MS: i64 = 1_000;
const GREGORIAN_EPOCH_JDATE: i64 = 2_299_161;

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
    let mut p = 0;
    for (i, byte) in uuid.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf[p] = b'-';
            p += 1;
        }
        buf[p] = HEX_CHARS[(byte >> 4) as usize];
        buf[p + 1] = HEX_CHARS[(byte & 0x0F) as usize];
        p += 2;
    }
    p
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
