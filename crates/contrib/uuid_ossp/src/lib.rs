//! `contrib/uuid-ossp/uuid-ossp.c` — `uuid_generate_*` + namespace constants,
//! on the HAVE_UUID_E2FS (libuuid) build path. The `uuid` type is core
//! (utils/adt/uuid.c); this module only adds generators.
//!
//! v1 reproduces libuuid `uuid_generate_time` semantics: strictly monotonic
//! (timestamp, clock_seq) per session, node = cached random multicast node
//! (no hardware MAC is ever read, matching libuuid's random-node fallback).

use std::cell::Cell;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_EXTERNAL_ROUTINE_EXCEPTION};
use types_fmgr::{
    byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction, UUID_LEN,
};
use adt_uuid::PgUuid;

const LIBRARY: &str = "uuid-ossp";

fn parse_const(s: &str) -> PgUuid {
    let mut data = [0u8; UUID_LEN];
    let mut idx = 0;
    let mut hi: Option<u8> = None;
    for c in s.bytes() {
        if c == b'-' {
            continue;
        }
        let nib = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => unreachable!("constant uuid strings are canonical"),
        };
        match hi.take() {
            None => hi = Some(nib),
            Some(h) => {
                data[idx] = (h << 4) | nib;
                idx += 1;
            }
        }
    }
    data
}

// UUID_V3_OR_V5 on the E2FS path: the time_low/mid/hi byteswaps cancel; the
// version/variant patch lands on bytes 6/8 of the big-endian image.
fn uuid_name_based(version: u8, ns: &PgUuid, name: &[u8]) -> PgUuid {
    let mut data = [0u8; UUID_LEN];
    if version == 3 {
        let mut buf = Vec::with_capacity(UUID_LEN + name.len());
        buf.extend_from_slice(ns);
        buf.extend_from_slice(name);
        data.copy_from_slice(&pg_md5::pg_md5_binary(&buf));
    } else {
        let mut ctx = pg_sha1::Sha1::init();
        ctx.update(ns);
        ctx.update(name);
        data.copy_from_slice(&ctx.finish()[..UUID_LEN]);
    }
    data[6] = (data[6] & 0x0F) | (version << 4);
    data[8] = (data[8] & 0x3F) | 0x80;
    data
}

#[cold]
#[inline(never)]
fn no_random_err() -> PgError {
    PgError::error("could not generate random values")
        .with_sqlstate(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION)
}

fn fill_random(buf: &mut [u8]) -> PgResult<()> {
    if pg_strong_random::pg_strong_random(buf) {
        Ok(())
    } else {
        Err(no_random_err().into())
    }
}

fn uuid_v4() -> PgResult<PgUuid> {
    let mut data = [0u8; UUID_LEN];
    fill_random(&mut data)?;
    data[6] = (data[6] & 0x0F) | 0x40;
    data[8] = (data[8] & 0x3F) | 0x80;
    Ok(data)
}

#[derive(Clone, Copy)]
struct V1State {
    // 60-bit timestamp (100ns ticks since 1582-10-15), last value emitted.
    last_ts: u64,
    clock_seq: u16,
    node: [u8; 6],
}

std::thread_local! {
    static V1_STATE: Cell<Option<V1State>> = const { Cell::new(None) };
}

// 100ns ticks between the Gregorian UUID epoch (1582-10-15) and the Unix epoch.
const GREGORIAN_TO_UNIX_100NS: u64 = 0x01B2_1DD2_1381_4000;

fn now_uuid_ticks() -> u64 {
    // DST P2 (contract §1.2): SystemTime -> pg_clock::wall_ns() (pre-epoch
    // clamps to 0, as duration_since().unwrap_or_default() did).
    pg_clock::wall_ns().max(0) as u64 / 100 + GREGORIAN_TO_UNIX_100NS
}

fn v1_state() -> PgResult<V1State> {
    if let Some(s) = V1_STATE.with(Cell::get) {
        return Ok(s);
    }
    let mut seed = [0u8; 8];
    fill_random(&mut seed)?;
    let clock_seq = u16::from_be_bytes([seed[0], seed[1]]) & 0x3FFF;
    let mut node = [seed[2], seed[3], seed[4], seed[5], seed[6], seed[7]];
    // libuuid sets the multicast bit on a generated random node.
    node[0] |= 0x01;
    let s = V1State { last_ts: 0, clock_seq, node };
    V1_STATE.with(|c| c.set(Some(s)));
    Ok(s)
}

fn v1_next() -> PgResult<(u64, u16)> {
    let mut s = v1_state()?;
    let mut ts = now_uuid_ticks();
    if ts <= s.last_ts {
        ts = s.last_ts + 1;
    }
    s.last_ts = ts;
    let clock_seq = s.clock_seq;
    V1_STATE.with(|c| c.set(Some(s)));
    Ok((ts, clock_seq))
}

fn assemble_v1(ts: u64, clock_seq: u16, node: [u8; 6]) -> PgUuid {
    let mut data = [0u8; UUID_LEN];
    data[0..4].copy_from_slice(&((ts & 0xFFFF_FFFF) as u32).to_be_bytes());
    data[4..6].copy_from_slice(&(((ts >> 32) & 0xFFFF) as u16).to_be_bytes());
    data[6..8].copy_from_slice(&((((ts >> 48) & 0x0FFF) as u16) | (1 << 12)).to_be_bytes());
    data[8] = (((clock_seq >> 8) & 0x3F) as u8) | 0x80;
    data[9] = clock_seq as u8;
    data[10..16].copy_from_slice(&node);
    data
}

fn uuid_v1() -> PgResult<PgUuid> {
    let (ts, clock_seq) = v1_next()?;
    Ok(assemble_v1(ts, clock_seq, v1_state()?.node))
}

fn uuid_v1mc() -> PgResult<PgUuid> {
    let (ts, clock_seq) = v1_next()?;
    let mut node = [0u8; 6];
    fill_random(&mut node)?;
    // IEEE802 multicast + local-admin bits (C: node[0] |= 0x03).
    node[0] |= 0x03;
    Ok(assemble_v1(ts, clock_seq, node))
}

fn ret_uuid(fcinfo: &mut Fcinfo, u: PgUuid) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &u)
}

macro_rules! fc_const_uuid {
    ($($fname:ident: $lit:literal;)*) => {$(
        fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            ret_uuid(fcinfo, parse_const($lit))
        }
    )*};
}

fc_const_uuid! {
    fc_uuid_nil: "00000000-0000-0000-0000-000000000000";
    fc_uuid_ns_dns: "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    fc_uuid_ns_url: "6ba7b811-9dad-11d1-80b4-00c04fd430c8";
    fc_uuid_ns_oid: "6ba7b812-9dad-11d1-80b4-00c04fd430c8";
    fc_uuid_ns_x500: "6ba7b814-9dad-11d1-80b4-00c04fd430c8";
}

fn fc_uuid_generate_v1(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let u = uuid_v1()?;
    ret_uuid(fcinfo, u)
}

fn fc_uuid_generate_v1mc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let u = uuid_v1mc()?;
    ret_uuid(fcinfo, u)
}

fn fc_uuid_generate_v4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let u = uuid_v4()?;
    ret_uuid(fcinfo, u)
}

fn name_based(fcinfo: &mut Fcinfo, version: u8) -> PgResult<Datum> {
    // SAFETY: catalog args of these strict fns are a non-null 16-byte uuid
    // block and a non-null text varlena.
    let ns = *unsafe { fcinfo.arg_uuid(0) };
    let name = unsafe { fcinfo.arg_varlena_packed(1)? };
    let u = uuid_name_based(version, &ns, name.data());
    ret_uuid(fcinfo, u)
}

fn fc_uuid_generate_v3(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    name_based(fcinfo, 3)
}

fn fc_uuid_generate_v5(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    name_based(fcinfo, 5)
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "uuid_nil" => fc_uuid_nil,
        "uuid_ns_dns" => fc_uuid_ns_dns,
        "uuid_ns_url" => fc_uuid_ns_url,
        "uuid_ns_oid" => fc_uuid_ns_oid,
        "uuid_ns_x500" => fc_uuid_ns_x500,
        "uuid_generate_v1" => fc_uuid_generate_v1,
        "uuid_generate_v1mc" => fc_uuid_generate_v1mc,
        "uuid_generate_v3" => fc_uuid_generate_v3,
        "uuid_generate_v4" => fc_uuid_generate_v4,
        "uuid_generate_v5" => fc_uuid_generate_v5,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // uuid-ossp.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(u: &PgUuid) -> String {
        u.iter().map(|b| format!("{b:02x}")).collect()
    }

    // C oracle: SELECT uuid_generate_v3(uuid_ns_dns(), 'www.widgets.com').
    #[test]
    fn v3_matches_c() {
        let ns = parse_const("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        let u = uuid_name_based(3, &ns, b"www.widgets.com");
        assert_eq!(hex(&u), "3d813cbb47fb32ba91df831e1593ac29");
    }

    #[test]
    fn v5_matches_c() {
        let ns = parse_const("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        let u = uuid_name_based(5, &ns, b"www.widgets.com");
        assert_eq!(hex(&u), "21f7f8de80515b8986800195ef798b6a");
    }

    #[test]
    fn constants() {
        assert_eq!(
            hex(&parse_const("00000000-0000-0000-0000-000000000000")),
            "00000000000000000000000000000000"
        );
        assert_eq!(
            hex(&parse_const("6ba7b810-9dad-11d1-80b4-00c04fd430c8")),
            "6ba7b8109dad11d180b400c04fd430c8"
        );
    }

    #[test]
    fn v4_version_and_variant() {
        let u = uuid_v4().unwrap();
        assert_eq!(u[6] & 0xF0, 0x40);
        assert_eq!(u[8] & 0xC0, 0x80);
    }

    #[test]
    fn v1_version_variant_monotonic() {
        let a = uuid_v1().unwrap();
        let b = uuid_v1().unwrap();
        assert_eq!(a[6] & 0xF0, 0x10);
        assert_eq!(a[8] & 0xC0, 0x80);
        let ts = |u: &PgUuid| -> u128 {
            let hi = ((u[6] as u128 & 0x0F) << 8) | u[7] as u128;
            let mid = ((u[4] as u128) << 8) | u[5] as u128;
            let low = u32::from_be_bytes(u[0..4].try_into().unwrap()) as u128;
            (hi << 48) | (mid << 32) | low
        };
        assert!(ts(&a) < ts(&b));
    }

    #[test]
    fn v1mc_node_bits() {
        let u = uuid_v1mc().unwrap();
        assert_eq!(u[10] & 0x03, 0x03);
    }
}
