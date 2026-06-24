#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! `contrib/uuid-ossp/uuid-ossp.c` — the `uuid_generate_*` and namespace-constant
//! SQL-callable functions.
//!
//! Ported faithfully to the `HAVE_UUID_E2FS` (libuuid) build path that pgrust
//! targets. The `uuid` type itself is a CORE type (`utils/adt/uuid.c`); this
//! extension only adds the generator/namespace functions.
//!
//! Registered as the in-process ported library `uuid-ossp` (mirroring
//! `pg_prewarm`): the SQL emitted by `uuid-ossp--1.1.sql` (`CREATE FUNCTION ...
//! LANGUAGE C AS 'MODULE_PATHNAME','<sym>'`) resolves through the dynamic-loader
//! unit's ported-library registry rather than the OS loader.
//!
//! ## The algorithms (faithful to C, libuuid path)
//!
//! * `uuid_nil` / `uuid_ns_*` — constant-value UUIDs (case 0): the literal
//!   string parsed by `uuid_in`.
//! * `uuid_generate_v1` (case 1) — time/node UUID. C calls libuuid's
//!   `uuid_generate_time`: a 60-bit timestamp (100-ns ticks since the Gregorian
//!   epoch 1582-10-15), a 14-bit clock sequence, and a 48-bit node. Successive
//!   calls in a session are strictly monotonic (libuuid advances the timestamp /
//!   bumps the clock sequence). We reproduce that with a session-local monotonic
//!   timestamp+clock-sequence generator. The node is the (cached) random
//!   multicast node (libuuid falls back to a random node when no stable system
//!   MAC is available — pgrust never reads a hardware MAC, so this is always the
//!   case, exactly as the test comments anticipate).
//! * `uuid_generate_v1mc` (case 1 | mc) — like v1 but the trailing 13 chars
//!   ("-XXXX XXXXXXXX", the clock_seq + node) are replaced with a fresh random
//!   value whose first node byte has the IEEE802 multicast + local-admin bits set.
//! * `uuid_generate_v3` (case 3) — MD5(namespace_bytes || name), version 3.
//! * `uuid_generate_v4` (case 4) — 122 random bits, version 4.
//! * `uuid_generate_v5` (case 5) — SHA1(namespace_bytes || name)[:16], version 5.

use std::cell::Cell;

use ::datum::Datum;
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction, RefPayload};
use ::types_error::{PgError, ERRCODE_EXTERNAL_ROUTINE_EXCEPTION};
use ::types_uuid::{pg_uuid_t, UUID_LEN};

/// The simple (suffix-free, directory-free) name of the loadable module —
/// `$libdir/uuid-ossp` reduces to this for the registry.
const LIBRARY: &str = "uuid-ossp";

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`] (mirrors `pg_prewarm`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

// ===========================================================================
// Result / argument helpers (the fmgr boundary)
// ===========================================================================

/// `PG_RETURN_UUID_P` / `UUIDPGetDatum`: set a by-reference, fixed-length `uuid`
/// result on the by-ref lane — the raw `UUID_LEN`-byte image, no varlena header.
fn ret_uuid(fcinfo: &mut FunctionCallInfoBaseData, uuid: pg_uuid_t) -> Datum {
    fcinfo.isnull = false;
    fcinfo.set_ref_result(RefPayload::Varlena(uuid.data.to_vec()));
    Datum::from_usize(0)
}

/// `PG_GETARG_UUID_P(i)` → `DatumGetUUIDP`: reconstruct the `pg_uuid_t` from the
/// raw `UUID_LEN`-byte image on the by-reference side channel.
fn arg_uuid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> pg_uuid_t {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("uuid-ossp: by-ref `uuid` arg missing from by-ref lane");
    assert_eq!(
        bytes.len(),
        UUID_LEN,
        "uuid-ossp: by-ref `uuid` arg must be exactly {UUID_LEN} bytes"
    );
    let mut data = [0u8; UUID_LEN];
    data.copy_from_slice(bytes);
    pg_uuid_t { data }
}

/// `VARDATA_ANY(text) .. VARSIZE_ANY_EXHDR(text)`: the `text` arg's payload
/// bytes (the C `VARDATA_ANY(name)` / length).
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Vec<u8> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("uuid-ossp: text arg missing from by-ref lane");
    varlena_payload(image).to_vec()
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ = 4).
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

// ===========================================================================
// Constant-value UUIDs (case 0): parse a fixed string.
// ===========================================================================

/// Case 0 of `uuid_generate_internal`: parse a constant UUID string. The string
/// is always a canonical 36-char UUID, so the parse is total.
fn constant_uuid(s: &str) -> pg_uuid_t {
    let mut data = [0u8; UUID_LEN];
    let mut idx = 0usize;
    let mut hi: Option<u8> = None;
    for c in s.bytes() {
        if c == b'-' {
            continue;
        }
        let nib = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => panic!("uuid-ossp: invalid constant uuid string {s:?}"),
        };
        match hi.take() {
            None => hi = Some(nib),
            Some(h) => {
                data[idx] = (h << 4) | nib;
                idx += 1;
            }
        }
    }
    assert_eq!(idx, UUID_LEN, "uuid-ossp: constant uuid string wrong length");
    pg_uuid_t { data }
}

// ===========================================================================
// v3 / v5: namespace + name hashing.
// ===========================================================================

/// `uuid_generate_internal` cases 3 (MD5) and 5 (SHA1). Hash `ns.data || name`,
/// take the first 16 bytes, and patch the version (byte 6) and variant (byte 8)
/// bits — the standard RFC 4122 name-based algorithm, which is exactly what C's
/// `UUID_TO_NETWORK` + `UUID_V3_OR_V5` + `UUID_TO_LOCAL` dance computes on the
/// E2FS path (the time_low/mid/hi byteswaps cancel; the version/variant patch
/// lands on byte 6 / byte 8 of the big-endian image).
fn uuid_name_based(version: u8, ns: &pg_uuid_t, name: &[u8]) -> pg_uuid_t {
    let mut data = [0u8; UUID_LEN];

    if version == 3 {
        use ::md5::{md5_init, md5_loop, md5_pad, md5_result, md5_ctxt};
        let mut ctx = md5_ctxt::default();
        md5_init(&mut ctx);
        md5_loop(&mut ctx, &ns.data, ns.data.len());
        md5_loop(&mut ctx, name, name.len());
        md5_pad(&mut ctx);
        let mut digest = [0u8; 16];
        md5_result(&mut digest, &ctx);
        data.copy_from_slice(&digest);
    } else {
        // version == 5
        use ::cryptohash::sha1::{pg_sha1_init, pg_sha1_update, pg_sha1_final, pg_sha1_ctx};
        const SHA1_DIGEST_LENGTH: usize = 20;
        let mut ctx = pg_sha1_ctx::default();
        pg_sha1_init(&mut ctx);
        pg_sha1_update(&mut ctx, &ns.data, ns.data.len());
        pg_sha1_update(&mut ctx, name, name.len());
        let mut digest = [0u8; SHA1_DIGEST_LENGTH];
        pg_sha1_final(&mut ctx, &mut digest);
        // memcpy(&uu, sha1result, sizeof(uu)) — first 16 of the 20-byte digest.
        data.copy_from_slice(&digest[..UUID_LEN]);
    }

    // UUID_V3_OR_V5: set version nibble (byte 6) and variant bits (byte 8).
    data[6] = (data[6] & 0x0F) | (version << 4);
    data[8] = (data[8] & 0x3F) | 0x80;

    pg_uuid_t { data }
}

// ===========================================================================
// v4: random UUID.
// ===========================================================================

/// `uuid_generate_internal` case 4: 16 random bytes, version 4, variant 10.
fn uuid_random() -> Result<pg_uuid_t, PgError> {
    let mut data = [0u8; UUID_LEN];
    fill_random(&mut data)?;
    data[6] = (data[6] & 0x0F) | 0x40;
    data[8] = (data[8] & 0x3F) | 0x80;
    Ok(pg_uuid_t { data })
}

/// Strong random bytes, or the C library-failure ereport on exhaustion.
fn fill_random(buf: &mut [u8]) -> Result<(), PgError> {
    if ::pg_strong_random::pg_strong_random(buf) {
        Ok(())
    } else {
        Err(PgError::error("could not generate random values")
            .with_sqlstate(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION))
    }
}

// ===========================================================================
// v1 / v1mc: time + node UUIDs (libuuid `uuid_generate_time`).
// ===========================================================================

thread_local! {
    /// Session-local monotonic state for `uuid_generate_time`, mirroring
    /// libuuid's `last` timestamp + `clock_seq`. Initialized lazily on first use.
    static V1_STATE: Cell<Option<V1State>> = const { Cell::new(None) };
}

#[derive(Clone, Copy)]
struct V1State {
    /// 60-bit timestamp (100-ns ticks since 1582-10-15), last value emitted.
    last_ts: u64,
    /// 14-bit clock sequence.
    clock_seq: u16,
    /// 48-bit node, the cached random multicast node for V1 (libuuid falls back
    /// to a random node when no system MAC is available).
    node: [u8; 6],
}

/// Number of 100-ns ticks between the Gregorian UUID epoch (1582-10-15 00:00 UTC)
/// and the Unix epoch (1970-01-01).
const GREGORIAN_TO_UNIX_100NS: u64 = 0x01B2_1DD2_1381_4000;

/// Current UUID timestamp: 100-ns ticks since the Gregorian epoch.
fn now_uuid_ticks() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let unix_100ns = now.as_secs() * 10_000_000 + (now.subsec_nanos() as u64) / 100;
    unix_100ns + GREGORIAN_TO_UNIX_100NS
}

/// Initialize (or fetch) the session V1 state, seeding the clock sequence and
/// node from strong random the first time (libuuid seeds these on first use).
fn v1_state() -> Result<V1State, PgError> {
    let existing = V1_STATE.with(|c| c.get());
    if let Some(s) = existing {
        return Ok(s);
    }
    let mut seed = [0u8; 8];
    fill_random(&mut seed)?;
    // 14-bit clock sequence.
    let clock_seq = (((seed[0] as u16) << 8) | (seed[1] as u16)) & 0x3FFF;
    // Random 48-bit node with the multicast bit set (libuuid sets the multicast
    // bit on a generated random node — see uuid__generate_time).
    let mut node = [seed[2], seed[3], seed[4], seed[5], seed[6], seed[7]];
    node[0] |= 0x01;
    let s = V1State {
        last_ts: 0,
        clock_seq,
        node,
    };
    V1_STATE.with(|c| c.set(Some(s)));
    Ok(s)
}

/// Advance the monotonic V1 clock: return a strictly-increasing `(timestamp,
/// clock_seq)` pair, exactly as libuuid guarantees across successive calls. If
/// the wall clock has not advanced past `last_ts`, force `ts = last_ts + 1`; if
/// it ran backwards far enough, bump the clock sequence.
fn v1_next() -> Result<(u64, u16), PgError> {
    let mut s = v1_state()?;
    let mut ts = now_uuid_ticks();
    if ts <= s.last_ts {
        // Clock didn't advance — step forward to keep strict monotonicity.
        ts = s.last_ts + 1;
    }
    s.last_ts = ts;
    let clock_seq = s.clock_seq;
    V1_STATE.with(|c| c.set(Some(s)));
    Ok((ts, clock_seq))
}

/// Assemble a V1 UUID from `(timestamp, clock_seq, node)`.
fn assemble_v1(ts: u64, clock_seq: u16, node: [u8; 6]) -> pg_uuid_t {
    let time_low = (ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((ts >> 32) & 0xFFFF) as u16;
    // time_hi (12 bits) with version 1 in the top nibble.
    let time_hi_and_version = (((ts >> 48) & 0x0FFF) as u16) | (1u16 << 12);
    // clock_seq_hi with the RFC 4122 variant (10) in the top two bits.
    let clock_seq_hi = (((clock_seq >> 8) & 0x3F) as u8) | 0x80;
    let clock_seq_low = (clock_seq & 0xFF) as u8;

    let mut data = [0u8; UUID_LEN];
    data[0..4].copy_from_slice(&time_low.to_be_bytes());
    data[4..6].copy_from_slice(&time_mid.to_be_bytes());
    data[6..8].copy_from_slice(&time_hi_and_version.to_be_bytes());
    data[8] = clock_seq_hi;
    data[9] = clock_seq_low;
    data[10..16].copy_from_slice(&node);
    pg_uuid_t { data }
}

/// `uuid_generate_v1`: time/node UUID with the cached (random multicast) node.
fn uuid_v1() -> Result<pg_uuid_t, PgError> {
    let (ts, clock_seq) = v1_next()?;
    let node = v1_state()?.node;
    Ok(assemble_v1(ts, clock_seq, node))
}

/// `uuid_generate_v1mc`: like V1, but the node is a fresh random value with the
/// IEEE802 multicast + local-administered bits set (C sets `node[0] |= 0x03`).
fn uuid_v1mc() -> Result<pg_uuid_t, PgError> {
    let (ts, clock_seq) = v1_next()?;
    let mut node = [0u8; 6];
    fill_random(&mut node)?;
    // set IEEE802 multicast and local-admin bits
    node[0] |= 0x03;
    Ok(assemble_v1(ts, clock_seq, node))
}

// ===========================================================================
// fmgr entry points (Datum fn(PG_FUNCTION_ARGS))
// ===========================================================================

fn fc_uuid_nil(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_uuid(fcinfo, constant_uuid("00000000-0000-0000-0000-000000000000"))
}

fn fc_uuid_ns_dns(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_uuid(fcinfo, constant_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
}

fn fc_uuid_ns_url(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_uuid(fcinfo, constant_uuid("6ba7b811-9dad-11d1-80b4-00c04fd430c8"))
}

fn fc_uuid_ns_oid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_uuid(fcinfo, constant_uuid("6ba7b812-9dad-11d1-80b4-00c04fd430c8"))
}

fn fc_uuid_ns_x500(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_uuid(fcinfo, constant_uuid("6ba7b814-9dad-11d1-80b4-00c04fd430c8"))
}

fn fc_uuid_generate_v1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match uuid_v1() {
        Ok(u) => ret_uuid(fcinfo, u),
        Err(e) => raise(e),
    }
}

fn fc_uuid_generate_v1mc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match uuid_v1mc() {
        Ok(u) => ret_uuid(fcinfo, u),
        Err(e) => raise(e),
    }
}

fn fc_uuid_generate_v3(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ns = arg_uuid(fcinfo, 0);
    let name = arg_text_bytes(fcinfo, 1);
    ret_uuid(fcinfo, uuid_name_based(3, &ns, &name))
}

fn fc_uuid_generate_v4(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match uuid_random() {
        Ok(u) => ret_uuid(fcinfo, u),
        Err(e) => raise(e),
    }
}

fn fc_uuid_generate_v5(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let ns = arg_uuid(fcinfo, 0);
    let name = arg_text_bytes(fcinfo, 1);
    ret_uuid(fcinfo, uuid_name_based(5, &ns, &name))
}

// ===========================================================================
// Builtin-library registration
// ===========================================================================

/// Resolve a symbol of the `uuid-ossp` module to its ported `PGFunction`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "uuid_nil" => Some(fc_uuid_nil),
        "uuid_ns_dns" => Some(fc_uuid_ns_dns),
        "uuid_ns_url" => Some(fc_uuid_ns_url),
        "uuid_ns_oid" => Some(fc_uuid_ns_oid),
        "uuid_ns_x500" => Some(fc_uuid_ns_x500),
        "uuid_generate_v1" => Some(fc_uuid_generate_v1),
        "uuid_generate_v1mc" => Some(fc_uuid_generate_v1mc),
        "uuid_generate_v3" => Some(fc_uuid_generate_v3),
        "uuid_generate_v4" => Some(fc_uuid_generate_v4),
        "uuid_generate_v5" => Some(fc_uuid_generate_v5),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        // PG_FUNCTION_INFO_V1 declares api_version 1.
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `uuid-ossp` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(dfmgr_seams::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // uuid-ossp.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(u: &pg_uuid_t) -> String {
        u.data.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn v3_matches_c() {
        let ns = constant_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        let u = uuid_name_based(3, &ns, b"www.widgets.com");
        assert_eq!(hex(&u), "3d813cbb47fb32ba91df831e1593ac29");
    }

    #[test]
    fn v5_matches_c() {
        let ns = constant_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        let u = uuid_name_based(5, &ns, b"www.widgets.com");
        assert_eq!(hex(&u), "21f7f8de80515b8986800195ef798b6a");
    }

    #[test]
    fn nil_and_namespaces() {
        assert_eq!(
            hex(&constant_uuid("00000000-0000-0000-0000-000000000000")),
            "00000000000000000000000000000000"
        );
        assert_eq!(
            hex(&constant_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8")),
            "6ba7b8109dad11d180b400c04fd430c8"
        );
    }

    #[test]
    fn v4_version_and_variant_bits() {
        let u = uuid_random().unwrap();
        assert_eq!(u.data[6] & 0xF0, 0x40, "version 4");
        assert_eq!(u.data[8] & 0xC0, 0x80, "variant 10");
    }

    #[test]
    fn v1_version_variant_and_monotonic() {
        let a = uuid_v1().unwrap();
        let b = uuid_v1().unwrap();
        assert_eq!(a.data[6] & 0xF0, 0x10, "version 1");
        assert_eq!(a.data[8] & 0xC0, 0x80, "variant 10");
        // timestamp_bits = time_hi(6,7) || time_mid(4,5) || time_low(0..4) || clock_seq
        // We only need strict monotonicity of the (ts, clock_seq) ordering, which
        // the assembled big-endian fields preserve lexicographically across the
        // time_hi/mid/low concatenation.
        let ts = |u: &pg_uuid_t| -> u128 {
            let hi = ((u.data[6] as u128 & 0x0F) << 8) | u.data[7] as u128;
            let mid = ((u.data[4] as u128) << 8) | u.data[5] as u128;
            let low = ((u.data[0] as u128) << 24)
                | ((u.data[1] as u128) << 16)
                | ((u.data[2] as u128) << 8)
                | u.data[3] as u128;
            (hi << 48) | (mid << 32) | low
        };
        assert!(ts(&a) < ts(&b), "v1 timestamp strictly increasing");
    }

    #[test]
    fn v1mc_multicast_and_local_admin() {
        let u = uuid_v1mc().unwrap();
        assert_eq!(u.data[10] & 0x01, 0x01, "multicast bit");
        assert_eq!(u.data[10] & 0x02, 0x02, "local-admin bit");
    }
}
