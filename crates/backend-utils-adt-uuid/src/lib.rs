//! Port of PostgreSQL `src/backend/utils/adt/uuid.c`: the built-in `uuid`
//! datatype, RFC 9562 generation (v4 / v7), and timestamp / version extraction.
//!
//! Every function in `uuid.c` is implemented here with logic identical to
//! postgres-18.3. Where C `palloc`s a `pg_uuid_t` / cstring / `bytea` and
//! returns a pointer, this returns the owned value (`pg_uuid_t`, a `Vec<u8>` of
//! cstring bytes, or a `Bytea<'mcx>`). The `uuid_in` / `string_to_uuid`
//! soft-error path routes through a [`SoftErrorContext`], mirroring C's
//! `ereturn`.
//!
//! Genuinely-external dependencies:
//!  * **CSPRNG** (`pg_strong_random`) and the **wall clock** via the
//!    `port-pg-strong-random-seams` seams (owner unported).
//!  * **`timestamptz_pl_interval`** via `backend-utils-adt-timestamp-seams`
//!    (owner unported).
//!
//! Ported-and-directly-called dependencies: `common-hashfn`,
//! `backend-lib-hyperloglog`, `backend-libpq-pqformat`, and the `trace_sort`
//! GUC slot.
//!
//! SortSupport / SkipSupport: the comparison / abbreviation / increment /
//! decrement *kernels* are ported here in full and pure. C installs function
//! pointers into the C-ABI `SortSupport` / `SkipSupport` node; the repo's
//! trimmed node carries opaque tokens the sort substrate mints, so — exactly as
//! `varstr_sortsupport` does — the strategy routines return a decision struct
//! ([`UuidSortSupport`]) / boundary values ([`UuidSkipSupport`]) for the
//! substrate to install, rather than mutating the node's hook tokens.

#![allow(non_snake_case)]

use backend_utils_error::ereport;
use common_hashfn::{hash_bytes, hash_bytes_extended, hash_bytes_uint32};
use mcx::Mcx;
use types_datetime::{
    Interval, TimestampTz, POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC,
};
use types_datum::Bytea;
use types_error::{ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERROR};
use types_stringinfo::StringInfo;
use types_uuid::{pg_uuid_t, uuid_sortsupport_state, UUID_LEN};

use backend_utils_adt_timestamp_seams as timestamp_seam;
use port_pg_strong_random_seams as port_seam;

// ---------------------------------------------------------------------------
// helper macros / constants (uuid.c:30-57)
// ---------------------------------------------------------------------------

/// `NS_PER_MS` (uuid.c:32).
const NS_PER_MS: i64 = 1_000_000;
/// `NS_PER_US` (uuid.c:33).
const NS_PER_US: i64 = 1_000;
/// `US_PER_MS` (uuid.c:34).
const US_PER_MS: i64 = 1_000;

/// `SUBMS_MINIMAL_STEP_BITS` (uuid.c:51-55): 10 on darwin / MSVC (microsecond
/// clocks), else 12. Mirrors the C `#if defined(__darwin__) || defined(_MSC_VER)`.
#[cfg(any(target_os = "macos", target_os = "ios", windows))]
const SUBMS_MINIMAL_STEP_BITS: i32 = 10;
#[cfg(not(any(target_os = "macos", target_os = "ios", windows)))]
const SUBMS_MINIMAL_STEP_BITS: i32 = 12;

/// `SUBMS_BITS` (uuid.c:56).
const SUBMS_BITS: i32 = 12;

/// `SUBMS_MINIMAL_STEP_NS` (uuid.c:57):
/// `(NS_PER_MS / (1 << SUBMS_MINIMAL_STEP_BITS)) + 1`.
const SUBMS_MINIMAL_STEP_NS: i64 = (NS_PER_MS / (1i64 << SUBMS_MINIMAL_STEP_BITS)) + 1;

/// `GREGORIAN_EPOCH_JDATE == date2j(1582,10,15)` (uuid.c:707).
const GREGORIAN_EPOCH_JDATE: i64 = 2_299_161;

// ===========================================================================
// uuid_in / uuid_out (uuid.c:77-122)
// ===========================================================================

/// `uuid_in` (uuid.c:77): parse a cstring into a `uuid`.
///
/// `uuid_str` is the input cstring content (sans the NUL terminator). On a
/// syntax error [`string_to_uuid`] runs C's `ereturn(escontext, , ...)`: with a
/// soft context the `ERRCODE_INVALID_TEXT_REPRESENTATION` error is *saved* into
/// `escontext` and a (discarded) value is returned `Ok`; without one it is
/// rethrown as a hard `Err`.
pub fn uuid_in(uuid_str: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<pg_uuid_t> {
    let mut data = [0u8; UUID_LEN];
    string_to_uuid(uuid_str, &mut data, escontext)?;
    Ok(pg_uuid_t { data })
}

/// `uuid_out` (uuid.c:88): render a `uuid` as the canonical `8x-4x-4x-4x-12x`
/// lowercase-hex cstring.
///
/// Returns the cstring bytes *including* the trailing NUL, matching C's
/// `palloc(2 * UUID_LEN + 5)` buffer (`2*16 + 4 hyphens + 1 NUL`).
pub fn uuid_out(uuid: &pg_uuid_t) -> Vec<u8> {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    let mut out = [0u8; 2 * UUID_LEN + 5];
    let mut p = 0usize;
    for i in 0..UUID_LEN {
        // 8, 4, 4, 4, 12 grouping: hyphens before bytes 4, 6, 8, 10.
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out[p] = b'-';
            p += 1;
        }

        let hi = (uuid.data[i] >> 4) as usize;
        let lo = (uuid.data[i] & 0x0F) as usize;

        out[p] = HEX_CHARS[hi];
        p += 1;
        out[p] = HEX_CHARS[lo];
        p += 1;
    }
    out[p] = b'\0';

    out.to_vec()
}

/// `string_to_uuid` (uuid.c:130): the canonical / relaxed parser.
///
/// Accepts 32 hex digits with an optional dash after each group of 4, optionally
/// surrounded by `{}`. Writes the `UUID_LEN` decoded bytes into `out`. On any
/// deviation it runs C's `ereturn`: the `ERRCODE_INVALID_TEXT_REPRESENTATION`
/// error with the exact C message is built; with a soft context it is saved and
/// `Ok(())` returned, else rethrown as `Err`.
fn string_to_uuid(
    source: &[u8],
    out: &mut [u8; UUID_LEN],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    // C treats `source` as a NUL-terminated cstring; reproduce that by treating
    // an out-of-range index as the terminating '\0'.
    let byte_at = |idx: usize| -> u8 { source.get(idx).copied().unwrap_or(0) };

    let mut pos = 0usize;
    let braces = byte_at(0) == b'{';
    if braces {
        pos += 1;
    }

    let mut ok = true;
    for i in 0..UUID_LEN {
        // need two more chars
        if byte_at(pos) == b'\0' || byte_at(pos + 1) == b'\0' {
            ok = false;
            break;
        }
        let c0 = byte_at(pos);
        let c1 = byte_at(pos + 1);
        if !c0.is_ascii_hexdigit() || !c1.is_ascii_hexdigit() {
            ok = false;
            break;
        }

        out[i] = (hex_val(c0) << 4) | hex_val(c1);
        pos += 2;
        if byte_at(pos) == b'-' && (i % 2) == 1 && i < UUID_LEN - 1 {
            pos += 1;
        }
    }

    if ok && braces {
        if byte_at(pos) != b'}' {
            ok = false;
        } else {
            pos += 1;
        }
    }
    if ok && byte_at(pos) != b'\0' {
        ok = false;
    }

    if ok {
        return Ok(());
    }

    // syntax_error: ereturn(escontext, , (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
    //   errmsg("invalid input syntax for type %s: \"%s\"", "uuid", source))).
    let error = ereport(ERROR)
        .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .errmsg(format!(
            "invalid input syntax for type {}: \"{}\"",
            "uuid",
            String::from_utf8_lossy(source)
        ))
        .into_error();
    ereturn(escontext, (), error)
}

/// `strtoul(str_buf, NULL, 16)` of a single validated hex digit (uuid.c:155).
#[inline]
fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

// ===========================================================================
// uuid_recv / uuid_send (uuid.c:180-200)
// ===========================================================================

/// `uuid_recv` (uuid.c:180): read `UUID_LEN` raw bytes from the message into a
/// `uuid` (`pq_getmsgbytes(buffer, UUID_LEN)`).
pub fn uuid_recv(buffer: &mut StringInfo<'_>) -> PgResult<pg_uuid_t> {
    let bytes = backend_libpq_pqformat::pq_getmsgbytes(buffer, UUID_LEN)?;
    let mut data = [0u8; UUID_LEN];
    // pq_getmsgbytes guarantees exactly UUID_LEN bytes (it errors otherwise).
    data.copy_from_slice(&bytes[..UUID_LEN]);
    Ok(pg_uuid_t { data })
}

/// `uuid_send` (uuid.c:191): write the `uuid`'s `UUID_LEN` bytes to a typsend
/// buffer, returning the `bytea` body. C: `pq_begintypsend` / `pq_sendbytes` /
/// `pq_endtypsend`.
pub fn uuid_send<'mcx>(mcx: Mcx<'mcx>, uuid: &pg_uuid_t) -> PgResult<Bytea<'mcx>> {
    let mut buffer = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendbytes(&mut buffer, &uuid.data)?;
    Ok(backend_libpq_pqformat::pq_endtypsend(buffer))
}

// ===========================================================================
// comparisons (uuid.c:203-271)
// ===========================================================================

/// `uuid_internal_cmp` (uuid.c:203): `memcmp(arg1->data, arg2->data, UUID_LEN)`.
///
/// Returns a value with the sign of the difference of the first differing byte
/// (the C `memcmp` contract), not normalized to -1/0/1.
pub fn uuid_internal_cmp(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> i32 {
    memcmp(&arg1.data, &arg2.data)
}

/// `memcmp(a, b, UUID_LEN)` returning a signed byte difference, matching C.
#[inline]
fn memcmp(a: &[u8; UUID_LEN], b: &[u8; UUID_LEN]) -> i32 {
    for i in 0..UUID_LEN {
        if a[i] != b[i] {
            return a[i] as i32 - b[i] as i32;
        }
    }
    0
}

/// `uuid_lt` (uuid.c:209).
pub fn uuid_lt(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) < 0
}

/// `uuid_le` (uuid.c:218).
pub fn uuid_le(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) <= 0
}

/// `uuid_eq` (uuid.c:227).
pub fn uuid_eq(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) == 0
}

/// `uuid_ge` (uuid.c:236).
pub fn uuid_ge(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) >= 0
}

/// `uuid_gt` (uuid.c:245).
pub fn uuid_gt(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) > 0
}

/// `uuid_ne` (uuid.c:254).
pub fn uuid_ne(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> bool {
    uuid_internal_cmp(arg1, arg2) != 0
}

/// `uuid_cmp` (uuid.c:264): btree ordering proc, the raw `uuid_internal_cmp`.
pub fn uuid_cmp(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> i32 {
    uuid_internal_cmp(arg1, arg2)
}

// ===========================================================================
// SortSupport (uuid.c:60-66, 276-421)
// ===========================================================================

/// What [`uuid_sortsupport`] resolved (uuid.c:276): the comparator and, when
/// abbreviating, the freshly-built `uuid_sortsupport_state` scratch and the
/// abbreviation hooks. C installs these as function pointers / `ssup_extra` on
/// the C-ABI node; the trimmed node cannot carry the hooks, so the strategy
/// routine returns the decision for the substrate to install.
#[derive(Debug)]
pub struct UuidSortSupport<'mcx> {
    /// Whether the abbreviated-key optimization is in play (C: `ssup->abbreviate`
    /// at routine end). When `false`, the authoritative comparator is
    /// [`uuid_fast_cmp`]; when `true`, the substrate installs
    /// `ssup_datum_unsigned_cmp` as the comparator, [`uuid_abbrev_convert`] as
    /// the converter, [`uuid_abbrev_abort`] as the abort callback, and
    /// [`uuid_fast_cmp`] as the full comparator, with `extra` as `ssup_extra`.
    pub abbreviate: bool,
    /// The `ssup->ssup_extra` scratch (C: `palloc`'d in `ssup->ssup_cxt`), built
    /// only when abbreviating.
    pub extra: Option<uuid_sortsupport_state<'mcx>>,
}

/// `uuid_sortsupport` (uuid.c:276): SortSupport strategy routine.
///
/// C: `ssup->comparator = uuid_fast_cmp; ssup->ssup_extra = NULL;` then, when
/// `ssup->abbreviate`, allocate the state in `ssup->ssup_cxt`, init it
/// (`initHyperLogLog(&uss->abbr_card, 10)`), and install the abbreviated
/// comparators. The pointer install is the substrate's; this returns the
/// decision (see [`UuidSortSupport`]).
pub fn uuid_sortsupport<'mcx>(
    mcx: Mcx<'mcx>,
    abbreviate: bool,
) -> PgResult<UuidSortSupport<'mcx>> {
    if abbreviate {
        // C: uss->input_count = 0; uss->estimating = true;
        //    initHyperLogLog(&uss->abbr_card, 10);
        let abbr_card = backend_lib_hyperloglog::initHyperLogLog(mcx, 10)?;
        let uss = uuid_sortsupport_state {
            input_count: 0,
            estimating: true,
            abbr_card,
        };
        Ok(UuidSortSupport {
            abbreviate: true,
            extra: Some(uss),
        })
    } else {
        Ok(UuidSortSupport {
            abbreviate: false,
            extra: None,
        })
    }
}

/// `uuid_fast_cmp` (uuid.c:312): the SortSupport comparison kernel. Inputs are
/// the already-unpacked `DatumGetUUIDP(x)` / `DatumGetUUIDP(y)` values.
#[inline]
pub fn uuid_fast_cmp(arg1: &pg_uuid_t, arg2: &pg_uuid_t) -> i32 {
    uuid_internal_cmp(arg1, arg2)
}

/// `uuid_abbrev_abort` (uuid.c:327): callback estimating abbreviation
/// effectiveness, operating on the `uuid_sortsupport_state`.
///
/// Returns `true` to abort abbreviation. The `trace_sort` LOG `elog`s in the C
/// are diagnostics with no SQL-visible effect and are elided (same posture as
/// the merged `numeric_abbrev_abort`); the cardinality threshold logic and the
/// `estimating` toggle are reproduced exactly.
pub fn uuid_abbrev_abort(memtupcount: i32, uss: &mut uuid_sortsupport_state<'_>) -> bool {
    if memtupcount < 10000 || uss.input_count < 10000 || !uss.estimating {
        return false;
    }

    let abbr_card = backend_lib_hyperloglog::estimateHyperLogLog(&uss.abbr_card);

    // >100k distinct values: stop counting; break even is assured.
    if abbr_card > 100000.0 {
        uss.estimating = false;
        return false;
    }

    // Target minimum cardinality is 1 per ~2k of non-null inputs (with a 0.5
    // row fudge factor).
    if abbr_card < uss.input_count as f64 / 2000.0 + 0.5 {
        return true;
    }

    false
}

/// `uuid_abbrev_convert` (uuid.c:387): pack the first `sizeof(Datum)` bytes of
/// the uuid into a Datum-sized integer and byteswap to big-endian-native so the
/// unsigned 3-way comparator orders correctly.
///
/// Returns the abbreviated key as a `usize` (the Datum), updating `uss`.
pub fn uuid_abbrev_convert(authoritative: &pg_uuid_t, uss: &mut uuid_sortsupport_state<'_>) -> usize {
    const SIZEOF_DATUM: usize = core::mem::size_of::<usize>();

    // memcpy(&res, authoritative->data, sizeof(Datum));
    let mut res_bytes = [0u8; SIZEOF_DATUM];
    res_bytes.copy_from_slice(&authoritative.data[..SIZEOF_DATUM]);
    let res = usize::from_ne_bytes(res_bytes);

    uss.input_count += 1;

    if uss.estimating {
        let tmp: u32 = if SIZEOF_DATUM == 8 {
            // tmp = (uint32) res ^ (uint32) ((uint64) res >> 32);
            (res as u64) as u32 ^ ((res as u64) >> 32) as u32
        } else {
            res as u32
        };
        // addHyperLogLog(&uss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
        backend_lib_hyperloglog::addHyperLogLog(&mut uss.abbr_card, hash_bytes_uint32(tmp));
    }

    // res = DatumBigEndianToNative(res); -- byteswap on little-endian.
    if cfg!(target_endian = "little") {
        res.swap_bytes()
    } else {
        res
    }
}

// ===========================================================================
// SkipSupport (uuid.c:423-489)
// ===========================================================================

/// `uuid_decrement` (uuid.c:423): subtract 1 from the big-endian UUID integer.
///
/// Decrements `existing` in place and returns the C `*underflow` bool. C
/// operates on a fresh copy of `existing`; on the non-underflow path it returns
/// the decremented copy, and on underflow it discards the copy and returns
/// `(Datum) 0`. The non-underflow path leaves `existing` holding the decremented
/// value (bit-identical to C's returned copy); on `true` (underflow), `existing`
/// is left fully-wrapped (all `0xFF`) and must be discarded.
pub fn uuid_decrement(existing: &mut [u8; UUID_LEN]) -> bool {
    for i in (0..UUID_LEN).rev() {
        if existing[i] > 0 {
            existing[i] -= 1;
            return false;
        }
        existing[i] = u8::MAX;
    }
    // underflow
    true
}

/// `uuid_increment` (uuid.c:448): add 1 to the big-endian UUID integer.
///
/// As [`uuid_decrement`], with the wrapped overflow state being all `0x00`.
pub fn uuid_increment(existing: &mut [u8; UUID_LEN]) -> bool {
    for i in (0..UUID_LEN).rev() {
        if existing[i] < u8::MAX {
            existing[i] += 1;
            return false;
        }
        existing[i] = 0;
    }
    // overflow
    true
}

/// What [`uuid_skipsupport`] resolved (uuid.c:473): the increment / decrement
/// kernels are [`uuid_increment`] / [`uuid_decrement`]; the boundary elements
/// are the all-`0x00` and all-`0xFF` UUIDs. C stores these (and the callback
/// pointers) onto the C-ABI node; the substrate installs them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UuidSkipSupport {
    /// `sksup->low_elem` (C: `UUIDPGetDatum(uuid_min)`), all bytes `0x00`.
    pub low_elem: pg_uuid_t,
    /// `sksup->high_elem` (C: `UUIDPGetDatum(uuid_max)`), all bytes `0xFF`.
    pub high_elem: pg_uuid_t,
}

/// `uuid_skipsupport` (uuid.c:473): SkipSupport strategy routine. Builds the
/// all-`0x00` / all-`0xFF` boundary UUIDs (C `palloc(UUID_LEN)` + `memset`) and
/// returns them with the increment / decrement kernels for the substrate.
pub fn uuid_skipsupport() -> UuidSkipSupport {
    UuidSkipSupport {
        low_elem: pg_uuid_t {
            data: [0x00; UUID_LEN],
        },
        high_elem: pg_uuid_t {
            data: [0xFF; UUID_LEN],
        },
    }
}

// ===========================================================================
// hashing (uuid.c:491-506)
// ===========================================================================

/// `uuid_hash` (uuid.c:492): `hash_any(key->data, UUID_LEN)`.
pub fn uuid_hash(key: &pg_uuid_t) -> u32 {
    hash_bytes(&key.data)
}

/// `uuid_hash_extended` (uuid.c:500): `hash_any_extended(key->data, UUID_LEN, seed)`.
pub fn uuid_hash_extended(key: &pg_uuid_t, seed: u64) -> u64 {
    hash_bytes_extended(&key.data, seed)
}

// ===========================================================================
// generation (uuid.c:511-701)
// ===========================================================================

/// `uuid_set_version` (uuid.c:511): set the version (top 4 bits of byte 6) and
/// the RFC 9562 variant (top two bits of byte 8 -> `10`).
#[inline]
fn uuid_set_version(uuid: &mut [u8; UUID_LEN], version: u8) {
    // version field, top four bits
    uuid[6] = (uuid[6] & 0x0f) | (version << 4);
    // variant field, top two bits are 1, 0
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
}

/// `gen_random_uuid` (uuid.c:527): a "version 4" (pseudorandom) UUID.
///
/// All bytes are filled from `pg_strong_random` except the version / variant
/// bits.
pub fn gen_random_uuid() -> PgResult<pg_uuid_t> {
    let mut data = [0u8; UUID_LEN];

    if !port_seam::pg_strong_random::call(&mut data) {
        return Err(could_not_generate_random_values());
    }

    // Set magic numbers for a "version 4" (pseudorandom) UUID and variant.
    uuid_set_version(&mut data, 4);

    Ok(pg_uuid_t { data })
}

/// `could_not_generate_random_values` — the shared error
/// (`ERRCODE_INTERNAL_ERROR`, uuid.c:533-535 / 630-632).
fn could_not_generate_random_values() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg("could not generate random values")
        .into_error()
}

/// `get_real_time_ns_ascending` (uuid.c:551): current real time in ns, with the
/// per-backend guarantee that it strictly advances by at least
/// `SUBMS_MINIMAL_STEP_NS`.
///
/// The raw clock read crosses the `clock_realtime_ns` seam; the C
/// `static int64 previous_ns` is a per-backend (per-thread) cell.
fn get_real_time_ns_ascending() -> i64 {
    use core::cell::Cell;
    thread_local! {
        // C: static int64 previous_ns = 0;
        static PREVIOUS_NS: Cell<i64> = const { Cell::new(0) };
    }

    // C: ns = tv_sec * NS_PER_S + tv_nsec (the seam returns this combined).
    let mut ns = port_seam::clock_realtime_ns::call();

    PREVIOUS_NS.with(|previous| {
        let prev = previous.get();
        // Guarantee the minimal step advancement of the timestamp.
        if prev + SUBMS_MINIMAL_STEP_NS >= ns {
            ns = prev + SUBMS_MINIMAL_STEP_NS;
        }
        previous.set(ns);
    });

    ns
}

/// `generate_uuidv7` (uuid.c:604): a "version 7" UUID for the given Unix-epoch
/// millisecond timestamp `unix_ts_ms` and sub-millisecond nanoseconds `sub_ms`.
fn generate_uuidv7(unix_ts_ms: u64, sub_ms: u32) -> PgResult<pg_uuid_t> {
    let mut data = [0u8; UUID_LEN];

    // Fill in time part (48-bit big-endian millisecond timestamp).
    data[0] = (unix_ts_ms >> 40) as u8;
    data[1] = (unix_ts_ms >> 32) as u8;
    data[2] = (unix_ts_ms >> 24) as u8;
    data[3] = (unix_ts_ms >> 16) as u8;
    data[4] = (unix_ts_ms >> 8) as u8;
    data[5] = unix_ts_ms as u8;

    // sub-millisecond timestamp fraction (SUBMS_BITS bits, not
    // SUBMS_MINIMAL_STEP_BITS).
    // C: (sub_ms * (1 << SUBMS_BITS)) / NS_PER_MS -- uint32 * int -> int64 / int64.
    let increased_clock_precision: u32 =
        ((sub_ms.wrapping_mul(1u32 << SUBMS_BITS) as i64) / NS_PER_MS) as u32;

    // Fill the increased clock precision into "rand_a" bits.
    data[6] = (increased_clock_precision >> 8) as u8;
    data[7] = increased_clock_precision as u8;

    // Fill everything after the increased clock precision with random bytes.
    if !port_seam::pg_strong_random::call(&mut data[8..UUID_LEN]) {
        return Err(could_not_generate_random_values());
    }

    // On 10-bit sub-ms systems, mix two CSPRNG bits into the low rand_a bits.
    if SUBMS_MINIMAL_STEP_BITS == 10 {
        data[7] ^= data[8] >> 6;
    }

    // Set magic numbers for a "version 7" UUID and variant.
    uuid_set_version(&mut data, 7);

    Ok(pg_uuid_t { data })
}

/// `uuidv7` (uuid.c:658): a "version 7" UUID with the current timestamp.
pub fn uuidv7() -> PgResult<pg_uuid_t> {
    let ns = get_real_time_ns_ascending();
    generate_uuidv7((ns / NS_PER_MS) as u64, (ns % NS_PER_MS) as u32)
}

/// `uuidv7_interval` (uuid.c:670): `uuidv7()` with the timestamp shifted by
/// `shift`.
///
/// The shift uses `timestamptz_pl_interval` (microsecond precision), reached
/// through the timestamp seam.
pub fn uuidv7_interval(shift: &Interval) -> PgResult<pg_uuid_t> {
    let ns = get_real_time_ns_ascending();

    // Convert the UNIX-epoch ns to a TimestampTz (Postgres-epoch us).
    let ts: TimestampTz = (ns / NS_PER_US)
        - (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
            * SECS_PER_DAY as i64
            * USECS_PER_SEC;

    // Compute time shift (DirectFunctionCall2(timestamptz_pl_interval, ...)).
    let ts = timestamp_seam::timestamptz_pl_interval::call(ts, *shift)?;

    // Convert a TimestampTz value back to a UNIX-epoch timestamp (us).
    let us: i64 = ts
        + (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
            * SECS_PER_DAY as i64
            * USECS_PER_SEC;

    // Generate a UUIDv7.
    generate_uuidv7(
        (us / US_PER_MS) as u64,
        ((us % US_PER_MS) * NS_PER_US + ns % NS_PER_US) as u32,
    )
}

// ===========================================================================
// extraction (uuid.c:714-783)
// ===========================================================================

/// `uuid_extract_timestamp` (uuid.c:714): timestamp from a v1 / v7 UUID.
///
/// Returns `None` (C: `PG_RETURN_NULL()`) if not an RFC 9562 variant or not a
/// version that carries a timestamp.
pub fn uuid_extract_timestamp(uuid: &pg_uuid_t) -> Option<TimestampTz> {
    // check if RFC 9562 variant
    if (uuid.data[8] & 0xc0) != 0x80 {
        return None;
    }

    let version = uuid.data[6] >> 4;

    if version == 1 {
        let tms: u64 = ((uuid.data[0] as u64) << 24)
            .wrapping_add((uuid.data[1] as u64) << 16)
            .wrapping_add((uuid.data[2] as u64) << 8)
            .wrapping_add(uuid.data[3] as u64)
            .wrapping_add((uuid.data[4] as u64) << 40)
            .wrapping_add((uuid.data[5] as u64) << 32)
            .wrapping_add(((uuid.data[6] as u64) & 0xf) << 56)
            .wrapping_add((uuid.data[7] as u64) << 48);

        // convert 100-ns intervals to us, then adjust.
        //
        // C computes this entirely in uint64 (wrapping) before the cast to
        // TimestampTz; the signed `i64` subtraction here is bit-identical
        // because `tms` is 60-bit-bounded (byte 6's top nibble is masked to
        // `& 0xf`), so `tms / 10` is well under i64::MAX.
        let ts: TimestampTz = (tms / 10) as TimestampTz
            - (POSTGRES_EPOCH_JDATE as u64 - GREGORIAN_EPOCH_JDATE as u64) as i64
                * SECS_PER_DAY as i64
                * USECS_PER_SEC;
        return Some(ts);
    }

    if version == 7 {
        let tms: u64 = (uuid.data[5] as u64)
            .wrapping_add((uuid.data[4] as u64) << 8)
            .wrapping_add((uuid.data[3] as u64) << 16)
            .wrapping_add((uuid.data[2] as u64) << 24)
            .wrapping_add((uuid.data[1] as u64) << 32)
            .wrapping_add((uuid.data[0] as u64) << 40);

        // convert ms to us, then adjust
        let ts: TimestampTz = (tms.wrapping_mul(US_PER_MS as u64)) as TimestampTz
            - (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
                * SECS_PER_DAY as i64
                * USECS_PER_SEC;
        return Some(ts);
    }

    // not a timestamp-containing UUID version
    None
}

/// `uuid_extract_version` (uuid.c:770): the version nibble.
///
/// Returns `None` (C: `PG_RETURN_NULL()`) if not an RFC 9562 variant.
pub fn uuid_extract_version(uuid: &pg_uuid_t) -> Option<i16> {
    // check if RFC 9562 variant
    if (uuid.data[8] & 0xc0) != 0x80 {
        return None;
    }

    let version = (uuid.data[6] >> 4) as u16;
    Some(version as i16)
}

/// This crate owns no inward seams (no cyclic caller reaches into uuid.c yet).
/// It registers the by-reference fmgr builtin wrappers (C: their
/// `fmgr_builtins[]` rows) so by-OID dispatch resolves the SQL-callable `uuid`
/// I/O, comparison, and hash functions; the aggregator calls this at boot.
pub fn init_seams() {
    fmgr_builtins::register_uuid_builtins();
}

pub mod fmgr_builtins;

#[cfg(test)]
mod tests;
