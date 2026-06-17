#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`
// (== `Result<_, PgError>`), the project-wide error contract.
#![allow(clippy::result_large_err)]

//! Idiomatic port of `src/backend/utils/adt/xid8funcs.c` (postgres-18.3): the
//! user-visible transaction-snapshot functions (`pg_current_xact_id`,
//! `pg_current_snapshot`, the `pg_snapshot` type's I/O, `pg_xact_status`, the
//! `pg_visible_in_snapshot` / `pg_snapshot_{xmin,xmax,xip}` accessors).
//!
//! Every function defined in the C file is ported here, with control flow,
//! branch order, loop bounds, message text and SQLSTATE preserved 1:1.
//!
//! ## Representation of `pg_snapshot`
//!
//! In C, `pg_snapshot` is a *varlena* with a flexible `xip[]` tail, manipulated
//! both as a struct and through `StringInfo`. Here it is the owned
//! [`PgSnapshot`] (`nxip`/`xmin`/`xmax`/`xip`). The file-owned logic — sorting +
//! dedup ([`sort_snapshot`]), visibility ([`is_visible_fxid`]), text/binary
//! parsing ([`parse_snapshot`], [`pg_snapshot_recv`]) and rendering
//! ([`pg_snapshot_out`], [`pg_snapshot_send`]) — is ported exactly, including
//! the on-disk/wire byte layout via [`PgSnapshot::to_varlena_bytes`] /
//! [`PgSnapshot::from_varlena_bytes`].
//!
//! ## Externals / seams
//!
//! Live transaction / snapshot / clog state goes through the real owners' seam
//! crates (all landed): `ReadNextFullTransactionId` and
//! `TransamVariables->oldestClogXid` (varsup), `GetTopFullTransactionId{,IfAny}`
//! (xact), `GetActiveSnapshot` + `TransactionXmin` (snapmgr),
//! `TransactionIdIsInProgress` (procarray), `TransactionIdDidCommit` (transam),
//! `PreventCommandDuringRecovery` (utility), and `LWLockAcquire(XactTruncationLock)`
//! (lwlock). `FullTransactionIdFromAllowableAt` is the pure `access/transam.h`
//! static inline, reproduced over `read_next_full_transaction_id`.
//!
//! ## fmgr / Datum boundary
//!
//! As with the other ported `utils/adt` crates, this crate exposes the pure
//! cores (decoded scalars / owned [`PgSnapshot`]); the bare-word `PGFunction`
//! v1 registry + `Datum`/varlena marshalling is deferred project-wide. The
//! `pg_snapshot_xip` set-returning glue (`FuncCallContext`) is likewise the
//! fmgr/funcapi boundary; the core returns the exact `xip[]` value sequence.

use backend_access_transam_transam_seams as transam_seams;
use backend_access_transam_varsup_seams as varsup_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_storage_ipc_procarray_seams as procarray_seams;
use backend_storage_lmgr_lwlock_seams as lwlock_seams;
use backend_tcop_utility_seams as utility_seams;
use backend_utils_time_snapmgr_pc_seams as snapmgr_pc_seams;
use backend_utils_time_snapmgr_seams as snapmgr_seams;

use types_core::xact::{TransactionIdIsNormal, TransactionIdIsValid};
use types_core::{FullTransactionId, InvalidFullTransactionId, TransactionId};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use types_storage::{LWLockMode, MAX_BACKENDS, XACT_TRUNCATION_LOCK};

// ---------------------------------------------------------------------------
// Constants and varlena layout matching the C file.
// ---------------------------------------------------------------------------

/// `#define USE_BSEARCH_IF_NXIP_GREATER 30` (xid8funcs.c:48).
///
/// If a snapshot has more than this many xips, [`is_visible_fxid`] uses a
/// binary search instead of a linear scan.
pub const USE_BSEARCH_IF_NXIP_GREATER: u32 = 30;

/// `MaxAllocSize` (utils/memutils.h): `0x3fffffff`.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

/// Size of the fixed `pg_snapshot` header that precedes `xip[]` in the varlena
/// byte image: the 4-byte varlena length word, the 4-byte `nxip`, and the two
/// 8-byte `FullTransactionId`s `xmin`/`xmax`. Equals `offsetof(pg_snapshot, xip)`.
const SNAPSHOT_HEADER_LEN: usize = 4 /* __varsz */ + 4 /* nxip */ + 8 /* xmin */ + 8 /* xmax */;

/// `#define PG_SNAPSHOT_SIZE(nxip)
///     (offsetof(pg_snapshot, xip) + sizeof(FullTransactionId) * (nxip))`
/// (xid8funcs.c:70).
const fn PG_SNAPSHOT_SIZE(nxip: usize) -> usize {
    SNAPSHOT_HEADER_LEN + 8 * nxip
}

/// `#define PG_SNAPSHOT_MAX_NXIP
///     ((MaxAllocSize - offsetof(pg_snapshot, xip)) / sizeof(FullTransactionId))`
/// (xid8funcs.c:72).
pub const PG_SNAPSHOT_MAX_NXIP: usize = (MAX_ALLOC_SIZE - SNAPSHOT_HEADER_LEN) / 8;

// `StaticAssertDecl(MAX_BACKENDS * 2 <= PG_SNAPSHOT_MAX_NXIP, ...)`
// (xid8funcs.c:79): compile-time guarantee that the procarray can never produce
// an over-large nxip.
const _: () = assert!(
    (MAX_BACKENDS as usize) * 2 <= PG_SNAPSHOT_MAX_NXIP,
    "possible overflow in pg_current_snapshot()"
);

// ---------------------------------------------------------------------------
// FullTransactionId comparison/conversion macros (`access/transam.h`).
//
// These operate strictly on the 64-bit `value`, so wraparound logic does not
// apply (unlike the 32-bit `TransactionId` comparisons).
// ---------------------------------------------------------------------------

/// `#define XidFromFullTransactionId(x)  ((uint32) (x).value)`
#[inline]
const fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId {
    x.value as TransactionId
}

/// `#define U64FromFullTransactionId(x)  ((x).value)`
#[inline]
const fn U64FromFullTransactionId(x: FullTransactionId) -> u64 {
    x.value
}

/// `static inline FullTransactionId FullTransactionIdFromU64(uint64 value)`
#[inline]
const fn FullTransactionIdFromU64(value: u64) -> FullTransactionId {
    FullTransactionId { value }
}

/// `#define FullTransactionIdEquals(a, b)  ((a).value == (b).value)`
#[inline]
const fn FullTransactionIdEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value == b.value
}

/// `#define FullTransactionIdPrecedes(a, b)  ((a).value < (b).value)`
#[inline]
const fn FullTransactionIdPrecedes(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value < b.value
}

/// `#define FullTransactionIdFollowsOrEquals(a, b)  ((a).value >= (b).value)`
#[inline]
const fn FullTransactionIdFollowsOrEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value >= b.value
}

/// `#define FullTransactionIdIsValid(fxid)  TransactionIdIsValid(XidFromFullTransactionId(fxid))`
/// — note this checks the *low 32 bits* (the embedded xid), not the whole u64.
#[inline]
const fn FullTransactionIdIsValid(fxid: FullTransactionId) -> bool {
    fxid.is_valid()
}

/// `FullTransactionIdFromAllowableAt(nextFullXid, xid)` (`access/transam.h`:380)
/// — recover the full xid (epoch) for a bare `xid` known to be in
/// `[oldestXid, nextXid]` when `TransamVariables->nextXid` was `nextFullXid`.
///
/// Pure `access/transam.h` static inline; reproduced here over the already-read
/// `next_full_xid`. The C `Assert(TransactionIdPrecedesOrEquals(xid, ...))` is a
/// debug-only sanity check that does not affect the result.
#[inline]
fn FullTransactionIdFromAllowableAt(
    next_full_xid: FullTransactionId,
    xid: TransactionId,
) -> FullTransactionId {
    // Special transaction ID.
    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }

    // The 64-bit result must be <= nextFullXid; the xid is from nextFullXid's
    // epoch or the one before.
    let mut epoch = next_full_xid.epoch();
    if xid > XidFromFullTransactionId(next_full_xid) {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }

    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

// ---------------------------------------------------------------------------
// pg_snapshot
// ---------------------------------------------------------------------------

/// Owned representation of the C `pg_snapshot` varlena (xid8funcs.c:54-68).
///
/// ```c
/// typedef struct {
///     int32             __varsz;   /* 4-byte length hdr */
///     uint32            nxip;      /* number of fxids in xip array */
///     FullTransactionId xmin;
///     FullTransactionId xmax;
///     FullTransactionId xip[FLEXIBLE_ARRAY_MEMBER];  /* xmin <= xip[i] < xmax */
/// } pg_snapshot;
/// ```
///
/// `nxip` is always `xip.len()`; it is kept as a struct member to mirror the C
/// field (and its `uint32` width matters for the wire format).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgSnapshot {
    /// `nxip` — number of in-progress fxids. Always equal to `xip.len()`.
    pub nxip: u32,
    /// `xmin`.
    pub xmin: FullTransactionId,
    /// `xmax`.
    pub xmax: FullTransactionId,
    /// `xip[0..nxip]` — in-progress fxids, sorted ascending, `xmin <= xip[i] <
    /// xmax`.
    pub xip: Vec<FullTransactionId>,
}

impl PgSnapshot {
    /// Serialize to the exact varlena byte image the C `pg_snapshot` occupies:
    /// the 4-byte varlena length word (`SET_VARSIZE`, low tag bits 00, length
    /// stored as `len << 2`) followed by `nxip` (u32), `xmin`/`xmax` (u64) and
    /// the `xip[]` u64s, all host-endian — the in-memory/on-disk image.
    ///
    /// The binary *wire* format (`pg_snapshot_send`) is big-endian and laid out
    /// differently — see [`pg_snapshot_send`].
    pub fn to_varlena_bytes(&self) -> Vec<u8> {
        let total = PG_SNAPSHOT_SIZE(self.xip.len());
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
        out.extend_from_slice(&self.nxip.to_ne_bytes());
        out.extend_from_slice(&U64FromFullTransactionId(self.xmin).to_ne_bytes());
        out.extend_from_slice(&U64FromFullTransactionId(self.xmax).to_ne_bytes());
        for &fxid in &self.xip {
            out.extend_from_slice(&U64FromFullTransactionId(fxid).to_ne_bytes());
        }
        out
    }

    /// Reconstruct from the varlena byte image produced by
    /// [`to_varlena_bytes`](Self::to_varlena_bytes).
    pub fn from_varlena_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < SNAPSHOT_HEADER_LEN {
            return None;
        }
        let nxip = u32::from_ne_bytes(bytes[4..8].try_into().ok()?);
        let xmin = FullTransactionIdFromU64(u64::from_ne_bytes(bytes[8..16].try_into().ok()?));
        let xmax = FullTransactionIdFromU64(u64::from_ne_bytes(bytes[16..24].try_into().ok()?));
        let mut xip = Vec::with_capacity(nxip as usize);
        let mut off = SNAPSHOT_HEADER_LEN;
        for _ in 0..nxip {
            if off + 8 > bytes.len() {
                return None;
            }
            xip.push(FullTransactionIdFromU64(u64::from_ne_bytes(
                bytes[off..off + 8].try_into().ok()?,
            )));
            off += 8;
        }
        Some(PgSnapshot {
            nxip,
            xmin,
            xmax,
            xip,
        })
    }
}

// ---------------------------------------------------------------------------
// TransactionIdInRecentPast (xid8funcs.c:96-147)
// ---------------------------------------------------------------------------

/// Result of [`TransactionIdInRecentPast`]: the bool the C function returns,
/// plus the `*extracted_xid` out-parameter it sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecentPast {
    /// Whether the transaction is still new enough to look up in clog.
    pub determinable: bool,
    /// `*extracted_xid` — the low 32 bits of `fxid` (the actual XID, no epoch).
    pub extracted_xid: TransactionId,
}

/// `TransactionIdInRecentPast(fxid, extracted_xid)` (xid8funcs.c:96).
///
/// Errors if `fxid` is in the future. Otherwise returns
/// [`RecentPast::determinable`] = whether the transaction is recent enough that
/// we can determine its commit status, and always reports the extracted low-32
/// xid (mirroring `if (extracted_xid != NULL) *extracted_xid = xid;`, which runs
/// unconditionally before any early return).
///
/// The caller must hold `XactTruncationLock` (the C `Assert(LWLockHeldByMe(...))`)
/// — here that is the caller's responsibility ([`pg_xact_status`] holds it).
pub fn TransactionIdInRecentPast(fxid: FullTransactionId) -> PgResult<RecentPast> {
    let xid = XidFromFullTransactionId(fxid);

    let now_fullxid = varsup_seams::read_next_full_transaction_id::call();

    // `if (extracted_xid != NULL) *extracted_xid = xid;` — always set, even on
    // the early `false` returns below.
    let extracted_xid = xid;

    if !TransactionIdIsValid(xid) {
        return Ok(RecentPast {
            determinable: false,
            extracted_xid,
        });
    }

    // For non-normal transaction IDs, we can ignore the epoch.
    if !TransactionIdIsNormal(xid) {
        return Ok(RecentPast {
            determinable: true,
            extracted_xid,
        });
    }

    // If the transaction ID is in the future, throw an error.
    if !FullTransactionIdPrecedes(fxid, now_fullxid) {
        return Err(PgError::error(format!(
            "transaction ID {} is in the future",
            U64FromFullTransactionId(fxid)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // `Assert(LWLockHeldByMe(XactTruncationLock));` — caller-held; no-op here.

    let oldest_clog_xid = varsup_seams::get_oldest_clog_xid::call();
    let oldest_clog_fxid = FullTransactionIdFromAllowableAt(now_fullxid, oldest_clog_xid);
    Ok(RecentPast {
        determinable: !FullTransactionIdPrecedes(fxid, oldest_clog_fxid),
        extracted_xid,
    })
}

// ---------------------------------------------------------------------------
// cmp_fxid / sort_snapshot / is_visible_fxid (xid8funcs.c:152-215)
// ---------------------------------------------------------------------------

/// `cmp_fxid(aa, bb)` (xid8funcs.c:152): qsort/bsearch comparator.
fn cmp_fxid(a: &FullTransactionId, b: &FullTransactionId) -> core::cmp::Ordering {
    if FullTransactionIdPrecedes(*a, *b) {
        core::cmp::Ordering::Less
    } else if FullTransactionIdPrecedes(*b, *a) {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Equal
    }
}

/// `sort_snapshot(snap)` (xid8funcs.c:172): sort the xips ascending and remove
/// duplicates (`qsort` + `qunique`). Always sorts (even when bsearch will not be
/// used) for a consistent on-disk representation. Keeps `nxip` in sync.
pub fn sort_snapshot(snap: &mut PgSnapshot) {
    if snap.nxip > 1 {
        snap.xip.sort_by(cmp_fxid);
        snap.xip
            .dedup_by(|a, b| cmp_fxid(a, b) == core::cmp::Ordering::Equal);
        snap.nxip = snap.xip.len() as u32;
    }
}

/// `is_visible_fxid(value, snap)` (xid8funcs.c:186): is `value` visible in
/// `snap`?
pub fn is_visible_fxid(value: FullTransactionId, snap: &PgSnapshot) -> bool {
    if FullTransactionIdPrecedes(value, snap.xmin) {
        true
    } else if !FullTransactionIdPrecedes(value, snap.xmax) {
        false
    } else if snap.nxip > USE_BSEARCH_IF_NXIP_GREATER {
        // bsearch over the sorted xip[]: if found, the transaction is still in
        // progress (-> not visible).
        snap.xip
            .binary_search_by(|probe| cmp_fxid(probe, &value))
            .is_err()
    } else {
        for i in 0..snap.nxip as usize {
            if FullTransactionIdEquals(value, snap.xip[i]) {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// StringInfo helpers buf_init / buf_add_txid / buf_finalize (xid8funcs.c:221-259)
//
// In C these incrementally build the varlena via a StringInfo. Here the same
// incremental construction builds a `PgSnapshot`, which `parse_snapshot`
// finalizes (its varlena bytes come from `PgSnapshot::to_varlena_bytes`).
// ---------------------------------------------------------------------------

/// `buf_init(xmin, xmax)` (xid8funcs.c:221): start a snapshot with the given
/// bounds and no xips.
fn buf_init(xmin: FullTransactionId, xmax: FullTransactionId) -> PgSnapshot {
    PgSnapshot {
        nxip: 0,
        xmin,
        xmax,
        xip: Vec::new(),
    }
}

/// `buf_add_txid(buf, fxid)` (xid8funcs.c:236): append one in-progress fxid.
fn buf_add_txid(snap: &mut PgSnapshot, fxid: FullTransactionId) {
    // do this before possible realloc (mirrors the C ordering exactly)
    snap.nxip += 1;
    snap.xip.push(fxid);
}

// ---------------------------------------------------------------------------
// strtou64 (libc) — base-10 unsigned parse with an endptr, as used by
// parse_snapshot (xid8funcs.c:275/280/298 call `strtou64(str, &endp, 10)`).
//
// Faithful to C `strtoull(nptr, &endptr, 10)`: skip leading isspace, accept an
// optional '+'/'-', consume base-10 digits, set endptr past the last digit (or
// to nptr if no digits were consumed), saturate to ULLONG_MAX on overflow. A
// leading '-' negates modulo 2^64 (C's documented behavior).
// ---------------------------------------------------------------------------

/// Result of [`strtou64`]: the parsed value and the byte offset of `endptr`
/// within the original input.
struct StrtoU64 {
    value: u64,
    end: usize,
}

/// `strtou64(str, &endp, 10)` — C `strtoull` with base 10.
fn strtou64(s: &[u8]) -> StrtoU64 {
    let mut i = 0;

    // Skip leading whitespace (C isspace).
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }

    // Optional sign.
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }

    let digits_start = i;
    let mut value: u64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        let digit = (s[i] - b'0') as u64;
        match value.checked_mul(10).and_then(|v| v.checked_add(digit)) {
            Some(v) => value = v,
            None => overflow = true,
        }
        i += 1;
    }

    // C: if no digits were converted, endptr == nptr (the original start),
    // value 0.
    if i == digits_start {
        return StrtoU64 { value: 0, end: 0 };
    }

    if overflow {
        value = u64::MAX; // ULLONG_MAX on overflow
    }
    if neg {
        value = value.wrapping_neg();
    }

    StrtoU64 { value, end: i }
}

/// C `isspace` for the ASCII bytes `strtou64` may encounter.
#[inline]
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

// ---------------------------------------------------------------------------
// parse_snapshot (xid8funcs.c:264-325)
// ---------------------------------------------------------------------------

/// `parse_snapshot(str, escontext)` (xid8funcs.c:264): parse a `pg_snapshot`
/// from its text representation `xmin:xmax:xip,xip,...`.
///
/// On a malformed input, reports `ERRCODE_INVALID_TEXT_REPRESENTATION`
/// ("invalid input syntax for type pg_snapshot: ..."). With a soft-error context
/// this returns `Ok(None)` (the C `ereturn(escontext, NULL, ...)`).
pub fn parse_snapshot(
    str: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgSnapshot>> {
    let bytes = str.as_bytes();
    let str_start = str;

    let mut last_val = InvalidFullTransactionId;

    // xmin = FullTransactionIdFromU64(strtou64(str, &endp, 10));
    let r = strtou64(bytes);
    let xmin = FullTransactionIdFromU64(r.value);
    let mut pos = r.end;
    // if (*endp != ':') goto bad_format;
    if bytes.get(pos) != Some(&b':') {
        return bad_format(str_start, escontext.as_deref_mut());
    }
    pos += 1; // str = endp + 1;

    // xmax = FullTransactionIdFromU64(strtou64(str, &endp, 10));
    let r = strtou64(&bytes[pos..]);
    let xmax = FullTransactionIdFromU64(r.value);
    pos += r.end;
    if bytes.get(pos) != Some(&b':') {
        return bad_format(str_start, escontext.as_deref_mut());
    }
    pos += 1;

    // it should look sane
    if !FullTransactionIdIsValid(xmin)
        || !FullTransactionIdIsValid(xmax)
        || FullTransactionIdPrecedes(xmax, xmin)
    {
        return bad_format(str_start, escontext.as_deref_mut());
    }

    // allocate buffer
    let mut buf = buf_init(xmin, xmax);

    // loop over values: while (*str != '\0')
    while pos < bytes.len() {
        // read next value
        let r = strtou64(&bytes[pos..]);
        let val = FullTransactionIdFromU64(r.value);
        pos += r.end; // str = endp;

        // require the input to be in order
        if FullTransactionIdPrecedes(val, xmin)
            || FullTransactionIdFollowsOrEquals(val, xmax)
            || FullTransactionIdPrecedes(val, last_val)
        {
            return bad_format(str_start, escontext.as_deref_mut());
        }

        // skip duplicates
        if !FullTransactionIdEquals(val, last_val) {
            buf_add_txid(&mut buf, val);
        }
        last_val = val;

        match bytes.get(pos) {
            Some(&b',') => pos += 1,
            None => {} // *str == '\0'
            Some(_) => return bad_format(str_start, escontext.as_deref_mut()),
        }
    }

    // buf_finalize(buf): SET_VARSIZE + return the built value.
    Ok(Some(buf))
}

/// `bad_format:` label (xid8funcs.c:320).
///
/// `ereturn(escontext, NULL, ...)`: with a soft-error context this saves the
/// error and returns `NULL` (`Ok(None)`); without one it raises (`Err`).
fn bad_format(
    str_start: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgSnapshot>> {
    let err = PgError::error(format!(
        "invalid input syntax for type {}: \"{}\"",
        "pg_snapshot", str_start
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    ereturn(escontext, None, err)
}

// ---------------------------------------------------------------------------
// SQL-callable functions
// ---------------------------------------------------------------------------

/// `pg_current_xact_id()` (xid8funcs.c:333) -> xid8.
///
/// Returns the current top-level full transaction ID, assigning one if needed.
/// Errors during recovery (`PreventCommandDuringRecovery`).
pub fn pg_current_xact_id() -> PgResult<FullTransactionId> {
    // Must prevent during recovery (see the C comment): assigning an xid would
    // fail, and callers rely on always getting a valid current xid.
    utility_seams::prevent_command_during_recovery::call("pg_current_xact_id()")?;
    xact_seams::get_top_full_transaction_id::call()
}

/// `pg_current_xact_id_if_assigned()` (xid8funcs.c:351) -> xid8 or NULL.
///
/// Like [`pg_current_xact_id`] but does not assign a new xid; returns `None`
/// (SQL NULL) when the current transaction has not been assigned one.
pub fn pg_current_xact_id_if_assigned() -> PgResult<Option<FullTransactionId>> {
    let topfxid = xact_seams::get_top_full_transaction_id_if_any::call();

    if !FullTransactionIdIsValid(topfxid) {
        return Ok(None); // PG_RETURN_NULL();
    }

    Ok(Some(topfxid))
}

/// `pg_current_snapshot()` (xid8funcs.c:369) -> pg_snapshot.
///
/// Builds the current snapshot from the backend's active snapshot, expressing
/// every XID as a `FullTransactionId` allowable relative to the next FXID, then
/// sorts + dedups (`sort_snapshot`). Only top-transaction XIDs are included.
pub fn pg_current_snapshot() -> PgResult<PgSnapshot> {
    let next_fxid = varsup_seams::read_next_full_transaction_id::call();

    let cur = snapmgr_seams::get_active_snapshot::call()?;
    let cur = match cur {
        Some(cur) => cur,
        // elog(ERROR, "no active snapshot set");
        None => return Err(PgError::error("no active snapshot set")),
    };

    // allocate
    let nxip = cur.xcnt; // cur->xcnt

    // Fill. These XIDs remain allowable relative to next_fxid (see C comment).
    let xmin = FullTransactionIdFromAllowableAt(next_fxid, cur.xmin);
    let xmax = FullTransactionIdFromAllowableAt(next_fxid, cur.xmax);
    let mut xip = Vec::with_capacity(nxip as usize);
    for i in 0..nxip as usize {
        xip.push(FullTransactionIdFromAllowableAt(next_fxid, cur.xip[i]));
    }

    let mut snap = PgSnapshot {
        nxip,
        xmin,
        xmax,
        xip,
    };

    // Guarantee ascending order; removes duplicates (relevant during 2PC
    // prepare, when a backend and its dummy PGPROC transiently share an XID).
    sort_snapshot(&mut snap);

    // SET_VARSIZE(snap, PG_SNAPSHOT_SIZE(snap->nxip)) — handled by
    // PgSnapshot::to_varlena_bytes at the fmgr boundary.
    Ok(snap)
}

/// `pg_snapshot_in(cstring)` (xid8funcs.c:419) -> pg_snapshot.
///
/// Input function for `pg_snapshot`.
pub fn pg_snapshot_in(
    str: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgSnapshot>> {
    parse_snapshot(str, escontext)
}

/// `pg_snapshot_out(pg_snapshot)` (xid8funcs.c:435) -> cstring.
///
/// Output function for `pg_snapshot`: `xmin:xmax:xip,xip,...`.
pub fn pg_snapshot_out(snap: &PgSnapshot) -> String {
    let mut str = String::new();

    // appendStringInfo(&str, UINT64_FORMAT ":", U64FromFullTransactionId(xmin));
    str.push_str(&format!("{}:", U64FromFullTransactionId(snap.xmin)));
    str.push_str(&format!("{}:", U64FromFullTransactionId(snap.xmax)));

    for i in 0..snap.nxip as usize {
        if i > 0 {
            str.push(',');
        }
        str.push_str(&format!("{}", U64FromFullTransactionId(snap.xip[i])));
    }

    str
}

/// `pg_snapshot_recv(internal)` (xid8funcs.c:467) -> pg_snapshot.
///
/// Binary input function. Wire format: int4 nxip, int8 xmin, int8 xmax,
/// int8 xip... (big-endian). The reads come from a `pq_getmsg*` cursor over the
/// message bytes ([`Pq8Cursor`]); the validation and dedup logic is ported 1:1.
///
/// `bad_format` errors with `ERRCODE_INVALID_BINARY_REPRESENTATION` ("invalid
/// external pg_snapshot data").
pub fn pg_snapshot_recv(buf: &mut Pq8Cursor<'_>) -> PgResult<PgSnapshot> {
    let mut last = InvalidFullTransactionId;

    // load and validate nxip
    let mut nxip = buf.get_int32()?;
    if nxip < 0 || nxip as usize > PG_SNAPSHOT_MAX_NXIP {
        return recv_bad_format();
    }

    let xmin = FullTransactionIdFromU64(buf.get_int64()? as u64);
    let xmax = FullTransactionIdFromU64(buf.get_int64()? as u64);
    if !FullTransactionIdIsValid(xmin)
        || !FullTransactionIdIsValid(xmax)
        || FullTransactionIdPrecedes(xmax, xmin)
    {
        return recv_bad_format();
    }

    // palloc(PG_SNAPSHOT_SIZE(nxip)); we allocate the xip vec instead.
    let mut xip: Vec<FullTransactionId> = Vec::with_capacity(nxip as usize);

    // for (i = 0; i < nxip; i++) — with the C in-place dedup that decrements
    // both `i` and `nxip` on a duplicate (xid8funcs.c:494-514).
    let mut i: i32 = 0;
    while i < nxip {
        let cur = FullTransactionIdFromU64(buf.get_int64()? as u64);

        if FullTransactionIdPrecedes(cur, last)
            || FullTransactionIdPrecedes(cur, xmin)
            || FullTransactionIdPrecedes(xmax, cur)
        {
            return recv_bad_format();
        }

        // skip duplicate xips. C does `i--; nxip--; continue;`, where the
        // `continue` re-runs the for-loop's `i++`, so the net effect is `i`
        // unchanged and `nxip` decremented. This `while` loop's `i += 1` lives
        // at the bottom, so on a dup we leave `i` untouched and just decrement
        // `nxip` — the same net effect.
        if FullTransactionIdEquals(cur, last) {
            nxip -= 1;
            continue;
        }

        // snap->xip[i] = cur; (xip is appended in order, index == i)
        xip.push(cur);
        last = cur;
        i += 1;
    }

    Ok(PgSnapshot {
        nxip: nxip as u32,
        xmin,
        xmax,
        xip,
    })
}

/// `bad_format:` label of `pg_snapshot_recv` (xid8funcs.c:519).
fn recv_bad_format() -> PgResult<PgSnapshot> {
    Err(PgError::error("invalid external pg_snapshot data")
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION))
}

/// `pg_snapshot_send(pg_snapshot)` (xid8funcs.c:533) -> bytea.
///
/// Binary output function. Wire format (big-endian): int4 nxip, u64 xmin,
/// u64 xmax, u64 xip... Returns the raw message bytes that `pq_endtypsend` would
/// wrap in a bytea at the fmgr boundary.
pub fn pg_snapshot_send(snap: &PgSnapshot) -> Vec<u8> {
    let mut buf = Vec::new();
    // pq_sendint32(&buf, snap->nxip);
    buf.extend_from_slice(&snap.nxip.to_be_bytes());
    // pq_sendint64(&buf, (int64) U64FromFullTransactionId(snap->xmin));
    buf.extend_from_slice(&U64FromFullTransactionId(snap.xmin).to_be_bytes());
    buf.extend_from_slice(&U64FromFullTransactionId(snap.xmax).to_be_bytes());
    for i in 0..snap.nxip as usize {
        buf.extend_from_slice(&U64FromFullTransactionId(snap.xip[i]).to_be_bytes());
    }
    buf
}

/// `pg_visible_in_snapshot(xid8, pg_snapshot)` (xid8funcs.c:554) -> bool.
pub fn pg_visible_in_snapshot(value: FullTransactionId, snap: &PgSnapshot) -> bool {
    is_visible_fxid(value, snap)
}

/// `pg_snapshot_xmin(pg_snapshot)` (xid8funcs.c:568) -> xid8.
pub fn pg_snapshot_xmin(snap: &PgSnapshot) -> FullTransactionId {
    snap.xmin
}

/// `pg_snapshot_xmax(pg_snapshot)` (xid8funcs.c:581) -> xid8.
pub fn pg_snapshot_xmax(snap: &PgSnapshot) -> FullTransactionId {
    snap.xmax
}

/// `pg_snapshot_xip(pg_snapshot)` (xid8funcs.c:594) -> setof xid8.
///
/// The C function is a set-returning function driven by a `FuncCallContext`,
/// emitting `snap->xip[call_cntr]` for `call_cntr < snap->nxip` and
/// `SRF_RETURN_DONE` afterwards. The `FuncCallContext`/`multi_call_memory_ctx`
/// glue is the fmgr/funcapi boundary; the value sequence it produces is exactly
/// the snapshot's `xip[]`. Returns that sequence.
pub fn pg_snapshot_xip(snap: &PgSnapshot) -> Vec<FullTransactionId> {
    // for (call_cntr = 0; call_cntr < snap->nxip; call_cntr++) emit xip[call_cntr]
    snap.xip[..snap.nxip as usize].to_vec()
}

/// The status string returned by [`pg_xact_status`] (xid8funcs.c:663-676): one
/// of "in progress" / "committed" / "aborted", or `None` (SQL NULL) for a
/// wrapped / truncated / too-old XID.
pub type XactStatus = Option<&'static str>;

/// `pg_xact_status(xid8)` (xid8funcs.c:639) -> text or NULL.
///
/// Reports the status of a recent transaction ID. Acquires `XactTruncationLock`
/// in shared mode for the duration (guarding against concurrent clog
/// truncation), exactly as the C does.
pub fn pg_xact_status(fxid: FullTransactionId) -> PgResult<XactStatus> {
    // We must protect against concurrent truncation of clog entries to avoid an
    // I/O error on SLRU lookup. The guard releases the lock on `Drop` (the abort
    // path) or on the explicit `release()` below (C's single LWLockRelease at the
    // bottom of the function); held across the clog lookups, so no `?` leaks it.
    let guard = lwlock_seams::lwlock_acquire_main::call(XACT_TRUNCATION_LOCK, LWLockMode::LW_SHARED)?;

    let result = pg_xact_status_locked(fxid);

    // LWLockRelease(XactTruncationLock); on the success path. On an error from
    // the body, `result?` propagates and `guard` is dropped (releasing the lock).
    let status = result?;
    guard.release()?;

    if status.is_none() {
        Ok(None) // PG_RETURN_NULL();
    } else {
        Ok(status) // PG_RETURN_TEXT_P(cstring_to_text(status));
    }
}

/// The body of [`pg_xact_status`] executed while `XactTruncationLock` is held.
fn pg_xact_status_locked(fxid: FullTransactionId) -> PgResult<XactStatus> {
    let rp = TransactionIdInRecentPast(fxid)?;
    if rp.determinable {
        let xid = rp.extracted_xid;
        debug_assert!(TransactionIdIsValid(xid)); // Assert(TransactionIdIsValid(xid));

        // Like row visibility checks, test in-progress before consulting the
        // CLOG, else a committing-but-not-yet-removed XID reads as "committed"
        // incorrectly (see heapam_visibility.c).
        if procarray_seams::transaction_id_is_in_progress::call(xid)? {
            Ok(Some("in progress"))
        } else {
            // TransactionIdDidCommit(xid): C reads the TransactionXmin global
            // when chasing a sub-committed xid's parent through pg_subtrans; the
            // repo threads that value explicitly.
            let transaction_xmin = snapmgr_pc_seams::transaction_xmin::call()?;
            if transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)? {
                Ok(Some("committed"))
            } else {
                // it must have aborted or crashed
                Ok(Some("aborted"))
            }
        }
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Pq8Cursor — the minimal big-endian message reader pg_snapshot_recv needs.
//
// Stands in for the `pq_getmsgint`/`pq_getmsgint64` reads against the incoming
// StringInfo at the fmgr boundary; reproduces their "insufficient data left in
// message" error so the recv logic is exercised end-to-end.
// ---------------------------------------------------------------------------

/// Cursor over an incoming binary message, supplying the `pq_getmsgint(buf, 4)`
/// and `pq_getmsgint64(buf)` reads used by [`pg_snapshot_recv`].
pub struct Pq8Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Pq8Cursor<'a> {
    /// Wrap a slice of message bytes.
    pub fn new(bytes: &'a [u8]) -> Self {
        Pq8Cursor { bytes, pos: 0 }
    }

    /// `pq_getmsgint(buf, 4)` — read a big-endian int32.
    pub fn get_int32(&mut self) -> PgResult<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `pq_getmsgint64(buf)` — read a big-endian int64.
    pub fn get_int64(&mut self) -> PgResult<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn take(&mut self, n: usize) -> PgResult<&'a [u8]> {
        if self.pos + n > self.bytes.len() {
            return Err(PgError::error("insufficient data left in message"));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

/// The fmgr builtin layer (C: `fmgr_builtins[]`) for this file's scalar / text
/// transaction-id functions; registered by [`fmgr_builtins::register_xid8funcs_builtins`].
pub mod fmgr_builtins;

/// Install this crate's seams (called by `seams-init::init_all`). Registers the
/// `xid8funcs.c` builtin rows into the fmgr-core builtin table so by-OID
/// dispatch resolves `pg_current_xact_id` / `txid_current` / `pg_xact_status`
/// (and their aliases).
pub fn init_seams() {
    fmgr_builtins::register_xid8funcs_builtins();
}

#[cfg(test)]
mod tests;
