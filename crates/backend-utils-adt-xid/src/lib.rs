#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/xid.c`: the POSTGRES
//! transaction identifier (`xid`, `xid8`) and command identifier (`cid`)
//! datatypes.
//!
//! `xid` (`TransactionId`) and `cid` (`CommandId`) are pass-by-value 32-bit
//! unsigned types; `xid8` (`FullTransactionId`) is a pass-by-value 64-bit
//! unsigned type. Following the sibling adt ports (`oid.c`), the value cores
//! here are plain typed Rust functions over `TransactionId` / `CommandId` /
//! `FullTransactionId` / `&str`; the fmgr/`Datum` boundary lives in
//! [`fmgr_builtins`], where each SQL-callable entry is a `fc_<name>` adapter
//! registered into the fmgr-core builtin table (C: `fmgr_builtins[]`) by
//! [`fmgr_builtins::register_xid_builtins`] / `register_cid_builtins`.
//!
//! The wraparound-aware `TransactionIdPrecedes` / `TransactionIdIsNormal`
//! (`access/transam/transam.c`) are tiny inline predicates ported in-crate (as
//! the sibling crates' local copies do). The `FullTransactionId` comparison /
//! conversion macros (`access/transam.h`) operate strictly on the 64-bit
//! `value`, so wraparound logic does not apply.
//!
//! `xid_age` / `mxid_age` read the live shared transaction state
//! (`GetStableLatestTransactionId` / `ReadNextMultiXactId`) through the owning
//! units' seam crates; the function bodies themselves are ported exactly.
//!
//! No `extern "C"`, no `*mut`/`*const`, no `libc`; soft errors flow through
//! `types_error::SoftErrorContext`.

pub mod fmgr_builtins;

use types_core::{CommandId, FullTransactionId, MultiXactId, TransactionId};
use types_datum::Bytea;
use types_error::{PgResult, SoftErrorContext};

// ---------------------------------------------------------------------------
// Local constants matching the C headers.
// ---------------------------------------------------------------------------

/// `#define InvalidMultiXactId  ((MultiXactId) 0)` (`access/multixact.h`).
const InvalidMultiXactId: MultiXactId = 0;

/// `#define FirstNormalTransactionId  ((TransactionId) 3)` (`access/transam.h`).
const FirstNormalTransactionId: TransactionId = 3;

/// `#define INT_MAX` (`<limits.h>`): the "infinitely old" age sentinel.
const INT_MAX: i32 = i32::MAX;

/// `#define MultiXactIdIsValid(multi)  ((multi) != InvalidMultiXactId)`
/// (`access/multixact.h`).
#[inline]
const fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId
}

// ---------------------------------------------------------------------------
// Wraparound-aware TransactionId predicates (`access/transam/transam.c`).
// ---------------------------------------------------------------------------

/// `#define TransactionIdIsNormal(xid)  ((xid) >= FirstNormalTransactionId)`
/// (`access/transam.h`).
#[inline]
const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes` (`access/transam/transam.c`): wraparound-aware "less
/// than". Both ids must be normal for the wraparound test to apply; otherwise
/// (one is a permanent XID) the comparison is a plain unsigned `<`.
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    // If either ID is a permanent XID then we can't do comparison, so we just
    // assume that the smaller one precedes the other one.
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

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

/// `#define FullTransactionIdPrecedesOrEquals(a, b)  ((a).value <= (b).value)`
#[inline]
const fn FullTransactionIdPrecedesOrEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value <= b.value
}

/// `#define FullTransactionIdFollows(a, b)  ((a).value > (b).value)`
#[inline]
const fn FullTransactionIdFollows(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value > b.value
}

/// `#define FullTransactionIdFollowsOrEquals(a, b)  ((a).value >= (b).value)`
#[inline]
const fn FullTransactionIdFollowsOrEquals(a: FullTransactionId, b: FullTransactionId) -> bool {
    a.value >= b.value
}

// ---------------------------------------------------------------------------
// Integer hash folds (`common/hashfn.c`, `access/hash/hashfunc.c`).
// ---------------------------------------------------------------------------

#[inline]
fn hash_uint32(k: u32) -> u32 {
    common_hashfn::hash_bytes_uint32(k)
}

#[inline]
fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    common_hashfn::hash_bytes_uint32_extended(k, seed)
}

/// `hashint8()` (`access/hash/hashfunc.c`): fold an int8 to a 32-bit hash that
/// is compatible with `hashint4`/`hashint2` for logically-equal inputs.
#[inline]
fn hashint8(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    hash_uint32(lohalf)
}

/// `hashint8extended()` (`access/hash/hashfunc.c`): seeded variant of [`hashint8`].
#[inline]
fn hashint8extended(val: i64, seed: u64) -> u64 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(lohalf, seed)
}

/// `pg_cmp_u32()` (`common/int.h`): `(a > b) - (a < b)`.
#[inline]
fn pg_cmp_u32(a: u32, b: u32) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

// ---------------------------------------------------------------------------
// xid
// ---------------------------------------------------------------------------

/// `xidin` (xid.c:33): parse external text format into an `xid`.
///
/// `result = uint32in_subr(str, NULL, "xid", fcinfo->context);`
pub fn xidin(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<TransactionId> {
    let (result, _rest) = backend_utils_adt_numutils::uint32in_subr(s, false, "xid", escontext)?;
    Ok(result as TransactionId)
}

/// `xidout` (xid.c:43): render an `xid` as text.
///
/// `snprintf(result, 16, "%lu", (unsigned long) transactionId);`
pub fn xidout(transactionId: TransactionId) -> String {
    transactionId.to_string()
}

/// `xidrecv` (xid.c:56): external binary format to `xid`.
///
/// `PG_RETURN_TRANSACTIONID((TransactionId) pq_getmsgint(buf, sizeof(TransactionId)));`
pub fn xidrecv(buf: &mut types_stringinfo::StringInfo<'_>) -> PgResult<TransactionId> {
    let v = backend_libpq_pqformat::pq_getmsgint(buf, core::mem::size_of::<TransactionId>() as i32)?;
    Ok(v as TransactionId)
}

/// `xidsend` (xid.c:67): `xid` to external binary format.
///
/// `pq_begintypsend` + `pq_sendint32(arg1)` + `pq_endtypsend`.
pub fn xidsend<'mcx>(mcx: mcx::Mcx<'mcx>, arg1: TransactionId) -> PgResult<Bytea<'mcx>> {
    let mut buf = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendint32(&mut buf, arg1)?;
    Ok(backend_libpq_pqformat::pq_endtypsend(buf))
}

/// `xideq` (xid.c:81): are two xids equal? `TransactionIdEquals(xid1, xid2)`.
pub fn xideq(xid1: TransactionId, xid2: TransactionId) -> bool {
    xid1 == xid2
}

/// `xidneq` (xid.c:93): are two xids different?
pub fn xidneq(xid1: TransactionId, xid2: TransactionId) -> bool {
    xid1 != xid2
}

/// `hashxid` (xid.c:102): `return hash_uint32(PG_GETARG_TRANSACTIONID(0));`
pub fn hashxid(xid: TransactionId) -> u32 {
    hash_uint32(xid)
}

/// `hashxidextended` (xid.c:108).
pub fn hashxidextended(xid: TransactionId, seed: u64) -> u64 {
    hash_uint32_extended(xid, seed)
}

/// `xid_age` (xid.c:117): compute age of an XID relative to the latest stable
/// xid.
///
/// ```c
/// TransactionId now = GetStableLatestTransactionId();
/// if (!TransactionIdIsNormal(xid)) PG_RETURN_INT32(INT_MAX);
/// PG_RETURN_INT32((int32) (now - xid));
/// ```
pub fn xid_age(xid: TransactionId) -> PgResult<i32> {
    let now = backend_access_transam_xact_seams::get_stable_latest_transaction_id::call()?;

    /* Permanent XIDs are always infinitely old */
    if !TransactionIdIsNormal(xid) {
        return Ok(INT_MAX);
    }

    Ok(now.wrapping_sub(xid) as i32)
}

/// `mxid_age` (xid.c:133): compute age of a multi XID relative to the latest
/// stable mxid.
///
/// ```c
/// MultiXactId now = ReadNextMultiXactId();
/// if (!MultiXactIdIsValid(xid)) PG_RETURN_INT32(INT_MAX);
/// PG_RETURN_INT32((int32) (now - xid));
/// ```
pub fn mxid_age(xid: TransactionId) -> PgResult<i32> {
    let now = backend_access_transam_multixact_seams::read_next_multixact_id::call()?;

    if !MultiXactIdIsValid(xid) {
        return Ok(INT_MAX);
    }

    Ok(now.wrapping_sub(xid) as i32)
}

// ---------------------------------------------------------------------------
// XID qsort comparators (not SQL-callable).
// ---------------------------------------------------------------------------

/// `xidComparator` (xid.c:152): qsort comparison for XIDs.
///
/// Uses a plain unsigned comparison (`pg_cmp_u32`), *not* wraparound
/// comparison, because wraparound comparison violates the triangle inequality.
pub fn xidComparator(xid1: TransactionId, xid2: TransactionId) -> i32 {
    pg_cmp_u32(xid1, xid2)
}

/// `xidLogicalComparator` (xid.c:169): qsort comparison for same-epoch XIDs.
///
/// Used to compare only XIDs from the same epoch, so both must be normal and
/// the wraparound comparison (`TransactionIdPrecedes`) is well-defined.
pub fn xidLogicalComparator(xid1: TransactionId, xid2: TransactionId) -> i32 {
    debug_assert!(TransactionIdIsNormal(xid1));
    debug_assert!(TransactionIdIsNormal(xid2));

    if TransactionIdPrecedes(xid1, xid2) {
        return -1;
    }

    if TransactionIdPrecedes(xid2, xid1) {
        return 1;
    }

    0
}

// ---------------------------------------------------------------------------
// xid8
// ---------------------------------------------------------------------------

/// `xid8toxid` (xid.c:187): downcast `xid8` to `xid` (the low 32 bits).
pub fn xid8toxid(fxid: FullTransactionId) -> TransactionId {
    XidFromFullTransactionId(fxid)
}

/// `xid8in` (xid.c:195).
///
/// `result = uint64in_subr(str, NULL, "xid8", fcinfo->context);`
pub fn xid8in(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<FullTransactionId> {
    let (result, _rest) = backend_utils_adt_numutils::uint64in_subr(s, false, "xid8", escontext)?;
    Ok(FullTransactionIdFromU64(result))
}

/// `xid8out` (xid.c:205): `snprintf(result, 21, UINT64_FORMAT, U64FromFullTransactionId(fxid));`
pub fn xid8out(fxid: FullTransactionId) -> String {
    U64FromFullTransactionId(fxid).to_string()
}

/// `xid8recv` (xid.c:215).
///
/// `value = (uint64) pq_getmsgint64(buf);`
pub fn xid8recv(buf: &mut types_stringinfo::StringInfo<'_>) -> PgResult<FullTransactionId> {
    let value = backend_libpq_pqformat::pq_getmsgint64(buf)? as u64;
    Ok(FullTransactionIdFromU64(value))
}

/// `xid8send` (xid.c:225): `pq_sendint64(&buf, (uint64) U64FromFullTransactionId(arg1));`
pub fn xid8send<'mcx>(mcx: mcx::Mcx<'mcx>, arg1: FullTransactionId) -> PgResult<Bytea<'mcx>> {
    let mut buf = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendint64(&mut buf, U64FromFullTransactionId(arg1))?;
    Ok(backend_libpq_pqformat::pq_endtypsend(buf))
}

/// `xid8eq` (xid.c:236).
pub fn xid8eq(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    FullTransactionIdEquals(fxid1, fxid2)
}

/// `xid8ne` (xid.c:245).
pub fn xid8ne(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    !FullTransactionIdEquals(fxid1, fxid2)
}

/// `xid8lt` (xid.c:254).
pub fn xid8lt(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    FullTransactionIdPrecedes(fxid1, fxid2)
}

/// `xid8gt` (xid.c:263).
pub fn xid8gt(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    FullTransactionIdFollows(fxid1, fxid2)
}

/// `xid8le` (xid.c:272).
pub fn xid8le(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    FullTransactionIdPrecedesOrEquals(fxid1, fxid2)
}

/// `xid8ge` (xid.c:281).
pub fn xid8ge(fxid1: FullTransactionId, fxid2: FullTransactionId) -> bool {
    FullTransactionIdFollowsOrEquals(fxid1, fxid2)
}

/// `xid8cmp` (xid.c:290).
pub fn xid8cmp(fxid1: FullTransactionId, fxid2: FullTransactionId) -> i32 {
    if FullTransactionIdFollows(fxid1, fxid2) {
        1
    } else if FullTransactionIdEquals(fxid1, fxid2) {
        0
    } else {
        -1
    }
}

/// `hashxid8` (xid.c:304): `return hashint8(fcinfo);`
///
/// The xid8's 64-bit value is reinterpreted from `uint64` to `int64` and folded
/// through `hashint8`, matching the C fmgr convention.
pub fn hashxid8(fxid: FullTransactionId) -> u32 {
    hashint8(U64FromFullTransactionId(fxid) as i64)
}

/// `hashxid8extended` (xid.c:310): `return hashint8extended(fcinfo);`
pub fn hashxid8extended(fxid: FullTransactionId, seed: u64) -> u64 {
    hashint8extended(U64FromFullTransactionId(fxid) as i64, seed)
}

/// `xid8_larger` (xid.c:316).
pub fn xid8_larger(fxid1: FullTransactionId, fxid2: FullTransactionId) -> FullTransactionId {
    if FullTransactionIdFollows(fxid1, fxid2) {
        fxid1
    } else {
        fxid2
    }
}

/// `xid8_smaller` (xid.c:328).
pub fn xid8_smaller(fxid1: FullTransactionId, fxid2: FullTransactionId) -> FullTransactionId {
    if FullTransactionIdPrecedes(fxid1, fxid2) {
        fxid1
    } else {
        fxid2
    }
}

// ---------------------------------------------------------------------------
// cid
// ---------------------------------------------------------------------------

/// `cidin` (xid.c:347): `result = uint32in_subr(str, NULL, "cid", fcinfo->context);`
pub fn cidin(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<CommandId> {
    let (result, _rest) = backend_utils_adt_numutils::uint32in_subr(s, false, "cid", escontext)?;
    Ok(result as CommandId)
}

/// `cidout` (xid.c:360): `snprintf(result, 16, "%lu", (unsigned long) c);`
pub fn cidout(c: CommandId) -> String {
    c.to_string()
}

/// `cidrecv` (xid.c:373): `(CommandId) pq_getmsgint(buf, sizeof(CommandId))`.
pub fn cidrecv(buf: &mut types_stringinfo::StringInfo<'_>) -> PgResult<CommandId> {
    let v = backend_libpq_pqformat::pq_getmsgint(buf, core::mem::size_of::<CommandId>() as i32)?;
    Ok(v as CommandId)
}

/// `cidsend` (xid.c:384).
pub fn cidsend<'mcx>(mcx: mcx::Mcx<'mcx>, arg1: CommandId) -> PgResult<Bytea<'mcx>> {
    let mut buf = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendint32(&mut buf, arg1)?;
    Ok(backend_libpq_pqformat::pq_endtypsend(buf))
}

/// `cideq` (xid.c:395): `PG_RETURN_BOOL(arg1 == arg2);`
pub fn cideq(arg1: CommandId, arg2: CommandId) -> bool {
    arg1 == arg2
}

/// `hashcid` (xid.c:404): `return hash_uint32(PG_GETARG_COMMANDID(0));`
pub fn hashcid(c: CommandId) -> u32 {
    hash_uint32(c)
}

/// `hashcidextended` (xid.c:410).
pub fn hashcidextended(c: CommandId, seed: u64) -> u64 {
    hash_uint32_extended(c, seed)
}

// ===========================================================================
// fmgr builtin registration.
// ===========================================================================

/// Register the `xid.c` fmgr builtins (the `xid` / `xid8` / `cid` families) into
/// the process-wide fmgr-core registry, so `fmgr_isbuiltin` resolves them on the
/// fast path. Called once, single-threaded, at process startup.
pub fn init_seams() {
    fmgr_builtins::register_xid_builtins();
    fmgr_builtins::register_cid_builtins();
}

#[cfg(test)]
mod tests;
