#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! xid8funcs.c: pg_snapshot I/O + pg_current_xact_id / pg_current_snapshot /
//! pg_visible_in_snapshot / pg_xact_status and the legacy txid_* aliases.

use ::mcx::{Mcx, PgVec};
use ::types_core::xact::{TransactionIdIsNormal, TransactionIdIsValid};
use ::types_core::TransactionId;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};

pub mod builtins;
#[cfg(test)]
mod tests;

pub const USE_BSEARCH_IF_NXIP_GREATER: u32 = 30;

const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

// Payload offsets inside VARDATA: nxip u32 at 0, xmin u64 at 4, xmax u64 at
// 12, xip[] u64s from 20 (C's struct offsets minus the 4-byte length word).
const SNAP_DATA_HDR: usize = 4 + 8 + 8;

pub const PG_SNAPSHOT_MAX_NXIP: usize = (MAX_ALLOC_SIZE - (4 + SNAP_DATA_HDR)) / 8;

const _: () = assert!(
    (::types_storage::MAX_BACKENDS as usize) * 2 <= PG_SNAPSHOT_MAX_NXIP,
    "possible overflow in pg_current_snapshot()"
);

// Borrowed decode of a detoasted pg_snapshot payload (VARDATA bytes).
#[derive(Clone, Copy)]
pub struct SnapView<'a> {
    data: &'a [u8],
}

impl<'a> SnapView<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert!(data.len() >= SNAP_DATA_HDR);
        SnapView { data }
    }

    #[inline]
    fn u32_at(&self, off: usize) -> u32 {
        u32::from_ne_bytes(self.data[off..off + 4].try_into().unwrap())
    }

    #[inline]
    fn u64_at(&self, off: usize) -> u64 {
        u64::from_ne_bytes(self.data[off..off + 8].try_into().unwrap())
    }

    pub fn nxip(&self) -> u32 {
        self.u32_at(0)
    }

    pub fn xmin(&self) -> u64 {
        self.u64_at(4)
    }

    pub fn xmax(&self) -> u64 {
        self.u64_at(12)
    }

    pub fn xip(&self, i: usize) -> u64 {
        self.u64_at(SNAP_DATA_HDR + 8 * i)
    }
}

#[inline]
const fn fxid_is_valid(fxid: u64) -> bool {
    TransactionIdIsValid(fxid as TransactionId)
}

// access/transam.h FullTransactionIdFromAllowableAt over a pre-read nextXid.
#[inline]
pub fn full_xid_from_allowable_at(next_full_xid: u64, xid: TransactionId) -> u64 {
    if !TransactionIdIsNormal(xid) {
        return xid as u64;
    }
    let mut epoch = (next_full_xid >> 32) as u32;
    if xid > next_full_xid as u32 {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }
    ((epoch as u64) << 32) | xid as u64
}

pub fn is_visible_fxid(value: u64, snap: &SnapView<'_>) -> bool {
    if value < snap.xmin() {
        true
    } else if value >= snap.xmax() {
        false
    } else if snap.nxip() > USE_BSEARCH_IF_NXIP_GREATER {
        let mut lo = 0usize;
        let mut hi = snap.nxip() as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let probe = snap.xip(mid);
            if probe == value {
                return false;
            } else if probe < value {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        true
    } else {
        for i in 0..snap.nxip() as usize {
            if snap.xip(i) == value {
                return false;
            }
        }
        true
    }
}

// libc strtou64(str, &endp, 10): skip isspace, optional sign (a '-' negates
// mod 2^64), digits; saturates to u64::MAX on overflow; end == 0 if no digits.
pub fn strtou64(s: &[u8]) -> (u64, usize) {
    let mut i = 0usize;
    while i < s.len() && matches!(s[i], b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r') {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        neg = s[i] == b'-';
        i += 1;
    }
    let digits_start = i;
    let mut value: u64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        let d = (s[i] - b'0') as u64;
        match value.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => value = v,
            None => overflow = true,
        }
        i += 1;
    }
    if i == digits_start {
        return (0, 0);
    }
    if overflow {
        value = u64::MAX;
    }
    if neg {
        value = value.wrapping_neg();
    }
    (value, i)
}

pub fn snapshot_image<'mcx>(
    mcx: Mcx<'mcx>,
    xmin: u64,
    xmax: u64,
    xips: &[u64],
) -> PgResult<::datum::Varlena<'mcx>> {
    let total = 4 + SNAP_DATA_HDR + 8 * xips.len();
    let mut image: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut image, &(xips.len() as u32).to_ne_bytes())?;
    ::mcx::vec_append_bytes(&mut image, &xmin.to_ne_bytes())?;
    ::mcx::vec_append_bytes(&mut image, &xmax.to_ne_bytes())?;
    for &x in xips {
        ::mcx::vec_append_bytes(&mut image, &x.to_ne_bytes())?;
    }
    Ok(::datum::Varlena::from_image(image))
}

#[cold]
#[inline(never)]
fn bad_format_err(input: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type pg_snapshot: \"{input}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

// parse_snapshot: "xmin:xmax:xip,xip,..."; Ok(None) = soft-reported failure.
pub fn parse_snapshot<'mcx>(
    mcx: Mcx<'mcx>,
    input: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<::datum::Varlena<'mcx>>> {
    let bytes = input.as_bytes();

    let (xmin, end) = strtou64(bytes);
    let mut pos = end;
    if bytes.get(pos) != Some(&b':') {
        return ereturn(escontext.as_deref_mut(), None, bad_format_err(input));
    }
    pos += 1;

    let (xmax, end) = strtou64(&bytes[pos..]);
    pos += end;
    if bytes.get(pos) != Some(&b':') {
        return ereturn(escontext.as_deref_mut(), None, bad_format_err(input));
    }
    pos += 1;

    if !fxid_is_valid(xmin) || !fxid_is_valid(xmax) || xmax < xmin {
        return ereturn(escontext.as_deref_mut(), None, bad_format_err(input));
    }

    let mut xips: PgVec<'mcx, u64> = ::mcx::vec_new_in(mcx);
    let mut last_val: u64 = 0;
    while pos < bytes.len() {
        let (val, end) = strtou64(&bytes[pos..]);
        pos += end;

        if val < xmin || val >= xmax || val < last_val {
            return ereturn(escontext.as_deref_mut(), None, bad_format_err(input));
        }

        if val != last_val {
            xips.push(val);
        }
        last_val = val;

        match bytes.get(pos) {
            Some(&b',') => pos += 1,
            None => {}
            Some(_) => return ereturn(escontext.as_deref_mut(), None, bad_format_err(input)),
        }
    }

    Ok(Some(snapshot_image(mcx, xmin, xmax, &xips)?))
}

pub fn snapshot_out_bytes<'mcx>(mcx: Mcx<'mcx>, snap: &SnapView<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let nxip = snap.nxip() as usize;
    let mut out: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, 2 + 21 * (2 + nxip))?;
    push_u64(&mut out, snap.xmin());
    out.push(b':');
    push_u64(&mut out, snap.xmax());
    out.push(b':');
    for i in 0..nxip {
        if i > 0 {
            out.push(b',');
        }
        push_u64(&mut out, snap.xip(i));
    }
    Ok(out)
}

fn push_u64(out: &mut PgVec<'_, u8>, v: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut v = v;
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

#[track_caller]
#[cold]
#[inline(never)]
fn recv_bad_format() -> Box<PgError> {
    Box::new(
        PgError::error("invalid external pg_snapshot data")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION),
    )
}

pub fn snapshot_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut ::stringinfo::StringInfo<'_>,
) -> PgResult<::datum::Varlena<'mcx>> {
    let nxip = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    if nxip < 0 || nxip as usize > PG_SNAPSHOT_MAX_NXIP {
        return Err(recv_bad_format());
    }
    let xmin = ::pqformat::pq_getmsgint64(buf)? as u64;
    let xmax = ::pqformat::pq_getmsgint64(buf)? as u64;
    if !fxid_is_valid(xmin) || !fxid_is_valid(xmax) || xmax < xmin {
        return Err(recv_bad_format());
    }

    let mut xips: PgVec<'mcx, u64> = ::mcx::vec_with_capacity_in(mcx, nxip as usize)?;
    let mut last: u64 = 0;
    let mut i: i32 = 0;
    let mut nxip = nxip;
    while i < nxip {
        let cur = ::pqformat::pq_getmsgint64(buf)? as u64;
        if cur < last || cur < xmin || cur > xmax {
            return Err(recv_bad_format());
        }
        if cur == last {
            nxip -= 1;
            continue;
        }
        xips.push(cur);
        last = cur;
        i += 1;
    }
    snapshot_image(mcx, xmin, xmax, &xips)
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_execute_during_recovery(cmdname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot execute {cmdname} during recovery"))
            .with_sqlstate(::types_error::ERRCODE_READ_ONLY_SQL_TRANSACTION),
    )
}

pub fn pg_current_xact_id() -> PgResult<u64> {
    if transam_xlog_seams::recovery_in_progress::call() {
        return Err(cannot_execute_during_recovery("pg_current_xact_id()"));
    }
    Ok(::xact::GetTopFullTransactionId()?.to_u64())
}

pub fn pg_current_xact_id_if_assigned() -> Option<u64> {
    let topfxid = ::xact::GetTopFullTransactionIdIfAny();
    topfxid.is_valid().then(|| topfxid.to_u64())
}

pub fn pg_current_snapshot<'mcx>(mcx: Mcx<'mcx>) -> PgResult<::datum::Varlena<'mcx>> {
    let next_fxid = ::varsup::ReadNextFullTransactionId()?.to_u64();

    if !::snapmgr::ActiveSnapshotSet() {
        return Err(Box::new(PgError::error("no active snapshot set")));
    }
    let cur = ::snapmgr::GetActiveSnapshot();

    let xmin = full_xid_from_allowable_at(next_fxid, cur.xmin);
    let xmax = full_xid_from_allowable_at(next_fxid, cur.xmax);
    let mut xips: PgVec<'mcx, u64> = ::mcx::vec_with_capacity_in(mcx, cur.xcnt as usize)?;
    for i in 0..cur.xcnt as usize {
        xips.push(full_xid_from_allowable_at(next_fxid, cur.xip[i]));
    }

    // sort_snapshot: ascending + dedup (2PC prepare can transiently duplicate
    // an XID between a backend and its dummy PGPROC).
    xips.sort_unstable();
    let mut w = 0usize;
    for r in 0..xips.len() {
        if r == 0 || xips[r] != xips[w - 1] {
            xips[w] = xips[r];
            w += 1;
        }
    }
    xips.truncate(w);

    snapshot_image(mcx, xmin, xmax, &xips)
}

fn transaction_id_in_recent_past(fxid: u64) -> PgResult<(bool, TransactionId)> {
    let xid = fxid as TransactionId;
    let now_fullxid = ::varsup::ReadNextFullTransactionId()?.to_u64();

    if !TransactionIdIsValid(xid) {
        return Ok((false, xid));
    }
    if !TransactionIdIsNormal(xid) {
        return Ok((true, xid));
    }
    if fxid >= now_fullxid {
        return Err(Box::new(
            PgError::error(format!("transaction ID {fxid} is in the future"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    let oldest_clog_xid = ::varsup::TransamVariables()
        .oldestClogXid
        .load(core::sync::atomic::Ordering::Relaxed);
    let oldest_clog_fxid = full_xid_from_allowable_at(now_fullxid, oldest_clog_xid);
    Ok((fxid >= oldest_clog_fxid, xid))
}

pub fn pg_xact_status(fxid: u64) -> PgResult<Option<&'static str>> {
    // Concurrent clog truncation must be excluded across the clog lookups.
    let lock = ::lwlock::main_lock(::varsup::XACT_TRUNCATION_LOCK);
    ::lwlock::LWLockAcquire(
        lock,
        ::lwlock::LW_SHARED,
        ::init_small::globals::MyProcNumber(),
    )?;
    let result = pg_xact_status_locked(fxid);
    let released = ::lwlock::LWLockRelease(lock);
    let status = result?;
    released?;
    Ok(status)
}

fn pg_xact_status_locked(fxid: u64) -> PgResult<Option<&'static str>> {
    let (determinable, xid) = transaction_id_in_recent_past(fxid)?;
    if !determinable {
        return Ok(None);
    }
    debug_assert!(TransactionIdIsValid(xid));
    if procarray_seams::transaction_id_is_in_progress::call(xid)? {
        Ok(Some("in progress"))
    } else if transam_seams::transaction_id_did_commit::call(xid)? {
        Ok(Some("committed"))
    } else {
        Ok(Some("aborted"))
    }
}
