//! stub:snapshot — shared CONSTRUCTED-STATE builder: SnapshotData as a plain
//! value (xmin, xmax, xip[], subxip[], flags, curcid, ...) built identically
//! on the Rust side (types/snapshot SnapshotData) and on the C-oracle side
//! (csrc/pg_stub_snapshot.c, struct definition vendored from 18.3
//! src/include/utils/snapshot.h) from the same fuzz bytes.
//!
//! Unlocks visibility arithmetic (XidInMVCCSnapshot and the pure core of
//! heapam_visibility) with zero transaction machinery: a future target
//! decodes a SnapSpec, hands `snap_wire` to its C oracle, builds the Rust
//! side with `build_rust_snapshot`, and compares whatever it computes over
//! the two structures. The construction itself is compared through the
//! field-plane serializers (`ser_snapshot_plane` here == SECTION-S writer in
//! the C shim), so a builder asymmetry is a caught divergence, never silent
//! agreement.
//!
//! BOTH-SIDES DISCIPLINE: neither side defaults anything — every compared
//! field comes from the spec bytes. Fields NOT in the compared plane are the
//! environment pointers C also leaves meaningless here (vistest, ph_node)
//! and the Rust-only marshal cells (dirty_*, refcounts) — all pinned to
//! zero on both sides and documented.
//!
//! CLAMPS (part of the compared-input contract; applied to the SPEC before
//! either side builds):
//!   - snapshot_type : u8 % 7            (the 7 SnapshotType arms)
//!   - xcnt          : u8 % (MAX_XIP+1)  (0..=64)
//!   - subxcnt       : u8 % (MAX_XIP+1)  (0..=64)
//!   - xmin/xmax/xip[]/subxip[]/curcid/speculativeToken: raw LE u32 — NOT
//!     normalized (xmin <= xmax and xip in [xmin,xmax) are C invariants a
//!     *consumer* target may impose; the builder never fabricates them)
//!   - flags byte    : bit0 suboverflowed, bit1 takenDuringRecovery,
//!                     bit2 copied
//!
//! WIRE (== the C decoder in pg_stub_snapshot.c; keep in lockstep):
//!   [u8 type][u32 xmin][u32 xmax][u8 xcnt][u32 xip]*xcnt
//!   [u8 subxcnt][u32 subxip]*subxcnt [u8 flags][u32 curcid]
//!   [u32 speculativeToken][u64 snapXactCompletionCount]     (all LE)

extern crate alloc;

use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_snapshot::{SnapshotData, SnapshotType};
use types_core::{CommandId, GlobalVisStateHandle, TransactionId};

use crate::stub_tupdesc::Cursor;

/// xip/subxip length ceiling (identical both sides; documented clamp).
pub const MAX_XIP: usize = 64;

/// Normalized snapshot spec — the compared input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapSpec {
    pub snapshot_type: u8, // 0..=6, SnapshotType enum order
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub xip: Vec<TransactionId>,
    pub subxip: Vec<TransactionId>,
    pub flags: u8, // bit0 suboverflowed, bit1 takenDuringRecovery, bit2 copied
    pub curcid: CommandId,
    pub speculative_token: u32,
    pub snap_xact_completion_count: u64,
}

/// Decode a SnapSpec from fuzz bytes (missing bytes read as 0, Cursor
/// contract — identical both sides because only the WIRE reaches C).
pub fn decode_snap(cur: &mut Cursor<'_>) -> SnapSpec {
    let snapshot_type = cur.u8() % 7;
    let xmin = u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]);
    let xmax = u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]);
    let xcnt = cur.u8() as usize % (MAX_XIP + 1);
    let mut xip = Vec::with_capacity(xcnt);
    for _ in 0..xcnt {
        xip.push(u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]));
    }
    let subxcnt = cur.u8() as usize % (MAX_XIP + 1);
    let mut subxip = Vec::with_capacity(subxcnt);
    for _ in 0..subxcnt {
        subxip.push(u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]));
    }
    let flags = cur.u8() & 0x07;
    let curcid = u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]);
    let speculative_token = u32::from_le_bytes([cur.u8(), cur.u8(), cur.u8(), cur.u8()]);
    let mut xcc = [0u8; 8];
    for b in xcc.iter_mut() {
        *b = cur.u8();
    }
    SnapSpec {
        snapshot_type,
        xmin,
        xmax,
        xip,
        subxip,
        flags,
        curcid,
        speculative_token,
        snap_xact_completion_count: u64::from_le_bytes(xcc),
    }
}

/// Wire-encode the spec for the C shim (see WIRE above).
pub fn snap_wire(s: &SnapSpec) -> Vec<u8> {
    let mut w = Vec::with_capacity(32 + 4 * (s.xip.len() + s.subxip.len()));
    w.push(s.snapshot_type);
    w.extend_from_slice(&s.xmin.to_le_bytes());
    w.extend_from_slice(&s.xmax.to_le_bytes());
    w.push(s.xip.len() as u8);
    for x in &s.xip {
        w.extend_from_slice(&x.to_le_bytes());
    }
    w.push(s.subxip.len() as u8);
    for x in &s.subxip {
        w.extend_from_slice(&x.to_le_bytes());
    }
    w.push(s.flags);
    w.extend_from_slice(&s.curcid.to_le_bytes());
    w.extend_from_slice(&s.speculative_token.to_le_bytes());
    w.extend_from_slice(&s.snap_xact_completion_count.to_le_bytes());
    w
}

fn snap_type(t: u8) -> SnapshotType {
    match t {
        0 => SnapshotType::SNAPSHOT_MVCC,
        1 => SnapshotType::SNAPSHOT_SELF,
        2 => SnapshotType::SNAPSHOT_ANY,
        3 => SnapshotType::SNAPSHOT_TOAST,
        4 => SnapshotType::SNAPSHOT_DIRTY,
        5 => SnapshotType::SNAPSHOT_HISTORIC_MVCC,
        _ => SnapshotType::SNAPSHOT_NON_VACUUMABLE,
    }
}

/// Build the Rust SnapshotData from the spec (mirror of the C shim's
/// decoder: same field-by-field staging, nothing defaulted).
pub fn build_rust_snapshot<'m>(mcx: Mcx<'m>, s: &SnapSpec) -> SnapshotData<'m> {
    let mut xip: PgVec<'m, TransactionId> = PgVec::new_in(mcx);
    for x in &s.xip {
        xip.push(*x);
    }
    let mut subxip: PgVec<'m, TransactionId> = PgVec::new_in(mcx);
    for x in &s.subxip {
        subxip.push(*x);
    }
    let mut snap = SnapshotData::sentinel(mcx, snap_type(s.snapshot_type));
    snap.xmin = s.xmin;
    snap.xmax = s.xmax;
    snap.xcnt = s.xip.len() as u32;
    snap.xip = xip;
    snap.subxcnt = s.subxip.len() as i32;
    snap.subxip = subxip;
    snap.suboverflowed = s.flags & 0x01 != 0;
    snap.takenDuringRecovery = s.flags & 0x02 != 0;
    snap.copied = s.flags & 0x04 != 0;
    snap.curcid.set(s.curcid);
    snap.speculativeToken = s.speculative_token;
    snap.snapXactCompletionCount = s.snap_xact_completion_count;
    // Environment pointers / Rust-only marshal cells: pinned zero (sentinel)
    // on this side; the C shim's vistest/ph_node stay zeroed. NOT compared.
    snap.vistest = GlobalVisStateHandle::new(0);
    snap
}

/// Field-plane serializer — MUST stay in lockstep with the C shim's
/// SECTION-S writer (pg_stub_snapshot.c). The C side serializes FROM ITS
/// CONSTRUCTED STRUCT, never from the wire, so construction differences are
/// visible here.
pub fn ser_snapshot_plane(w: &mut Vec<u8>, s: &SnapshotData<'_>) {
    w.extend_from_slice(&(s.snapshot_type as i32 as u32).to_le_bytes());
    w.extend_from_slice(&s.xmin.to_le_bytes());
    w.extend_from_slice(&s.xmax.to_le_bytes());
    w.extend_from_slice(&s.xcnt.to_le_bytes());
    for x in s.xip[..s.xcnt as usize].iter() {
        w.extend_from_slice(&x.to_le_bytes());
    }
    w.extend_from_slice(&(s.subxcnt as u32).to_le_bytes());
    for x in s.subxip[..s.subxcnt as usize].iter() {
        w.extend_from_slice(&x.to_le_bytes());
    }
    w.push(u8::from(s.suboverflowed));
    w.push(u8::from(s.takenDuringRecovery));
    w.push(u8::from(s.copied));
    w.extend_from_slice(&s.curcid.get().to_le_bytes());
    w.extend_from_slice(&s.speculativeToken.to_le_bytes());
    w.extend_from_slice(&s.snapXactCompletionCount.to_le_bytes());
}

extern "C" {
    /// csrc/pg_stub_snapshot.c: decode `wire`, construct a C SnapshotData,
    /// serialize its fields (SECTION-S). Returns 0 ok / negative = harness
    /// internal failure (undersized output buffer or truncated wire).
    fn pg_stub_snapshot_build(
        wire: *const u8,
        wirelen: core::ffi::c_int,
        out: *mut u8,
        outcap: core::ffi::c_int,
        outlen: *mut core::ffi::c_int,
    ) -> core::ffi::c_int;
}

/// C-side plane for a wire (test/differential helper).
pub fn c_snapshot_plane(wire: &[u8]) -> Vec<u8> {
    let mut out = alloc::vec![0u8; 4096];
    let mut outlen: core::ffi::c_int = 0;
    // SAFETY: buffers live for the call.
    let st = unsafe {
        pg_stub_snapshot_build(
            wire.as_ptr(),
            wire.len() as core::ffi::c_int,
            out.as_mut_ptr(),
            out.len() as core::ffi::c_int,
            &mut outlen,
        )
    };
    assert!(st == 0, "C snapshot builder internal failure {st}");
    out.truncate(outlen as usize);
    out
}

/// The dual-construction differential: build both sides from the same spec
/// and panic on any field-plane difference. Consumer targets call this once
/// per exec before computing over the snapshot.
pub fn assert_snapshot_construction_agrees(mcx: Mcx<'_>, s: &SnapSpec) {
    let wire = snap_wire(s);
    let cplane = c_snapshot_plane(&wire);
    let snap = build_rust_snapshot(mcx, s);
    let mut rplane = Vec::new();
    ser_snapshot_plane(&mut rplane, &snap);
    assert_eq!(
        rplane, cplane,
        "stub:snapshot construction divergence (spec = {s:?})"
    );
}
