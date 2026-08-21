//! Shipped-in-lib test kit (the `pgrc2_write::testkit` posture): the
//! deterministic heap model the M5-B batteries drive, and the seeding
//! faces M5-N and the M5-O bar rigs consume.
//!
//! ## What [`SimHeap`] models (and what it deliberately does not)
//!
//! A faithful **heap-MVCC semantics model** — tuples stamped
//! (xmin, cmin, xmax, cmax), top-level + sub-transactions (parent chains,
//! sub-abort/sub-commit, txn-global command counter), MVCC snapshots
//! (xmin/xmax/in-progress set/curcid) with `HeapTupleSatisfiesMVCC`-shaped
//! visibility — plus a **WAL-as-source-of-truth durability model** (every
//! mutation appends a record; a crash replays exactly the flushed prefix;
//! commit-then-ack choreography with a seedable sync-skip — the #253
//! tooth's knob). No file bytes, no clocks, no RNG: schedules are explicit
//! and every run is deterministic.
//!
//! At PRODUCT grain none of this code runs: the delta pair is real heap
//! (real WAL, real snapshots, C-exact by construction), and M5-M binds the
//! seams to it. The model exists so the M5-B differentials can drive
//! subxact/combo-cid/concurrent schedules and kill-9 ladders at crate
//! grain — the M3-G pattern applied to the write side.
//!
//! ## Fault knobs (born-RED teeth; scan-path only)
//!
//! - [`SimHeap::skip_commit_flush`]: commit acks WITHOUT flushing the
//!   commit record (the exact #248/#253 defect shape). The crash checker
//!   must then report loss — proving it CAN.
//! - [`SkewTarget`]: perturbs the SCAN faces only (the visible-tombstone
//!   scan or the delta feed), never the per-row oracle — the seeded
//!   visibility skew that makes the scan-merge/bitmap differentials go
//!   red.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::bitmap::{DeletionIndex, DeletionIndexBuilder};
use crate::feed::{DeltaCell, DeltaFeed, DeltaRowsWindow};
use crate::lifecycle::{DeltaBinding, DeltaLifecycle};
use crate::ops::DeltaWrite;
use crate::tombstone::{rowid_of_tombstone, tombstone_payload};
use crate::{DeltaError, DeltaResult};

pub type Xid = u64;
pub type RelId = u64;
pub type Tid = (u32, u16);

/// Rows per sim heap block (small on purpose: multi-block coverage at tiny
/// corpus sizes).
pub const SIM_ROWS_PER_BLOCK: u32 = 64;

const FIRST_XID: Xid = 100;

#[inline]
fn tid_of_index(idx: usize) -> Tid {
    // Release-effective bound (the silent-lossy-serialization class): an
    // `idx as u32` wrap would alias two tuples onto one tid.
    assert!(idx <= u32::MAX as usize, "sim heap: tuple index beyond tid space");
    (
        (idx as u32) / SIM_ROWS_PER_BLOCK,
        ((idx as u32) % SIM_ROWS_PER_BLOCK + 1) as u16,
    )
}

#[inline]
fn index_of_tid(tid: Tid) -> DeltaResult<usize> {
    if tid.1 == 0 || tid.1 as u32 > SIM_ROWS_PER_BLOCK {
        return Err(DeltaError::Contract { detail: "sim tid offset out of range" });
    }
    Ok((tid.0 as usize) * SIM_ROWS_PER_BLOCK as usize + (tid.1 as usize - 1))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XidStatus {
    Committed,
    Aborted,
}

#[derive(Clone, Debug)]
struct SimTuple {
    xmin: Xid,
    cmin: u32,
    xmax: Option<Xid>,
    cmax: Option<u32>,
    row: Vec<DeltaCell>,
}

#[derive(Clone, Debug, Default)]
struct SimRel {
    tuples: Vec<SimTuple>,
}

/// WAL record vocabulary (the durability source of truth: a crash replays
/// exactly the flushed prefix).
#[derive(Clone, Debug)]
pub enum WalRec {
    Insert { rel: RelId, idx: usize, xmin: Xid, cmin: u32, row: Vec<DeltaCell> },
    Delete { rel: RelId, idx: usize, xmax: Xid, cmax: u32 },
    Commit { xid: Xid, subxids: Vec<Xid> },
    Abort { xid: Xid, subxids: Vec<Xid> },
    CreatePair { table: RelId, binding: DeltaBinding },
    ResetPair { table: RelId },
}

#[derive(Clone, Debug)]
struct OpenTxn {
    /// Stamp stack: `[top, sub, subsub, …]`; writes stamp the top of it.
    stamp_stack: Vec<Xid>,
    /// Every subxid ever started under this top (live, committed or
    /// aborted — final commit resolves the non-aborted ones).
    subxids: Vec<Xid>,
    /// Txn-global command counter (PG: cids span subxacts).
    next_cid: u32,
}

/// Scan-path-only visibility skew (the seeded teeth; the oracle never
/// consults these).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkewTarget {
    /// The visible-tombstone scan omits this sealed rowid.
    HideTombstoneRowid(u64),
    /// The delta feed emits the tuple at this tid even when invisible.
    ShowDeltaTid(Tid),
}

/// MVCC snapshot (taken via [`SimHeap::snapshot`] /
/// [`SimHeap::snapshot_of_current`]).
#[derive(Clone, Debug)]
pub struct SimSnapshot {
    xmin: Xid,
    xmax: Xid,
    /// In-progress xids (top + sub) at snapshot time, own excluded.
    xip: BTreeSet<Xid>,
    /// Snapshot taker's top-level xid, if taken inside a transaction.
    own_top: Option<Xid>,
    /// Command-id horizon: own-txn effects with cid < curcid are visible.
    curcid: u32,
}

impl SimSnapshot {
    /// The snapshot's oldest-in-progress bound (every xid < xmin is
    /// resolved). The M5-N horizon rung's consumption face: a tombstone is
    /// horizon-passed when its xmin < the oldest snapshot's xmin (and
    /// committed) — only then may it compile into a DV.
    pub fn xmin(&self) -> Xid {
        self.xmin
    }
}

/// The deterministic heap + xact + WAL model.
#[derive(Debug, Default)]
pub struct SimHeap {
    rels: BTreeMap<RelId, SimRel>,
    next_rel: RelId,
    bindings: BTreeMap<RelId, DeltaBinding>,
    next_xid: Xid,
    clog: BTreeMap<Xid, XidStatus>,
    /// sub → TOP-LEVEL parent (chains collapse to the top; cid space is
    /// the top's).
    parents: BTreeMap<Xid, Xid>,
    open_txns: BTreeMap<Xid, OpenTxn>,
    /// The transaction the write faces stamp (multi-txn schedules switch
    /// it explicitly).
    current: Option<Xid>,
    wal: Vec<WalRec>,
    flushed: usize,
    /// (xid, wal length at ack) — the checker's "was acked" witness.
    acked: Vec<(Xid, usize)>,
    /// Op-boundary checkpoints: (wal length, flushed) after every
    /// mutating op — the kill -9 ladder's crash points.
    op_log: Vec<(usize, usize)>,
    /// The #253 seeded sync-skip (commit acks without flushing).
    pub skip_commit_flush: bool,
    /// Scan-path visibility skew (the differential teeth).
    pub skew: Option<SkewTarget>,
}

impl SimHeap {
    pub fn new() -> SimHeap {
        SimHeap { next_rel: 1, next_xid: FIRST_XID, ..SimHeap::default() }
    }

    fn checkpoint(&mut self) {
        self.op_log.push((self.wal.len(), self.flushed));
    }

    // -- xact API ------------------------------------------------------------

    /// Begin a top-level transaction; becomes current.
    pub fn begin(&mut self) -> Xid {
        let xid = self.next_xid;
        self.next_xid += 1;
        self.open_txns.insert(
            xid,
            OpenTxn { stamp_stack: vec![xid], subxids: Vec::new(), next_cid: 0 },
        );
        self.current = Some(xid);
        xid
    }

    /// Switch the write faces to another OPEN transaction (concurrent
    /// schedules).
    pub fn set_current(&mut self, xid: Xid) {
        assert!(self.open_txns.contains_key(&xid), "set_current on a closed txn");
        self.current = Some(xid);
    }

    fn current_txn(&mut self) -> DeltaResult<(Xid, &mut OpenTxn)> {
        let xid = self
            .current
            .ok_or(DeltaError::Contract { detail: "write outside a transaction" })?;
        let t = self
            .open_txns
            .get_mut(&xid)
            .ok_or(DeltaError::Contract { detail: "current txn is closed" })?;
        Ok((xid, t))
    }

    /// Begin a subtransaction of the CURRENT transaction; writes stamp it
    /// until `commit_sub`/`abort_sub`.
    pub fn begin_sub(&mut self) -> DeltaResult<Xid> {
        let sub = self.next_xid;
        self.next_xid += 1;
        let (top, t) = self.current_txn()?;
        t.stamp_stack.push(sub);
        t.subxids.push(sub);
        self.parents.insert(sub, top);
        Ok(sub)
    }

    /// Subcommit: the subxid's fate now follows the top-level outcome.
    pub fn commit_sub(&mut self, sub: Xid) -> DeltaResult<()> {
        let (_, t) = self.current_txn()?;
        if t.stamp_stack.last() != Some(&sub) {
            return Err(DeltaError::Contract { detail: "commit_sub out of order" });
        }
        t.stamp_stack.pop();
        Ok(())
    }

    /// Subabort: the subxid is Aborted immediately (its writes are dead
    /// regardless of the top-level outcome).
    pub fn abort_sub(&mut self, sub: Xid) -> DeltaResult<()> {
        let (_, t) = self.current_txn()?;
        if t.stamp_stack.last() != Some(&sub) {
            return Err(DeltaError::Contract { detail: "abort_sub out of order" });
        }
        t.stamp_stack.pop();
        self.clog.insert(sub, XidStatus::Aborted);
        Ok(())
    }

    /// CommandCounterIncrement for the current transaction.
    pub fn cci(&mut self) -> DeltaResult<()> {
        let (_, t) = self.current_txn()?;
        t.next_cid += 1;
        Ok(())
    }

    /// Commit the current transaction. `sync = true` flushes the commit
    /// record BEFORE the ack — unless the seeded `skip_commit_flush` knob
    /// is armed (the #253 defect shape). The ack is the return itself
    /// (recorded for the crash checker).
    pub fn commit(&mut self, sync: bool) -> DeltaResult<()> {
        let (xid, t) = self.current_txn()?;
        if t.stamp_stack.len() != 1 {
            return Err(DeltaError::Contract { detail: "commit with open subxacts" });
        }
        let subxids = t.subxids.clone();
        let live_subs: Vec<Xid> = subxids
            .iter()
            .copied()
            .filter(|s| self.clog.get(s) != Some(&XidStatus::Aborted))
            .collect();
        self.wal.push(WalRec::Commit { xid, subxids: live_subs.clone() });
        if sync && !self.skip_commit_flush {
            self.flushed = self.wal.len();
        }
        self.clog.insert(xid, XidStatus::Committed);
        for s in live_subs {
            self.clog.insert(s, XidStatus::Committed);
        }
        self.open_txns.remove(&xid);
        self.current = None;
        self.acked.push((xid, self.wal.len()));
        self.checkpoint();
        Ok(())
    }

    /// Abort the current transaction (top + every non-aborted subxid).
    pub fn abort(&mut self) -> DeltaResult<()> {
        let xid = self
            .current
            .take()
            .ok_or(DeltaError::Contract { detail: "abort outside a transaction" })?;
        let t = self
            .open_txns
            .remove(&xid)
            .ok_or(DeltaError::Contract { detail: "abort of a closed txn" })?;
        let subxids = t.subxids;
        self.wal.push(WalRec::Abort { xid, subxids: subxids.clone() });
        self.clog.insert(xid, XidStatus::Aborted);
        for s in subxids {
            self.clog.entry(s).or_insert(XidStatus::Aborted);
        }
        self.checkpoint();
        Ok(())
    }

    /// Explicit WAL flush (the walwriter/XLogFlush analog).
    pub fn flush_wal(&mut self) {
        self.flushed = self.wal.len();
        self.checkpoint();
    }

    // -- snapshots + visibility ----------------------------------------------

    fn live_xids(&self) -> BTreeSet<Xid> {
        let mut xip = BTreeSet::new();
        for (top, t) in &self.open_txns {
            xip.insert(*top);
            for s in &t.subxids {
                if self.clog.get(s) != Some(&XidStatus::Aborted) {
                    xip.insert(*s);
                }
            }
        }
        xip
    }

    /// Snapshot from OUTSIDE any transaction (a fresh observer).
    pub fn snapshot(&self) -> SimSnapshot {
        let xip = self.live_xids();
        SimSnapshot {
            xmin: xip.iter().next().copied().unwrap_or(self.next_xid),
            xmax: self.next_xid,
            xip,
            own_top: None,
            curcid: 0,
        }
    }

    /// Snapshot for the CURRENT transaction at its current command.
    pub fn snapshot_of_current(&self) -> DeltaResult<SimSnapshot> {
        let own = self
            .current
            .ok_or(DeltaError::Contract { detail: "snapshot_of_current outside a txn" })?;
        let t = &self.open_txns[&own];
        let mut xip = self.live_xids();
        xip.remove(&own);
        for s in &t.subxids {
            xip.remove(s);
        }
        Ok(SimSnapshot {
            xmin: xip.iter().next().copied().unwrap_or(self.next_xid),
            xmax: self.next_xid,
            xip,
            own_top: Some(own),
            curcid: t.next_cid,
        })
    }

    #[inline]
    fn top_of(&self, xid: Xid) -> Xid {
        self.parents.get(&xid).copied().unwrap_or(xid)
    }

    fn xid_committed_for(&self, xid: Xid, snap: &SimSnapshot) -> bool {
        // Committed AND not in-progress at snapshot time AND assigned
        // before the snapshot horizon.
        self.clog.get(&xid) == Some(&XidStatus::Committed)
            && xid < snap.xmax
            && !snap.xip.contains(&xid)
    }

    /// `HeapTupleSatisfiesMVCC`-shaped visibility (the oracle law; shared
    /// by the scan faces and the naive per-row oracle — independence
    /// between differential arms lives in the AGGREGATION paths, and at
    /// product grain visibility is the real heap's, C-exact by
    /// construction).
    fn tuple_visible(&self, snap: &SimSnapshot, t: &SimTuple) -> bool {
        let own_insert = snap.own_top == Some(self.top_of(t.xmin));
        let inserted = if own_insert {
            self.clog.get(&t.xmin) != Some(&XidStatus::Aborted) && t.cmin < snap.curcid
        } else {
            self.xid_committed_for(t.xmin, snap)
        };
        if !inserted {
            return false;
        }
        let Some(xmax) = t.xmax else { return true };
        if self.clog.get(&xmax) == Some(&XidStatus::Aborted) {
            return true;
        }
        if snap.own_top == Some(self.top_of(xmax)) {
            // Own delete: visible until the deleting command's horizon.
            t.cmax.is_none_or(|cmax| cmax >= snap.curcid)
        } else {
            !self.xid_committed_for(xmax, snap)
        }
    }

    // -- relation plumbing ---------------------------------------------------

    fn rel_mut(&mut self, rel: RelId) -> DeltaResult<&mut SimRel> {
        self.rels
            .get_mut(&rel)
            .ok_or(DeltaError::Heap { at: "rel lookup", detail: format!("no rel {rel}") })
    }

    fn insert_row(&mut self, rel: RelId, row: Vec<DeltaCell>) -> DeltaResult<Tid> {
        let (_, t) = self.current_txn()?;
        let xmin = *t.stamp_stack.last().expect("stack nonempty");
        let cmin = t.next_cid;
        let r = self.rel_mut(rel)?;
        let idx = r.tuples.len();
        r.tuples.push(SimTuple { xmin, cmin, xmax: None, cmax: None, row: row.clone() });
        self.wal.push(WalRec::Insert { rel, idx, xmin, cmin, row });
        self.checkpoint();
        Ok(tid_of_index(idx))
    }

    fn delete_row(&mut self, rel: RelId, tid: Tid) -> DeltaResult<()> {
        let (_, t) = self.current_txn()?;
        let xmax = *t.stamp_stack.last().expect("stack nonempty");
        let cmax = t.next_cid;
        let idx = index_of_tid(tid)?;
        // Overwrite a prior xmax only when it aborted (write-write waits/
        // EPQ are product-grain semantics, outside this model — schedules
        // that need them are M5-H/M territory).
        let prior_xmax = self
            .rels
            .get(&rel)
            .and_then(|r| r.tuples.get(idx))
            .map(|t| t.xmax)
            .ok_or_else(|| DeltaError::Heap {
                at: "delete",
                detail: format!("no tuple {tid:?}"),
            })?;
        let prior_aborted = match prior_xmax {
            None => true,
            Some(x) => self.clog.get(&x) == Some(&XidStatus::Aborted),
        };
        if !prior_aborted {
            return Err(DeltaError::Contract {
                detail: "write-write conflict (outside the model; EPQ is M5-H)",
            });
        }
        let r = self.rels.get_mut(&rel).expect("checked above");
        let tuple = &mut r.tuples[idx];
        tuple.xmax = Some(xmax);
        tuple.cmax = Some(cmax);
        self.wal.push(WalRec::Delete { rel, idx, xmax, cmax });
        self.checkpoint();
        Ok(())
    }

    // -- crash model ---------------------------------------------------------

    /// Number of recorded op-boundary crash points.
    pub fn op_points(&self) -> usize {
        self.op_log.len()
    }

    /// kill -9 at op boundary `k`: a fresh SimHeap replayed from exactly
    /// the WAL prefix that was FLUSHED at that boundary (unflushed tail
    /// lost, live state lost, clog rebuilt from surviving commit/abort
    /// records — in-progress xids resolve aborted, exactly recovery).
    pub fn revive_at(&self, k: usize) -> SimHeap {
        let (_, flushed) = self.op_log[k];
        self.replay(flushed)
    }

    fn replay(&self, upto: usize) -> SimHeap {
        let mut h = SimHeap::new();
        h.next_rel = self.next_rel;
        // Relations exist structurally (catalog transactionality is
        // product-grain dependency machinery, out of model scope — the
        // battery's subject is row/tombstone durability).
        for rel in self.rels.keys() {
            h.rels.insert(*rel, SimRel::default());
        }
        let mut max_xid = FIRST_XID;
        for rec in &self.wal[..upto] {
            match rec {
                WalRec::Insert { rel, idx, xmin, cmin, row } => {
                    let r = h.rels.entry(*rel).or_default();
                    assert_eq!(r.tuples.len(), *idx, "sim WAL replay order");
                    r.tuples.push(SimTuple {
                        xmin: *xmin,
                        cmin: *cmin,
                        xmax: None,
                        cmax: None,
                        row: row.clone(),
                    });
                    max_xid = max_xid.max(*xmin);
                }
                WalRec::Delete { rel, idx, xmax, cmax } => {
                    let r = h.rels.entry(*rel).or_default();
                    let t = &mut r.tuples[*idx];
                    t.xmax = Some(*xmax);
                    t.cmax = Some(*cmax);
                    max_xid = max_xid.max(*xmax);
                }
                WalRec::Commit { xid, subxids } => {
                    h.clog.insert(*xid, XidStatus::Committed);
                    for s in subxids {
                        h.clog.insert(*s, XidStatus::Committed);
                    }
                    max_xid = max_xid.max(*xid);
                }
                WalRec::Abort { xid, subxids } => {
                    h.clog.insert(*xid, XidStatus::Aborted);
                    for s in subxids {
                        h.clog.entry(*s).or_insert(XidStatus::Aborted);
                    }
                    max_xid = max_xid.max(*xid);
                }
                WalRec::CreatePair { table, binding } => {
                    h.bindings.insert(*table, *binding);
                    h.rels.entry(binding.delta_rel).or_default();
                    h.rels.entry(binding.tombstone_rel).or_default();
                }
                WalRec::ResetPair { table } => {
                    if let Some(b) = h.bindings.get(table).copied() {
                        h.rels.insert(b.delta_rel, SimRel::default());
                        h.rels.insert(b.tombstone_rel, SimRel::default());
                    }
                }
            }
        }
        // Recovery advances nextXid past every xid in surviving records
        // (the #253 fix's redo law — no recycling-resurrection).
        h.next_xid = max_xid + 1;
        h.flushed = 0;
        h.wal.clear();
        h
    }

    /// The crash checker (reusable by M5-N's battery): at op boundary `k`,
    /// every transaction ACKED by then must survive the revive — clog-
    /// committed, all its inserts visible to a fresh snapshot, all its
    /// deletes applied; and nothing resurrects (every revived-visible
    /// tuple's xmin committed in the ORIGINAL run's clog).
    pub fn check_acked_survive(&self, k: usize) -> Result<(), String> {
        let (wal_at_k, _) = self.op_log[k];
        let revived = self.revive_at(k);
        let snap = revived.snapshot();
        for (xid, ack_at) in &self.acked {
            if *ack_at > wal_at_k {
                continue; // acked after this crash point
            }
            // The acked txn's xid set (top + live subs at its commit).
            let mut xids = BTreeSet::new();
            xids.insert(*xid);
            for rec in &self.wal[..*ack_at] {
                if let WalRec::Commit { xid: cx, subxids } = rec {
                    if cx == xid {
                        xids.extend(subxids.iter().copied());
                    }
                }
            }
            if revived.clog.get(xid) != Some(&XidStatus::Committed) {
                return Err(format!("acked xid {xid} not committed after revive (LOST)"));
            }
            for (i, rec) in self.wal[..*ack_at].iter().enumerate() {
                match rec {
                    WalRec::Insert { rel, idx, xmin, .. } if xids.contains(xmin) => {
                        let visible = revived
                            .rels
                            .get(rel)
                            .and_then(|r| r.tuples.get(*idx))
                            .is_some_and(|t| revived.tuple_visible(&snap, t));
                        if !visible {
                            return Err(format!(
                                "acked xid {xid}: insert (wal op {i}) not visible after revive"
                            ));
                        }
                    }
                    WalRec::Delete { rel, idx, xmax, .. } if xids.contains(xmax) => {
                        let deleted = revived
                            .rels
                            .get(rel)
                            .and_then(|r| r.tuples.get(*idx))
                            .is_some_and(|t| !revived.tuple_visible(&snap, t));
                        if !deleted {
                            return Err(format!(
                                "acked xid {xid}: delete (wal op {i}) not applied after revive"
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
        // Anti-resurrection: revived-visible rows come only from xids the
        // original run committed.
        for (rel, r) in &revived.rels {
            for (idx, t) in r.tuples.iter().enumerate() {
                if revived.tuple_visible(&snap, t)
                    && self.clog.get(&t.xmin) != Some(&XidStatus::Committed)
                {
                    return Err(format!(
                        "rel {rel} tuple {idx}: visible after revive but xmin {} never \
                         committed (RESURRECTION)",
                        t.xmin
                    ));
                }
            }
        }
        Ok(())
    }

    // -- scan faces (skew applies HERE, never in the oracle) -----------------

    /// Sealed rowids of every tombstone VISIBLE under `snap` (the bitmap
    /// build's input at crate grain — product grain feeds the builder from
    /// a real heap scan). The seeded `HideTombstoneRowid` skew perturbs
    /// this face only.
    pub fn visible_tombstones(&self, snap: &SimSnapshot, tomb_rel: RelId) -> DeltaResult<Vec<u64>> {
        let r = self
            .rels
            .get(&tomb_rel)
            .ok_or(DeltaError::Heap { at: "tombstone scan", detail: format!("no rel {tomb_rel}") })?;
        let mut out = Vec::new();
        for t in &r.tuples {
            if !self.tuple_visible(snap, t) {
                continue;
            }
            let payload = match t.row.as_slice() {
                [DeltaCell::Word(w)] => *w as i64,
                _ => {
                    return Err(DeltaError::Contract { detail: "tombstone row shape" });
                }
            };
            let rowid = rowid_of_tombstone(payload)?;
            if self.skew == Some(SkewTarget::HideTombstoneRowid(rowid)) {
                continue;
            }
            out.push(rowid);
        }
        Ok(out)
    }

    /// Build the visible-tombstone index for `snap` through the PRODUCTION
    /// builder (scan face + [`DeletionIndexBuilder`] — the path under
    /// test).
    pub fn build_deletion_index(
        &self,
        snap: &SimSnapshot,
        tomb_rel: RelId,
    ) -> DeltaResult<DeletionIndex> {
        let mut b = DeletionIndexBuilder::new();
        b.add_all(self.visible_tombstones(snap, tomb_rel)?)?;
        Ok(b.finish())
    }

    /// The NAIVE per-row oracle (independent aggregation path; never
    /// consults the skew): is `sealed_rowid` deleted under `snap`?
    pub fn oracle_is_deleted(&self, snap: &SimSnapshot, tomb_rel: RelId, sealed_rowid: u64) -> bool {
        let Some(r) = self.rels.get(&tomb_rel) else { return false };
        r.tuples.iter().any(|t| {
            self.tuple_visible(snap, t)
                && matches!(t.row.as_slice(), [DeltaCell::Word(w)] if *w == sealed_rowid)
        })
    }

    /// Visible delta rows under `snap` in tid order (harness comparisons).
    pub fn visible_delta_rows(&self, snap: &SimSnapshot, delta_rel: RelId) -> Vec<(Tid, Vec<DeltaCell>)> {
        let Some(r) = self.rels.get(&delta_rel) else { return Vec::new() };
        r.tuples
            .iter()
            .enumerate()
            .filter(|(_, t)| self.tuple_visible(snap, t))
            .map(|(i, t)| (tid_of_index(i), t.row.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// lifecycle implementor
// ---------------------------------------------------------------------------

impl DeltaLifecycle for SimHeap {
    fn lookup(&mut self, table: u64) -> DeltaResult<Option<DeltaBinding>> {
        Ok(self.bindings.get(&table).copied())
    }

    fn lookup_or_create(&mut self, table: u64) -> DeltaResult<DeltaBinding> {
        if let Some(b) = self.bindings.get(&table) {
            return Ok(*b);
        }
        let delta_rel = self.next_rel;
        let tombstone_rel = self.next_rel + 1;
        self.next_rel += 2;
        let binding = DeltaBinding { delta_rel, tombstone_rel };
        self.rels.insert(delta_rel, SimRel::default());
        self.rels.insert(tombstone_rel, SimRel::default());
        self.bindings.insert(table, binding);
        self.wal.push(WalRec::CreatePair { table, binding });
        self.checkpoint();
        Ok(binding)
    }

    fn reset(&mut self, table: u64) -> DeltaResult<()> {
        if let Some(b) = self.bindings.get(&table).copied() {
            self.rels.insert(b.delta_rel, SimRel::default());
            self.rels.insert(b.tombstone_rel, SimRel::default());
            self.wal.push(WalRec::ResetPair { table });
            self.checkpoint();
        }
        Ok(())
    }

    fn drop_pair(&mut self, table: u64) -> DeltaResult<()> {
        if let Some(b) = self.bindings.remove(&table) {
            self.rels.remove(&b.delta_rel);
            self.rels.remove(&b.tombstone_rel);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// write implementor (per-table view)
// ---------------------------------------------------------------------------

/// [`DeltaWrite`] over one table's pair (the per-statement view M5-M's
/// binding will mirror over real relations).
pub struct SimTableWriter<'a> {
    pub heap: &'a mut SimHeap,
    pub binding: DeltaBinding,
}

impl DeltaWrite for SimTableWriter<'_> {
    fn insert_delta_row(&mut self, row: &[DeltaCell]) -> DeltaResult<(u32, u16)> {
        self.heap.insert_row(self.binding.delta_rel, row.to_vec())
    }

    fn insert_tombstone_row(&mut self, payload: i64) -> DeltaResult<(u32, u16)> {
        self.heap
            .insert_row(self.binding.tombstone_rel, vec![DeltaCell::Word(payload as u64)])
    }

    fn delete_delta_row(&mut self, tid: (u32, u16)) -> DeltaResult<()> {
        self.heap.delete_row(self.binding.delta_rel, tid)
    }

    fn update_delta_row(
        &mut self,
        tid: (u32, u16),
        row: &[DeltaCell],
    ) -> DeltaResult<(u32, u16)> {
        self.heap.delete_row(self.binding.delta_rel, tid)?;
        self.heap.insert_row(self.binding.delta_rel, row.to_vec())
    }
}

// ---------------------------------------------------------------------------
// feed implementor
// ---------------------------------------------------------------------------

/// [`DeltaFeed`] over a snapshot-bound view of a sim delta relation.
/// `Rc<RefCell<…>>` is harness plumbing (single-threaded tests; the
/// product feed is worker-private over a real heap scan).
pub struct SimDeltaFeed {
    heap: Rc<RefCell<SimHeap>>,
    delta_rel: RelId,
    snap: SimSnapshot,
    ncols: usize,
    nblocks: u64,
}

impl SimDeltaFeed {
    pub fn new(
        heap: Rc<RefCell<SimHeap>>,
        delta_rel: RelId,
        snap: SimSnapshot,
        ncols: usize,
    ) -> SimDeltaFeed {
        let ntuples = heap
            .borrow()
            .rels
            .get(&delta_rel)
            .map_or(0, |r| r.tuples.len());
        let nblocks = (ntuples as u64).div_ceil(SIM_ROWS_PER_BLOCK as u64);
        SimDeltaFeed { heap, delta_rel, snap, ncols, nblocks }
    }
}

impl DeltaFeed for SimDeltaFeed {
    fn ncols(&self) -> usize {
        self.ncols
    }

    fn nblocks(&self) -> u64 {
        self.nblocks
    }

    fn fetch_window(
        &mut self,
        blocks: core::ops::Range<u64>,
        out: &mut DeltaRowsWindow,
    ) -> DeltaResult<()> {
        out.clear();
        out.cols = vec![Vec::new(); self.ncols];
        let heap = self.heap.borrow();
        let Some(r) = heap.rels.get(&self.delta_rel) else {
            return Err(DeltaError::Heap {
                at: "delta feed",
                detail: format!("no rel {}", self.delta_rel),
            });
        };
        let lo = (blocks.start * SIM_ROWS_PER_BLOCK as u64) as usize;
        let hi = ((blocks.end * SIM_ROWS_PER_BLOCK as u64) as usize).min(r.tuples.len());
        for idx in lo..hi {
            let t = &r.tuples[idx];
            let tid = tid_of_index(idx);
            let shown_by_skew = heap.skew == Some(SkewTarget::ShowDeltaTid(tid));
            if !heap.tuple_visible(&self.snap, t) && !shown_by_skew {
                continue;
            }
            if t.row.len() != self.ncols {
                return Err(DeltaError::Contract { detail: "delta row width" });
            }
            out.tids.push(tid);
            for (c, cell) in t.row.iter().enumerate() {
                out.cols[c].push(cell.clone());
            }
            out.nrows += 1;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// seeding faces (M5-N + the M5-O bar rigs consume these)
// ---------------------------------------------------------------------------

/// Seed `rows` as committed (sync-acked) trickle inserts on `table`'s
/// delta relation. Returns the pair binding.
pub fn seed_delta_rows(
    heap: &mut SimHeap,
    table: RelId,
    rows: &[Vec<DeltaCell>],
) -> DeltaResult<DeltaBinding> {
    let binding = heap.lookup_or_create(table)?;
    heap.begin();
    for row in rows {
        let mut w = SimTableWriter { heap, binding };
        crate::ops::trickle_insert(&mut w, row)?;
    }
    heap.commit(true)?;
    Ok(binding)
}

/// Seed committed (sync-acked) tombstones for `sealed_rowids` on `table`.
pub fn seed_tombstones(
    heap: &mut SimHeap,
    table: RelId,
    sealed_rowids: &[u64],
) -> DeltaResult<DeltaBinding> {
    let binding = heap.lookup_or_create(table)?;
    heap.begin();
    for &r in sealed_rowids {
        // Validate before writing (the encode guard).
        let _ = tombstone_payload(r)?;
        let mut w = SimTableWriter { heap, binding };
        crate::ops::delete_sealed(&mut w, r)?;
    }
    heap.commit(true)?;
    Ok(binding)
}
