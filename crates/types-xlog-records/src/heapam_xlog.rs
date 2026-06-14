//! Heap rmgr WAL record bodies (`access/heapam_xlog.h`), trimmed to the
//! fields ports consume so far.

use crate::arrays::{Oids, OffsetNumbers};
use crate::bytes::{bool_at, i32_at, item_pointer_at, locator_at, u16_at, u32_at, u8_at};
use types_core::{CommandId, Oid, OffsetNumber, TransactionId};
use types_storage::sinval::SharedInvalMessages;
use types_storage::RelFileLocator;
use types_tuple::ItemPointerData;

/// `xl_heap_insert`: `{OffsetNumber offnum; uint8 flags;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_insert {
    pub offnum: OffsetNumber,
    pub flags: u8,
}

impl xl_heap_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnum: u16_at(rec, 0),
            flags: u8_at(rec, 2),
        }
    }
}

/// `xl_heap_delete`: `{TransactionId xmax; OffsetNumber offnum;
/// uint8 infobits_set; uint8 flags;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_delete {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: u8,
    pub flags: u8,
}

impl xl_heap_delete {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xmax: u32_at(rec, 0),
            offnum: u16_at(rec, 4),
            infobits_set: u8_at(rec, 6),
            flags: u8_at(rec, 7),
        }
    }
}

/// `xl_heap_update`: old/new xmax, offsets, infomask bits, flags.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_update {
    pub old_xmax: TransactionId,
    pub old_offnum: OffsetNumber,
    pub old_infobits_set: u8,
    pub flags: u8,
    pub new_xmax: TransactionId,
    pub new_offnum: OffsetNumber,
}

impl xl_heap_update {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            old_xmax: u32_at(rec, 0),
            old_offnum: u16_at(rec, 4),
            old_infobits_set: u8_at(rec, 6),
            flags: u8_at(rec, 7),
            new_xmax: u32_at(rec, 8),
            new_offnum: u16_at(rec, 12),
        }
    }
}

/// `xl_heap_truncate`: `{Oid dbId; uint32 nrelids; uint8 flags;
/// Oid relids[FLEXIBLE_ARRAY_MEMBER];}` — `relids` 4-aligned at 12.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_truncate {
    pub dbId: Oid,
    pub nrelids: u32,
    pub flags: u8,
}

impl xl_heap_truncate {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            dbId: u32_at(rec, 0),
            nrelids: u32_at(rec, 4),
            flags: u8_at(rec, 8),
        }
    }

    /// The trailing `relids` array.
    pub fn relids(rec: &[u8]) -> Oids<'_> {
        Oids::from_bytes(&rec[12..])
    }
}

/// `xl_heap_confirm`: `{OffsetNumber offnum;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_confirm {
    pub offnum: OffsetNumber,
}

impl xl_heap_confirm {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { offnum: u16_at(rec, 0) }
    }
}

/// `xl_heap_lock`: same layout as [`xl_heap_delete`].
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_lock {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: u8,
    pub flags: u8,
}

impl xl_heap_lock {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xmax: u32_at(rec, 0),
            offnum: u16_at(rec, 4),
            infobits_set: u8_at(rec, 6),
            flags: u8_at(rec, 7),
        }
    }
}

/// `xl_heap_lock_updated`: same layout as [`xl_heap_lock`].
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_lock_updated {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: u8,
    pub flags: u8,
}

impl xl_heap_lock_updated {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xmax: u32_at(rec, 0),
            offnum: u16_at(rec, 4),
            infobits_set: u8_at(rec, 6),
            flags: u8_at(rec, 7),
        }
    }
}

/// `xl_heap_inplace`: `{OffsetNumber offnum; Oid dbId; Oid tsId;
/// bool relcacheInitFileInval; int nmsgs;
/// SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];}` — `msgs` at 20.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_inplace {
    pub offnum: OffsetNumber,
    pub dbId: Oid,
    pub tsId: Oid,
    pub relcacheInitFileInval: bool,
    pub nmsgs: i32,
}

impl xl_heap_inplace {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnum: u16_at(rec, 0),
            dbId: u32_at(rec, 4),
            tsId: u32_at(rec, 8),
            relcacheInitFileInval: bool_at(rec, 12),
            nmsgs: i32_at(rec, 16),
        }
    }

    /// The trailing `msgs` array.
    pub fn msgs(rec: &[u8]) -> SharedInvalMessages<'_> {
        SharedInvalMessages::from_bytes(&rec[20..])
    }
}

/// `xl_heap_prune`: `{uint8 reason; uint8 flags;}`. If
/// `XLHP_HAS_CONFLICT_HORIZON` is set, the conflict horizon XID follows,
/// unaligned.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_prune {
    pub reason: u8,
    pub flags: u8,
}

/// `SizeOfHeapPrune` — `offsetof(xl_heap_prune, flags) + sizeof(uint8)`.
pub const SIZE_OF_HEAP_PRUNE: usize = 2;

impl xl_heap_prune {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            reason: u8_at(rec, 0),
            flags: u8_at(rec, 1),
        }
    }

    /// The unaligned conflict-horizon XID following the struct (valid only
    /// when `XLHP_HAS_CONFLICT_HORIZON` is set in `flags`).
    pub fn conflict_horizon(rec: &[u8]) -> TransactionId {
        u32_at(rec, SIZE_OF_HEAP_PRUNE)
    }
}

/// `xlhp_freeze_plan`: one freeze plan of an `XLOG_HEAP2_PRUNE_*` record.
#[derive(Clone, Copy, Debug)]
pub struct xlhp_freeze_plan {
    pub xmax: TransactionId,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub frzflags: u8,
    /// Length of this plan's slice of the shared page-offset-number array.
    pub ntuples: u16,
}

/// `sizeof(xlhp_freeze_plan)` — 12 bytes, 4-aligned.
pub const SIZEOF_XLHP_FREEZE_PLAN: usize = 12;

impl xlhp_freeze_plan {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            xmax: u32_at(rec, 0),
            t_infomask2: u16_at(rec, 4),
            t_infomask: u16_at(rec, 6),
            frzflags: u8_at(rec, 8),
            ntuples: u16_at(rec, 10),
        }
    }
}

/// An `xlhp_freeze_plan[]` borrowed from a record body.
#[derive(Clone, Copy, Debug)]
pub struct FreezePlans<'a> {
    bytes: &'a [u8],
}

impl<'a> FreezePlans<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Plan `i`; panics past the end of the bytes.
    pub fn get(&self, i: usize) -> xlhp_freeze_plan {
        xlhp_freeze_plan::from_bytes(&self.bytes[i * SIZEOF_XLHP_FREEZE_PLAN..])
    }

    /// The raw bytes of the first `count` plans (for generic array walkers).
    pub fn bytes_of(&self, count: usize) -> &'a [u8] {
        &self.bytes[..count * SIZEOF_XLHP_FREEZE_PLAN]
    }
}

/// `xlhp_freeze_plans`: `{uint16 nplans;
/// xlhp_freeze_plan plans[FLEXIBLE_ARRAY_MEMBER];}` — `plans` 4-aligned.
#[derive(Clone, Copy, Debug)]
pub struct xlhp_freeze_plans {
    pub nplans: u16,
}

impl xlhp_freeze_plans {
    /// `offsetof(xlhp_freeze_plans, plans)`.
    pub const OFFSETOF_PLANS: usize = 4;

    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nplans: u16_at(rec, 0) }
    }

    /// The trailing `plans` array.
    pub fn plans(rec: &[u8]) -> FreezePlans<'_> {
        FreezePlans::from_bytes(&rec[Self::OFFSETOF_PLANS..])
    }
}

/// `xlhp_prune_items`: `{uint16 ntargets;
/// OffsetNumber data[FLEXIBLE_ARRAY_MEMBER];}`.
#[derive(Clone, Copy, Debug)]
pub struct xlhp_prune_items {
    pub ntargets: u16,
}

impl xlhp_prune_items {
    /// `offsetof(xlhp_prune_items, data)`.
    pub const OFFSETOF_DATA: usize = 2;

    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { ntargets: u16_at(rec, 0) }
    }

    /// The trailing `data` offset-number array.
    pub fn data(rec: &[u8]) -> OffsetNumbers<'_> {
        OffsetNumbers::from_bytes(&rec[Self::OFFSETOF_DATA..])
    }
}

/// `xl_heap_visible`: `{TransactionId snapshotConflictHorizon; uint8 flags;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_visible {
    pub snapshotConflictHorizon: TransactionId,
    pub flags: u8,
}

impl xl_heap_visible {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: u32_at(rec, 0),
            flags: u8_at(rec, 4),
        }
    }
}

/// `xl_heap_multi_insert`: `{uint8 flags; uint16 ntuples;
/// OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];}` — `offsets` at 4.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_multi_insert {
    pub flags: u8,
    pub ntuples: u16,
}

impl xl_heap_multi_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            flags: u8_at(rec, 0),
            ntuples: u16_at(rec, 2),
        }
    }

    /// The trailing `offsets` array.
    pub fn offsets(rec: &[u8]) -> OffsetNumbers<'_> {
        OffsetNumbers::from_bytes(&rec[4..])
    }
}

/// `xl_heap_new_cid`: toplevel xid, cmin/cmax/combocid, and the
/// relfilelocator/ctid pair of the target tuple.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_new_cid {
    pub top_xid: TransactionId,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId,
    pub target_locator: RelFileLocator,
    pub target_tid: ItemPointerData,
}

/// `SizeOfHeapNewCid` (`access/heapam_xlog.h`): `offsetof(xl_heap_new_cid,
/// target_tid) + sizeof(ItemPointerData)` — 28 + 6 = 34 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapNewCid: usize = 34;

impl xl_heap_new_cid {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            top_xid: u32_at(rec, 0),
            cmin: u32_at(rec, 4),
            cmax: u32_at(rec, 8),
            combocid: u32_at(rec, 12),
            target_locator: locator_at(rec, 16),
            target_tid: item_pointer_at(rec, 28),
        }
    }

    /// Serialize into the `SizeOfHeapNewCid`-byte on-disk layout that
    /// `from_bytes` reads back, matching the C struct field order.
    pub fn to_bytes(&self) -> [u8; SizeOfHeapNewCid] {
        let mut out = [0u8; SizeOfHeapNewCid];
        out[0..4].copy_from_slice(&self.top_xid.to_ne_bytes());
        out[4..8].copy_from_slice(&self.cmin.to_ne_bytes());
        out[8..12].copy_from_slice(&self.cmax.to_ne_bytes());
        out[12..16].copy_from_slice(&self.combocid.to_ne_bytes());
        out[16..20].copy_from_slice(&self.target_locator.spcOid.to_ne_bytes());
        out[20..24].copy_from_slice(&self.target_locator.dbOid.to_ne_bytes());
        out[24..28].copy_from_slice(&self.target_locator.relNumber.to_ne_bytes());
        out[28..30].copy_from_slice(&self.target_tid.ip_blkid.bi_hi.to_ne_bytes());
        out[30..32].copy_from_slice(&self.target_tid.ip_blkid.bi_lo.to_ne_bytes());
        out[32..34].copy_from_slice(&self.target_tid.ip_posid.to_ne_bytes());
        out
    }
}
