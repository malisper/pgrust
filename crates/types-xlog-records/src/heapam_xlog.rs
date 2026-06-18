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

/// `SizeOfHeapInsert` (`access/heapam_xlog.h`): `offsetof(xl_heap_insert,
/// flags) + sizeof(uint8)` — `offnum`(u16)@0, `flags`(u8)@2 => 3 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapInsert: usize = 3;

impl xl_heap_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnum: u16_at(rec, 0),
            flags: u8_at(rec, 2),
        }
    }

    /// Serialize into the `SizeOfHeapInsert`-byte on-disk layout, matching the
    /// C struct field order (`offnum`@0, `flags`@2). The `xl_heap_header` &
    /// tuple data ride in backup block 0.
    pub fn to_bytes(&self) -> [u8; SizeOfHeapInsert] {
        let mut out = [0u8; SizeOfHeapInsert];
        out[0..2].copy_from_slice(&self.offnum.to_ne_bytes());
        out[2] = self.flags;
        out
    }
}

/// `xl_heap_header` (`access/heapam_xlog.h`): the parts of a `HeapTupleHeader`
/// we must store in WAL — `{uint16 t_infomask2; uint16 t_infomask; uint8
/// t_hoff;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_header {
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
}

/// `SizeOfHeapHeader` (`access/heapam_xlog.h`): `offsetof(xl_heap_header,
/// t_hoff) + sizeof(uint8)` — `t_infomask2`(u16)@0, `t_infomask`(u16)@2,
/// `t_hoff`(u8)@4 => 5 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapHeader: usize = 5;

impl xl_heap_header {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            t_infomask2: u16_at(rec, 0),
            t_infomask: u16_at(rec, 2),
            t_hoff: u8_at(rec, 4),
        }
    }

    /// Serialize into the `SizeOfHeapHeader`-byte on-disk layout.
    pub fn to_bytes(&self) -> [u8; SizeOfHeapHeader] {
        let mut out = [0u8; SizeOfHeapHeader];
        out[0..2].copy_from_slice(&self.t_infomask2.to_ne_bytes());
        out[2..4].copy_from_slice(&self.t_infomask.to_ne_bytes());
        out[4] = self.t_hoff;
        out
    }
}

// `XLH_INSERT_*` flags (access/heapam_xlog.h) — the `flags` field of
// `xl_heap_insert` / `xl_heap_multi_insert`.
/// `XLH_INSERT_ALL_VISIBLE_CLEARED` (`access/heapam_xlog.h`).
pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
/// `XLH_INSERT_LAST_IN_MULTI` (`access/heapam_xlog.h`).
pub const XLH_INSERT_LAST_IN_MULTI: u8 = 1 << 1;
/// `XLH_INSERT_IS_SPECULATIVE` (`access/heapam_xlog.h`).
pub const XLH_INSERT_IS_SPECULATIVE: u8 = 1 << 2;
/// `XLH_INSERT_CONTAINS_NEW_TUPLE` (`access/heapam_xlog.h`).
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;
/// `XLH_INSERT_ON_TOAST_RELATION` (`access/heapam_xlog.h`).
pub const XLH_INSERT_ON_TOAST_RELATION: u8 = 1 << 4;
/// `XLH_INSERT_ALL_FROZEN_SET` (`access/heapam_xlog.h`).
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = 1 << 5;

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

    /// Serialize into the `SizeOfHeapDelete`-byte on-disk layout, matching the
    /// C struct field order (`xmax`@0, `offnum`@4, `infobits_set`@6,
    /// `flags`@7).
    pub fn to_bytes(&self) -> [u8; SizeOfHeapDelete] {
        let mut out = [0u8; SizeOfHeapDelete];
        out[0..4].copy_from_slice(&self.xmax.to_ne_bytes());
        out[4..6].copy_from_slice(&self.offnum.to_ne_bytes());
        out[6] = self.infobits_set;
        out[7] = self.flags;
        out
    }
}

/// `SizeOfHeapDelete` (`access/heapam_xlog.h`): `offsetof(xl_heap_delete,
/// flags) + sizeof(uint8)` == 8.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapDelete: usize = 8;

// `XLH_DELETE_*` flags (access/heapam_xlog.h) — the `flags` field of
// `xl_heap_delete`.
/// `XLH_DELETE_ALL_VISIBLE_CLEARED` (`access/heapam_xlog.h`).
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
/// `XLH_DELETE_CONTAINS_OLD_TUPLE` (`access/heapam_xlog.h`).
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
/// `XLH_DELETE_CONTAINS_OLD_KEY` (`access/heapam_xlog.h`).
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
/// `XLH_DELETE_IS_SUPER` (`access/heapam_xlog.h`).
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
/// `XLH_DELETE_IS_PARTITION_MOVE` (`access/heapam_xlog.h`).
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;

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

    /// Serialize into the `SizeOfHeapUpdate`-byte on-disk layout. C field
    /// order: `old_xmax`@0, `old_offnum`@4, `old_infobits_set`@6, `flags`@7,
    /// `new_xmax`@8, `new_offnum`@12.
    pub fn to_bytes(&self) -> [u8; SizeOfHeapUpdate] {
        let mut out = [0u8; SizeOfHeapUpdate];
        out[0..4].copy_from_slice(&self.old_xmax.to_ne_bytes());
        out[4..6].copy_from_slice(&self.old_offnum.to_ne_bytes());
        out[6] = self.old_infobits_set;
        out[7] = self.flags;
        out[8..12].copy_from_slice(&self.new_xmax.to_ne_bytes());
        out[12..14].copy_from_slice(&self.new_offnum.to_ne_bytes());
        out
    }
}

/// `SizeOfHeapUpdate` (`access/heapam_xlog.h`): `offsetof(xl_heap_update,
/// new_offnum) + sizeof(OffsetNumber)` == 14.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapUpdate: usize = 14;

// `XLH_UPDATE_*` flags (access/heapam_xlog.h) — the `flags` field of
// `xl_heap_update` (8 bits available).
/// `XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
/// `XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = 1 << 1;
/// `XLH_UPDATE_CONTAINS_OLD_TUPLE` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
/// `XLH_UPDATE_CONTAINS_OLD_KEY` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
/// `XLH_UPDATE_CONTAINS_NEW_TUPLE` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
/// `XLH_UPDATE_PREFIX_FROM_OLD` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
/// `XLH_UPDATE_SUFFIX_FROM_OLD` (`access/heapam_xlog.h`).
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;

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

    /// Serialize into the `SizeOfHeapConfirm`-byte on-disk layout
    /// (`offnum`@0).
    pub fn to_bytes(&self) -> [u8; SizeOfHeapConfirm] {
        let mut out = [0u8; SizeOfHeapConfirm];
        out[0..2].copy_from_slice(&self.offnum.to_ne_bytes());
        out
    }
}

/// `SizeOfHeapConfirm` (`access/heapam_xlog.h`): `offsetof(xl_heap_confirm,
/// offnum) + sizeof(OffsetNumber)` == 2.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapConfirm: usize = 2;

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

    /// Serialize into the `SizeOfHeapLock`-byte on-disk layout (same field
    /// order as `xl_heap_delete`).
    pub fn to_bytes(&self) -> [u8; SizeOfHeapLock] {
        let mut out = [0u8; SizeOfHeapLock];
        out[0..4].copy_from_slice(&self.xmax.to_ne_bytes());
        out[4..6].copy_from_slice(&self.offnum.to_ne_bytes());
        out[6] = self.infobits_set;
        out[7] = self.flags;
        out
    }
}

/// `SizeOfHeapLock` (`access/heapam_xlog.h`): `offsetof(xl_heap_lock, flags) +
/// sizeof(uint8)` == 8.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapLock: usize = 8;

/// `XLH_LOCK_ALL_FROZEN_CLEARED` (`access/heapam_xlog.h`) — the `flags` bit of
/// `xl_heap_lock` / `xl_heap_lock_updated` saying the all-frozen VM bit was
/// cleared.
pub const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 0x01;

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

    /// Serialize into the `SizeOfHeapLockUpdated`-byte on-disk layout (same
    /// field order as `xl_heap_lock`).
    pub fn to_bytes(&self) -> [u8; SizeOfHeapLockUpdated] {
        let mut out = [0u8; SizeOfHeapLockUpdated];
        out[0..4].copy_from_slice(&self.xmax.to_ne_bytes());
        out[4..6].copy_from_slice(&self.offnum.to_ne_bytes());
        out[6] = self.infobits_set;
        out[7] = self.flags;
        out
    }
}

/// `SizeOfHeapLockUpdated` (`access/heapam_xlog.h`):
/// `offsetof(xl_heap_lock_updated, flags) + sizeof(uint8)` == 8.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapLockUpdated: usize = 8;

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

    /// Serialize the fixed `MinSizeOfHeapInplace`-byte struct part (the
    /// trailing `msgs` array is registered separately). C field order:
    /// `offnum`@0, `dbId`@4, `tsId`@8, `relcacheInitFileInval`@12, `nmsgs`@16.
    pub fn to_bytes(&self) -> [u8; MinSizeOfHeapInplace] {
        let mut out = [0u8; MinSizeOfHeapInplace];
        out[0..2].copy_from_slice(&self.offnum.to_ne_bytes());
        out[4..8].copy_from_slice(&self.dbId.to_ne_bytes());
        out[8..12].copy_from_slice(&self.tsId.to_ne_bytes());
        out[12] = self.relcacheInitFileInval as u8;
        out[16..20].copy_from_slice(&self.nmsgs.to_ne_bytes());
        out
    }
}

/// `MinSizeOfHeapInplace` (`access/heapam_xlog.h`): `offsetof(xl_heap_inplace,
/// msgs)` == 20 (the fixed struct part before the flexible `msgs` array).
#[allow(non_upper_case_globals)]
pub const MinSizeOfHeapInplace: usize = 20;

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

/// `SizeOfHeapVisible` (`access/heapam_xlog.h`): `offsetof(xl_heap_visible,
/// flags) + sizeof(uint8)` — `snapshotConflictHorizon`(u32)@0, `flags`(u8)@4 =>
/// 5 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapVisible: usize = 5;

impl xl_heap_visible {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: u32_at(rec, 0),
            flags: u8_at(rec, 4),
        }
    }

    /// Serialize into the `SizeOfHeapVisible`-byte on-disk layout, matching the
    /// C struct field order (`snapshotConflictHorizon`@0, `flags`@4).
    pub fn to_bytes(&self) -> [u8; SizeOfHeapVisible] {
        let mut out = [0u8; SizeOfHeapVisible];
        out[0..4].copy_from_slice(&self.snapshotConflictHorizon.to_ne_bytes());
        out[4] = self.flags;
        out
    }
}

/// `xl_heap_multi_insert`: `{uint8 flags; uint16 ntuples;
/// OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];}` — `offsets` at 4.
#[derive(Clone, Copy, Debug)]
pub struct xl_heap_multi_insert {
    pub flags: u8,
    pub ntuples: u16,
}

/// `SizeOfHeapMultiInsert` (`access/heapam_xlog.h`): `offsetof(
/// xl_heap_multi_insert, offsets)` — `flags`(u8)@0, `ntuples`(u16)@2 (aligned),
/// `offsets`@4 => 4 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfHeapMultiInsert: usize = 4;

impl xl_heap_multi_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            flags: u8_at(rec, 0),
            ntuples: u16_at(rec, 2),
        }
    }

    /// Serialize the fixed `SizeOfHeapMultiInsert`-byte header (the trailing
    /// `offsets` array is appended separately by the caller). Byte 1 is the C
    /// struct's alignment padding before `ntuples`.
    pub fn to_bytes(&self) -> [u8; SizeOfHeapMultiInsert] {
        let mut out = [0u8; SizeOfHeapMultiInsert];
        out[0] = self.flags;
        out[2..4].copy_from_slice(&self.ntuples.to_ne_bytes());
        out
    }

    /// The trailing `offsets` array.
    pub fn offsets(rec: &[u8]) -> OffsetNumbers<'_> {
        OffsetNumbers::from_bytes(&rec[4..])
    }
}

/// `xl_multi_insert_tuple` (`access/heapam_xlog.h`): the per-tuple header in a
/// multi-insert record's block-0 data — `{uint16 datalen; uint16 t_infomask2;
/// uint16 t_infomask; uint8 t_hoff;}`, followed by the tuple data.
#[derive(Clone, Copy, Debug)]
pub struct xl_multi_insert_tuple {
    pub datalen: u16,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
}

/// `SizeOfMultiInsertTuple` (`access/heapam_xlog.h`): `offsetof(
/// xl_multi_insert_tuple, t_hoff) + sizeof(uint8)` — `datalen`(u16)@0,
/// `t_infomask2`(u16)@2, `t_infomask`(u16)@4, `t_hoff`(u8)@6 => 7 bytes.
#[allow(non_upper_case_globals)]
pub const SizeOfMultiInsertTuple: usize = 7;

impl xl_multi_insert_tuple {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            datalen: u16_at(rec, 0),
            t_infomask2: u16_at(rec, 2),
            t_infomask: u16_at(rec, 4),
            t_hoff: u8_at(rec, 6),
        }
    }

    /// Serialize into the `SizeOfMultiInsertTuple`-byte on-disk layout.
    pub fn to_bytes(&self) -> [u8; SizeOfMultiInsertTuple] {
        let mut out = [0u8; SizeOfMultiInsertTuple];
        out[0..2].copy_from_slice(&self.datalen.to_ne_bytes());
        out[2..4].copy_from_slice(&self.t_infomask2.to_ne_bytes());
        out[4..6].copy_from_slice(&self.t_infomask.to_ne_bytes());
        out[6] = self.t_hoff;
        out
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
