use std::collections::BTreeSet;

pub type TransactionId = u32;
pub type CommandId = u32;

pub const INVALID_TRANSACTION_ID: TransactionId = 0;
pub const BOOTSTRAP_TRANSACTION_ID: TransactionId = 1;
pub const FROZEN_TRANSACTION_ID: TransactionId = 2;
pub const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;

pub const fn transaction_id_is_normal(xid: TransactionId) -> bool {
    xid >= FIRST_NORMAL_TRANSACTION_ID
}

pub type Lsn = u64;
pub const INVALID_LSN: Lsn = 0;
pub const WAL_SEG_SIZE_BYTES: u32 = 16 * 1024 * 1024;

pub const XLOG_FPI: u8 = 0;
pub const XLOG_HEAP_INSERT: u8 = 1;
pub const XLOG_XACT_COMMIT: u8 = 0;
pub const XLOG_XACT_PREPARE: u8 = 1;
pub const XLOG_XACT_ABORT: u8 = 2;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x11;

pub const RM_HEAP_ID: u8 = 0;
pub const RM_HEAP2_ID: u8 = 1;
pub const RM_XACT_ID: u8 = 2;
pub const RM_BTREE_ID: u8 = 3;
pub const RM_XLOG_ID: u8 = 4;
pub const RM_GIST_ID: u8 = 5;
pub const RM_HASH_ID: u8 = 6;
pub const RM_GIN_ID: u8 = 7;

pub const REGBUF_STANDARD: u8 = 1 << 0;
pub const REGBUF_WILL_INIT: u8 = 1 << 1;
pub const REGBUF_FORCE_IMAGE: u8 = 1 << 2;

pub const XLOG_BTREE_INSERT_LEAF: u8 = 0x10;
pub const XLOG_BTREE_INSERT_UPPER: u8 = 0x11;
pub const XLOG_BTREE_INSERT_META: u8 = 0x12;
pub const XLOG_BTREE_SPLIT_L: u8 = 0x13;
pub const XLOG_BTREE_SPLIT_R: u8 = 0x14;
pub const XLOG_BTREE_NEWROOT: u8 = 0x15;
pub const XLOG_BTREE_VACUUM: u8 = 0x16;
pub const XLOG_BTREE_DELETE: u8 = 0x17;
pub const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0x18;
pub const XLOG_BTREE_UNLINK_PAGE: u8 = 0x19;
pub const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x1a;
pub const XLOG_BTREE_REUSE_PAGE: u8 = 0x1b;
pub const XLOG_GIST_PAGE_INIT: u8 = 0x10;
pub const XLOG_GIST_INSERT: u8 = 0x11;
pub const XLOG_GIST_SPLIT: u8 = 0x12;
pub const XLOG_GIST_PAGE_UPDATE: u8 = 0x13;
pub const XLOG_GIST_SPLIT_COMPLETE: u8 = 0x14;
pub const XLOG_GIST_VACUUM: u8 = 0x15;
pub const XLOG_HASH_INIT_META_PAGE: u8 = 0x10;
pub const XLOG_HASH_INSERT: u8 = 0x20;
pub const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30;
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40;
pub const XLOG_HASH_SPLIT_PAGE: u8 = 0x50;
pub const XLOG_HASH_DELETE: u8 = 0x60;
pub const XLOG_HASH_VACUUM: u8 = 0x70;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub current_xid: TransactionId,
    pub current_cid: CommandId,
    pub heap_current_cid: Option<CommandId>,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub in_progress: BTreeSet<TransactionId>,
    pub own_xids: BTreeSet<TransactionId>,
}

impl Snapshot {
    pub fn bootstrap() -> Self {
        Self {
            current_xid: INVALID_TRANSACTION_ID,
            current_cid: CommandId::MAX,
            heap_current_cid: None,
            xmin: 1,
            xmax: 1,
            in_progress: BTreeSet::new(),
            own_xids: BTreeSet::new(),
        }
    }

    pub fn transaction_active_in_snapshot(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID
            && xid != self.current_xid
            && xid >= self.xmin
            && xid < self.xmax
            && self.in_progress.contains(&xid)
    }

    pub fn transaction_is_own(&self, xid: TransactionId) -> bool {
        xid != INVALID_TRANSACTION_ID && (xid == self.current_xid || self.own_xids.contains(&xid))
    }

    pub fn heap_current_cid(&self) -> Option<CommandId> {
        self.heap_current_cid
    }

    pub fn set_heap_current_cid(&mut self, cid: CommandId) {
        self.heap_current_cid = Some(cid);
    }
}
