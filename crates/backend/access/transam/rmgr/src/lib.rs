//! rmgr.c + GetRmgr/RmgrIdExists (xlog_internal.h); the RmgrIds registry
//! (rmgr.h) lives in types_core, re-exported here. The table is a fixed
//! compile-time array indexed by RmgrIds; entry order fixes the WAL-visible
//! numeric ids. C divergences: custom-rmgr extension slots (ids 128..=255),
//! RegisterCustomRmgr, and the pg_get_wal_resource_managers SRF are omitted —
//! there is no extension ABI (unregistered ids take the same RmgrNotFound
//! ERROR as C's empty slots; the SRF waits on funcapi/tuplestore). The
//! rm_decode column is omitted until logical-decoding vocabulary exists.
//! Unported callbacks are #[cold] panics naming the owning unit; a manager
//! landing replaces its row's fns with direct calls (or a seam iff the dep
//! would cycle, e.g. transam_xlog).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use elog::ereport;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_core::{BlockNumber, RmgrId};
use types_error::{ErrorLocation, PgResult, ERROR};
use xlogreader_seams::XLogReaderState;

pub type RmRedo = fn(record: &mut XLogReaderState) -> PgResult<()>;

fn xlog_redo(record: &mut XLogReaderState) -> PgResult<()> {
    transam_xlog_seams::xlog_redo::call(record)
}

fn xact_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let rec = record.record.as_ref().expect("xact_redo with no decoded record");
    // SAFETY: main_data points into the reader's decode buffer, valid for the
    // redo callback's duration.
    let data = unsafe { rec.main_data_bytes() };
    xact::xact_redo(xact::XactRedoInfo {
        info: rec.xl_info,
        xid: rec.xl_xid,
        origin_id: rec.record_origin,
        read_rec_ptr: record.ReadRecPtr,
        end_rec_ptr: record.EndRecPtr,
        data,
    })
}
pub type RmDesc = for<'mcx> fn(buf: &mut StringInfo<'mcx>, record: &XLogReaderState) -> PgResult<()>;
pub type RmIdentify = fn(info: u8) -> Option<&'static str>;
// C rm_startup is void(void); impls allocate under CurrentMemoryContext, threaded as `parent`.
pub type RmStartup = for<'mcx> fn(parent: Mcx<'mcx>) -> PgResult<()>;
pub type RmCleanup = fn();
pub type RmMask = fn(pagedata: &mut [u8], blkno: BlockNumber) -> PgResult<()>;

// RmgrData (xlog_internal.h) minus rm_decode; redo/desc/identify are
// non-null on every builtin row, so they are not Option.
pub struct RmgrData {
    pub rm_name: &'static str,
    pub rm_redo: RmRedo,
    pub rm_desc: RmDesc,
    pub rm_identify: RmIdentify,
    pub rm_startup: Option<RmStartup>,
    pub rm_cleanup: Option<RmCleanup>,
    pub rm_mask: Option<RmMask>,
}

pub use types_core::RmgrIds;
pub use RmgrIds::*;

pub const RM_MAX_ID: usize = u8::MAX as usize;
pub const RM_MAX_BUILTIN_ID: usize = RM_NEXT_ID as usize - 1;
pub const RM_MIN_CUSTOM_ID: usize = 128;
pub const RM_MAX_CUSTOM_ID: usize = u8::MAX as usize;
pub const RM_N_IDS: usize = u8::MAX as usize + 1;
pub const RM_N_BUILTIN_IDS: usize = RM_MAX_BUILTIN_ID + 1;
pub const RM_N_CUSTOM_IDS: usize = RM_MAX_CUSTOM_ID - RM_MIN_CUSTOM_ID + 1;
pub const RM_EXPERIMENTAL_ID: usize = 128;

pub const fn RmgrIdIsBuiltin(rmid: i32) -> bool {
    rmid <= RM_MAX_BUILTIN_ID as i32
}

pub const fn RmgrIdIsCustom(rmid: i32) -> bool {
    rmid >= RM_MIN_CUSTOM_ID as i32 && rmid <= RM_MAX_CUSTOM_ID as i32
}

pub const fn RmgrIdIsValid(rmid: i32) -> bool {
    RmgrIdIsBuiltin(rmid) || RmgrIdIsCustom(rmid)
}

macro_rules! unported_redo {
    ($($name:ident => $unit:literal;)+) => {$(
        #[cold]
        #[inline(never)]
        fn $name(_record: &mut XLogReaderState) -> PgResult<()> {
            panic!(concat!("rmgr callback not ported: ", stringify!($name), " — land ", $unit))
        }
    )+};
}

macro_rules! unported_desc {
    ($($name:ident => $unit:literal;)+) => {$(
        #[cold]
        #[inline(never)]
        fn $name(_buf: &mut StringInfo<'_>, _record: &XLogReaderState) -> PgResult<()> {
            panic!(concat!("rmgr callback not ported: ", stringify!($name), " — land ", $unit))
        }
    )+};
}

macro_rules! unported_identify {
    ($($name:ident => $unit:literal;)+) => {$(
        #[cold]
        #[inline(never)]
        fn $name(_info: u8) -> Option<&'static str> {
            panic!(concat!("rmgr callback not ported: ", stringify!($name), " — land ", $unit))
        }
    )+};
}

macro_rules! unported_mask {
    ($($name:ident => $unit:literal;)+) => {$(
        #[cold]
        #[inline(never)]
        fn $name(_pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
            panic!(concat!("rmgr callback not ported: ", stringify!($name), " — land ", $unit))
        }
    )+};
}

unported_redo! {
    dbase_redo => "backend-commands-dbcommands";
    tblspc_redo => "backend-commands-tablespace";
    hash_redo => "backend-access-hash-xlog";
    gin_redo => "backend-access-gin-xlog";
    gist_redo => "backend-access-gist-xlog";
    seq_redo => "backend-commands-sequence";
    spg_redo => "backend-access-spgist-xlog";
    brin_redo => "backend-access-brin-xlog";
    commit_ts_redo => "backend-access-transam-commit-ts";
    replorigin_redo => "backend-replication-origin";
    generic_redo => "backend-access-transam-generic-xlog";
    logicalmsg_redo => "backend-replication-message";
}

unported_desc! {
    xlog_desc => "backend-access-rmgrdesc-xlogdesc";
    xact_desc => "backend-access-rmgrdesc-xactdesc";
    smgr_desc => "backend-access-rmgrdesc-smgrdesc";
    clog_desc => "backend-access-rmgrdesc-small";
    dbase_desc => "backend-access-rmgrdesc-small";
    tblspc_desc => "backend-access-rmgrdesc-small";
    multixact_desc => "backend-rmgrdesc-next";
    relmap_desc => "backend-access-rmgrdesc-small";
    standby_desc => "backend-rmgrdesc-next";
    heap2_desc => "backend-rmgrdesc-next";
    heap_desc => "backend-rmgrdesc-next";
    btree_desc => "backend-rmgrdesc-next";
    hash_desc => "backend-rmgrdesc-next";
    gin_desc => "backend-rmgrdesc-next";
    gist_desc => "backend-rmgrdesc-next";
    seq_desc => "backend-access-rmgrdesc-small";
    spg_desc => "backend-rmgrdesc-next";
    brin_desc => "backend-rmgrdesc-next";
    commit_ts_desc => "backend-access-rmgrdesc-small";
    replorigin_desc => "backend-rmgrdesc-extra-small";
    generic_desc => "backend-access-rmgrdesc-small";
    logicalmsg_desc => "backend-access-rmgrdesc-small";
}

unported_identify! {
    xlog_identify => "backend-access-rmgrdesc-xlogdesc";
    xact_identify => "backend-access-rmgrdesc-xactdesc";
    smgr_identify => "backend-access-rmgrdesc-smgrdesc";
    clog_identify => "backend-access-rmgrdesc-small";
    dbase_identify => "backend-access-rmgrdesc-small";
    tblspc_identify => "backend-access-rmgrdesc-small";
    multixact_identify => "backend-rmgrdesc-next";
    relmap_identify => "backend-access-rmgrdesc-small";
    standby_identify => "backend-rmgrdesc-next";
    heap2_identify => "backend-rmgrdesc-next";
    heap_identify => "backend-rmgrdesc-next";
    btree_identify => "backend-rmgrdesc-next";
    hash_identify => "backend-rmgrdesc-next";
    gin_identify => "backend-rmgrdesc-next";
    gist_identify => "backend-rmgrdesc-next";
    seq_identify => "backend-access-rmgrdesc-small";
    spg_identify => "backend-rmgrdesc-next";
    brin_identify => "backend-rmgrdesc-next";
    commit_ts_identify => "backend-access-rmgrdesc-small";
    replorigin_identify => "backend-rmgrdesc-extra-small";
    generic_identify => "backend-access-rmgrdesc-small";
    logicalmsg_identify => "backend-access-rmgrdesc-small";
}

// btree/gin/gist/spgist rm_startup/rm_cleanup only allocate the recovery
// scratch their redo callbacks read; the rows carry None (gin/gist/spgist
// redo still loud; btree redo uses stack scratch instead of C's opCtx).
unported_mask! {
    heap_mask => "backend-access-heap-heapam-xlog";
    btree_mask => "backend-access-nbtree-nbtxlog";
    hash_mask => "backend-access-hash-xlog";
    gin_mask => "backend-access-gin-xlog";
    gist_mask => "backend-access-gist-xlog";
    seq_mask => "backend-commands-sequence";
    spg_mask => "backend-access-spgist-xlog";
    brin_mask => "backend-access-brin-xlog";
    generic_mask => "backend-access-transam-generic-xlog";
}

// rmgrlist.h rows, in declaration order (rm_decode column omitted; see crate
// docs).
pub static RmgrTable: [RmgrData; RM_N_BUILTIN_IDS] = [
    RmgrData {
        rm_name: "XLOG",
        rm_redo: xlog_redo,
        rm_desc: xlog_desc,
        rm_identify: xlog_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Transaction",
        rm_redo: xact_redo,
        rm_desc: xact_desc,
        rm_identify: xact_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Storage",
        rm_redo: storage_xlog::smgr_redo,
        rm_desc: smgr_desc,
        rm_identify: smgr_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "CLOG",
        rm_redo: clog::clog_redo,
        rm_desc: clog_desc,
        rm_identify: clog_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Database",
        rm_redo: dbase_redo,
        rm_desc: dbase_desc,
        rm_identify: dbase_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Tablespace",
        rm_redo: tblspc_redo,
        rm_desc: tblspc_desc,
        rm_identify: tblspc_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "MultiXact",
        rm_redo: multixact::multixact_redo,
        rm_desc: multixact_desc,
        rm_identify: multixact_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "RelMap",
        rm_redo: relmapper::relmap_redo,
        rm_desc: relmap_desc,
        rm_identify: relmap_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Standby",
        rm_redo: standby::standby_redo,
        rm_desc: standby_desc,
        rm_identify: standby_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Heap2",
        rm_redo: heapam_xlog::heap2_redo,
        rm_desc: heap2_desc,
        rm_identify: heap2_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heap_mask),
    },
    RmgrData {
        rm_name: "Heap",
        rm_redo: heapam_xlog::heap_redo,
        rm_desc: heap_desc,
        rm_identify: heap_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heap_mask),
    },
    RmgrData {
        rm_name: "Btree",
        rm_redo: nbtree_xlog::btree_redo,
        rm_desc: btree_desc,
        rm_identify: btree_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(btree_mask),
    },
    RmgrData {
        rm_name: "Hash",
        rm_redo: hash_redo,
        rm_desc: hash_desc,
        rm_identify: hash_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(hash_mask),
    },
    RmgrData {
        rm_name: "Gin",
        rm_redo: gin_redo,
        rm_desc: gin_desc,
        rm_identify: gin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(gin_mask),
    },
    RmgrData {
        rm_name: "Gist",
        rm_redo: gist_redo,
        rm_desc: gist_desc,
        rm_identify: gist_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(gist_mask),
    },
    RmgrData {
        rm_name: "Sequence",
        rm_redo: seq_redo,
        rm_desc: seq_desc,
        rm_identify: seq_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(seq_mask),
    },
    RmgrData {
        rm_name: "SPGist",
        rm_redo: spg_redo,
        rm_desc: spg_desc,
        rm_identify: spg_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(spg_mask),
    },
    RmgrData {
        rm_name: "BRIN",
        rm_redo: brin_redo,
        rm_desc: brin_desc,
        rm_identify: brin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(brin_mask),
    },
    RmgrData {
        rm_name: "CommitTs",
        rm_redo: commit_ts_redo,
        rm_desc: commit_ts_desc,
        rm_identify: commit_ts_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "ReplicationOrigin",
        rm_redo: replorigin_redo,
        rm_desc: replorigin_desc,
        rm_identify: replorigin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Generic",
        rm_redo: generic_redo,
        rm_desc: generic_desc,
        rm_identify: generic_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(generic_mask),
    },
    RmgrData {
        rm_name: "LogicalMessage",
        rm_redo: logicalmsg_redo,
        rm_desc: logicalmsg_desc,
        rm_identify: logicalmsg_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
];

// RmgrIdExists (xlog_internal.h): with custom registration omitted, exactly
// the builtin range is populated.
pub fn RmgrIdExists(rmid: RmgrId) -> bool {
    (rmid as usize) < RM_N_BUILTIN_IDS
}

// GetRmgr (xlog_internal.h).
pub fn GetRmgr(rmid: RmgrId) -> PgResult<&'static RmgrData> {
    if !RmgrIdExists(rmid) {
        RmgrNotFound(rmid)?;
    }
    Ok(&RmgrTable[rmid as usize])
}

pub fn RmgrStartup(parent: Mcx<'_>) -> PgResult<()> {
    for rmgr in &RmgrTable {
        if let Some(startup) = rmgr.rm_startup {
            startup(parent)?;
        }
    }
    Ok(())
}

pub fn RmgrCleanup() {
    for rmgr in &RmgrTable {
        if let Some(cleanup) = rmgr.rm_cleanup {
            cleanup();
        }
    }
}

#[cold]
pub fn RmgrNotFound(rmid: RmgrId) -> PgResult<()> {
    ereport(ERROR)
        .errmsg(format!("resource manager with ID {rmid} not registered"))
        .errhint(
            "Include the extension module that implements this resource manager in \
             \"shared_preload_libraries\".",
        )
        .finish(ErrorLocation::new("rmgr.c", 0, "RmgrNotFound"))
}

pub fn init_seams() {}
