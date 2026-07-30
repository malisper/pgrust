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

macro_rules! unported_mask {
    ($($name:ident => $unit:literal;)+) => {$(
        #[cold]
        #[inline(never)]
        fn $name(_pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
            panic!(concat!("rmgr callback not ported: ", stringify!($name), " — land ", $unit))
        }
    )+};
}

fn dbase_redo(record: &mut XLogReaderState) -> PgResult<()> {
    dbcommands_seams::dbase_redo::call(record)
}

fn tblspc_redo(record: &mut XLogReaderState) -> PgResult<()> {
    tablespace_seams::tblspc_redo::call(record)
}

// replorigin_redo (origin.c) lives in the origin crate (off the serial
// path); installed as the origin_seams::replorigin_redo seam.
fn replorigin_redo(record: &mut XLogReaderState) -> PgResult<()> {
    origin_seams::replorigin_redo::call(record)
}

// btree/gin/gist/spgist rm_startup/rm_cleanup only allocate the recovery
// scratch their redo callbacks read; the rows carry None (gin/gist/spgist
// redo still loud; btree redo uses stack scratch instead of C's opCtx).
unported_mask! {
    heap_mask => "backend-access-heap-heapam-xlog";
    btree_mask => "backend-access-nbtree-nbtxlog";
    gin_mask => "backend-access-gin-xlog";
    seq_mask => "backend-commands-sequence";
    brin_mask => "backend-access-brin-xlog";
}

// rmgrlist.h rows, in declaration order (rm_decode column omitted; see crate
// docs).
pub static RmgrTable: [RmgrData; RM_N_BUILTIN_IDS] = [
    RmgrData {
        rm_name: "XLOG",
        rm_redo: xlog_redo,
        rm_desc: rmgrdesc::xlogdesc::xlog_desc,
        rm_identify: rmgrdesc::xlogdesc::xlog_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Transaction",
        rm_redo: xact_redo,
        rm_desc: rmgrdesc::xactdesc::xact_desc,
        rm_identify: rmgrdesc::xactdesc::xact_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Storage",
        rm_redo: storage_xlog::smgr_redo,
        rm_desc: rmgrdesc::smgrdesc::smgr_desc,
        rm_identify: rmgrdesc::smgrdesc::smgr_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "CLOG",
        rm_redo: clog::clog_redo,
        rm_desc: rmgrdesc::clogdesc::clog_desc,
        rm_identify: rmgrdesc::clogdesc::clog_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Database",
        rm_redo: dbase_redo,
        rm_desc: rmgrdesc::dbasedesc::dbase_desc,
        rm_identify: rmgrdesc::dbasedesc::dbase_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Tablespace",
        rm_redo: tblspc_redo,
        rm_desc: rmgrdesc::tblspcdesc::tblspc_desc,
        rm_identify: rmgrdesc::tblspcdesc::tblspc_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "MultiXact",
        rm_redo: multixact::multixact_redo,
        rm_desc: rmgrdesc::mxactdesc::multixact_desc,
        rm_identify: rmgrdesc::mxactdesc::multixact_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "RelMap",
        rm_redo: relmapper::relmap_redo,
        rm_desc: rmgrdesc::relmapdesc::relmap_desc,
        rm_identify: rmgrdesc::relmapdesc::relmap_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Standby",
        rm_redo: standby::standby_redo,
        rm_desc: rmgrdesc::standbydesc::standby_desc,
        rm_identify: rmgrdesc::standbydesc::standby_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Heap2",
        rm_redo: heapam_xlog::heap2_redo,
        rm_desc: rmgrdesc::heapdesc::heap2_desc,
        rm_identify: rmgrdesc::heapdesc::heap2_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heap_mask),
    },
    RmgrData {
        rm_name: "Heap",
        rm_redo: heapam_xlog::heap_redo,
        rm_desc: rmgrdesc::heapdesc::heap_desc,
        rm_identify: rmgrdesc::heapdesc::heap_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heap_mask),
    },
    RmgrData {
        rm_name: "Btree",
        rm_redo: nbtree_xlog::btree_redo,
        rm_desc: rmgrdesc::nbtdesc::btree_desc,
        rm_identify: rmgrdesc::nbtdesc::btree_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(btree_mask),
    },
    RmgrData {
        rm_name: "Hash",
        rm_redo: hash_xlog::hash_redo,
        rm_desc: rmgrdesc::hashdesc::hash_desc,
        rm_identify: rmgrdesc::hashdesc::hash_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(hash_xlog::hash_mask),
    },
    RmgrData {
        rm_name: "Gin",
        rm_redo: gin_xlog::gin_redo,
        rm_desc: rmgrdesc::gindesc::gin_desc,
        rm_identify: rmgrdesc::gindesc::gin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(gin_xlog::gin_mask),
    },
    RmgrData {
        rm_name: "Gist",
        rm_redo: gist_xlog::gist_redo,
        rm_desc: rmgrdesc::gistdesc::gist_desc,
        rm_identify: rmgrdesc::gistdesc::gist_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(gist_xlog::gist_mask),
    },
    RmgrData {
        rm_name: "Sequence",
        rm_redo: sequence_xlog::seq_redo,
        rm_desc: rmgrdesc::seqdesc::seq_desc,
        rm_identify: rmgrdesc::seqdesc::seq_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(seq_mask),
    },
    RmgrData {
        rm_name: "SPGist",
        rm_redo: spgist_xlog::spg_redo,
        rm_desc: rmgrdesc::spgdesc::spg_desc,
        rm_identify: rmgrdesc::spgdesc::spg_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(spgist_xlog::spg_mask),
    },
    RmgrData {
        rm_name: "BRIN",
        rm_redo: brin_xlog::brin_redo,
        rm_desc: rmgrdesc::brindesc::brin_desc,
        rm_identify: rmgrdesc::brindesc::brin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(brin_mask),
    },
    RmgrData {
        rm_name: "CommitTs",
        rm_redo: commit_ts::commit_ts_redo,
        rm_desc: rmgrdesc::committsdesc::commit_ts_desc,
        rm_identify: rmgrdesc::committsdesc::commit_ts_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "ReplicationOrigin",
        rm_redo: replorigin_redo,
        rm_desc: rmgrdesc::replorigindesc::replorigin_desc,
        rm_identify: rmgrdesc::replorigindesc::replorigin_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
    },
    RmgrData {
        rm_name: "Generic",
        rm_redo: generic_xlog::generic_redo,
        rm_desc: rmgrdesc::genericdesc::generic_desc,
        rm_identify: rmgrdesc::genericdesc::generic_identify,
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(generic_xlog::generic_mask),
    },
    RmgrData {
        rm_name: "LogicalMessage",
        rm_redo: logical_message::logicalmsg_redo,
        rm_desc: rmgrdesc::logicalmsgdesc::logicalmsg_desc,
        rm_identify: rmgrdesc::logicalmsgdesc::logicalmsg_identify,
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
        .finish(ErrorLocation::new(file!(), line!() as i32, "RmgrNotFound"))
}

pub fn init_seams() {}
