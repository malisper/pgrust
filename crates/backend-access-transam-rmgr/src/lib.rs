//! `src/backend/access/transam/rmgr.c` — the resource-manager table and its
//! dispatch (`RmgrStartup` / `RmgrCleanup` / `RmgrNotFound` /
//! `RegisterCustomRmgr` / `pg_get_wal_resource_managers`), plus the
//! `RmgrIdExists` / `GetRmgr` inline helpers from `access/xlog_internal.h`.
//!
//! `RmgrTable[RM_MAX_ID + 1]` is C backend-private global state, so here it
//! is a `thread_local!`. The builtin slots (ids `0..=RM_MAX_BUILTIN_ID`) are
//! populated from `access/rmgrlist.h`; entry order fixes the numeric rmgr id.
//! Every non-NULL slot is a callback owned by another subsystem (xlog, the
//! AMs, rmgrdesc, logical decoding, ...), reached through the owner's seam
//! crate: the slot holds the seam's `call` fn, which panics loudly until the
//! owning crate lands and installs it — exactly the external-symbol reference
//! the C table makes across the link. C NULL slots are `None`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use mcx::Mcx;
use port_pgstrcasecmp::pg_strcasecmp;
use types_core::RmgrId;
use types_datum::Datum;
use types_error::{ErrorLocation, PgError, PgResult, ERROR, LOG};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_wal::rmgr::{
    RmgrData, RmgrIdIsBuiltin, RmgrIdIsCustom, RM_MAX_CUSTOM_ID, RM_MIN_CUSTOM_ID,
    RM_N_BUILTIN_IDS, RM_N_IDS,
};

use backend_access_brin_xlog_seams as brin_xlog;
use backend_access_gin_core_seams as ginxlog;
use backend_access_gist_core_seams as gistxlog;
use backend_access_hash_xlog_seams as hash_xlog;
use backend_access_heap_heapam_xlog_seams as heapam_xlog;
use backend_access_nbt_xlog_seams as nbtxlog;
use backend_access_rmgrdesc_brindesc_seams as brindesc;
use backend_access_rmgrdesc_clogdesc_seams as clogdesc;
use backend_access_rmgrdesc_committsdesc_seams as committsdesc;
use backend_access_rmgrdesc_dbasedesc_seams as dbasedesc;
use backend_access_rmgrdesc_genericdesc_seams as genericdesc;
use backend_access_rmgrdesc_gindesc_seams as gindesc;
use backend_access_rmgrdesc_gistdesc_seams as gistdesc;
use backend_access_rmgrdesc_hashdesc_seams as hashdesc;
use backend_access_rmgrdesc_heapdesc_seams as heapdesc;
use backend_access_rmgrdesc_logicalmsgdesc_seams as logicalmsgdesc;
use backend_access_rmgrdesc_mxactdesc_seams as mxactdesc;
use backend_access_rmgrdesc_nbtdesc_seams as nbtdesc;
use backend_access_rmgrdesc_relmapdesc_seams as relmapdesc;
use backend_access_rmgrdesc_replorigindesc_seams as replorigindesc;
use backend_access_rmgrdesc_seqdesc_seams as seqdesc;
use backend_access_rmgrdesc_smgrdesc_seams as smgrdesc;
use backend_access_rmgrdesc_spgdesc_seams as spgdesc;
use backend_access_rmgrdesc_standbydesc_seams as standbydesc;
use backend_access_rmgrdesc_tblspcdesc_seams as tblspcdesc;
use backend_access_rmgrdesc_xactdesc_seams as xactdesc;
use backend_access_rmgrdesc_xlogdesc_seams as xlogdesc;
use backend_access_spg_xlog_seams as spgxlog;
use backend_access_transam_clog_seams as clog;
use backend_access_transam_commit_ts_seams as commit_ts;
use backend_access_transam_generic_xlog_seams as generic_xlog;
use backend_access_transam_multixact_seams as multixact;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_catalog_storage_seams as storage;
use backend_commands_dbcommands_seams as dbcommands;
use backend_commands_sequence_seams as sequence;
use backend_commands_tablespace_seams as tablespace;
use backend_replication_logical_decode_seams as decode;
use backend_replication_logical_message_seams as message;
use backend_replication_logical_origin_seams as origin;
use backend_storage_ipc_standby_seams as standby;
use backend_utils_adt_varlena_seams as varlena;
use backend_utils_cache_relmapper_seams as relmapper;
use backend_utils_error_seams as error_seams;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_utils_init_miscinit_seams as miscinit;

/// Source file recorded in `ErrorLocation`s, matching C `__FILE__`.
const SRCFILE: &str = "src/backend/access/transam/rmgr.c";

/// The builtin rows of `RmgrData RmgrTable[RM_MAX_ID + 1] = { #include
/// "access/rmgrlist.h" };` (rmgr.c:50), one per `PG_RMGR(...)` line, in
/// declaration order.
///
/// rmgrlist.h column order: symbol name, textual name, redo, desc, identify,
/// startup, cleanup, mask, decode.
static RMGR_BUILTIN_TABLE: [RmgrData; RM_N_BUILTIN_IDS] = [
    // PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, xlog_identify,
    //         NULL, NULL, NULL, xlog_decode)
    RmgrData {
        rm_name: Some("XLOG"),
        rm_redo: Some(xlog::xlog_redo::call),
        rm_desc: Some(xlogdesc::xlog_desc::call),
        rm_identify: Some(xlogdesc::xlog_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: Some(decode::xlog_decode::call),
    },
    // PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, xact_identify,
    //         NULL, NULL, NULL, xact_decode)
    RmgrData {
        rm_name: Some("Transaction"),
        rm_redo: Some(xact::xact_redo::call),
        rm_desc: Some(xactdesc::xact_desc::call),
        rm_identify: Some(xactdesc::xact_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: Some(decode::xact_decode::call),
    },
    // PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, smgr_identify,
    //         NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("Storage"),
        rm_redo: Some(storage::smgr_redo::call),
        rm_desc: Some(smgrdesc::smgr_desc::call),
        rm_identify: Some(smgrdesc::smgr_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, clog_identify,
    //         NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("CLOG"),
        rm_redo: Some(clog::clog_redo::call),
        rm_desc: Some(clogdesc::clog_desc::call),
        rm_identify: Some(clogdesc::clog_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, dbase_identify,
    //         NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("Database"),
        rm_redo: Some(dbcommands::dbase_redo::call),
        rm_desc: Some(dbasedesc::dbase_desc::call),
        rm_identify: Some(dbasedesc::dbase_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc,
    //         tblspc_identify, NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("Tablespace"),
        rm_redo: Some(tablespace::tblspc_redo::call),
        rm_desc: Some(tblspcdesc::tblspc_desc::call),
        rm_identify: Some(tblspcdesc::tblspc_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc,
    //         multixact_identify, NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("MultiXact"),
        rm_redo: Some(multixact::multixact_redo::call),
        rm_desc: Some(mxactdesc::multixact_desc::call),
        rm_identify: Some(mxactdesc::multixact_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc,
    //         relmap_identify, NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("RelMap"),
        rm_redo: Some(relmapper::relmap_redo::call),
        rm_desc: Some(relmapdesc::relmap_desc::call),
        rm_identify: Some(relmapdesc::relmap_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc,
    //         standby_identify, NULL, NULL, NULL, standby_decode)
    RmgrData {
        rm_name: Some("Standby"),
        rm_redo: Some(standby::standby_redo::call),
        rm_desc: Some(standbydesc::standby_desc::call),
        rm_identify: Some(standbydesc::standby_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: Some(decode::standby_decode::call),
    },
    // PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, heap2_identify,
    //         NULL, NULL, heap_mask, heap2_decode)
    RmgrData {
        rm_name: Some("Heap2"),
        rm_redo: Some(heapam_xlog::heap2_redo::call),
        rm_desc: Some(heapdesc::heap2_desc::call),
        rm_identify: Some(heapdesc::heap2_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heapam_xlog::heap_mask::call),
        rm_decode: Some(decode::heap2_decode::call),
    },
    // PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, heap_identify,
    //         NULL, NULL, heap_mask, heap_decode)
    RmgrData {
        rm_name: Some("Heap"),
        rm_redo: Some(heapam_xlog::heap_redo::call),
        rm_desc: Some(heapdesc::heap_desc::call),
        rm_identify: Some(heapdesc::heap_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(heapam_xlog::heap_mask::call),
        rm_decode: Some(decode::heap_decode::call),
    },
    // PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_identify,
    //         btree_xlog_startup, btree_xlog_cleanup, btree_mask, NULL)
    RmgrData {
        rm_name: Some("Btree"),
        rm_redo: Some(nbtxlog::btree_redo::call),
        rm_desc: Some(nbtdesc::btree_desc::call),
        rm_identify: Some(nbtdesc::btree_identify::call),
        rm_startup: Some(nbtxlog::btree_xlog_startup::call),
        rm_cleanup: Some(nbtxlog::btree_xlog_cleanup::call),
        rm_mask: Some(nbtxlog::btree_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, hash_identify,
    //         NULL, NULL, hash_mask, NULL)
    RmgrData {
        rm_name: Some("Hash"),
        rm_redo: Some(hash_xlog::hash_redo::call),
        rm_desc: Some(hashdesc::hash_desc::call),
        rm_identify: Some(hashdesc::hash_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(hash_xlog::hash_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_identify,
    //         gin_xlog_startup, gin_xlog_cleanup, gin_mask, NULL)
    RmgrData {
        rm_name: Some("Gin"),
        rm_redo: Some(ginxlog::gin_redo::call),
        rm_desc: Some(gindesc::gin_desc::call),
        rm_identify: Some(gindesc::gin_identify::call),
        rm_startup: Some(ginxlog::gin_xlog_startup::call),
        rm_cleanup: Some(ginxlog::gin_xlog_cleanup::call),
        rm_mask: Some(ginxlog::gin_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_identify,
    //         gist_xlog_startup, gist_xlog_cleanup, gist_mask, NULL)
    RmgrData {
        rm_name: Some("Gist"),
        rm_redo: Some(gistxlog::gist_redo::call),
        rm_desc: Some(gistdesc::gist_desc::call),
        rm_identify: Some(gistdesc::gist_identify::call),
        rm_startup: Some(gistxlog::gist_xlog_startup::call),
        rm_cleanup: Some(gistxlog::gist_xlog_cleanup::call),
        rm_mask: Some(gistxlog::gist_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, seq_identify,
    //         NULL, NULL, seq_mask, NULL)
    RmgrData {
        rm_name: Some("Sequence"),
        rm_redo: Some(sequence::seq_redo::call),
        rm_desc: Some(seqdesc::seq_desc::call),
        rm_identify: Some(seqdesc::seq_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(sequence::seq_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_identify,
    //         spg_xlog_startup, spg_xlog_cleanup, spg_mask, NULL)
    RmgrData {
        rm_name: Some("SPGist"),
        rm_redo: Some(spgxlog::spg_redo::call),
        rm_desc: Some(spgdesc::spg_desc::call),
        rm_identify: Some(spgdesc::spg_identify::call),
        rm_startup: Some(spgxlog::spg_xlog_startup::call),
        rm_cleanup: Some(spgxlog::spg_xlog_cleanup::call),
        rm_mask: Some(spgxlog::spg_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_BRIN_ID, "BRIN", brin_redo, brin_desc, brin_identify,
    //         NULL, NULL, brin_mask, NULL)
    RmgrData {
        rm_name: Some("BRIN"),
        rm_redo: Some(brin_xlog::brin_redo::call),
        rm_desc: Some(brindesc::brin_desc::call),
        rm_identify: Some(brindesc::brin_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(brin_xlog::brin_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_COMMIT_TS_ID, "CommitTs", commit_ts_redo, commit_ts_desc,
    //         commit_ts_identify, NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("CommitTs"),
        rm_redo: Some(commit_ts::commit_ts_redo::call),
        rm_desc: Some(committsdesc::commit_ts_desc::call),
        rm_identify: Some(committsdesc::commit_ts_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo,
    //         replorigin_desc, replorigin_identify, NULL, NULL, NULL, NULL)
    RmgrData {
        rm_name: Some("ReplicationOrigin"),
        rm_redo: Some(origin::replorigin_redo::call),
        rm_desc: Some(replorigindesc::replorigin_desc::call),
        rm_identify: Some(replorigindesc::replorigin_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: None,
    },
    // PG_RMGR(RM_GENERIC_ID, "Generic", generic_redo, generic_desc,
    //         generic_identify, NULL, NULL, generic_mask, NULL)
    RmgrData {
        rm_name: Some("Generic"),
        rm_redo: Some(generic_xlog::generic_redo::call),
        rm_desc: Some(genericdesc::generic_desc::call),
        rm_identify: Some(genericdesc::generic_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: Some(generic_xlog::generic_mask::call),
        rm_decode: None,
    },
    // PG_RMGR(RM_LOGICALMSG_ID, "LogicalMessage", logicalmsg_redo,
    //         logicalmsg_desc, logicalmsg_identify, NULL, NULL, NULL,
    //         logicalmsg_decode)
    RmgrData {
        rm_name: Some("LogicalMessage"),
        rm_redo: Some(message::logicalmsg_redo::call),
        rm_desc: Some(logicalmsgdesc::logicalmsg_desc::call),
        rm_identify: Some(logicalmsgdesc::logicalmsg_identify::call),
        rm_startup: None,
        rm_cleanup: None,
        rm_mask: None,
        rm_decode: Some(decode::logicalmsg_decode::call),
    },
];

thread_local! {
    /// `RmgrData RmgrTable[RM_MAX_ID + 1]` (rmgr.c:50). Backend-private in C
    /// (custom registrations happen per-process during
    /// `shared_preload_libraries`), hence thread-local here.
    static RMGR_TABLE: RefCell<[RmgrData; RM_N_IDS]> = RefCell::new(initial_rmgr_table());
}

/// C's load-time aggregate initializer: builtin rows from `rmgrlist.h`,
/// every other slot zero-initialized (`rm_name == NULL`).
fn initial_rmgr_table() -> [RmgrData; RM_N_IDS] {
    let mut table = [RmgrData::EMPTY; RM_N_IDS];
    table[..RM_N_BUILTIN_IDS].copy_from_slice(&RMGR_BUILTIN_TABLE);
    table
}

/// `RmgrTable[rmid]`.
fn rmgr_table_slot(rmid: usize) -> RmgrData {
    RMGR_TABLE.with(|table| table.borrow()[rmid])
}

/// `RmgrIdExists(rmid)` (access/xlog_internal.h):
/// `return RmgrTable[rmid].rm_name != NULL;`
pub fn RmgrIdExists(rmid: RmgrId) -> bool {
    rmgr_table_slot(rmid as usize).rm_name.is_some()
}

/// `GetRmgr(rmid)` (access/xlog_internal.h): the table row, or
/// `RmgrNotFound`'s ERROR for an unregistered id.
pub fn GetRmgr(rmid: RmgrId) -> PgResult<RmgrData> {
    if !RmgrIdExists(rmid) {
        RmgrNotFound(rmid)?;
    }
    Ok(rmgr_table_slot(rmid as usize))
}

/// `RmgrStartup(void)` (rmgr.c:58) — start up all resource managers.
/// Fallible because `rm_startup` callbacks can `ereport(ERROR)` (recovery
/// context allocation). `parent` is the context the callbacks create their
/// recovery contexts under (C: their `AllocSetContextCreate` parent is
/// `CurrentMemoryContext` at this call).
pub fn RmgrStartup(parent: Mcx<'_>) -> PgResult<()> {
    for rmid in 0..RM_N_IDS {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        if let Some(startup) = rmgr_table_slot(rmid).rm_startup {
            startup(parent)?;
        }
    }
    Ok(())
}

/// `RmgrCleanup(void)` (rmgr.c:74) — clean up all resource managers.
pub fn RmgrCleanup() {
    for rmid in 0..RM_N_IDS {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        if let Some(cleanup) = rmgr_table_slot(rmid).rm_cleanup {
            cleanup();
        }
    }
}

/// `RmgrNotFound(RmgrId rmid)` (rmgr.c:90) — the ERROR for a record with an
/// unrecognized RmgrId. Always returns `Err`.
pub fn RmgrNotFound(rmid: RmgrId) -> PgResult<()> {
    Err(
        PgError::new(ERROR, format!("resource manager with ID {rmid} not registered"))
            .with_hint(
                "Include the extension module that implements this resource manager in \
                 \"shared_preload_libraries\".",
            )
            .with_error_location(ErrorLocation::new(SRCFILE, 94, "RmgrNotFound")),
    )
}

/// `RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr)` (rmgr.c:107) —
/// register a new custom WAL resource manager.
///
/// Resource manager IDs must be globally unique across all extensions. Refer
/// to https://wiki.postgresql.org/wiki/CustomWALResourceManagers to reserve a
/// unique RmgrId for your extension; during development, use
/// `RM_EXPERIMENTAL_ID` to avoid needlessly reserving a new ID.
pub fn RegisterCustomRmgr(rmid: RmgrId, rmgr: &RmgrData) -> PgResult<()> {
    let rm_name = match rmgr.rm_name {
        Some(name) if !name.is_empty() => name,
        // C: rm_name == NULL || strlen(rm_name) == 0
        _ => {
            return Err(PgError::new(ERROR, "custom resource manager name is invalid")
                .with_hint("Provide a non-empty name for the custom resource manager.")
                .with_error_location(ErrorLocation::new(SRCFILE, 111, "RegisterCustomRmgr")));
        }
    };

    if !RmgrIdIsCustom(rmid as i32) {
        return Err(
            PgError::new(ERROR, format!("custom resource manager ID {rmid} is out of range"))
                .with_hint(format!(
                    "Provide a custom resource manager ID between {RM_MIN_CUSTOM_ID} and \
                     {RM_MAX_CUSTOM_ID}."
                ))
                .with_error_location(ErrorLocation::new(SRCFILE, 116, "RegisterCustomRmgr")),
        );
    }

    if !miscinit::process_shared_preload_libraries_in_progress::call() {
        return Err(PgError::new(
            ERROR,
            format!("failed to register custom resource manager \"{rm_name}\" with ID {rmid}"),
        )
        .with_detail(
            "Custom resource manager must be registered while initializing modules in \
             \"shared_preload_libraries\".",
        )
        .with_error_location(ErrorLocation::new(SRCFILE, 121, "RegisterCustomRmgr")));
    }

    if let Some(existing_name) = rmgr_table_slot(rmid as usize).rm_name {
        return Err(PgError::new(
            ERROR,
            format!("failed to register custom resource manager \"{rm_name}\" with ID {rmid}"),
        )
        .with_detail(format!(
            "Custom resource manager \"{existing_name}\" already registered with the same ID."
        ))
        .with_error_location(ErrorLocation::new(SRCFILE, 127, "RegisterCustomRmgr")));
    }

    // check for existing rmgr with the same name
    for existing_rmid in 0..RM_N_IDS {
        if !RmgrIdExists(existing_rmid as RmgrId) {
            continue;
        }

        let existing_name = rmgr_table_slot(existing_rmid)
            .rm_name
            .expect("RmgrIdExists guarantees rm_name");
        if pg_strcasecmp(existing_name.as_bytes(), rm_name.as_bytes()) == 0 {
            return Err(PgError::new(
                ERROR,
                format!("failed to register custom resource manager \"{rm_name}\" with ID {rmid}"),
            )
            .with_detail(format!(
                "Existing resource manager with ID {existing_rmid} has the same name."
            ))
            .with_error_location(ErrorLocation::new(SRCFILE, 138, "RegisterCustomRmgr")));
        }
    }

    // register it
    RMGR_TABLE.with(|table| table.borrow_mut()[rmid as usize] = *rmgr);

    error_seams::ereport::call(
        PgError::new(
            LOG,
            format!("registered custom resource manager \"{rm_name}\" with ID {rmid}"),
        )
        .with_error_location(ErrorLocation::new(SRCFILE, 145, "RegisterCustomRmgr")),
    )?;

    Ok(())
}

/// `pg_get_wal_resource_managers(PG_FUNCTION_ARGS)` (rmgr.c:151) — SQL SRF
/// showing loaded resource managers.
///
/// The tuplestore materialization crosses the funcapi seams (funcapi owns
/// `InitMaterializedSRF` and the `setResult`/`setDesc` resolution); `mcx` is
/// the per-query context the C function's `CStringGetTextDatum` pallocs in.
pub fn pg_get_wal_resource_managers(
    mcx: Mcx<'_>,
    fcinfo: &mut FunctionCallInfoBaseData<'_>,
) -> PgResult<Datum> {
    const PG_GET_RESOURCE_MANAGERS_COLS: usize = 3;

    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;

    // ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    for rmid in 0..RM_N_IDS {
        if !RmgrIdExists(rmid as RmgrId) {
            continue;
        }

        let name = GetRmgr(rmid as RmgrId)?
            .rm_name
            .expect("RmgrIdExists guarantees rm_name");
        // Datum values[3]; bool nulls[3] = {0};  (stack arrays in C)
        let values: [Datum; PG_GET_RESOURCE_MANAGERS_COLS] = [
            // values[0] = Int32GetDatum(rmid)
            Datum::from_i32(rmid as i32),
            // values[1] = CStringGetTextDatum(GetRmgr(rmid).rm_name)
            varlena::cstring_to_text::call(mcx, name)?,
            // values[2] = BoolGetDatum(RmgrIdIsBuiltin(rmid))
            Datum::from_bool(RmgrIdIsBuiltin(rmid as i32)),
        ];
        let nulls = [false; PG_GET_RESOURCE_MANAGERS_COLS];

        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
    }

    // return (Datum) 0;
    Ok(Datum::null())
}

#[cfg(test)]
mod tests;
