//! `contrib/pg_prewarm/pg_prewarm.c` — the `pg_prewarm()` SQL function —
//! plus autoprewarm.c's module-load surface (_PG_init:126): the
//! `pg_prewarm.autoprewarm_interval` and `pg_prewarm.autoprewarm` GUCs
//! (table rows in guc_tables, the C statics' cells here), the reserved
//! prefix, and the leader registration under shared_preload_libraries.
//! The worker, shared state, dump file and SQL entry points are in
//! `autoprewarm.rs`.

mod autoprewarm;

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use datum::Datum;
use types_core::{BlockNumber, ForkNumber, Oid, OidIsValid, BLCKSZ};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::parsenodes::{ObjectType, ACL_SELECT};
use types_rel::pg_class::{
    RELKIND_FOREIGN_TABLE, RELKIND_HAS_STORAGE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_VIEW,
};
use types_storage::storage::ReadBufferMode;
use types_storage::RelFileLocatorBackend;

const LIBRARY: &str = "pg_prewarm";

// autoprewarm.c:120 `static int autoprewarm_interval = 300` (dump interval,
// seconds). PGC_SIGHUP, so a process-global cell like the backend's
// sighup-scope GUCs, not a per-session backing. The leader worker reads it
// each loop (autoprewarm_main:239); SHOW/pg_settings go through the
// registry.
static AUTOPREWARM_INTERVAL: AtomicI32 = AtomicI32::new(300);

// autoprewarm.c:119 `static bool autoprewarm = true` (start the leader?).
// PGC_POSTMASTER; read by pg_init (:158) and autoprewarm_start_worker (:829).
static AUTOPREWARM: AtomicBool = AtomicBool::new(true);

fn autoprewarm() -> bool {
    AUTOPREWARM.load(Ordering::Relaxed)
}

fn set_autoprewarm(v: bool) {
    AUTOPREWARM.store(v, Ordering::Relaxed);
}

fn autoprewarm_interval() -> i32 {
    AUTOPREWARM_INTERVAL.load(Ordering::Relaxed)
}

fn set_autoprewarm_interval(v: i32) {
    AUTOPREWARM_INTERVAL.store(v, Ordering::Relaxed);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrewarmType {
    Prefetch,
    Read,
    Buffer,
}

#[track_caller]
#[cold]
fn param_err(msg: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn get_relkind_objtype(relkind: u8) -> ObjectType {
    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => ObjectType::OBJECT_TABLE,
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => ObjectType::OBJECT_INDEX,
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

// forkname_to_number (common/relpath.c), backend flavor.
fn forkname_to_number(name: &str) -> PgResult<ForkNumber> {
    Ok(match name {
        "main" => ForkNumber::MAIN_FORKNUM,
        "fsm" => ForkNumber::FSM_FORKNUM,
        "vm" => ForkNumber::VISIBILITYMAP_FORKNUM,
        "init" => ForkNumber::INIT_FORKNUM,
        _ => {
            return Err(Box::new(
                PgError::error("invalid fork name")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."),
            ))
        }
    })
}

fn arg_text_string(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: arg i was null-checked by the caller (pg_prewarm is not strict).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(String::from_utf8_lossy(v.data()).into_owned())
}

// RelationGetSmgr: smgropen is idempotent; guarantees the md entry exists
// before smgrexists/smgrread.
pub(crate) fn rel_smgr_key(rel: &types_rel::Relation<'_>) -> PgResult<RelFileLocatorBackend> {
    let locator = rel.rd_locator.get();
    smgr::smgropen(locator, rel.rd_backend)?;
    Ok(RelFileLocatorBackend { locator, backend: rel.rd_backend })
}

fn fc_pg_prewarm(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a_rel, a_type, a_fork, a_first, a_last] = *fcinfo.args_n::<5>();

    if a_rel.isnull {
        return Err(param_err("relation cannot be null"));
    }
    let rel_oid: Oid = a_rel.value.as_oid();

    if a_type.isnull {
        return Err(param_err("prewarm type cannot be null"));
    }
    let ttype = arg_text_string(fcinfo, 1)?;
    let ptype = match ttype.as_str() {
        "prefetch" => PrewarmType::Prefetch,
        "read" => PrewarmType::Read,
        "buffer" => PrewarmType::Buffer,
        _ => {
            return Err(Box::new(
                PgError::error("invalid prewarm type")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint(
                        "Valid prewarm types are \"prefetch\", \"read\", and \"buffer\".",
                    ),
            ))
        }
    };

    if a_fork.isnull {
        return Err(param_err("relation fork cannot be null"));
    }
    let fork_string = arg_text_string(fcinfo, 2)?;
    let fork_number = forkname_to_number(&fork_string)?;

    // Index: check privileges on the parent table; lock table before index.
    let mcx = fcinfo.result_mcx();
    let relkind = lsyscache::get_rel_relkind(rel_oid)? as u8;
    let priv_oid = if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        let p = catalog_index::IndexGetRelation(mcx, rel_oid, true)?;
        if OidIsValid(p) {
            lmgr_seams::lock_relation_oid::call(p, types_rel::AccessShareLock)?;
        }
        p
    } else {
        rel_oid
    };

    let rel = relation::relation_open(mcx, rel_oid, types_rel::AccessShareLock)?;

    // privOid may have been dropped and reused before we locked it.
    if !OidIsValid(priv_oid)
        || (priv_oid != rel_oid && priv_oid != catalog_index::IndexGetRelation(mcx, rel_oid, true)?)
    {
        return Err(Box::new(
            PgError::error(format!(
                "could not find parent table of index \"{}\"",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
        ));
    }

    let aclresult = aclchk::pg_class_aclcheck(priv_oid, miscinit::GetUserId(), ACL_SELECT)?;
    if aclresult != aclchk::ACLCHECK_OK {
        let name = lsyscache::get_rel_name(mcx, rel_oid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        aclchk::aclcheck_error(aclresult, get_relkind_objtype(rel.rd_rel.relkind), &name)?;
    }

    if !RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" does not have storage", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    let smgr_key = rel_smgr_key(&rel)?;
    if !smgr::smgrexists(smgr_key, fork_number)? {
        return Err(param_err(format!(
            "fork \"{fork_string}\" does not exist for this relation"
        )));
    }

    let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(&rel, fork_number)? as i64;

    let first_block = if a_first.isnull {
        0
    } else {
        let fb = a_first.value.as_i64();
        if fb < 0 || fb >= nblocks {
            return Err(param_err(format!(
                "starting block number must be between 0 and {}",
                nblocks - 1
            )));
        }
        fb
    };
    let last_block = if a_last.isnull {
        nblocks - 1
    } else {
        let lb = a_last.value.as_i64();
        if lb < 0 || lb >= nblocks {
            return Err(param_err(format!(
                "ending block number must be between 0 and {}",
                nblocks - 1
            )));
        }
        lb
    };

    let mut blocks_done: i64 = 0;
    match ptype {
        PrewarmType::Prefetch => {
            for block in first_block..=last_block {
                postgres_seams::check_for_interrupts::call()?;
                bufmgr::PrefetchBuffer(&rel, fork_number, block as BlockNumber)?;
                blocks_done += 1;
            }
        }
        PrewarmType::Read => {
            // pg_prewarm.c: static PGIOAlignedBlock blockbuffer — I/O-aligned
            // (4096) so smgrread works on an O_DIRECT fd (debug_io_direct=data).
            #[repr(align(4096))]
            struct BlockBuffer([u8; BLCKSZ]);
            let mut blockbuffer = BlockBuffer([0u8; BLCKSZ]);
            for block in first_block..=last_block {
                postgres_seams::check_for_interrupts::call()?;
                smgr::smgrread(smgr_key, fork_number, block as BlockNumber, &mut blockbuffer.0)?;
                blocks_done += 1;
            }
        }
        PrewarmType::Buffer => {
            // C 18 streams via read_stream; no read_stream surface exists here,
            // so this is the pre-17 per-block ReadBuffer loop — same buffers
            // pulled into shared_buffers, same count returned.
            // read_stream_begin_impl (read_stream.c:563) rejects before the
            // first block, so an empty range still errors.
            if rel.is_other_temp() {
                return Err(Box::new(
                    PgError::error("cannot access temporary tables of other sessions")
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            for block in first_block..=last_block {
                postgres_seams::check_for_interrupts::call()?;
                let buf = bufmgr::ReadBufferExtended(
                    &rel,
                    fork_number,
                    block as BlockNumber,
                    ReadBufferMode::Normal,
                    None,
                )?;
                bufmgr::ReleaseBuffer(buf)?;
                blocks_done += 1;
            }
        }
    }

    rel.close(types_rel::AccessShareLock)?;
    if priv_oid != rel_oid {
        lmgr_seams::unlock_relation_oid::call(priv_oid, types_rel::AccessShareLock)?;
    }

    Ok(Datum::from_i64(blocks_done))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_prewarm" => fc_pg_prewarm,
        "autoprewarm_start_worker" => autoprewarm::fc_autoprewarm_start_worker,
        "autoprewarm_dump_now" => autoprewarm::fc_autoprewarm_dump_now,
        _ => return None,
    })
}

pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::autoprewarm_interval.install(GucVarAccessors {
        get: autoprewarm_interval,
        set: set_autoprewarm_interval,
    });
    guc_tables::vars::autoprewarm.install(GucVarAccessors {
        get: autoprewarm,
        set: set_autoprewarm,
    });
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: Some(pg_init),
    });
}

/// `_PG_init` (autoprewarm.c:126). Both GUCs are static guc_tables rows
/// (this port has no DefineCustomXxxVariable; the auto_explain / pgss
/// pattern), so only the preload arm's prefix reservation and leader
/// registration happen here.
fn pg_init() -> PgResult<()> {
    if !miscinit::process_shared_preload_libraries_in_progress() {
        return Ok(());
    }

    guc::MarkGUCPrefixReserved("pg_prewarm");

    // Register autoprewarm worker, if enabled.
    if autoprewarm() {
        autoprewarm::apw_start_leader_worker()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forknames() {
        assert_eq!(forkname_to_number("main").unwrap(), ForkNumber::MAIN_FORKNUM);
        assert_eq!(forkname_to_number("vm").unwrap(), ForkNumber::VISIBILITYMAP_FORKNUM);
        let err = forkname_to_number("bogus").unwrap_err();
        assert!(err.message().contains("invalid fork name"));
    }
}
