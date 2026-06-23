#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `contrib/pg_prewarm/pg_prewarm.c` — the `pg_prewarm()` SQL-callable function.
//!
//! Ported 1:1 from C. Registered as the in-process ported library `pg_prewarm`
//! (mirroring `backend-test-regress`'s `regress` registration): the SQL emitted
//! by `pg_prewarm--1.1.sql` (`CREATE FUNCTION pg_prewarm(...) LANGUAGE C AS
//! 'MODULE_PATHNAME','pg_prewarm'`) resolves through the dynamic-loader unit's
//! ported-library registry rather than the OS loader (the Rust backend exposes
//! no C ABI). The autoprewarm sibling functions (`autoprewarm_start_worker` /
//! `autoprewarm_dump_now`, added by `pg_prewarm--1.1--1.2.sql`) are registered as
//! loud-panic stubs so `CREATE FUNCTION`'s C validator (`fmgr_c_validator` →
//! `load_external_function`, which is NOT gated by `check_function_bodies`) finds
//! the symbol — their bgworker/shmem bodies in `autoprewarm.c` are unported, so an
//! actual call mirror-pg-and-panics.

extern crate alloc;

use alloc::rc::Rc;
use core::cell::RefCell;

use ::mcx::MemoryContext;
use ::types_core::primitive::{BlockNumber, ForkNumber, Oid};
use ::datum::Datum;
use ::types_error::{
    PgError, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE,
};
use ::fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};
use ::rel::Relation;
use ::types_storage::lock::AccessShareLock;
use ::types_storage::storage::InvalidBuffer;
use ::types_storage::RelFileLocatorBackend;

use ::utils_error::ereport;

use read_stream as read_stream;
use ::bufmgr::BufferManager;
use smgr as smgr;

use aclchk_seams as aclchk_seams;
use objectaddress_seams as objectaddress_seams;
use pg_class_seams as pg_class_seams;
use bufmgr_seams as bufmgr_seams;
use lmgr_seams as lmgr_seams;
use lsyscache_seams as lsyscache_seams;

use ::types_acl::acl::{ACLCHECK_OK, ACL_SELECT};
use ::types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};

/// The simple (suffix-free, directory-free) name of the loadable module —
/// `$libdir/pg_prewarm` reduces to this for the registry.
const LIBRARY: &str = "pg_prewarm";

/// `BLCKSZ` (pg_config.h) — the block size; the `blockbuffer` scratch is one
/// page (the C `PGIOAlignedBlock blockbuffer`).
const BLCKSZ: usize = 8192;

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`] (mirrors
/// `backend-test-regress`).
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `PrewarmType` (pg_prewarm.c) — the prewarming strategy.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PrewarmType {
    Prefetch,
    Read,
    Buffer,
}

/// `RELKIND_HAS_STORAGE(relkind)` (pg_class.h) — does a relation of this kind
/// have physical storage?
fn RELKIND_HAS_STORAGE(relkind: u8) -> bool {
    use ::types_tuple::access::{
        RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    };
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

/// `RelationGetRelationName(rel)` — the relation's name as a `String`.
fn rel_name(rel: &Relation) -> String {
    rel.name().to_string()
}

/// `RelationGetSmgr(rel)` (rel.h) — the relation's `SMgrRelation`, as the
/// `RelFileLocatorBackend` smgr key. The C inline lazily `smgropen`s the relation
/// the first time; `smgropen`/`cache_open` is idempotent, so call it to guarantee
/// the md entry exists before the smgr operations (`smgrexists` / `smgrread`),
/// which otherwise panic ("md operation on an unopened SMgrRelation").
fn rel_smgr_key(rel: &Relation) -> Result<RelFileLocatorBackend, PgError> {
    smgr::smgropen(rel.rd_locator, rel.rd_backend)?;
    Ok(RelFileLocatorBackend {
        locator: rel.rd_locator,
        backend: rel.rd_backend,
    })
}

/// `pg_prewarm(regclass, mode text, fork text, first_block int8, last_block
/// int8) RETURNS int8` (pg_prewarm.c) — the SQL-callable function.
///
/// The first argument is the relation to be prewarmed; the second controls how
/// prewarming is done ('prefetch', 'read', 'buffer'); the third is the name of
/// the relation fork; the fourth and fifth specify the first and last block. If
/// the fourth is NULL, it is taken as 0; if the fifth is NULL, the number of
/// blocks in the relation. The return value is the number of blocks successfully
/// prewarmed.
fn fc_pg_prewarm(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match pg_prewarm_impl(fcinfo) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}

fn pg_prewarm_impl(fcinfo: &mut FunctionCallInfoBaseData) -> Result<Datum, PgError> {
    let mut blocks_done: i64 = 0;

    // Basic sanity checking.
    //   if (PG_ARGISNULL(0)) ereport(ERROR, errmsg("relation cannot be null"));
    if arg_isnull(fcinfo, 0) {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("relation cannot be null")
            .into_error());
    }
    let rel_oid: Oid = arg_oid(fcinfo, 0);

    //   if (PG_ARGISNULL(1)) ereport(ERROR, errmsg("prewarm type cannot be null"));
    if arg_isnull(fcinfo, 1) {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("prewarm type cannot be null")
            .into_error());
    }
    let ttype = arg_text_cstring(fcinfo, 1);
    let ptype = if ttype == "prefetch" {
        PrewarmType::Prefetch
    } else if ttype == "read" {
        PrewarmType::Read
    } else if ttype == "buffer" {
        PrewarmType::Buffer
    } else {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("invalid prewarm type")
            .errhint("Valid prewarm types are \"prefetch\", \"read\", and \"buffer\".")
            .into_error());
    };

    //   if (PG_ARGISNULL(2)) ereport(ERROR, errmsg("relation fork cannot be null"));
    if arg_isnull(fcinfo, 2) {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("relation fork cannot be null")
            .into_error());
    }
    let fork_string = arg_text_cstring(fcinfo, 2);
    let fork_number = relpath::forkname_to_number(&fork_string)?;

    // Open relation and check privileges. If the relation is an index, we must
    // check the privileges on its parent table instead.
    let relkind = lsyscache_seams::get_rel_relkind::call(rel_oid)?;
    let priv_oid: Oid;
    if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        priv_oid = index::IndexGetRelation(rel_oid, true)?;
        // Lock table before index to avoid deadlock.
        if ::types_core::OidIsValid(priv_oid) {
            lmgr_seams::lock_relation_oid::call(priv_oid, AccessShareLock)?.keep();
        }
    } else {
        priv_oid = rel_oid;
    }

    let scratch = MemoryContext::new("pg_prewarm relation");
    let mcx = scratch.mcx();
    let rel: Relation = common_relation::relation_open(mcx, rel_oid, AccessShareLock)?;

    // It's possible that the relation with OID "privOid" was dropped and the OID
    // was reused before we locked it. If that happens, we could be left with the
    // wrong parent table OID, in which case we must ERROR.
    //   if (!OidIsValid(privOid) ||
    //       (privOid != relOid &&
    //        privOid != IndexGetRelation(relOid, true)))
    if !::types_core::OidIsValid(priv_oid)
        || (priv_oid != rel_oid
            && priv_oid != index::IndexGetRelation(rel_oid, true)?)
    {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!(
                "could not find parent table of index \"{}\"",
                rel_name(&rel)
            ))
            .into_error());
    }

    //   aclresult = pg_class_aclcheck(privOid, GetUserId(), ACL_SELECT);
    //   if (aclresult != ACLCHECK_OK)
    //       aclcheck_error(aclresult, get_relkind_objtype(rel->rd_rel->relkind),
    //                      get_rel_name(relOid));
    let aclresult = aclchk_seams::pg_class_aclcheck::call(
        priv_oid,
        miscinit::GetUserId(),
        ACL_SELECT,
    )?;
    if aclresult != ACLCHECK_OK {
        let objtype = objectaddress_seams::get_relkind_objtype::call(rel.rd_rel.relkind);
        let name = lsyscache_seams::get_rel_name::call(mcx, rel_oid)?.map(|s| s.as_str().to_string());
        aclchk_seams::aclcheck_error::call(aclresult, objtype, name)?;
    }

    // Check that the relation has storage.
    if !RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "relation \"{}\" does not have storage",
                rel_name(&rel)
            ))
            .errdetail(detail)
            .into_error());
    }

    // Check that the fork exists.
    //   if (!smgrexists(RelationGetSmgr(rel), forkNumber))
    let smgr_key = rel_smgr_key(&rel)?;
    if !smgr::smgrexists(smgr_key, fork_number)? {
        return Err(ereport(::types_error::ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "fork \"{fork_string}\" does not exist for this relation"
            ))
            .into_error());
    }

    // Validate block numbers, or handle nulls.
    //   nblocks = RelationGetNumberOfBlocksInFork(rel, forkNumber);
    let nblocks: BlockNumber =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(&rel, fork_number)?;

    let first_block: i64 = if arg_isnull(fcinfo, 3) {
        0
    } else {
        let fb = arg_int64(fcinfo, 3);
        if fb < 0 || fb >= nblocks as i64 {
            return Err(ereport(::types_error::ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "starting block number must be between 0 and {}",
                    nblocks as i64 - 1
                ))
                .into_error());
        }
        fb
    };
    let last_block: i64 = if arg_isnull(fcinfo, 4) {
        nblocks as i64 - 1
    } else {
        let lb = arg_int64(fcinfo, 4);
        if lb < 0 || lb >= nblocks as i64 {
            return Err(ereport(::types_error::ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "ending block number must be between 0 and {}",
                    nblocks as i64 - 1
                ))
                .into_error());
        }
        lb
    };

    // Now we're ready to do the real work.
    match ptype {
        PrewarmType::Prefetch => {
            // In prefetch mode, we just hint the OS to read the blocks, but we
            // don't know whether it really does it, and we don't wait for it to
            // finish. (USE_PREFETCH is defined on the platforms pgrust targets.)
            let bm = BufferManager::global()
                .expect("pg_prewarm: the buffer manager is not registered for this process");
            let mut block = first_block;
            while block <= last_block {
                check_for_interrupts()?;
                bm.PrefetchBuffer(&rel, fork_number, block as BlockNumber)?;
                blocks_done += 1;
                block += 1;
            }
        }
        PrewarmType::Read => {
            // In read mode, we actually read the blocks, but not into shared
            // buffers. This is more portable than prefetch mode and is synchronous.
            let mut blockbuffer = [0u8; BLCKSZ];
            let mut block = first_block;
            while block <= last_block {
                check_for_interrupts()?;
                smgr::smgrread(smgr_key, fork_number, block as BlockNumber, &mut blockbuffer)?;
                blocks_done += 1;
                block += 1;
            }
        }
        PrewarmType::Buffer => {
            // In buffer mode, we actually pull the data into shared_buffers.

            // Set up the private state for our streaming buffer read callback.
            //   p.current_blocknum = first_block;
            //   p.last_exclusive = last_block + 1;
            let p = Rc::new(RefCell::new(read_stream::BlockRangeReadStreamPrivate {
                current_blocknum: first_block as BlockNumber,
                last_exclusive: (last_block + 1) as BlockNumber,
            }));

            // It is safe to use batchmode as block_range_read_stream_cb takes no
            // locks.
            //   stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
            //       READ_STREAM_FULL | READ_STREAM_USE_BATCHING, NULL, rel,
            //       forkNumber, block_range_read_stream_cb, &p, 0);
            let mut stream = read_stream::read_stream_begin_relation(
                read_stream::READ_STREAM_MAINTENANCE
                    | read_stream::READ_STREAM_FULL
                    | read_stream::READ_STREAM_USE_BATCHING,
                None, // NULL BufferAccessStrategy
                &rel,
                fork_number,
                read_stream::block_range_read_stream_cb(p.clone()),
                0,
            )?;

            let mut block = first_block;
            while block <= last_block {
                check_for_interrupts()?;
                //   buf = read_stream_next_buffer(stream, NULL);
                //   ReleaseBuffer(buf);
                let (buf, _) = stream.read_stream_next_buffer()?;
                bufmgr_seams::release_buffer::call(buf);
                blocks_done += 1;
                block += 1;
            }
            //   Assert(read_stream_next_buffer(stream, NULL) == InvalidBuffer);
            let (last, _) = stream.read_stream_next_buffer()?;
            debug_assert_eq!(last, InvalidBuffer);
            //   read_stream_end(stream);
            read_stream::read_stream_end(stream)?;
        }
    }

    // Close relation, release locks.
    //   relation_close(rel, AccessShareLock);
    rel.close(AccessShareLock)?;

    //   if (privOid != relOid) UnlockRelationOid(privOid, AccessShareLock);
    if priv_oid != rel_oid {
        lmgr_seams::unlock_relation_oid::call(priv_oid, AccessShareLock)?;
    }

    // PG_RETURN_INT64(blocks_done).
    fcinfo.isnull = false;
    Ok(Datum::from_i64(blocks_done))
}

// ===========================================================================
// fmgr argument accessors (PG_GETARG_* / PG_ARGISNULL)
// ===========================================================================

/// `PG_ARGISNULL(i)`.
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|a| a.isnull).unwrap_or(true)
}

/// `PG_GETARG_OID(i)`.
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("pg_prewarm: missing oid arg")
        .value
        .as_oid()
}

/// `PG_GETARG_INT64(i)`.
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo
        .arg(i)
        .expect("pg_prewarm: missing int8 arg")
        .value
        .as_i64()
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(i))` — a `text` arg's `VARDATA_ANY` payload
/// decoded to a `String` (the C `char *`).
fn arg_text_cstring(fcinfo: &FunctionCallInfoBaseData, i: usize) -> String {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_prewarm: text arg missing from by-ref lane");
    String::from_utf8_lossy(varlena_payload(image)).into_owned()
}

/// `VARDATA_ANY(ptr)` for an inline (non-compressed, non-external) varlena image.
fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        // VARATT_IS_1B && !VARATT_IS_1B_E: short 1-byte header (skip 1 byte).
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        // 4-byte uncompressed header (skip VARHDRSZ).
        Some(_) if image.len() >= ::datum::varlena::VARHDRSZ => {
            &image[::datum::varlena::VARHDRSZ..]
        }
        _ => &[],
    }
}

/// `CHECK_FOR_INTERRUPTS()`.
fn check_for_interrupts() -> Result<(), PgError> {
    postgres_seams::check_for_interrupts::call()
}

// ===========================================================================
// autoprewarm sibling stubs (autoprewarm.c — unported bgworker/shmem machinery)
// ===========================================================================

/// `autoprewarm_start_worker()` (autoprewarm.c) — start the autoprewarm
/// background worker. Its body needs `apw_init_shmem` + the bgworker leader
/// machinery (`autoprewarm.c`), which is unported. The symbol is registered so
/// `CREATE FUNCTION` (which validates the C symbol unconditionally via
/// `fmgr_c_validator` → `load_external_function`) succeeds; a real call
/// mirror-pg-and-panics.
fn fc_autoprewarm_start_worker(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error(
        "pg_prewarm: autoprewarm_start_worker (contrib/pg_prewarm/autoprewarm.c) is unported \
         (autoprewarm bgworker/shmem machinery)",
    ));
}

/// `autoprewarm_dump_now()` (autoprewarm.c) — perform an immediate block dump.
/// Unported (same autoprewarm machinery); see [`fc_autoprewarm_start_worker`].
fn fc_autoprewarm_dump_now(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    raise(PgError::error(
        "pg_prewarm: autoprewarm_dump_now (contrib/pg_prewarm/autoprewarm.c) is unported \
         (autoprewarm bgworker/shmem machinery)",
    ));
}

// ===========================================================================
// Builtin-library registration
// ===========================================================================

/// Resolve a symbol of the `pg_prewarm` module to its ported `PGFunction` (the
/// `PG_FUNCTION_INFO_V1`-exposed `(user_fn, api_version=1)` pair). Returns `None`
/// for an unknown symbol, exactly as the OS loader would fail to find it in
/// `pg_prewarm.so`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "pg_prewarm" => Some(fc_pg_prewarm),
        "autoprewarm_start_worker" => Some(fc_autoprewarm_start_worker),
        "autoprewarm_dump_now" => Some(fc_autoprewarm_dump_now),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        // PG_FUNCTION_INFO_V1 declares api_version 1.
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `pg_prewarm` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    dfmgr_seams::register_builtin_library(
        dfmgr_seams::BuiltinLibraryEntry {
            name: LIBRARY,
            lookup,
            // pg_prewarm.c's PG_MODULE_MAGIC_EXT has no _PG_init.
            pg_init: None,
        },
    );
}
