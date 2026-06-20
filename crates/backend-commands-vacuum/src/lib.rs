//! Faithful port of `backend/commands/vacuum.c` — the VACUUM/ANALYZE command
//! driver and the shared vacuum support routines (PostgreSQL 18.3).
//!
//! Control flow of every vacuum.c function (branch order, constants, struct
//! writes, error text + SQLSTATE/elevel, freeze-cutoff arithmetic) is preserved
//! here. The genuine externals (table-AM, relcache/lock, catalog DML + seqscans,
//! syscache, ACL, GUC, snapshot, transaction-command machinery, SLRU/CLOG
//! truncation, shared-memory cost balance, the cost-state globals) cross the
//! per-owner seam crates and panic loudly until their owner lands.
//!
//! Repo-model adaptation (vs the central-seam src-idiomatic reference):
//! relations are `Oid` tokens read through field-read seams; option/cutoff
//! structs are the real `types_vacuum` values; parse nodes are the real
//! `types_nodes` enum. The src-idiomatic `vac_context`/`MemoryContextHandle`
//! cross-transaction-context machinery and its charged `PgVec` accounting are
//! DROPPED — the cross-transaction substrate is the runtime's concern here, so
//! `vacuum()`/`expand_vacuum_rel`/`get_all_vacuum_rels` build plain owned
//! `Vec`s (behaviour-equivalent). `Mcx<'mcx>` is threaded where new node values
//! must be arena-allocated (mirroring cluster.c).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::needless_bool)]
#![allow(clippy::needless_bool_assign)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};

use backend_utils_error::ereport;
use types_error::{
    ErrorLevel, ErrorLocation, PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_LOCK_NOT_AVAILABLE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_TABLE, ERROR, LOG, WARNING,
};

use types_core::primitive::{bits32, BlockNumber, InvalidOid, MultiXactId, Oid, TransactionId};
use types_core::xact::{FirstNormalTransactionId, InvalidTransactionId};

use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, NoLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use types_storage::storage::LW_EXCLUSIVE;
use types_tuple::access::{
    RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use types_tuple::heaptuple::ItemPointerData;

use types_cluster::{ClusterParams, ParseState, CLUOPT_VERBOSE};
use types_nodes::ddlnodes::{VacuumRelation, VacuumStmt};
use types_nodes::nodes::Node;
use types_nodes::rawnodes::RangeVar;

use types_vacuum::vacuum::VacOptValue::{
    VACOPTVALUE_AUTO, VACOPTVALUE_DISABLED, VACOPTVALUE_ENABLED, VACOPTVALUE_UNSPECIFIED,
};
use types_vacuum::vacuum::{
    VacOptValue, VacuumCutoffs, VacuumParams, VACOPT_ANALYZE, VACOPT_DISABLE_PAGE_SKIPPING,
    VACOPT_FREEZE, VACOPT_FULL, VACOPT_ONLY_DATABASE_STATS, VACOPT_PROCESS_MAIN,
    VACOPT_PROCESS_TOAST, VACOPT_SKIP_DATABASE_STATS, VACOPT_SKIP_LOCKED, VACOPT_VACUUM,
    VACOPT_VERBOSE,
};
use types_storage::buf::BufferAccessStrategy;
use types_vacuum::vacuumlazy::{TidStore, UpdateRelStatsArgs};
use types_vacuum::vacuumparallel::{IndexBulkDeleteResult, IndexVacuumInfo, VacDeadItemsInfo};

use backend_commands_define_seams::DefElemArg;

use backend_access_transam_xact_seams as xact;
use backend_access_common_tidstore_seams as tidstore_seams;
use backend_commands_analyze_seams as analyze;
use backend_commands_define_seams as define;
use backend_commands_vacuum_seams as rt;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use backend_parser_small1_seams as parse_node;

// L1 re-home owners
use backend_access_table_table_seams as table_seam;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_commands_async_seams as async_seam;
use backend_commands_cluster_seams as cluster_seam;
use backend_storage_lmgr_lmgr_seams as lmgr_seam;
use backend_storage_lmgr_lwlock_seams as lwlock_seam;
use backend_storage_ipc_procarray_seams as procarray_seam;
use backend_access_transam_varsup_seams as varsup_seam;
use backend_catalog_pg_inherits_seams as pg_inherits_seam;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_utils_misc_guc_seams as guc_seam;
use backend_utils_init_miscinit_seams as miscinit_seam;
use backend_utils_init_small_seams as init_small_seam;
// L4 export+rehome owners
use backend_access_transam_clog_seams as clog_seam;
use backend_access_transam_commit_ts_seams as commit_ts_seam;
use backend_access_transam_multixact_seams as multixact_seam;

mod catalog_scan;
mod seams_install;
pub use seams_install::init_seams;

// ---------------------------------------------------------------------------
// Constants verified against PostgreSQL 18.3 headers.
// ---------------------------------------------------------------------------

/// `MIN_BAS_VAC_RING_SIZE_KB` (commands/vacuum.h) — 128 kB.
const MIN_BAS_VAC_RING_SIZE_KB: i32 = 128;
/// `MAX_BAS_VAC_RING_SIZE_KB` (commands/vacuum.h) — 16 GB in kB.
const MAX_BAS_VAC_RING_SIZE_KB: i32 = 16 * 1024 * 1024;
/// `MAX_PARALLEL_WORKER_LIMIT` (postmaster/bgworker_internals.h).
const MAX_PARALLEL_WORKER_LIMIT: i32 = 1024;
/// `SECURITY_RESTRICTED_OPERATION` (miscadmin.h).
const SECURITY_RESTRICTED_OPERATION: i32 = 0x0002;
/// `RVR_SKIP_LOCKED` (catalog/namespace.h).
const RVR_SKIP_LOCKED: i32 = 0x02;
/// `FirstMultiXactId` (access/multixact.h:25) — `((MultiXactId) 1)`.
const FirstMultiXactId: MultiXactId = 1;
/// `PARALLEL_VACUUM_DELAY_REPORT_INTERVAL_NS` (vacuum.c) — `NS_PER_S`.
const PARALLEL_VACUUM_DELAY_REPORT_INTERVAL_NS: i64 = 1_000_000_000;

/// `StdRdOptIndexCleanup` discriminants (access/reloptions.h).
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO: u8 = 0;
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON: u8 = 1;
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF: u8 = 2;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/vacuum.c", 0, funcname)
}

/// `OidIsValid(oid)` — `(oid) != InvalidOid`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// Faithful equivalent of the C `castNode()` "can't happen" invariant: the
/// parse tree always holds the expected node type at these positions, so an
/// unexpected tag mirrors the `elog(ERROR, ...)` it would raise in C.
fn elog_node_type_error<T>(funcname: &'static str, expected: &str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg(format!("unexpected node type, expected {}", expected))
        .finish(here(funcname))
        .map(|()| unreachable!("ereport(ERROR) does not return"))
}

/* =========================================================================
 * Pure transaction-id / multixact comparison macros (access/transam.h,
 * multixact.h).  These are C macros / trivial inline functions, inlined here.
 * ========================================================================= */

/// `TransactionIdIsValid(xid)` -- `(xid) != InvalidTransactionId`.
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` -- `(xid) >= FirstNormalTransactionId`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes(id1, id2)` — modular "id1 < id2".
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

/// `TransactionIdPrecedesOrEquals(id1, id2)` — modular "id1 <= id2".
#[inline]
fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }
    (id1.wrapping_sub(id2) as i32) <= 0
}

/// `MultiXactIdIsValid(multi)` -- `(multi) != InvalidMultiXactId`.
#[inline]
fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidTransactionId
}

/// `MultiXactIdPrecedes(multi1, multi2)` — modular "multi1 < multi2".
#[inline]
fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

/// `MultiXactIdPrecedesOrEquals(multi1, multi2)` — modular "multi1 <= multi2".
#[inline]
fn MultiXactIdPrecedesOrEquals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    (multi1.wrapping_sub(multi2) as i32) <= 0
}

/// `Min(a, b)` for ints.
#[inline]
fn min_i32(a: i32, b: i32) -> i32 {
    if a < b {
        a
    } else {
        b
    }
}

/// `Max(a, b)` for ints.
#[inline]
fn max_i32(a: i32, b: i32) -> i32 {
    if a > b {
        a
    } else {
        b
    }
}

/// `pg_strcasecmp(a, b)` — ASCII case-insensitive comparison; callers only use
/// the `== 0` result.
fn pg_strcasecmp(a: &str, b: &str) -> i32 {
    let mut ai = a.bytes();
    let mut bi = b.bytes();
    loop {
        match (ai.next(), bi.next()) {
            (None, None) => return 0,
            (None, Some(_)) => return -1,
            (Some(_), None) => return 1,
            (Some(x), Some(y)) => {
                let lx = x.to_ascii_lowercase();
                let ly = y.to_ascii_lowercase();
                if lx != ly {
                    return (lx as i32) - (ly as i32);
                }
            }
        }
    }
}

/// Project a `DefElem`'s value node into the `DefElemArg` the define.c value
/// accessors switch on (mirrors `nodeTag(def->arg)`); `None` for `arg == NULL`.
fn defel_arg(opt: &types_nodes::ddlnodes::DefElem<'_>) -> Option<DefElemArg> {
    use types_nodes::nodes::ntag;
    let node = opt.arg.as_deref()?;
    Some(match node.node_tag() {
        ntag::T_Integer => {
            let i = node.expect_integer();
            DefElemArg::Integer(i.ival as i64)
        }
        ntag::T_Float => {
            let f = node.expect_float();
            DefElemArg::Float(f.fval.as_str().to_string())
        }
        ntag::T_Boolean => {
            let b = node.expect_boolean();
            DefElemArg::Boolean(b.boolval)
        }
        ntag::T_String => {
            let s = node.expect_string();
            DefElemArg::String(s.sval.as_str().to_string())
        }
        _ => DefElemArg::AStar,
    })
}

/// `def->defname` of a parsed `DefElem`.
fn def_name(opt: &types_nodes::ddlnodes::DefElem<'_>) -> String {
    opt.defname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default()
}

/// `defGetBoolean(def)`.
fn defGetBoolean(opt: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<bool> {
    define::def_get_boolean::call(def_name(opt), defel_arg(opt))
}

/// `defGetString(def)`.
fn defGetString(opt: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<String> {
    rt::def_get_string_text::call(def_name(opt), defel_arg(opt))
}

/// `defGetInt32(def)`.
fn defGetInt32(opt: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<i32> {
    rt::def_get_int32::call(def_name(opt), defel_arg(opt))
}

/// `relation->relname` of a `RangeVar` (the bare relation name).
fn range_var_relname(relation: &RangeVar<'_>) -> String {
    relation
        .relname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default()
}

/// Faithful equivalent of reading `VacuumRelation->relation` (a `RangeVar *`):
/// the field is either NULL or always a `RangeVar` node; clone out the inner
/// `RangeVar` into `mcx`, or `None` for a NULL relation.
fn vacrel_range_var<'mcx>(
    funcname: &'static str,
    relation: Option<&Node<'_>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<RangeVar<'mcx>>> {
    match relation {
        None => Ok(None),
        Some(n) if n.is_rangevar() => {
            let rv = n.expect_rangevar();
            Ok(Some(rv.clone_in(mcx)?))
        }
        Some(_) => elog_node_type_error(funcname, "RangeVar"),
    }
}

/// `makeVacuumRelation(relation, oid, va_cols)` (nodes/makefuncs.c) — pure node
/// construction.  `relation` and `va_cols` are arena-allocated through `mcx`.
fn make_vacuum_relation<'mcx>(
    relation: Option<&RangeVar<'mcx>>,
    oid: Oid,
    va_cols: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    mcx: Mcx<'mcx>,
) -> PgResult<VacuumRelation<'mcx>> {
    // C builds this into vac_context via palloc; a plain arena alloc is
    // behavior-equivalent here.
    let rel_node = match relation {
        Some(rv) => Some(mcx::alloc_in(mcx, Node::mk_range_var(mcx, rv.clone_in(mcx)?)?)?),
        None => None,
    };
    let mut cols: PgVec<types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
    for c in va_cols.iter() {
        cols.push(mcx::alloc_in(mcx, c.clone_in(mcx)?)?);
    }
    Ok(VacuumRelation {
        relation: rel_node,
        oid,
        va_cols: cols,
    })
}

/* =========================================================================
 * check_vacuum_buffer_usage_limit  (vacuum.c:138-153)
 * ========================================================================= */

/// `check_vacuum_buffer_usage_limit(newval, extra, source)` — GUC check
/// function ensuring the value is within the allowable range. Returns
/// `(true, None)` if valid, `(false, Some(detail))` (the `GUC_check_errdetail`
/// text) otherwise.
pub fn check_vacuum_buffer_usage_limit(newval: i32) -> (bool, Option<String>) {
    /* Value upper and lower hard limits are inclusive */
    if newval == 0 || (newval >= MIN_BAS_VAC_RING_SIZE_KB && newval <= MAX_BAS_VAC_RING_SIZE_KB) {
        return (true, None);
    }

    /* Value does not fall within any allowable range */
    let detail = format!(
        "\"{}\" must be 0 or between {} kB and {} kB.",
        "vacuum_buffer_usage_limit", MIN_BAS_VAC_RING_SIZE_KB, MAX_BAS_VAC_RING_SIZE_KB
    );

    (false, Some(detail))
}

/* =========================================================================
 * ExecVacuum  (vacuum.c:161-475)
 * ========================================================================= */

/// `ExecVacuum(pstate, vacstmt, isTopLevel)` (vacuum.c:162) — primary entry
/// point for manual VACUUM and ANALYZE commands.
pub fn ExecVacuum<'mcx>(
    pstate: &ParseState<'mcx>,
    vacstmt: &VacuumStmt<'mcx>,
    isTopLevel: bool,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let stmt = vacstmt;

    let mut params = VacuumParams {
        options: 0,
        freeze_min_age: 0,
        freeze_table_age: 0,
        multixact_freeze_min_age: 0,
        multixact_freeze_table_age: 0,
        is_wraparound: false,
        log_min_duration: 0,
        index_cleanup: VACOPTVALUE_UNSPECIFIED,
        truncate: VACOPTVALUE_UNSPECIFIED,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: 0.0,
        nworkers: 0,
    };
    let mut bstrategy: BufferAccessStrategy = None;
    let mut verbose = false;
    let mut skip_locked = false;
    let mut analyze_opt = false;
    let mut freeze = false;
    let mut full = false;
    let mut disable_page_skipping = false;
    let mut process_main = true;
    let mut process_toast = true;
    let mut ring_size: i32;
    let mut skip_database_stats = false;
    let mut only_database_stats = false;

    /* index_cleanup and truncate values unspecified for now */
    params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
    params.truncate = VACOPTVALUE_UNSPECIFIED;

    /* By default parallel vacuum is enabled */
    params.nworkers = 0;

    /* Will be set later if we recurse to a TOAST table. */
    params.toast_parent = InvalidOid;

    /*
     * Set this to an invalid value so it is clear whether or not a
     * BUFFER_USAGE_LIMIT was specified when making the access strategy.
     */
    ring_size = -1;

    /* Parse options list */
    for opt_node in stmt.options.iter() {
        /* DefElem *opt = (DefElem *) lfirst(lc); */
        let Some(opt) = opt_node.as_defelem() else {
            return elog_node_type_error("ExecVacuum", "DefElem");
        };
        let defname = def_name(opt);

        /* Parse common options for VACUUM and ANALYZE */
        if defname == "verbose" {
            verbose = defGetBoolean(opt)?;
        } else if defname == "skip_locked" {
            skip_locked = defGetBoolean(opt)?;
        } else if defname == "buffer_usage_limit" {
            let vac_buffer_size = defGetString(opt)?;

            /*
             * Check that the specified value is valid and the size falls
             * within the hard upper and lower limits if it is not 0.
             */
            let (ok, result, hintmsg) = rt::parse_int_kb::call(vac_buffer_size)?;
            if !ok
                || (result != 0
                    && (result < MIN_BAS_VAC_RING_SIZE_KB || result > MAX_BAS_VAC_RING_SIZE_KB))
            {
                let mut err = ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "BUFFER_USAGE_LIMIT option must be 0 or between {} kB and {} kB",
                        MIN_BAS_VAC_RING_SIZE_KB, MAX_BAS_VAC_RING_SIZE_KB
                    ));
                if let Some(hint) = hintmsg {
                    err = err.errhint(hint);
                }
                return err.finish(here("ExecVacuum"));
            }

            ring_size = result;
        } else if !stmt.is_vacuumcmd {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized {} option \"{}\"", "ANALYZE", defname))
                .errposition(parse_node::parser_errposition::call(pstate, opt.location)?)
                .finish(here("ExecVacuum"));
        }
        /* Parse options available on VACUUM */
        else if defname == "analyze" {
            analyze_opt = defGetBoolean(opt)?;
        } else if defname == "freeze" {
            freeze = defGetBoolean(opt)?;
        } else if defname == "full" {
            full = defGetBoolean(opt)?;
        } else if defname == "disable_page_skipping" {
            disable_page_skipping = defGetBoolean(opt)?;
        } else if defname == "index_cleanup" {
            /* Interpret no string as the default, which is 'auto' */
            if opt.arg.is_none() {
                params.index_cleanup = VACOPTVALUE_AUTO;
            } else {
                let sval = defGetString(opt)?;

                /* Try matching on 'auto' string, or fall back on boolean */
                if pg_strcasecmp(&sval, "auto") == 0 {
                    params.index_cleanup = VACOPTVALUE_AUTO;
                } else {
                    params.index_cleanup = get_vacoptval_from_boolean(opt)?;
                }
            }
        } else if defname == "process_main" {
            process_main = defGetBoolean(opt)?;
        } else if defname == "process_toast" {
            process_toast = defGetBoolean(opt)?;
        } else if defname == "truncate" {
            params.truncate = get_vacoptval_from_boolean(opt)?;
        } else if defname == "parallel" {
            if opt.arg.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "parallel option requires a value between 0 and {}",
                        MAX_PARALLEL_WORKER_LIMIT
                    ))
                    .errposition(parse_node::parser_errposition::call(pstate, opt.location)?)
                    .finish(here("ExecVacuum"));
            } else {
                let nworkers = defGetInt32(opt)?;
                if nworkers < 0 || nworkers > MAX_PARALLEL_WORKER_LIMIT {
                    return ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "parallel workers for vacuum must be between 0 and {}",
                            MAX_PARALLEL_WORKER_LIMIT
                        ))
                        .errposition(parse_node::parser_errposition::call(pstate, opt.location)?)
                        .finish(here("ExecVacuum"));
                }

                /*
                 * Disable parallel vacuum, if user has specified parallel
                 * degree as zero.
                 */
                if nworkers == 0 {
                    params.nworkers = -1;
                } else {
                    params.nworkers = nworkers;
                }
            }
        } else if defname == "skip_database_stats" {
            skip_database_stats = defGetBoolean(opt)?;
        } else if defname == "only_database_stats" {
            only_database_stats = defGetBoolean(opt)?;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized {} option \"{}\"", "VACUUM", defname))
                .errposition(parse_node::parser_errposition::call(pstate, opt.location)?)
                .finish(here("ExecVacuum"));
        }
    }

    /* Set vacuum options */
    params.options = (if stmt.is_vacuumcmd {
        VACOPT_VACUUM
    } else {
        VACOPT_ANALYZE
    }) | (if verbose { VACOPT_VERBOSE } else { 0 })
        | (if skip_locked { VACOPT_SKIP_LOCKED } else { 0 })
        | (if analyze_opt { VACOPT_ANALYZE } else { 0 })
        | (if freeze { VACOPT_FREEZE } else { 0 })
        | (if full { VACOPT_FULL } else { 0 })
        | (if disable_page_skipping {
            VACOPT_DISABLE_PAGE_SKIPPING
        } else {
            0
        })
        | (if process_main { VACOPT_PROCESS_MAIN } else { 0 })
        | (if process_toast {
            VACOPT_PROCESS_TOAST
        } else {
            0
        })
        | (if skip_database_stats {
            VACOPT_SKIP_DATABASE_STATS
        } else {
            0
        })
        | (if only_database_stats {
            VACOPT_ONLY_DATABASE_STATS
        } else {
            0
        });

    /* sanity checks on options */
    debug_assert!(params.options & (VACOPT_VACUUM | VACOPT_ANALYZE) != 0);
    debug_assert!(
        (params.options & VACOPT_VACUUM != 0)
            || (params.options & (VACOPT_FULL | VACOPT_FREEZE)) == 0
    );

    if (params.options & VACOPT_FULL != 0) && params.nworkers > 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM FULL cannot be performed in parallel")
            .finish(here("ExecVacuum"));
    }

    /*
     * BUFFER_USAGE_LIMIT does nothing for VACUUM (FULL) so just raise an
     * ERROR for that case.  VACUUM (FULL, ANALYZE) does make use of it, so
     * we'll permit that.
     */
    if ring_size != -1
        && (params.options & VACOPT_FULL != 0)
        && (params.options & VACOPT_ANALYZE == 0)
    {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("BUFFER_USAGE_LIMIT cannot be specified for VACUUM FULL")
            .finish(here("ExecVacuum"));
    }

    /*
     * Make sure VACOPT_ANALYZE is specified if any column lists are present.
     */
    if params.options & VACOPT_ANALYZE == 0 {
        for vrel_node in stmt.rels.iter() {
            /* VacuumRelation *vrel = lfirst_node(VacuumRelation, lc); */
            let Some(vrel) = vrel_node.as_vacuumrelation() else {
                return elog_node_type_error("ExecVacuum", "VacuumRelation");
            };
            if !vrel.va_cols.is_empty() {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("ANALYZE option must be specified when a column list is provided")
                    .finish(here("ExecVacuum"));
            }
        }
    }

    /*
     * Sanity check DISABLE_PAGE_SKIPPING option.
     */
    if (params.options & VACOPT_FULL) != 0 && (params.options & VACOPT_DISABLE_PAGE_SKIPPING) != 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")
            .finish(here("ExecVacuum"));
    }

    /* sanity check for PROCESS_TOAST */
    if (params.options & VACOPT_FULL) != 0 && (params.options & VACOPT_PROCESS_TOAST) == 0 {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("PROCESS_TOAST required with VACUUM FULL")
            .finish(here("ExecVacuum"));
    }

    /* sanity check for ONLY_DATABASE_STATS */
    if params.options & VACOPT_ONLY_DATABASE_STATS != 0 {
        debug_assert!(params.options & VACOPT_VACUUM != 0);
        if !stmt.rels.is_empty() {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ONLY_DATABASE_STATS cannot be specified with a list of tables")
                .finish(here("ExecVacuum"));
        }
        /* don't require people to turn off PROCESS_TOAST/MAIN explicitly */
        if params.options
            & !(VACOPT_VACUUM
                | VACOPT_VERBOSE
                | VACOPT_PROCESS_MAIN
                | VACOPT_PROCESS_TOAST
                | VACOPT_ONLY_DATABASE_STATS)
            != 0
        {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ONLY_DATABASE_STATS cannot be specified with other VACUUM options")
                .finish(here("ExecVacuum"));
        }
    }

    /*
     * All freeze ages are zero if the FREEZE option is given; otherwise pass
     * them as -1 which means to use the default values.
     */
    if params.options & VACOPT_FREEZE != 0 {
        params.freeze_min_age = 0;
        params.freeze_table_age = 0;
        params.multixact_freeze_min_age = 0;
        params.multixact_freeze_table_age = 0;
    } else {
        params.freeze_min_age = -1;
        params.freeze_table_age = -1;
        params.multixact_freeze_min_age = -1;
        params.multixact_freeze_table_age = -1;
    }

    /* user-invoked vacuum is never "for wraparound" */
    params.is_wraparound = false;

    /* user-invoked vacuum uses VACOPT_VERBOSE instead of log_min_duration */
    params.log_min_duration = -1;

    /*
     * Later, in vacuum_rel(), we check if a reloption override was specified.
     */
    params.max_eager_freeze_failure_rate = rt::vacuum_max_eager_freeze_failure_rate::call()?;

    /*
     * Create special memory context for cross-transaction storage.
     *
     * The cross-transaction context is the substrate's concern in this model;
     * we build plain owned Vecs and arena-allocate through `mcx`.
     *
     * Make a buffer strategy object.  We needn't bother making this for VACUUM
     * (FULL) or VACUUM (ONLY_DATABASE_STATS) as they'll not make use of it.
     * VACUUM (FULL, ANALYZE) is possible, so we'd better ensure that we make a
     * strategy when we see ANALYZE.
     */
    if (params.options & (VACOPT_ONLY_DATABASE_STATS | VACOPT_FULL)) == 0
        || (params.options & VACOPT_ANALYZE) != 0
    {
        debug_assert!(ring_size >= -1);

        /*
         * If BUFFER_USAGE_LIMIT was specified by the VACUUM or ANALYZE
         * command, it overrides the value of VacuumBufferUsageLimit.  Either
         * value may be 0, in which case GetAccessStrategyWithSize() will
         * return NULL, effectively allowing full use of shared buffers.
         */
        if ring_size == -1 {
            ring_size = rt::vacuum_buffer_usage_limit::call()?;
        }

        bstrategy = rt::get_access_strategy_with_size::call(ring_size)?;
    }

    /* Now go through the common routine */
    vacuum(&stmt.rels, &mut params, bstrategy, isTopLevel, mcx)
}

/* =========================================================================
 * vacuum  (vacuum.c:499-726)
 * ========================================================================= */

/* The `static bool in_vacuum` recursion guard.  Process-wide, backend-local. */
use core::cell::Cell;
thread_local! {
    static IN_VACUUM: Cell<bool> = const { Cell::new(false) };
}
fn in_vacuum_get() -> bool {
    IN_VACUUM.with(|v| v.get())
}
fn in_vacuum_set(v: bool) {
    IN_VACUUM.with(|c| c.set(v));
}

/// RAII guard modeling vacuum.c's `PG_FINALLY()` cleanup for the
/// `in_vacuum`/cost-accounting block (vacuum.c:601-686).
///
/// C clears `in_vacuum`, `VacuumCostActive`, `VacuumFailsafeActive`, and
/// `VacuumCostBalance` inside `PG_FINALLY()`, which runs whether the protected
/// section returns normally *or* `longjmp`s out via `ereport(ERROR)`.  In this
/// port an `ereport(ERROR)` is a returned `PgResult::Err`, but other failures
/// inside the protected section — a seam-miss `panic!`, a `todo!`, a slice
/// index, a failed downcast — unwind as a Rust panic.  A plain
/// closure-then-cleanup model lets such an unwind blow *past* the cleanup,
/// leaving `in_vacuum` stuck `true`; every subsequent VACUUM/ANALYZE in that
/// backend then wrongly trips the "cannot be executed from VACUUM or ANALYZE"
/// recursion guard once `catch_unwind` recovers the transaction.
///
/// Running the cleanup in `Drop` restores C's `PG_FINALLY` contract: it fires
/// on normal return, on `?`-early-return, and on unwind alike.
struct InVacuumGuard;

impl Drop for InVacuumGuard {
    fn drop(&mut self) {
        /* PG_FINALLY: matches vacuum.c:677-685. */
        in_vacuum_set(false);
        let _ = rt::set_vacuum_cost_active::call(false);
        let _ = rt::set_vacuum_failsafe_active::call(false);
        let _ = rt::set_vacuum_cost_balance::call(0);
    }
}

/// `vacuum(relations, params, bstrategy, vac_context, isTopLevel)`
/// (vacuum.c:500) — internal entry point for autovacuum and VACUUM/ANALYZE.
///
/// `relations` is the (possibly empty) list of parse-tree target nodes. An
/// empty list means "all vacuumable rels in the database" (unless
/// ONLY_DATABASE_STATS), matching the C NIL convention.
pub fn vacuum<'mcx>(
    relations: &PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    params: &mut VacuumParams,
    bstrategy: BufferAccessStrategy,
    isTopLevel: bool,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let p = params;

    let stmttype = if p.options & VACOPT_VACUUM != 0 {
        "VACUUM"
    } else {
        "ANALYZE"
    };

    /*
     * We cannot run VACUUM inside a user transaction block; ...
     * ANALYZE (without VACUUM) can run either way.
     */
    let in_outer_xact: bool;
    if p.options & VACOPT_VACUUM != 0 {
        xact::prevent_in_transaction_block::call(isTopLevel, stmttype)?;
        in_outer_xact = false;
    } else {
        in_outer_xact = xact::is_in_transaction_block::call(isTopLevel)?;
    }

    /*
     * Check for and disallow recursive calls.
     */
    if in_vacuum_get() {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "{} cannot be executed from VACUUM or ANALYZE",
                stmttype
            ))
            .finish(here("vacuum"));
    }

    /*
     * Build list of relation(s) to process.  (C builds these into vac_context
     * via palloc; a plain Vec is behavior-equivalent here.)
     */
    let relations: Vec<VacuumRelation<'mcx>> = if p.options & VACOPT_ONLY_DATABASE_STATS != 0 {
        /* We don't process any tables in this case */
        debug_assert!(relations.is_empty());
        Vec::new()
    } else if !relations.is_empty() {
        let mut newrels: Vec<VacuumRelation<'mcx>> = Vec::new();
        for vrel_node in relations.iter() {
            /* VacuumRelation *vrel = lfirst_node(VacuumRelation, cur); */
            let Some(vrel) = vrel_node.as_vacuumrelation() else {
                return elog_node_type_error("vacuum", "VacuumRelation");
            };
            let sublist = expand_vacuum_rel(vrel, p.options, mcx)?;
            /* newrels = list_concat(newrels, sublist); */
            newrels.extend(sublist);
        }
        newrels
    } else {
        get_all_vacuum_rels(p.options, mcx)?
    };

    /*
     * Decide whether we need to start/commit our own transactions.
     */
    let use_own_xacts: bool;
    if p.options & VACOPT_VACUUM != 0 {
        use_own_xacts = true;
    } else {
        debug_assert!(p.options & VACOPT_ANALYZE != 0);
        if rt::am_autovacuum_worker_process::call()? {
            use_own_xacts = true;
        } else if in_outer_xact {
            use_own_xacts = false;
        } else if relations.len() > 1 {
            use_own_xacts = true;
        } else {
            use_own_xacts = false;
        }
    }

    /*
     * vacuum_rel expects to be entered with no transaction active; ...
     */
    if use_own_xacts {
        debug_assert!(!in_outer_xact);

        /* ActiveSnapshot is not set by autovacuum */
        if snapmgr_seam::active_snapshot_set::call() {
            snapmgr_seam::pop_active_snapshot::call()?;
        }

        /* matches the StartTransaction in PostgresMain() */
        xact::commit_transaction_command::call()?;
    }

    /* Turn vacuum cost accounting on or off, and set/clear in_vacuum.  The
     * PG_TRY()/PG_FINALLY() block (vacuum.c:601-686) is modeled with an RAII
     * `InVacuumGuard`: its `Drop` runs the FINALLY cleanup on *every* exit path
     * — normal return, `?`-early-return, and Rust unwind (panic) — exactly like
     * C's `PG_FINALLY` running on both fall-through and `longjmp`.  Modeling it
     * as a closure-then-cleanup would let a panic inside the protected section
     * skip the cleanup and leave `in_vacuum` stuck `true`. */
    in_vacuum_set(true);
    let _in_vacuum_guard = InVacuumGuard;
    let try_result: PgResult<()> = (|| {
        rt::set_vacuum_failsafe_active::call(false)?;
        rt::vacuum_update_costs::call()?;
        rt::set_vacuum_cost_balance::call(0)?;
        rt::set_vacuum_cost_balance_local::call(0)?;
        rt::clear_parallel_cost_pointers::call()?;

        /*
         * Loop to process each selected relation.
         */
        for vrel in relations.iter() {
            if p.options & VACOPT_VACUUM != 0 {
                /*
                 * vacuum_rel() scribbles on the parameters, so give it a copy
                 * to avoid affecting other relations.
                 */
                let mut params_copy: VacuumParams = *p;
                let relation = vacrel_range_var("vacuum", vrel.relation.as_deref(), mcx)?;

                if !vacuum_rel(vrel.oid, relation, &mut params_copy, bstrategy.clone(), mcx)? {
                    continue;
                }
            }

            if p.options & VACOPT_ANALYZE != 0 {
                /*
                 * If using separate xacts, start one for analyze. Otherwise,
                 * we can use the outer transaction.
                 */
                if use_own_xacts {
                    xact::start_transaction_command::call()?;
                    /* functions in indexes may want a snapshot set */
                    snapmgr_seam::push_active_snapshot_transaction::call()?;
                }

                /* va_cols collected as a plain Vec<String> for the seam. */
                let mut va_cols: Vec<String> = Vec::new();
                for c in vrel.va_cols.iter() {
                    match c.as_string() {
                        Some(s) => va_cols.push(s.sval.as_str().to_string()),
                        None => return elog_node_type_error("vacuum", "String"),
                    }
                }

                analyze::analyze_rel::call(
                    mcx,
                    vrel.oid,
                    vacrel_range_var("vacuum", vrel.relation.as_deref(), mcx)?,
                    *p,
                    va_cols,
                    in_outer_xact,
                    bstrategy.clone(),
                )?;

                if use_own_xacts {
                    snapmgr_seam::pop_active_snapshot::call()?;
                    /* standard_ProcessUtility() does CCI if !use_own_xacts */
                    xact::command_counter_increment::call()?;
                    xact::commit_transaction_command::call()?;
                } else {
                    /*
                     * If we're not using separate xacts, better separate the
                     * ANALYZE actions with CCIs.  This avoids trouble if user
                     * says "ANALYZE t, t".
                     */
                    xact::command_counter_increment::call()?;
                }
            }

            /*
             * Ensure VacuumFailsafeActive has been reset before vacuuming the
             * next relation.
             */
            rt::set_vacuum_failsafe_active::call(false)?;
        }
        Ok(())
    })();

    /* PG_FINALLY: run the cleanup here, at the same point C's PG_FINALLY runs
     * (immediately after the protected loop, before finish-up), by dropping the
     * guard.  On a panic the guard would instead drop while unwinding out of
     * `vacuum()`; either way the cleanup runs exactly once. */
    drop(_in_vacuum_guard);

    /* PG_END_TRY: re-raise any error from the protected section */
    try_result?;

    /*
     * Finish up processing.
     */
    if use_own_xacts {
        /* here, we are not in a transaction */

        /*
         * This matches the CommitTransaction waiting for us in
         * PostgresMain().
         */
        xact::start_transaction_command::call()?;
    }

    if (p.options & VACOPT_VACUUM != 0) && (p.options & VACOPT_SKIP_DATABASE_STATS == 0) {
        /*
         * Update pg_database.datfrozenxid, and truncate pg_xact if possible.
         */
        vac_update_datfrozenxid()?;
    }

    Ok(())
}

/* =========================================================================
 * vacuum_is_permitted_for_relation  (vacuum.c:734-776)
 * ========================================================================= */

/// `vacuum_is_permitted_for_relation(relid, reltuple, options)` (vacuum.c:735)
/// — check whether the current user has privileges to vacuum or analyze the
/// relation.  Issues a WARNING and returns false if not permitted.
pub fn vacuum_is_permitted_for_relation(
    relid: Oid,
    reltuple: &types_rel::FormData_pg_class<'_>,
    options: bits32,
) -> PgResult<bool> {
    vacuum_is_permitted_for_relation_scalar(
        relid,
        reltuple.relisshared,
        reltuple.relname.as_str().to_string(),
        options,
    )
}

/// Seam adapter: `vacuum_is_permitted_for_relation` reading only the two
/// Form_pg_class fields the body uses (`relisshared`, `relname`), passed by
/// value so analyze.c can share this gate without crossing a borrow over the
/// seam. The privilege logic is identical to the Form-taking entry above.
pub fn vacuum_is_permitted_for_relation_scalar(
    relid: Oid,
    relisshared: bool,
    relname: String,
    options: bits32,
) -> PgResult<bool> {
    debug_assert!((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

    let userid = miscinit_seam::get_user_id::call();
    if (aclchk_seam::object_ownercheck::call(
        types_core::catalog::DATABASE_RELATION_ID,
        init_small_seam::my_database_id::call(),
        userid,
    )? && !relisshared)
        || aclchk_seam::pg_class_aclcheck::call(relid, userid, types_acl::acl::ACL_MAINTAIN)?
            == types_acl::acl::AclResult::AclcheckOk
    {
        return Ok(true);
    }

    if (options & VACOPT_VACUUM) != 0 {
        ereport(WARNING)
            .errmsg(format!(
                "permission denied to vacuum \"{}\", skipping it",
                relname
            ))
            .finish(here("vacuum_is_permitted_for_relation"))?;
        return Ok(false);
    }

    if (options & VACOPT_ANALYZE) != 0 {
        ereport(WARNING)
            .errmsg(format!(
                "permission denied to analyze \"{}\", skipping it",
                relname
            ))
            .finish(here("vacuum_is_permitted_for_relation"))?;
    }

    Ok(false)
}

/* =========================================================================
 * vacuum_open_relation  (vacuum.c:786-881)
 * ========================================================================= */

/// `vacuum_open_relation(relid, relation, options, verbose, lmode)`
/// (vacuum.c:787) — open and lock a relation to be vacuumed/analyzed,
/// emitting an appropriate log on failure.  Returns `None` if the relation
/// could not be opened/locked.
pub fn vacuum_open_relation(
    relid: Oid,
    relation: Option<&RangeVar<'_>>,
    options: bits32,
    verbose: bool,
    lmode: LOCKMODE,
) -> PgResult<Option<Oid>> {
    let mut rel_lock = true;

    debug_assert!((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

    /*
     * Open the relation and get the appropriate lock on it.
     */
    let rel: Option<Oid> = if options & VACOPT_SKIP_LOCKED == 0 {
        rt::try_relation_open::call(relid, lmode)?
    } else if rt::conditional_lock_relation_oid::call(relid, lmode)? {
        rt::try_relation_open::call(relid, NoLock)?
    } else {
        rel_lock = false;
        None
    };

    /* if relation is opened, leave */
    if rel.is_some() {
        return Ok(rel);
    }

    /*
     * Relation could not be opened, hence generate if possible a log
     * informing on the situation.
     */
    let Some(relation) = relation else {
        return Ok(None);
    };

    /*
     * Determine the log level.
     */
    let elevel;
    if !rt::am_autovacuum_worker_process::call()? {
        elevel = WARNING;
    } else if verbose {
        elevel = LOG;
    } else {
        return Ok(None);
    }

    let relname = range_var_relname(relation);

    if (options & VACOPT_VACUUM) != 0 {
        if !rel_lock {
            ereport(elevel)
                .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                .errmsg(format!(
                    "skipping vacuum of \"{}\" --- lock not available",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        } else {
            ereport(elevel)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "skipping vacuum of \"{}\" --- relation no longer exists",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        }

        /*
         * For VACUUM ANALYZE, both logs could show up, but just generate
         * information for VACUUM as that would be the first one to be
         * processed.
         */
        return Ok(None);
    }

    if (options & VACOPT_ANALYZE) != 0 {
        if !rel_lock {
            ereport(elevel)
                .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                .errmsg(format!(
                    "skipping analyze of \"{}\" --- lock not available",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        } else {
            ereport(elevel)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "skipping analyze of \"{}\" --- relation no longer exists",
                    relname
                ))
                .finish(here("vacuum_open_relation"))?;
        }
    }

    Ok(None)
}

/* =========================================================================
 * expand_vacuum_rel  (vacuum.c:898-1047)
 * ========================================================================= */

/// `expand_vacuum_rel(vrel, vac_context, options)` (vacuum.c:899) — fill in the
/// table OID if unspecified and add entries for partitions/children.  Returns
/// the list of resolved `VacuumRelation`s.  (C builds the list into vac_context
/// via palloc; a plain Vec is behavior-equivalent here.)
fn expand_vacuum_rel<'mcx>(
    vrel: &VacuumRelation<'mcx>,
    options: bits32,
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<VacuumRelation<'mcx>>> {
    let mut vacrels: Vec<VacuumRelation<'mcx>> = Vec::new();

    /* If caller supplied OID, there's nothing we need do here. */
    if OidIsValid(vrel.oid) {
        vacrels.push(make_vacuum_relation(
            vacrel_range_var("expand_vacuum_rel", vrel.relation.as_deref(), mcx)?.as_ref(),
            vrel.oid,
            &vrel.va_cols,
            mcx,
        )?);
    } else {
        /*
         * Process a specific relation, and possibly partitions or child
         * tables thereof.
         *
         * Since autovacuum workers supply OIDs when calling vacuum(), no
         * autovacuum worker should reach this code.
         */
        debug_assert!(!rt::am_autovacuum_worker_process::call()?);

        let Some(relation_node) = vrel.relation.as_deref() else {
            return Err(PgError::error(
                "expand_vacuum_rel: VacuumRelation with InvalidOid must carry a RangeVar",
            ));
        };
        let Some(relation) = relation_node.as_rangevar() else {
            return elog_node_type_error("expand_vacuum_rel", "RangeVar");
        };

        /*
         * We transiently take AccessShareLock to protect the syscache lookup
         * below, as well as find_all_inheritors's expectation that the caller
         * holds some lock on the starting relation.
         */
        let rvr_opts = if options & VACOPT_SKIP_LOCKED != 0 {
            RVR_SKIP_LOCKED
        } else {
            0
        };
        let relid = rt::range_var_get_relid_extended::call(
            relation.clone_in(mcx)?,
            AccessShareLock,
            rvr_opts,
        )?;

        /*
         * If the lock is unavailable, emit the same log statement that
         * vacuum_rel() and analyze_rel() would.
         */
        if !OidIsValid(relid) {
            let relname = range_var_relname(relation);
            if options & VACOPT_VACUUM != 0 {
                ereport(WARNING)
                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                    .errmsg(format!(
                        "skipping vacuum of \"{}\" --- lock not available",
                        relname
                    ))
                    .finish(here("expand_vacuum_rel"))?;
            } else {
                ereport(WARNING)
                    .errcode(ERRCODE_LOCK_NOT_AVAILABLE)
                    .errmsg(format!(
                        "skipping analyze of \"{}\" --- lock not available",
                        relname
                    ))
                    .finish(here("expand_vacuum_rel"))?;
            }
            return Ok(vacrels);
        }

        /*
         * To check whether the relation is a partitioned table and its
         * ownership, fetch its syscache entry.
         */
        let class_form = match rt::search_syscache_class::call(mcx, relid)? {
            Some(cf) => cf,
            None => {
                return ereport(ERROR)
                    .errmsg(format!("cache lookup failed for relation {}", relid))
                    .finish(here("expand_vacuum_rel"))
                    .map(|()| unreachable!("ereport(ERROR) does not return"));
            }
        };

        /*
         * Make a returnable VacuumRelation for this rel if the user has the
         * required privileges.
         */
        if vacuum_is_permitted_for_relation(relid, &class_form, options)? {
            vacrels.push(make_vacuum_relation(
                Some(relation),
                relid,
                &vrel.va_cols,
                mcx,
            )?);
        }

        /*
         * Vacuuming a partitioned table with ONLY will not do anything since
         * the partitioned table itself is empty.  Issue a warning if the user
         * requests this.
         */
        let include_children = relation.inh;
        let is_partitioned_table = class_form.relkind == RELKIND_PARTITIONED_TABLE;
        if (options & VACOPT_VACUUM != 0) && is_partitioned_table && !include_children {
            let relname = range_var_relname(relation);
            ereport(WARNING)
                .errmsg(format!(
                    "VACUUM ONLY of partitioned table \"{}\" has no effect",
                    relname
                ))
                .finish(here("expand_vacuum_rel"))?;
        }

        /*
         * Unless the user has specified ONLY, make relation list entries for
         * its partitions or inheritance child tables.  Note that the list
         * returned by find_all_inheritors() includes the passed-in OID, so we
         * have to skip that.
         */
        if include_children {
            let part_oids = pg_inherits_seam::find_all_inheritors::call(mcx, relid, NoLock)?;

            for &part_oid in part_oids.iter() {
                if part_oid == relid {
                    continue; /* ignore original table */
                }

                /*
                 * We omit a RangeVar since it wouldn't be appropriate to
                 * complain about failure to open one of these relations
                 * later.
                 */
                vacrels.push(make_vacuum_relation(None, part_oid, &vrel.va_cols, mcx)?);
            }
        }

        /*
         * Release lock again.
         */
        rt::unlock_relation_oid::call(relid, AccessShareLock)?;
    }

    Ok(vacrels)
}

/* =========================================================================
 * get_all_vacuum_rels  (vacuum.c:1053-1101)
 * ========================================================================= */

/// `get_all_vacuum_rels(vac_context, options)` (vacuum.c:1054) — construct a
/// list of `VacuumRelation`s for all vacuumable rels in the current database.
/// (C builds the list into vac_context via palloc; a plain Vec is
/// behavior-equivalent here.)
fn get_all_vacuum_rels<'mcx>(
    options: bits32,
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<VacuumRelation<'mcx>>> {
    let mut vacrels: Vec<VacuumRelation<'mcx>> = Vec::new();
    let empty: PgVec<types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);

    let rows = rt::scan_all_pg_class::call(mcx)?;

    for row in rows.iter() {
        let relid = row.oid;

        /*
         * We include partitioned tables here; depending on which operation is
         * to be performed, caller will decide whether to process or ignore
         * them.
         */
        if row.relkind != RELKIND_RELATION
            && row.relkind != RELKIND_MATVIEW
            && row.relkind != RELKIND_PARTITIONED_TABLE
        {
            continue;
        }

        /* check permissions of relation */
        if !vacuum_is_permitted_for_relation(relid, &row.class_form, options)? {
            continue;
        }

        /*
         * Build VacuumRelation(s) specifying the table OIDs to be processed.
         * We omit a RangeVar since it wouldn't be appropriate to complain
         * about failure to open one of these relations later.
         */
        vacrels.push(make_vacuum_relation(None, relid, &empty, mcx)?);
    }

    Ok(vacrels)
}

/* =========================================================================
 * vacuum_get_cutoffs  (vacuum.c:1115-1274)
 * ========================================================================= */

/// `vacuum_get_cutoffs(rel, params, cutoffs)` (vacuum.c:1116) — compute
/// OldestXmin and the freeze cutoff points; returns true if the caller should
/// run an aggressive VACUUM.
pub fn vacuum_get_cutoffs(
    rel: Oid,
    params: VacuumParams,
    cutoffs: &mut VacuumCutoffs,
) -> PgResult<bool> {
    let p = &params;
    let c = cutoffs;

    let mut freeze_min_age: i32;
    let mut multixact_freeze_min_age: i32;
    let mut freeze_table_age: i32;
    let mut multixact_freeze_table_age: i32;
    let effective_multixact_freeze_max_age: i32;
    let nextXID: TransactionId;
    let mut safeOldestXmin: TransactionId;
    let mut aggressiveXIDCutoff: TransactionId;
    let nextMXID: MultiXactId;
    let mut safeOldestMxact: MultiXactId;
    let mut aggressiveMXIDCutoff: MultiXactId;

    /* Use mutable copies of freeze age parameters */
    freeze_min_age = p.freeze_min_age;
    multixact_freeze_min_age = p.multixact_freeze_min_age;
    freeze_table_age = p.freeze_table_age;
    multixact_freeze_table_age = p.multixact_freeze_table_age;

    /* Set pg_class fields in cutoffs */
    let (relfrozenxid, relminmxid) = relcache_seam::rel_frozenxid_minmxid::call(rel)?;
    c.relfrozenxid = relfrozenxid;
    c.relminmxid = relminmxid;

    /*
     * Acquire OldestXmin.
     */
    c.OldestXmin = procarray_seam::get_oldest_non_removable_transaction_id::call(rel)?;

    debug_assert!(TransactionIdIsNormal(c.OldestXmin));

    /* Acquire OldestMxact */
    c.OldestMxact = multixact_seam::get_oldest_multi_xact_id::call()?;
    debug_assert!(MultiXactIdIsValid(c.OldestMxact));

    /* Acquire next XID/next MXID values used to apply age-based settings */
    nextXID = varsup_seam::read_next_transaction_id::call();
    nextMXID = multixact_seam::read_next_multixact_id::call()?;

    /*
     * Also compute the multixact age for which freezing is urgent.
     */
    effective_multixact_freeze_max_age = multixact_seam::multixact_member_freeze_threshold::call()?;

    /*
     * Almost ready to set freeze output parameters; check if OldestXmin or
     * OldestMxact are held back to an unsafe degree before we start on that
     */
    let autovacuum_freeze_max_age = rt::autovacuum_freeze_max_age::call()?;
    safeOldestXmin = nextXID.wrapping_sub(autovacuum_freeze_max_age as u32);
    if !TransactionIdIsNormal(safeOldestXmin) {
        safeOldestXmin = FirstNormalTransactionId;
    }
    safeOldestMxact = nextMXID.wrapping_sub(effective_multixact_freeze_max_age as u32);
    if safeOldestMxact < FirstMultiXactId {
        safeOldestMxact = FirstMultiXactId;
    }
    if TransactionIdPrecedes(c.OldestXmin, safeOldestXmin) {
        ereport(WARNING)
            .errmsg("cutoff for removing and freezing tuples is far in the past")
            .errhint(
                "Close open transactions soon to avoid wraparound problems.\n\
                 You might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
            )
            .finish(here("vacuum_get_cutoffs"))?;
    }
    if MultiXactIdPrecedes(c.OldestMxact, safeOldestMxact) {
        ereport(WARNING)
            .errmsg("cutoff for freezing multixacts is far in the past")
            .errhint(
                "Close open transactions soon to avoid wraparound problems.\n\
                 You might also need to commit or roll back old prepared transactions, or drop stale replication slots.",
            )
            .finish(here("vacuum_get_cutoffs"))?;
    }

    /*
     * Determine the minimum freeze age to use: ... not more than half
     * autovacuum_freeze_max_age ...
     */
    if freeze_min_age < 0 {
        freeze_min_age = rt::vacuum_freeze_min_age::call()?;
    }
    freeze_min_age = min_i32(freeze_min_age, autovacuum_freeze_max_age / 2);
    debug_assert!(freeze_min_age >= 0);

    /* Compute FreezeLimit, being careful to generate a normal XID */
    c.FreezeLimit = nextXID.wrapping_sub(freeze_min_age as u32);
    if !TransactionIdIsNormal(c.FreezeLimit) {
        c.FreezeLimit = FirstNormalTransactionId;
    }
    /* FreezeLimit must always be <= OldestXmin */
    if TransactionIdPrecedes(c.OldestXmin, c.FreezeLimit) {
        c.FreezeLimit = c.OldestXmin;
    }

    /*
     * Determine the minimum multixact freeze age to use: ...
     */
    if multixact_freeze_min_age < 0 {
        multixact_freeze_min_age = rt::vacuum_multixact_freeze_min_age::call()?;
    }
    multixact_freeze_min_age = min_i32(
        multixact_freeze_min_age,
        effective_multixact_freeze_max_age / 2,
    );
    debug_assert!(multixact_freeze_min_age >= 0);

    /* Compute MultiXactCutoff, being careful to generate a valid value */
    c.MultiXactCutoff = nextMXID.wrapping_sub(multixact_freeze_min_age as u32);
    if c.MultiXactCutoff < FirstMultiXactId {
        c.MultiXactCutoff = FirstMultiXactId;
    }
    /* MultiXactCutoff must always be <= OldestMxact */
    if MultiXactIdPrecedes(c.OldestMxact, c.MultiXactCutoff) {
        c.MultiXactCutoff = c.OldestMxact;
    }

    /*
     * Finally, figure out if caller needs to do an aggressive VACUUM or not.
     */
    if freeze_table_age < 0 {
        freeze_table_age = rt::vacuum_freeze_table_age::call()?;
    }
    freeze_table_age = min_i32(
        freeze_table_age,
        (autovacuum_freeze_max_age as f64 * 0.95) as i32,
    );
    debug_assert!(freeze_table_age >= 0);
    aggressiveXIDCutoff = nextXID.wrapping_sub(freeze_table_age as u32);
    if !TransactionIdIsNormal(aggressiveXIDCutoff) {
        aggressiveXIDCutoff = FirstNormalTransactionId;
    }
    if TransactionIdPrecedesOrEquals(c.relfrozenxid, aggressiveXIDCutoff) {
        return Ok(true);
    }

    /*
     * Similar to the above, determine the table freeze age to use for
     * multixacts ...
     */
    if multixact_freeze_table_age < 0 {
        multixact_freeze_table_age = rt::vacuum_multixact_freeze_table_age::call()?;
    }
    multixact_freeze_table_age = min_i32(
        multixact_freeze_table_age,
        (effective_multixact_freeze_max_age as f64 * 0.95) as i32,
    );
    debug_assert!(multixact_freeze_table_age >= 0);
    aggressiveMXIDCutoff = nextMXID.wrapping_sub(multixact_freeze_table_age as u32);
    if aggressiveMXIDCutoff < FirstMultiXactId {
        aggressiveMXIDCutoff = FirstMultiXactId;
    }
    if MultiXactIdPrecedesOrEquals(c.relminmxid, aggressiveMXIDCutoff) {
        return Ok(true);
    }

    /* Non-aggressive VACUUM */
    Ok(false)
}

/* =========================================================================
 * vacuum_xid_failsafe_check  (vacuum.c:1283-1330)
 * ========================================================================= */

/// `vacuum_xid_failsafe_check(cutoffs)` (vacuum.c:1284) — determine if the
/// table's relfrozenxid/relminmxid are dangerously far in the past.
pub fn vacuum_xid_failsafe_check(cutoffs: VacuumCutoffs) -> PgResult<bool> {
    let c = &cutoffs;

    let relfrozenxid = c.relfrozenxid;
    let relminmxid = c.relminmxid;
    let xid_skip_limit: TransactionId;
    let multi_skip_limit: MultiXactId;
    let mut skip_index_vacuum: i32;

    debug_assert!(TransactionIdIsNormal(relfrozenxid));
    debug_assert!(MultiXactIdIsValid(relminmxid));

    /*
     * Determine the index skipping age to use. In any case no less than
     * autovacuum_freeze_max_age * 1.05.
     */
    skip_index_vacuum = max_i32(
        rt::vacuum_failsafe_age::call()?,
        (rt::autovacuum_freeze_max_age::call()? as f64 * 1.05) as i32,
    );

    let mut xsl = varsup_seam::read_next_transaction_id::call().wrapping_sub(skip_index_vacuum as u32);
    if !TransactionIdIsNormal(xsl) {
        xsl = FirstNormalTransactionId;
    }
    xid_skip_limit = xsl;

    if TransactionIdPrecedes(relfrozenxid, xid_skip_limit) {
        /* The table's relfrozenxid is too old */
        return Ok(true);
    }

    /*
     * Similar to above, determine the index skipping age to use for
     * multixact. In any case no less than autovacuum_multixact_freeze_max_age *
     * 1.05.
     */
    skip_index_vacuum = max_i32(
        rt::vacuum_multixact_failsafe_age::call()?,
        (rt::autovacuum_multixact_freeze_max_age::call()? as f64 * 1.05) as i32,
    );

    let mut msl = multixact_seam::read_next_multixact_id::call()?.wrapping_sub(skip_index_vacuum as u32);
    if msl < FirstMultiXactId {
        msl = FirstMultiXactId;
    }
    multi_skip_limit = msl;

    if MultiXactIdPrecedes(relminmxid, multi_skip_limit) {
        /* The table's relminmxid is too old */
        return Ok(true);
    }

    Ok(false)
}

/* =========================================================================
 * vac_estimate_reltuples  (vacuum.c:1345-1398)
 * ========================================================================= */

/// `vac_estimate_reltuples(relation, total_pages, scanned_pages,
/// scanned_tuples)` (vacuum.c:1346) — estimate the new pg_class.reltuples.
pub fn vac_estimate_reltuples(
    relation: Oid,
    total_pages: BlockNumber,
    scanned_pages: BlockNumber,
    scanned_tuples: f64,
) -> PgResult<f64> {
    let (old_rel_pages, old_rel_tuples) = relcache_seam::rel_pages_tuples::call(relation)?;
    let old_density: f64;
    let unscanned_pages: f64;
    let total_tuples: f64;

    /* If we did scan the whole table, just use the count as-is */
    if scanned_pages >= total_pages {
        return Ok(scanned_tuples);
    }

    /*
     * ... keep the existing value of reltuples ...
     * (Note: we might be returning -1 here.)
     */
    if old_rel_pages == total_pages && (scanned_pages as f64) < (total_pages as f64 * 0.02) {
        return Ok(old_rel_tuples);
    }
    if scanned_pages <= 1 {
        return Ok(old_rel_tuples);
    }

    /*
     * If old density is unknown, we can't do much except scale up
     * scanned_tuples to match total_pages.
     */
    if old_rel_tuples < 0.0 || old_rel_pages == 0 {
        return Ok(((scanned_tuples / scanned_pages as f64) * total_pages as f64 + 0.5).floor());
    }

    /*
     * Okay, we've covered the corner cases. ...
     */
    old_density = old_rel_tuples / old_rel_pages as f64;
    unscanned_pages = total_pages as f64 - scanned_pages as f64;
    total_tuples = old_density * unscanned_pages + scanned_tuples;
    Ok((total_tuples + 0.5).floor())
}

/* =========================================================================
 * vac_update_relstats  (vacuum.c:1441-1602)
 * ========================================================================= */

/// `vac_update_relstats(...)` (vacuum.c:1442) — update the whole-relation
/// statistics in pg_class with an in-place (nontransactional) write.  Returns
/// `(frozenxid_updated, minmulti_updated)`.
pub fn vac_update_relstats(
    relation: Oid,
    num_pages: BlockNumber,
    num_tuples: f64,
    num_all_visible_pages: BlockNumber,
    num_all_frozen_pages: BlockNumber,
    hasindex: bool,
    frozenxid: TransactionId,
    minmulti: MultiXactId,
    in_outer_xact: bool,
) -> PgResult<(bool, bool)> {
    /*
     * The whole inplace-update mechanics (table_open(RelationRelationId),
     * systable_inplace_update_begin/finish/cancel, applying the per-field
     * dirty decisions and the relfrozenxid/relminmxid backward-guard, then
     * table_close) is performed by the catalog seam, which returns the
     * "futurexid"/"futuremxid" flags plus the old values so we emit the
     * corruption WARNINGs here, and the *_updated out-params, matching C.
     */
    let args = UpdateRelStatsArgs {
        relation,
        num_pages,
        num_tuples,
        num_all_visible_pages,
        num_all_frozen_pages,
        hasindex,
        frozenxid,
        minmulti,
        in_outer_xact,
    };

    let res = rt::vac_update_relstats_apply::call(relation, args)?;

    if res.futurexid {
        let relname = rt::relation_get_relation_name::call(relation)?;
        ereport(WARNING)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "overwrote invalid relfrozenxid value {} with new value {} for table \"{}\"",
                res.old_frozenxid, frozenxid, relname
            ))
            .finish(here("vac_update_relstats"))?;
    }
    if res.futuremxid {
        let relname = rt::relation_get_relation_name::call(relation)?;
        ereport(WARNING)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "overwrote invalid relminmxid value {} with new value {} for table \"{}\"",
                res.old_minmulti, minmulti, relname
            ))
            .finish(here("vac_update_relstats"))?;
    }

    Ok((res.frozenxid_updated, res.minmulti_updated))
}

/* =========================================================================
 * vac_update_datfrozenxid  (vacuum.c:1623-1822)
 * ========================================================================= */

/// `vac_update_datfrozenxid()` (vacuum.c:1624) — update
/// pg_database.datfrozenxid/datminmxid for our DB to the minimum of the
/// pg_class values, and try to truncate pg_xact/pg_multixact.
pub fn vac_update_datfrozenxid() -> PgResult<()> {
    let mut newFrozenXid: TransactionId;
    let mut newMinMulti: MultiXactId;
    let lastSaneFrozenXid: TransactionId;
    let lastSaneMinMulti: MultiXactId;
    let mut bogus = false;

    /*
     * Restrict this task to one backend per database. ...
     */
    lmgr_seam::lock_database_frozen_ids::call(ExclusiveLock)?;

    /*
     * Initialize the "min" calculation with
     * GetOldestNonRemovableTransactionId() ...
     */
    newFrozenXid = procarray_seam::get_oldest_non_removable_transaction_id::call(InvalidOid)?;

    /*
     * Similarly, initialize the MultiXact "min" ...
     */
    newMinMulti = multixact_seam::get_oldest_multi_xact_id::call()?;

    /*
     * Identify the latest relfrozenxid and relminmxid values that we could
     * validly see during the scan. ...
     */
    lastSaneFrozenXid = varsup_seam::read_next_transaction_id::call();
    lastSaneMinMulti = multixact_seam::read_next_multixact_id::call()?;

    /*
     * We must seqscan pg_class to find the minimum Xid ...
     */
    let rows = rt::scan_pg_class_frozenids::call()?;

    for row in rows.iter() {
        let relfrozenxid = row.relfrozenxid;
        let relminmxid = row.relminmxid;

        /*
         * Only consider relations able to hold unfrozen XIDs ...
         */
        if row.relkind != RELKIND_RELATION
            && row.relkind != RELKIND_MATVIEW
            && row.relkind != RELKIND_TOASTVALUE
        {
            debug_assert!(!TransactionIdIsValid(relfrozenxid));
            debug_assert!(!MultiXactIdIsValid(relminmxid));
            continue;
        }

        /*
         * ... validate and compute horizon for each only if set.
         */
        if TransactionIdIsValid(relfrozenxid) {
            debug_assert!(TransactionIdIsNormal(relfrozenxid));

            /* check for values in the future */
            if TransactionIdPrecedes(lastSaneFrozenXid, relfrozenxid) {
                bogus = true;
                break;
            }

            /* determine new horizon */
            if TransactionIdPrecedes(relfrozenxid, newFrozenXid) {
                newFrozenXid = relfrozenxid;
            }
        }

        if MultiXactIdIsValid(relminmxid) {
            /* check for values in the future */
            if MultiXactIdPrecedes(lastSaneMinMulti, relminmxid) {
                bogus = true;
                break;
            }

            /* determine new horizon */
            if MultiXactIdPrecedes(relminmxid, newMinMulti) {
                newMinMulti = relminmxid;
            }
        }
    }

    /* chicken out if bogus data found */
    if bogus {
        return Ok(());
    }

    debug_assert!(TransactionIdIsNormal(newFrozenXid));
    debug_assert!(MultiXactIdIsValid(newMinMulti));

    /*
     * Fetch the pg_database tuple we need to update, apply the (possibly
     * advanced) datfrozenxid/datminmxid in place, and learn the effective
     * values + whether anything was dirtied.  The inplace mechanics live in
     * the catalog seam.
     */
    let res = rt::vac_update_datfrozenxid_apply::call(
        newFrozenXid,
        newMinMulti,
        lastSaneFrozenXid,
        lastSaneMinMulti,
    )?;
    newFrozenXid = res.eff_frozen_xid;
    newMinMulti = res.eff_min_multi;
    let dirty = res.dirty;

    /*
     * If we were able to advance datfrozenxid or datminmxid, see if we can
     * truncate pg_xact and/or pg_multixact. ...
     */
    if dirty || varsup_seam::force_transaction_id_limit_update::call()? {
        vac_truncate_clog(
            newFrozenXid,
            newMinMulti,
            lastSaneFrozenXid,
            lastSaneMinMulti,
        )?;
    }

    Ok(())
}

/* =========================================================================
 * vac_truncate_clog  (vacuum.c:1842-1995)
 * ========================================================================= */

/// `vac_truncate_clog(frozenXID, minMulti, lastSaneFrozenXid, lastSaneMinMulti)`
/// (vacuum.c:1843) — scan pg_database for the system-wide oldest
/// datfrozenxid/datminmxid and truncate the commit logs accordingly.
fn vac_truncate_clog(
    mut frozenXID: TransactionId,
    mut minMulti: MultiXactId,
    lastSaneFrozenXid: TransactionId,
    lastSaneMinMulti: MultiXactId,
) -> PgResult<()> {
    let nextXID = varsup_seam::read_next_transaction_id::call();
    let mut oldestxid_datoid: Oid;
    let mut minmulti_datoid: Oid;
    let mut bogus = false;
    let mut frozenAlreadyWrapped = false;

    /* Restrict task to one backend per cluster; see SimpleLruTruncate(). */
    lwlock_seam::lwlock_acquire_wrap_limits_vacuum::call(LW_EXCLUSIVE)?;

    /* init oldest datoids to sync with my frozenXID/minMulti values */
    let my_database_id = init_small_seam::my_database_id::call();
    oldestxid_datoid = my_database_id;
    minmulti_datoid = my_database_id;

    /*
     * Scan pg_database to compute the minimum datfrozenxid/datminmxid ...
     */
    let rows = rt::scan_pg_database_frozenids::call()?;

    for row in rows.iter() {
        let datfrozenxid = row.datfrozenxid;
        let datminmxid = row.datminmxid;

        debug_assert!(TransactionIdIsNormal(datfrozenxid));
        debug_assert!(MultiXactIdIsValid(datminmxid));

        /*
         * If database is in the process of getting dropped ...
         */
        if row.is_invalid {
            rt::elog_debug2_skip_invalid_db::call(row.datname.clone())?;
            continue;
        }

        /*
         * If things are working properly, no database should have a
         * datfrozenxid or datminmxid that is "in the future". ...
         */
        if TransactionIdPrecedes(lastSaneFrozenXid, datfrozenxid)
            || MultiXactIdPrecedes(lastSaneMinMulti, datminmxid)
        {
            bogus = true;
        }

        if TransactionIdPrecedes(nextXID, datfrozenxid) {
            frozenAlreadyWrapped = true;
        } else if TransactionIdPrecedes(datfrozenxid, frozenXID) {
            frozenXID = datfrozenxid;
            oldestxid_datoid = row.oid;
        }

        if MultiXactIdPrecedes(datminmxid, minMulti) {
            minMulti = datminmxid;
            minmulti_datoid = row.oid;
        }
    }

    /*
     * Do not truncate CLOG if we seem to have suffered wraparound already; ...
     */
    if frozenAlreadyWrapped {
        ereport(WARNING)
            .errmsg("some databases have not been vacuumed in over 2 billion transactions")
            .errdetail("You might have already suffered transaction-wraparound data loss.")
            .finish(here("vac_truncate_clog"))?;
        lwlock_seam::lwlock_release_wrap_limits_vacuum::call()?;
        return Ok(());
    }

    /* chicken out if data is bogus in any other way */
    if bogus {
        lwlock_seam::lwlock_release_wrap_limits_vacuum::call()?;
        return Ok(());
    }

    /*
     * Freeze any old transaction IDs in the async notification queue before
     * CLOG truncation.
     */
    async_seam::async_notify_freeze_xids::call(frozenXID)?;

    /*
     * Advance the oldest value for commit timestamps before truncating ...
     */
    commit_ts_seam::advance_oldest_commit_ts_xid::call(frozenXID)?;

    /*
     * Truncate CLOG, multixact and CommitTs to the oldest computed value.
     */
    clog_seam::truncate_clog::call(frozenXID, oldestxid_datoid)?;
    commit_ts_seam::truncate_commit_ts::call(frozenXID)?;
    multixact_seam::truncate_multixact::call(minMulti, minmulti_datoid)?;

    /*
     * Update the wrap limit for GetNewTransactionId and creation of new
     * MultiXactIds. ...
     */
    rt::set_transaction_id_limit::call(frozenXID, oldestxid_datoid)?;
    multixact_seam::set_multi_xact_id_limit::call(minMulti, minmulti_datoid, false)?;

    lwlock_seam::lwlock_release_wrap_limits_vacuum::call()?;

    Ok(())
}

/* =========================================================================
 * vacuum_rel  (vacuum.c:2017-2364)
 * ========================================================================= */

/// `vacuum_rel(relid, relation, params, bstrategy)` (vacuum.c:2018) — vacuum
/// one heap relation; returns true if it's OK to proceed with a requested
/// ANALYZE on this table.  At entry and exit, we are not inside a transaction.
fn vacuum_rel<'mcx>(
    relid: Oid,
    relation: Option<RangeVar<'mcx>>,
    params: &mut VacuumParams,
    bstrategy: BufferAccessStrategy,
    mcx: Mcx<'mcx>,
) -> PgResult<bool> {
    let lmode: LOCKMODE;
    let mut rel: Option<Oid>;
    let lockrelid: types_storage::lock::LockRelId;
    let priv_relid: Oid;
    let toast_relid: Oid;
    let save_userid: Oid;
    let save_sec_context: i32;
    let save_nestlevel: i32;

    let p = params;

    /*
     * This function scribbles on the parameters, so make a copy early to
     * avoid affecting the TOAST table (if we do end up recursing to it).
     */
    let mut toast_vacuum_params: VacuumParams = *p;

    /* Begin a transaction for vacuuming this relation */
    xact::start_transaction_command::call()?;

    if p.options & VACOPT_FULL == 0 {
        /*
         * In lazy vacuum, we can set the PROC_IN_VACUUM flag ...
         */
        rt::set_proc_in_vacuum_flags::call(p.is_wraparound)?;
    }

    /*
     * Need to acquire a snapshot to prevent pg_subtrans from being truncated ...
     */
    snapmgr_seam::push_active_snapshot_transaction::call()?;

    /*
     * Check for user-requested abort. ...
     */
    rt::check_for_interrupts::call()?;

    /*
     * Determine the type of lock we want --- hard exclusive lock for a FULL
     * vacuum, but just ShareUpdateExclusiveLock for concurrent vacuum. ...
     */
    lmode = if p.options & VACOPT_FULL != 0 {
        AccessExclusiveLock
    } else {
        ShareUpdateExclusiveLock
    };

    /* open the relation and get the appropriate lock on it */
    rel = vacuum_open_relation(
        relid,
        relation.as_ref(),
        p.options,
        p.log_min_duration >= 0,
        lmode,
    )?;

    /* leave if relation could not be opened or locked */
    let Some(rel_handle) = rel else {
        snapmgr_seam::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        return Ok(false);
    };

    /*
     * When recursing to a TOAST table, check privileges on the parent. ...
     */
    if OidIsValid(p.toast_parent) {
        priv_relid = p.toast_parent;
    } else {
        priv_relid = rel_handle; // RelationGetRelid
    }

    /*
     * Check if relation needs to be skipped based on privileges. ...
     */
    let rd_rel = rt::search_syscache_class::call(mcx, rel_handle)?;
    let rd_rel = rd_rel.ok_or_else(|| {
        PgError::error("vacuum_rel: cache lookup failed for opened relation")
    })?;
    if !vacuum_is_permitted_for_relation(priv_relid, &rd_rel, p.options & !VACOPT_ANALYZE)? {
        table_seam::relation_close::call(rel_handle, lmode)?;
        snapmgr_seam::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        return Ok(false);
    }

    /*
     * Check that it's of a vacuumable relkind.
     */
    let relkind = rt::rel_relkind::call(rel_handle)?;
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_MATVIEW
        && relkind != RELKIND_TOASTVALUE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let relname = rt::relation_get_relation_name::call(rel_handle)?;
        ereport(WARNING)
            .errmsg(format!(
                "skipping \"{}\" --- cannot vacuum non-tables or special system tables",
                relname
            ))
            .finish(here("vacuum_rel"))?;
        table_seam::relation_close::call(rel_handle, lmode)?;
        snapmgr_seam::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        return Ok(false);
    }

    /*
     * Silently ignore tables that are temp tables of other backends ...
     */
    if rt::relation_is_other_temp::call(rel_handle)? {
        table_seam::relation_close::call(rel_handle, lmode)?;
        snapmgr_seam::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        return Ok(false);
    }

    /*
     * Silently ignore partitioned tables as there is no work to be done. ...
     */
    if relkind == RELKIND_PARTITIONED_TABLE {
        table_seam::relation_close::call(rel_handle, lmode)?;
        snapmgr_seam::pop_active_snapshot::call()?;
        xact::commit_transaction_command::call()?;
        /* It's OK to proceed with ANALYZE on this table */
        return Ok(true);
    }

    /*
     * Get a session-level lock too. ...
     */
    lockrelid = relcache_seam::rel_lock_relid::call(rel_handle)?;
    rt::lock_relation_id_for_session::call(lockrelid, lmode)?;

    /*
     * Set index_cleanup option based on index_cleanup reloption ...
     */
    let std_options = relcache_seam::rel_std_rd_options::call(rel_handle)?;
    if p.index_cleanup == VACOPTVALUE_UNSPECIFIED {
        let vacuum_index_cleanup: u8 = if !std_options.has_options {
            STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO
        } else {
            std_options.vacuum_index_cleanup
        };

        if vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO {
            p.index_cleanup = VACOPTVALUE_AUTO;
        } else if vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON {
            p.index_cleanup = VACOPTVALUE_ENABLED;
        } else {
            debug_assert!(vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF);
            p.index_cleanup = VACOPTVALUE_DISABLED;
        }
    }

    /* USE_INJECTION_POINTS */
    if p.index_cleanup == VACOPTVALUE_AUTO {
        rt::injection_point::call("vacuum-index-cleanup-auto".to_string())?;
    } else if p.index_cleanup == VACOPTVALUE_DISABLED {
        rt::injection_point::call("vacuum-index-cleanup-disabled".to_string())?;
    } else if p.index_cleanup == VACOPTVALUE_ENABLED {
        rt::injection_point::call("vacuum-index-cleanup-enabled".to_string())?;
    }

    /*
     * Check if the vacuum_max_eager_freeze_failure_rate table storage
     * parameter was specified. This overrides the GUC value.
     */
    if std_options.has_options && std_options.max_eager_freeze_failure_rate >= 0.0 {
        p.max_eager_freeze_failure_rate = std_options.max_eager_freeze_failure_rate;
    }

    /*
     * Set truncate option based on truncate reloption or GUC ...
     */
    if p.truncate == VACOPTVALUE_UNSPECIFIED {
        match std_options.vacuum_truncate {
            Some((true, truncate)) => {
                if truncate {
                    p.truncate = VACOPTVALUE_ENABLED;
                } else {
                    p.truncate = VACOPTVALUE_DISABLED;
                }
            }
            _ => {
                if rt::vacuum_truncate::call()? {
                    p.truncate = VACOPTVALUE_ENABLED;
                } else {
                    p.truncate = VACOPTVALUE_DISABLED;
                }
            }
        }
    }

    /* USE_INJECTION_POINTS */
    if p.truncate == VACOPTVALUE_AUTO {
        rt::injection_point::call("vacuum-truncate-auto".to_string())?;
    } else if p.truncate == VACOPTVALUE_DISABLED {
        rt::injection_point::call("vacuum-truncate-disabled".to_string())?;
    } else if p.truncate == VACOPTVALUE_ENABLED {
        rt::injection_point::call("vacuum-truncate-enabled".to_string())?;
    }

    /*
     * Remember the relation's TOAST relation for later ...
     */
    if (p.options & VACOPT_PROCESS_TOAST) != 0
        && ((p.options & VACOPT_FULL) == 0 || (p.options & VACOPT_PROCESS_MAIN) == 0)
    {
        toast_relid = relcache_seam::rel_reltoastrelid::call(rel_handle)?;
    } else {
        toast_relid = InvalidOid;
    }

    /*
     * Switch to the table owner's userid ...
     */
    let (su, ssc) = miscinit_seam::get_user_id_and_sec_context::call();
    save_userid = su;
    save_sec_context = ssc;
    rt::set_user_id_and_sec_context::call(
        relcache_seam::rel_relowner::call(rel_handle)?,
        save_sec_context | SECURITY_RESTRICTED_OPERATION,
    )?;
    save_nestlevel = rt::new_guc_nest_level::call()?;
    guc_seam::restrict_search_path::call()?;

    /*
     * If PROCESS_MAIN is set (the default), it's time to vacuum the main
     * relation. ...
     */
    if p.options & VACOPT_PROCESS_MAIN != 0 {
        /*
         * Do the actual work --- either FULL or "lazy" vacuum
         */
        if p.options & VACOPT_FULL != 0 {
            let verbose = (p.options & VACOPT_VERBOSE) != 0;

            /* VACUUM FULL is now a variant of CLUSTER; see cluster.c */
            let mut cluster_params = ClusterParams::new();
            if verbose {
                cluster_params.options |= CLUOPT_VERBOSE;
            }
            /*
             * The relation is already open with AccessExclusiveLock held; a
             * NoLock table_open recovers the Relation value from the relcache
             * without re-locking. cluster_rel closes it but keeps the lock.
             */
            let old_heap = table_seam::table_open::call(mcx, rel_handle, NoLock)?;
            cluster_seam::cluster_rel::call(mcx, old_heap, InvalidOid, cluster_params)?;

            rel = None;
        } else {
            /*
             * Recover the live, open Relation from the relcache without
             * re-locking (the lock from vacuum_open_relation is held), and hand
             * the OWNED value to the heap vacuum driver, which holds it for the
             * whole scan (the `PlannerRun(mcx)` analog). The driver drops its
             * own relcache reference when it finishes; the original reference
             * (rel_handle) is closed below.
             */
            let open_relation = table_seam::table_open::call(mcx, rel_handle, NoLock)?;
            rt::table_relation_vacuum::call(mcx, open_relation, *p, bstrategy.clone())?;
            rel = Some(rel_handle);
        }
    } else {
        rel = Some(rel_handle);
    }

    /* Roll back any GUC changes executed by index functions */
    rt::at_eoxact_guc::call(false, save_nestlevel)?;

    /* Restore userid and security context */
    rt::set_user_id_and_sec_context::call(save_userid, save_sec_context)?;

    /* all done with this class, but hold lock until commit */
    if let Some(open_rel) = rel {
        table_seam::relation_close::call(open_rel, NoLock)?;
    }

    /*
     * Complete the transaction and free all temporary memory used.
     */
    snapmgr_seam::pop_active_snapshot::call()?;
    xact::commit_transaction_command::call()?;

    /*
     * If the relation has a secondary toast rel, vacuum that too ...
     */
    if toast_relid != InvalidOid {
        /*
         * Force VACOPT_PROCESS_MAIN ... set toast_parent ...
         */
        toast_vacuum_params.options |= VACOPT_PROCESS_MAIN;
        toast_vacuum_params.toast_parent = relid;

        vacuum_rel(toast_relid, None, &mut toast_vacuum_params, bstrategy, mcx)?;
    }

    /*
     * Now release the session-level lock on the main table.
     */
    rt::unlock_relation_id_for_session::call(lockrelid, lmode)?;

    /* Report that we really did it. */
    Ok(true)
}

/* =========================================================================
 * vac_open_indexes  (vacuum.c:2379-2416)
 * ========================================================================= */

/// `vac_open_indexes(relation, lockmode, nindexes, Irel)` (vacuum.c:2380) —
/// open all the vacuumable indexes of the relation.  Returns the array of
/// index `Relation`s (those marked indisready) by value.  (C palloc's the Irel
/// array; a plain Vec is behavior-equivalent here.)
pub fn vac_open_indexes(relation: Oid, lockmode: LOCKMODE) -> PgResult<Vec<Oid>> {
    debug_assert!(lockmode != NoLock);

    let indexoidlist = rt::relation_get_index_list::call(relation)?;

    /* allocate enough memory for all indexes */
    let mut irel: Vec<Oid> = Vec::with_capacity(indexoidlist.len());

    /* collect just the ready indexes */
    for indexoid in indexoidlist {
        let opened = rt::index_open::call(indexoid, lockmode)?;
        if opened.indisready {
            irel.push(opened.index);
        } else {
            rt::index_close::call(opened.index, lockmode)?;
        }
    }

    Ok(irel)
}

/* =========================================================================
 * vac_close_indexes  (vacuum.c:2422-2435)
 * ========================================================================= */

/// `vac_close_indexes(nindexes, Irel, lockmode)` (vacuum.c:2423) — release the
/// resources acquired by `vac_open_indexes`.
pub fn vac_close_indexes(irel: &[Oid], lockmode: LOCKMODE) -> PgResult<()> {
    if irel.is_empty() {
        return Ok(());
    }

    /* while (nindexes--) { index_close(Irel[nindexes], lockmode); } */
    let mut nindexes = irel.len();
    while nindexes > 0 {
        nindexes -= 1;
        let ind = irel[nindexes];
        rt::index_close::call(ind, lockmode)?;
    }

    Ok(())
}

/* =========================================================================
 * vacuum_delay_point  (vacuum.c:2443-2571)
 * ========================================================================= */

/// `vacuum_delay_point(is_analyze)` (vacuum.c:2444) — check for interrupts and
/// perform the cost-based delay.
pub fn vacuum_delay_point(is_analyze: bool) -> PgResult<()> {
    let mut msec: f64 = 0.0;

    /* Always check for interrupts */
    rt::check_for_interrupts::call()?;

    if rt::interrupt_pending::call()?
        || (!rt::vacuum_cost_active::call()? && !rt::config_reload_pending::call()?)
    {
        return Ok(());
    }

    /*
     * Autovacuum workers should reload the configuration file if requested. ...
     */
    if rt::config_reload_pending::call()? && rt::am_autovacuum_worker_process::call()? {
        rt::set_config_reload_pending::call(false)?;
        rt::process_config_file_sighup::call()?;
        rt::vacuum_update_costs::call()?;
    }

    /*
     * If we disabled cost-based delays after reloading the config file, return.
     */
    if !rt::vacuum_cost_active::call()? {
        return Ok(());
    }

    let vacuum_cost_delay = rt::vacuum_cost_delay::call()?;
    let vacuum_cost_limit = rt::vacuum_cost_limit::call()?;

    /*
     * For parallel vacuum, the delay is computed based on the shared cost
     * balance.  See compute_parallel_delay.
     */
    if rt::vacuum_shared_cost_balance_is_set::call()? {
        msec = compute_parallel_delay()?;
    } else if rt::vacuum_cost_balance::call()? >= vacuum_cost_limit {
        msec =
            vacuum_cost_delay * rt::vacuum_cost_balance::call()? as f64 / vacuum_cost_limit as f64;
    }

    /* Nap if appropriate */
    if msec > 0.0 {
        if msec > vacuum_cost_delay * 4.0 {
            msec = vacuum_cost_delay * 4.0;
        }

        let track = rt::track_cost_delay_timing::call()?;

        /*
         * pgstat_report_wait_start(WAIT_EVENT_VACUUM_DELAY); pg_usleep(...);
         * pgstat_report_wait_end().  The seam measures the elapsed delay (when
         * `track` is set) and returns its nanoseconds.
         */
        let delay_ns = rt::vacuum_sleep::call((msec * 1000.0) as i64, track)?;

        if track {
            /*
             * For parallel workers, we only report the delay time every once
             * in a while to avoid overloading the leader ...
             */
            if rt::is_parallel_worker::call()? {
                debug_assert!(!is_analyze);

                /* Accumulate the delay time */
                rt::add_parallel_vacuum_worker_delay_ns::call(delay_ns)?;

                /* Calculate interval since last report */
                let time_since_last_report = rt::time_since_last_delay_report_ns::call()?;

                /* If we haven't reported in a while, do so now */
                if time_since_last_report >= PARALLEL_VACUUM_DELAY_REPORT_INTERVAL_NS {
                    rt::progress_parallel_incr_delay_time::call(
                        rt::parallel_vacuum_worker_delay_ns::call()?,
                    )?;

                    /* Reset variables */
                    rt::reset_last_delay_report_time::call()?;
                    rt::set_parallel_vacuum_worker_delay_ns::call(0)?;
                }
            } else if is_analyze {
                rt::progress_incr_analyze_delay_time::call(delay_ns)?;
            } else {
                rt::progress_incr_vacuum_delay_time::call(delay_ns)?;
            }
        }

        /*
         * We don't want to ignore postmaster death during very long vacuums ...
         */
        if rt::postmaster_died::call()? {
            rt::exit_process::call(1);
        }

        rt::set_vacuum_cost_balance::call(0)?;

        /*
         * Balance and update limit values for autovacuum workers. ...
         */
        rt::autovacuum_update_cost_limit::call()?;

        /* Might have gotten an interrupt while sleeping */
        rt::check_for_interrupts::call()?;
    }

    Ok(())
}

/* =========================================================================
 * compute_parallel_delay  (vacuum.c:2595-2631)
 * ========================================================================= */

/// `compute_parallel_delay()` (vacuum.c:2596) — compute the vacuum delay for
/// parallel workers based on the shared cost balance.
fn compute_parallel_delay() -> PgResult<f64> {
    let mut msec: f64 = 0.0;
    let shared_balance: u32;
    let nworkers: i32;

    /* Parallel vacuum must be active */
    debug_assert!(rt::vacuum_shared_cost_balance_is_set::call()?);

    nworkers = rt::read_vacuum_active_nworkers::call()? as i32;

    /* At least count itself */
    debug_assert!(nworkers >= 1);

    let vacuum_cost_balance = rt::vacuum_cost_balance::call()?;
    let vacuum_cost_limit = rt::vacuum_cost_limit::call()?;
    let vacuum_cost_delay = rt::vacuum_cost_delay::call()?;

    /* Update the shared cost balance value atomically */
    shared_balance = rt::shared_cost_balance_add_fetch::call(vacuum_cost_balance)?;

    /* Compute the total local balance for the current worker */
    let cost_balance_local = rt::add_vacuum_cost_balance_local::call(vacuum_cost_balance)?;

    if (shared_balance >= vacuum_cost_limit as u32)
        && (cost_balance_local as f64 > 0.5 * (vacuum_cost_limit as f64 / nworkers as f64))
    {
        /* Compute sleep time based on the local cost balance */
        msec = vacuum_cost_delay * cost_balance_local as f64 / vacuum_cost_limit as f64;
        rt::shared_cost_balance_sub_fetch::call(cost_balance_local)?;
        rt::set_vacuum_cost_balance_local::call(0)?;
    }

    /*
     * Reset the local balance as we accumulated it into the shared value.
     */
    rt::set_vacuum_cost_balance::call(0)?;

    Ok(msec)
}

/* =========================================================================
 * get_vacoptval_from_boolean  (vacuum.c:2639-2643)
 * ========================================================================= */

/// `get_vacoptval_from_boolean(def)` (vacuum.c:2640) — a wrapper around
/// `defGetBoolean` returning VACOPTVALUE_ENABLED/VACOPTVALUE_DISABLED.
pub fn get_vacoptval_from_boolean(
    def: &types_nodes::ddlnodes::DefElem<'_>,
) -> PgResult<VacOptValue> {
    Ok(if defGetBoolean(def)? {
        VACOPTVALUE_ENABLED
    } else {
        VACOPTVALUE_DISABLED
    })
}

/* =========================================================================
 * vac_bulkdel_one_index  (vacuum.c:2650-2664)
 * ========================================================================= */

/// `vac_bulkdel_one_index(ivinfo, istat, dead_items, dead_items_info)`
/// (vacuum.c:2651) — bulk-deletion for an index relation.
pub fn vac_bulkdel_one_index(
    ivinfo: IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: TidStore,
    dead_items_info: VacDeadItemsInfo,
) -> PgResult<IndexBulkDeleteResult> {
    let ivinfo_index = ivinfo.index;
    let ivinfo_message_level = ivinfo.message_level;
    /* Do bulk deletion (the callback is vac_tid_reaped). The AM consults the
     * dead-items store through the `vacuum_tid_is_dead` callback keyed by the
     * store's id; register it here so that lookup resolves while the AM scans. */
    register_tid_callback_state(dead_items);
    let istat = rt::index_bulk_delete::call(ivinfo, istat, dead_items)?;

    let relname = rt::relation_get_relation_name::call(ivinfo_index)?;
    ereport(ErrorLevel(ivinfo_message_level))
        .errmsg(format!(
            "scanned index \"{}\" to remove {} row versions",
            relname, dead_items_info.num_items
        ))
        .finish(here("vac_bulkdel_one_index"))?;

    Ok(istat)
}

/* =========================================================================
 * vac_cleanup_one_index  (vacuum.c:2671-2690)
 * ========================================================================= */

/// `vac_cleanup_one_index(ivinfo, istat)` (vacuum.c:2672) — post-vacuum cleanup
/// for an index relation.
pub fn vac_cleanup_one_index(
    ivinfo: IndexVacuumInfo,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let ivinfo_index = ivinfo.index;
    let ivinfo_message_level = ivinfo.message_level;
    let istat = rt::index_vacuum_cleanup::call(ivinfo, istat)?;

    if let Some(stat) = istat {
        let relname = rt::relation_get_relation_name::call(ivinfo_index)?;
        ereport(ErrorLevel(ivinfo_message_level))
            .errmsg(format!(
                "index \"{}\" now contains {:.0} row versions in {} pages",
                relname, stat.num_index_tuples, stat.num_pages
            ))
            .errdetail(format!(
                "{:.0} index row versions were removed.\n\
                 {} index pages were newly deleted.\n\
                 {} index pages are currently deleted, of which {} are currently reusable.",
                stat.tuples_removed,
                stat.pages_newly_deleted,
                stat.pages_deleted,
                stat.pages_free
            ))
            .finish(here("vac_cleanup_one_index"))?;
    }

    Ok(istat)
}

/* =========================================================================
 * vac_tid_reaped  (vacuum.c:2697-2703)
 * ========================================================================= */

/// `vac_tid_reaped(itemptr, state)` (vacuum.c:2698) — is a particular tid
/// deletable?  Has the right shape to be an `IndexBulkDeleteCallback`; the
/// `void *state` is the dead-items `TidStore`.
pub fn vac_tid_reaped(itemptr: ItemPointerData, dead_items: TidStore) -> PgResult<bool> {
    rt::tid_store_is_member::call(dead_items, itemptr)
}

/* =========================================================================
 * Inward seam glue (callable through vacuum-seams / vacuumlazy-seams).
 * ========================================================================= */

/// Crate-local registry mapping a `vacuum_tid_is_dead` callback-state handle to
/// the live dead-items `TidStore`.  The btbulkdelete callback (`vacuum_tid_is_dead`)
/// is mcx-free and infallible; the TidStore is identified by its `u64` id.
mod tid_callback_registry {
    use super::TidStore;
    use core::cell::RefCell;
    extern crate alloc;
    use alloc::collections::BTreeMap;

    thread_local! {
        static REG: RefCell<BTreeMap<u64, TidStore>> = const { RefCell::new(BTreeMap::new()) };
    }

    pub fn register(ts: TidStore) -> u64 {
        // The TidStore's own id is its stable handle.
        let key = ts.id;
        REG.with(|r| r.borrow_mut().insert(key, ts));
        key
    }
    pub fn lookup(key: u64) -> Option<TidStore> {
        REG.with(|r| r.borrow().get(&key).copied())
    }
}

/// Public registration helper used by the dead-TID owner to make a `TidStore`
/// reachable through the `vacuum_tid_is_dead` callback-state handle.
pub fn register_tid_callback_state(ts: TidStore) -> u64 {
    tid_callback_registry::register(ts)
}

/// Inward seam body for `vacuum_tid_is_dead(tid, callback_state_handle)`: the
/// btbulkdelete `IndexBulkDeleteCallback`. Resolves the `TidStore` and probes
/// membership. Infallible in C; an `Err` from the membership seam (unported
/// owner) surfaces here as a panic via `.expect`, matching C's infallibility.
fn vacuum_tid_is_dead_impl(tid: ItemPointerData, callback_state_handle: u64) -> bool {
    let ts = tid_callback_registry::lookup(callback_state_handle)
        .expect("vacuum_tid_is_dead: unknown callback-state handle");
    tidstore_seams::tidstore_is_member::call(ts, tid).expect("tidstore_is_member")
}

/// Inward seam body for the no-arg `vacuum_delay_point()` (consumed by index
/// AMs) == `vacuum_delay_point(false)`.
fn vacuum_delay_point_noarg() -> PgResult<()> {
    vacuum_delay_point(false)
}

/* =========================================================================
 * vacuum.c cost-state globals (VacuumFailsafeActive / VacuumCostActive /
 * VacuumCostBalance) — owned here as thread_local cells.  vacuum.c declares
 * these as backend-local globals.  The cost-rate/limit (vacuum_cost_delay /
 * vacuum_cost_limit) reads come from the GUC layer through vacuum-seams; the
 * vacuum.c-owned booleans/counters live here.
 * ========================================================================= */

thread_local! {
    static VACUUM_FAILSAFE_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static VACUUM_COST_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static VACUUM_COST_BALANCE: Cell<i32> = const { Cell::new(0) };
    static VACUUM_COST_BALANCE_LOCAL: Cell<i32> = const { Cell::new(0) };
    /// `double vacuum_cost_delay = 0;` and `int vacuum_cost_limit = 200;`
    /// (vacuum.c:91-92) — the live working cost parameters. Distinct from the
    /// `VacuumCostDelay` / `VacuumCostLimit` GUC globals (globals.c): these are
    /// recomputed by `VacuumUpdateCosts()` from the GUC source + per-table
    /// storage params, then read by `vacuum_delay_point` / `compute_parallel_delay`.
    static VACUUM_COST_DELAY_WORKING: Cell<f64> = const { Cell::new(0.0) };
    static VACUUM_COST_LIMIT_WORKING: Cell<i32> = const { Cell::new(200) };
    /// `pg_atomic_uint32 *VacuumSharedCostBalance;` and
    /// `pg_atomic_uint32 *VacuumActiveNWorkers;` (vacuum.c globals). Both point
    /// into the same parallel-vacuum DSM shared cost-state cell, so one shared
    /// handle (`None` == both `NULL`, the non-parallel case) models the pair —
    /// the leader/worker enable seams install it, vacuum.c's
    /// `compute_parallel_delay` and vacuumparallel.c's worker setup
    /// atomic-mutate it. A backend is in at most one parallel vacuum at a time,
    /// so a single cell is exact.
    static VACUUM_SHARED_COST_STATE:
        core::cell::RefCell<Option<alloc::sync::Arc<types_vacuum::vacuumparallel::VacuumSharedCostState>>> =
        const { core::cell::RefCell::new(None) };
}

/// Run `f` against the shared cost-state handle (the `VacuumSharedCostBalance` /
/// `VacuumActiveNWorkers` globals), or panic loudly if it is `NULL` — exactly
/// like dereferencing the C pointer when the caller has already checked it is
/// non-NULL (the `vacuum_*_is_set` guards gate every mutating call).
fn with_shared_cost_state<R>(
    f: impl FnOnce(&types_vacuum::vacuumparallel::VacuumSharedCostState) -> R,
) -> R {
    VACUUM_SHARED_COST_STATE.with(|s| {
        let s = s.borrow();
        let arc = s
            .as_ref()
            .expect("VacuumSharedCostBalance/VacuumActiveNWorkers is NULL");
        f(arc)
    })
}

/* =========================================================================
 * vacuum.c GUC variables (`conf->variable` backing).  These are plain int /
 * bool / double GUC globals declared in vacuum.c (vacuum_freeze_min_age,
 * vacuum_freeze_table_age, vacuum_multixact_freeze_min_age,
 * vacuum_multixact_freeze_table_age, vacuum_failsafe_age,
 * vacuum_multixact_failsafe_age, vacuum_max_eager_freeze_failure_rate,
 * track_cost_delay_timing, vacuum_truncate) and read directly from the GUC
 * slot by the VACUUM driver (and autovacuum).  None come from ControlFile.
 *
 * vacuum.c owns the C `conf->variable` storage, so the GUC var accessors are
 * installed here over these thread-local cells.  The cells are seeded with the
 * C boot_val (guc_tables.c) so a read before InitializeGUCOptions sees the
 * compiled-in default, exactly as the C global would; the GUC engine writes the
 * boot value (and any user override) through the installed `set`.
 * ========================================================================= */

pub(crate) mod guc_globals {
    use core::cell::Cell;

    macro_rules! guc_global {
        ($cell:ident, $get:ident, $set:ident, $ty:ty, $init:expr) => {
            thread_local! {
                static $cell: Cell<$ty> = const { Cell::new($init) };
            }
            #[inline]
            pub fn $get() -> $ty {
                $cell.with(|c| c.get())
            }
            #[inline]
            pub fn $set(value: $ty) {
                $cell.with(|c| c.set(value));
            }
        };
    }

    // `int vacuum_freeze_min_age;` — guc_tables.c boot_val 50000000.
    guc_global!(VACUUM_FREEZE_MIN_AGE, vacuum_freeze_min_age, set_vacuum_freeze_min_age, i32, 50000000);
    // `int vacuum_freeze_table_age;` — boot_val 150000000.
    guc_global!(VACUUM_FREEZE_TABLE_AGE, vacuum_freeze_table_age, set_vacuum_freeze_table_age, i32, 150000000);
    // `int vacuum_multixact_freeze_min_age;` — boot_val 5000000.
    guc_global!(VACUUM_MULTIXACT_FREEZE_MIN_AGE, vacuum_multixact_freeze_min_age, set_vacuum_multixact_freeze_min_age, i32, 5000000);
    // `int vacuum_multixact_freeze_table_age;` — boot_val 150000000.
    guc_global!(VACUUM_MULTIXACT_FREEZE_TABLE_AGE, vacuum_multixact_freeze_table_age, set_vacuum_multixact_freeze_table_age, i32, 150000000);
    // `int vacuum_failsafe_age;` — boot_val 1600000000.
    guc_global!(VACUUM_FAILSAFE_AGE, vacuum_failsafe_age, set_vacuum_failsafe_age, i32, 1600000000);
    // `int vacuum_multixact_failsafe_age;` — boot_val 1600000000.
    guc_global!(VACUUM_MULTIXACT_FAILSAFE_AGE, vacuum_multixact_failsafe_age, set_vacuum_multixact_failsafe_age, i32, 1600000000);
    // `double vacuum_max_eager_freeze_failure_rate;` — boot_val 0.03.
    guc_global!(VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE, vacuum_max_eager_freeze_failure_rate, set_vacuum_max_eager_freeze_failure_rate, f64, 0.03f64);
    // `bool track_cost_delay_timing;` — boot_val false.
    guc_global!(TRACK_COST_DELAY_TIMING, track_cost_delay_timing, set_track_cost_delay_timing, bool, false);
    // `bool vacuum_truncate;` — boot_val true.
    guc_global!(VACUUM_TRUNCATE, vacuum_truncate, set_vacuum_truncate, bool, true);
}

fn vacuum_failsafe_active_impl() -> PgResult<bool> {
    Ok(VACUUM_FAILSAFE_ACTIVE.with(|c| c.get()))
}
fn set_vacuum_failsafe_active_impl(v: bool) -> PgResult<()> {
    VACUUM_FAILSAFE_ACTIVE.with(|c| c.set(v));
    Ok(())
}
fn vacuum_cost_active_impl() -> PgResult<bool> {
    Ok(VACUUM_COST_ACTIVE.with(|c| c.get()))
}
fn set_vacuum_cost_active_impl(v: bool) -> PgResult<()> {
    VACUUM_COST_ACTIVE.with(|c| c.set(v));
    Ok(())
}
fn vacuum_cost_balance_impl() -> PgResult<i32> {
    Ok(VACUUM_COST_BALANCE.with(|c| c.get()))
}
// vacuum.c working cost parameters (`vacuum_cost_delay` / `vacuum_cost_limit`).
fn vacuum_cost_delay_impl() -> PgResult<f64> {
    Ok(VACUUM_COST_DELAY_WORKING.with(|c| c.get()))
}
fn set_vacuum_cost_delay_impl(v: f64) {
    VACUUM_COST_DELAY_WORKING.with(|c| c.set(v));
}
fn vacuum_cost_limit_impl() -> PgResult<i32> {
    Ok(VACUUM_COST_LIMIT_WORKING.with(|c| c.get()))
}
fn set_vacuum_cost_limit_impl(v: i32) {
    VACUUM_COST_LIMIT_WORKING.with(|c| c.set(v));
}
fn set_vacuum_cost_balance_impl(v: i32) -> PgResult<()> {
    VACUUM_COST_BALANCE.with(|c| c.set(v));
    Ok(())
}
fn set_vacuum_cost_balance_local_impl(v: i32) -> PgResult<()> {
    VACUUM_COST_BALANCE_LOCAL.with(|c| c.set(v));
    Ok(())
}
/// `VacuumCostBalanceLocal += v; return new value` (compute_parallel_delay).
fn add_vacuum_cost_balance_local_impl(v: i32) -> PgResult<i32> {
    Ok(VACUUM_COST_BALANCE_LOCAL.with(|c| {
        let n = c.get().wrapping_add(v);
        c.set(n);
        n
    }))
}

/* =========================================================================
 * VacuumSharedCostBalance / VacuumActiveNWorkers — the DSM-shared cost-state
 * pointers. The leader/worker enable seams install the shared handle; vacuum.c
 * (compute_parallel_delay) and vacuumparallel.c atomic-mutate it.
 * ========================================================================= */

use types_vacuum::vacuumparallel::VacuumSharedCostState;

/// `VacuumSharedCostBalance = enable ? &shared->cost_balance : NULL`.
fn set_vacuum_shared_cost_balance_enable_impl(
    shared: Option<alloc::sync::Arc<VacuumSharedCostState>>,
) -> PgResult<()> {
    VACUUM_SHARED_COST_STATE.with(|s| *s.borrow_mut() = shared);
    Ok(())
}

/// `VacuumActiveNWorkers = enable ? &shared->active_nworkers : NULL`. Both
/// globals alias the one shared cell, so this and
/// [`set_vacuum_shared_cost_balance_enable_impl`] install/clear the same handle.
fn set_vacuum_active_nworkers_enable_impl(
    shared: Option<alloc::sync::Arc<VacuumSharedCostState>>,
) -> PgResult<()> {
    VACUUM_SHARED_COST_STATE.with(|s| *s.borrow_mut() = shared);
    Ok(())
}

/// `VacuumSharedCostBalance = NULL; VacuumActiveNWorkers = NULL;` (vacuum.c) —
/// clear the shared parallel-cost-state handle when not running parallel vacuum.
fn clear_parallel_cost_pointers_impl() -> PgResult<()> {
    VACUUM_SHARED_COST_STATE.with(|s| *s.borrow_mut() = None);
    Ok(())
}

/// `VacuumSharedCostBalance != NULL`.
fn vacuum_shared_cost_balance_is_set_impl() -> PgResult<bool> {
    Ok(VACUUM_SHARED_COST_STATE.with(|s| s.borrow().is_some()))
}

/// `VacuumActiveNWorkers != NULL` (same handle as the cost balance).
fn vacuum_active_nworkers_is_set_impl() -> PgResult<bool> {
    Ok(VACUUM_SHARED_COST_STATE.with(|s| s.borrow().is_some()))
}

/// `pg_atomic_add_fetch_u32(VacuumActiveNWorkers, v)` (return discarded by the
/// caller).
fn vacuum_active_nworkers_add_impl(v: u32) -> PgResult<()> {
    with_shared_cost_state(|s| s.active_nworkers_add(v));
    Ok(())
}

/// `pg_atomic_sub_fetch_u32(VacuumActiveNWorkers, v)`.
fn vacuum_active_nworkers_sub_impl(v: u32) -> PgResult<()> {
    with_shared_cost_state(|s| s.active_nworkers_sub(v));
    Ok(())
}

/// `pg_atomic_read_u32(VacuumActiveNWorkers)` (compute_parallel_delay).
fn read_vacuum_active_nworkers_impl() -> PgResult<u32> {
    Ok(with_shared_cost_state(|s| s.active_nworkers_read()))
}

/// `pg_atomic_read_u32(VacuumSharedCostBalance)` — carry the balance back to the
/// heap scan when disabling shared costing.
fn vacuum_shared_cost_balance_read_impl() -> PgResult<u32> {
    Ok(with_shared_cost_state(|s| s.cost_balance_read()))
}

/// `pg_atomic_add_fetch_u32(VacuumSharedCostBalance, v)` (compute_parallel_delay).
fn shared_cost_balance_add_fetch_impl(v: i32) -> PgResult<u32> {
    Ok(with_shared_cost_state(|s| s.cost_balance_add_fetch(v as u32)))
}

/// `pg_atomic_sub_fetch_u32(VacuumSharedCostBalance, v)` (compute_parallel_delay).
fn shared_cost_balance_sub_fetch_impl(v: i32) -> PgResult<u32> {
    Ok(with_shared_cost_state(|s| s.cost_balance_sub_fetch(v as u32)))
}
