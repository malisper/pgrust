//! `pg_partition_tree(regclass)` (OID 3423) and `pg_partition_ancestors(regclass)`
//! (OID 3425) registered as executor-frame materialize-mode set-returning
//! functions.
//!
//! `partitionfuncs.c`'s `pg_partition_tree` is a value-per-call SRF emitting one
//! `(relid regclass, parentid regclass, isleaf bool, level int4)` row per member
//! of the partition tree rooted at the argument (`find_all_inheritors(rootrelid,
//! AccessShareLock, NULL)` enumerates the members; each member's immediate parent
//! and depth come from `get_partition_ancestors`). `pg_partition_ancestors`
//! emits one `(relid regclass)` row for the argument itself followed by each of
//! its ancestors (immediate parent first, root last). Both first apply
//! `check_rel_can_be_partition` — the relation must exist and be either a
//! partition or a partitioned table/index, else the result set is empty.
//!
//! The traversal cores are already ported:
//! [`pg_inherits_seams::find_all_inheritors`] (the BFS over
//! pg_inherits) and [`partition_seams::get_partition_ancestors`]
//! (the bottom-up parent walk). The `check_rel_can_be_partition` guard and the
//! per-row `(parentid, isleaf, level)` computation are the C inner block,
//! reproduced here over the executor frame.
//!
//! The member/ancestor sets are fully determined by the catalog at call time, so
//! these are driven in materialize mode (the whole tuplestore filled once,
//! emitting the identical rows the C per-call series would). Registered from
//! [`register_pg_partition_tree`] / [`register_pg_partition_ancestors`] (called
//! by `init_seams`); they bypass the by-OID builtin registry whose tag-only
//! `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX dual-home).

extern crate alloc;

use types_core::Oid;
use nodes::fmgr::FunctionCallInfoBaseData;
use nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_storage::lock::AccessShareLock;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_partition_tree(regclass)` (OID 3423).
const PG_PARTITION_TREE: Oid = 3423;
/// `pg_partition_ancestors(regclass)` (OID 3425).
const PG_PARTITION_ANCESTORS: Oid = 3425;

/// Register `pg_partition_tree` in the executor-frame SRF table.
pub(crate) fn register_pg_partition_tree() {
    register_srf(PG_PARTITION_TREE, pg_partition_tree);
}

/// Register `pg_partition_ancestors` in the executor-frame SRF table.
pub(crate) fn register_pg_partition_ancestors() {
    register_srf(PG_PARTITION_ANCESTORS, pg_partition_ancestors);
}

/// `RELKIND_HAS_PARTITIONS(relkind)` (`catalog/pg_class.h`) — true for a
/// partitioned table or partitioned index.
fn relkind_has_partitions(relkind: u8) -> bool {
    relkind == types_tuple::access::RELKIND_PARTITIONED_TABLE
        || relkind == types_tuple::access::RELKIND_PARTITIONED_INDEX
}

/// `check_rel_can_be_partition(relid)` (partitionfuncs.c): whether `relid` can
/// appear in a partition tree (it exists and is either a partition or a
/// partitioned table/index). A missing relation, or one that is neither a
/// partition nor a partitioned table/index, yields `false` (C: the function
/// returns an empty set in those cases).
fn check_rel_can_be_partition(relid: Oid) -> PgResult<bool> {
    // C: if (!SearchSysCacheExists1(RELOID, ...)) return false;
    if !syscache_seams::reloid_exists::call(relid)? {
        return Ok(false);
    }

    let relkind = lsyscache_seams::get_rel_relkind::call(relid)?;
    let relispartition = lsyscache_seams::get_rel_relispartition::call(relid)?;

    // C: only relations that can appear in partition trees.
    if !relispartition && !relkind_has_partitions(relkind) {
        return Ok(false);
    }

    Ok(true)
}

/// `pg_partition_tree(PG_FUNCTION_ARGS)` (partitionfuncs.c) over the executor
/// frame.
fn pg_partition_tree<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_partition_tree: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: rootrelid = PG_GETARG_OID(0). regclass is a by-value Oid argument.
    let rootrelid = fcinfo.args[0].value.as_oid();

    // C: if (!check_rel_can_be_partition(rootrelid)) SRF_RETURN_DONE — an empty
    // materialize set. InitMaterializedSRF still establishes the empty
    // tuplestore so the executor drains zero rows.
    let can_be_partition = check_rel_can_be_partition(rootrelid)?;

    // C: list of members via find_all_inheritors(rootrelid, AccessShareLock,
    // NULL) — the root plus every descendant partition, breadth-first.
    let members: alloc::vec::Vec<Oid> = if can_be_partition {
        pg_inherits_seams::find_all_inheritors::call(mcx, rootrelid, AccessShareLock)?
            .iter()
            .copied()
            .collect()
    } else {
        alloc::vec::Vec::new()
    };

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_partition_tree: InitMaterializedSRF establishes fcinfo->resultinfo");

    for relid in members {
        // C inner block: relkind = get_rel_relkind(relid); ancestors =
        // get_partition_ancestors(relid).
        let relkind = lsyscache_seams::get_rel_relkind::call(relid)?;
        let ancestors: alloc::vec::Vec<Oid> =
            partition_seams::get_partition_ancestors::call(mcx, relid)?
                .iter()
                .copied()
                .collect();

        // values[0] = relid (regclass).
        let relid_d = Datum::from_oid(relid);

        // values[1] = parentid = linitial_oid(ancestors), NULL if no ancestors
        // (the root).
        let parentid = ancestors.first().copied().unwrap_or(0);
        let (parent_d, parent_null) = if parentid != 0 {
            (Datum::from_oid(parentid), false)
        } else {
            (Datum::null(), true)
        };

        // values[2] = isleaf = !RELKIND_HAS_PARTITIONS(relkind).
        let isleaf_d = Datum::from_bool(!relkind_has_partitions(relkind));

        // values[3] = level: 0 for the root; else the 1-based depth of `relid`
        // below `rootrelid` (count ancestors up to and including rootrelid).
        let mut level = 0i32;
        if relid != rootrelid {
            for a in &ancestors {
                level += 1;
                if *a == rootrelid {
                    break;
                }
            }
        }
        let level_d = Datum::from_i32(level);

        let values = [relid_d, parent_d, isleaf_d, level_d];
        let nulls = [false, parent_null, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE.
    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `pg_partition_ancestors(PG_FUNCTION_ARGS)` (partitionfuncs.c) over the
/// executor frame.
fn pg_partition_ancestors<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_partition_ancestors: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: relid = PG_GETARG_OID(0).
    let relid = fcinfo.args[0].value.as_oid();

    // C: if (!check_rel_can_be_partition(relid)) SRF_RETURN_DONE — empty set.
    // ancestors = lcons_oid(relid, get_partition_ancestors(relid)): the relation
    // itself first, then each ancestor (immediate parent first, root last).
    let ancestors: alloc::vec::Vec<Oid> = if check_rel_can_be_partition(relid)? {
        let mut v: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
        v.push(relid);
        v.extend(
            partition_seams::get_partition_ancestors::call(mcx, relid)?
                .iter()
                .copied(),
        );
        v
    } else {
        alloc::vec::Vec::new()
    };

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_partition_ancestors: InitMaterializedSRF establishes fcinfo->resultinfo");

    for relid in ancestors {
        // values[0] = relid (regclass).
        let values = [Datum::from_oid(relid)];
        let nulls = [false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
