//! Partitioning vocabulary shared by the executor partition unit
//! (`executor/execPartition.c`) and the partitioning crates.
//!
//! Mirrors the C definitions, trimmed to the fields the ports so far consume
//! (docs/types.md rule 3):
//!   * `nodes/parsenodes.h`               (`PartitionStrategy`,
//!                                          `PartitionRangeDatumKind`)
//!   * `partitioning/partbounds.h`        (`PartitionBoundInfoData`)
//!   * `utils/partcache.h`                (`PartitionKeyData`)
//!   * `partitioning/partdesc.h`          (`PartitionDescData`)
//!   * `partitioning/partprune.h`         (`PartitionPruneContext`)
//!   * `executor/execPartition.h`         (`PartitionedRelPruningData`,
//!                                          `PartitionPruningData`,
//!                                          `PartitionPruneState`)
//!
//! The `PartitionDispatchData` / `PartitionTupleRouting` structs are private to
//! `execPartition.c`, so they live in the owning crate, not here.

use mcx::{Mcx, MemoryContext, PgBox, PgVec};
use ::types_core::fmgr::FmgrInfo;
use ::types_core::primitive::{AttrNumber, Oid};
use types_tuple::heaptuple::Datum;

use crate::bitmapset::Bitmapset;
use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, Opaque};
use crate::primnodes::Expr;

/* ---------------------------------------------------------------------------
 * parsenodes.h — PartitionStrategy / PartitionRangeDatumKind
 * ------------------------------------------------------------------------- */

/// `PartitionStrategy` (`nodes/parsenodes.h`) — partitioning strategy. C is a
/// `char`-valued enum; values verified against the header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum PartitionStrategy {
    /// `PARTITION_STRATEGY_LIST = 'l'`.
    List = b'l' as i8,
    /// `PARTITION_STRATEGY_RANGE = 'r'`.
    Range = b'r' as i8,
    /// `PARTITION_STRATEGY_HASH = 'h'`.
    Hash = b'h' as i8,
}

/// `PartitionRangeDatumKind` (`nodes/parsenodes.h`) — kind of a range bound
/// datum. Values verified against the header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum PartitionRangeDatumKind {
    /// `PARTITION_RANGE_DATUM_MINVALUE = -1` — less than any other value.
    MinValue = -1,
    /// `PARTITION_RANGE_DATUM_VALUE = 0` — a specific (bounded) value.
    Value = 0,
    /// `PARTITION_RANGE_DATUM_MAXVALUE = 1` — greater than any other value.
    MaxValue = 1,
}

/* ---------------------------------------------------------------------------
 * partbounds.h — PartitionBoundInfoData
 * ------------------------------------------------------------------------- */

/// `PartitionBoundInfoData` (`partitioning/partbounds.h`) — a set of partition
/// bounds. `PartitionBoundInfo` is `Option<PgBox<…>>` (the C nullable pointer).
#[derive(Debug)]
pub struct PartitionBoundInfoData<'mcx> {
    /// `char strategy` — hash, list or range.
    pub strategy: PartitionStrategy,
    /// `int ndatums` — length of the `datums[]` array.
    pub ndatums: i32,
    /// `Datum **datums`.
    pub datums: PgVec<'mcx, PgVec<'mcx, Datum<'mcx>>>,
    /// `PartitionRangeDatumKind **kind` — NULL for hash and list.
    pub kind: Option<PgVec<'mcx, PgVec<'mcx, PartitionRangeDatumKind>>>,
    /// `Bitmapset *interleaved_parts` — interleaved LIST partition indexes.
    pub interleaved_parts: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `int nindexes` — length of the `indexes[]` array.
    pub nindexes: i32,
    /// `int *indexes` — partition indexes.
    pub indexes: PgVec<'mcx, i32>,
    /// `int null_index` — null-accepting partition index; -1 if none.
    pub null_index: i32,
    /// `int default_index` — default partition index; -1 if none.
    pub default_index: i32,
}

/* ---------------------------------------------------------------------------
 * partcache.h — PartitionKeyData
 * ------------------------------------------------------------------------- */

/// `PartitionKeyData` (`utils/partcache.h`) — the partition key of a relation.
/// `PartitionKey` is `Option<PgBox<…>>` (the C nullable pointer).
#[derive(Debug)]
pub struct PartitionKeyData<'mcx> {
    /// `char strategy` — partitioning strategy.
    pub strategy: PartitionStrategy,
    /// `int16 partnatts` — number of columns in the partition key.
    pub partnatts: i16,
    /// `AttrNumber *partattrs` — attribute numbers, or 0 if it's an expr.
    pub partattrs: PgVec<'mcx, AttrNumber>,
    /// `List *partexprs` — expressions in the partitioning key.
    pub partexprs: PgVec<'mcx, Expr<'mcx>>,
    /// `Oid *partopfamily` — OIDs of operator families.
    pub partopfamily: PgVec<'mcx, Oid>,
    /// `Oid *partopcintype` — OIDs of opclass declared input data types.
    pub partopcintype: PgVec<'mcx, Oid>,
    /// `FmgrInfo *partsupfunc` — lookup info for support funcs.
    pub partsupfunc: PgVec<'mcx, FmgrInfo>,
    /// `Oid *partcollation` — partitioning collation per attribute.
    pub partcollation: PgVec<'mcx, Oid>,
    /// `Oid *parttypid` — type OID per attribute.
    pub parttypid: PgVec<'mcx, Oid>,
    /// `int32 *parttypmod` — typmod per attribute.
    pub parttypmod: PgVec<'mcx, i32>,
    /// `int16 *parttyplen` — typlen per attribute.
    pub parttyplen: PgVec<'mcx, i16>,
    /// `bool *parttypbyval` — typbyval per attribute.
    pub parttypbyval: PgVec<'mcx, bool>,
    /// `char *parttypalign` — typalign per attribute.
    pub parttypalign: PgVec<'mcx, i8>,
    /// `Oid *parttypcoll` — type collation per attribute.
    pub parttypcoll: PgVec<'mcx, Oid>,
}

/* ---------------------------------------------------------------------------
 * partdesc.h — PartitionDescData
 * ------------------------------------------------------------------------- */

/// `PartitionDescData` (`partitioning/partdesc.h`) — info about partitions of a
/// partitioned table. `PartitionDesc` is `Option<PgBox<…>>`.
#[derive(Debug)]
pub struct PartitionDescData<'mcx> {
    /// `int nparts` — number of partitions.
    pub nparts: i32,
    /// `bool detached_exist` — are there any detached partitions?
    pub detached_exist: bool,
    /// `Oid *oids` — `nparts` partition OIDs in order of their bounds.
    pub oids: PgVec<'mcx, Oid>,
    /// `bool *is_leaf` — per-partition leaf flag.
    pub is_leaf: PgVec<'mcx, bool>,
    /// `PartitionBoundInfo boundinfo` — collection of partition bounds.
    pub boundinfo: Option<PgBox<'mcx, PartitionBoundInfoData<'mcx>>>,
    /// `int last_found_datum_index` — boundinfo datum index of last found
    /// partition, or -1.
    pub last_found_datum_index: i32,
    /// `int last_found_part_index` — partition index of last found, or -1.
    pub last_found_part_index: i32,
    /// `int last_found_count` — run-length of consecutive last-found matches.
    pub last_found_count: i32,
}

/* ---------------------------------------------------------------------------
 * partprune.h — PartitionPruneContext
 * ------------------------------------------------------------------------- */

/// `PartitionPruneContext` (`partitioning/partprune.h`) — runtime pruning
/// context for a single partitioned table.
#[derive(Debug)]
pub struct PartitionPruneContext<'mcx> {
    /// `char strategy` — LIST/RANGE/HASH.
    pub strategy: PartitionStrategy,
    /// `int partnatts` — number of partition key columns.
    pub partnatts: i32,
    /// `int nparts` — number of partitions in this table.
    pub nparts: i32,
    /// `PartitionBoundInfo boundinfo` — partition boundary info. C aliases
    /// `partdesc->boundinfo`; in the owned model the boundinfo is moved out of
    /// the (freshly looked-up, owned) `PartitionDirectoryLookup` result into the
    /// context.
    pub boundinfo: PartitionBoundInfo<'mcx>,
    /// `Oid *partcollation` — collations of the partition key columns.
    pub partcollation: PgVec<'mcx, Oid>,
    /// `FmgrInfo *partsupfunc` — comparison/hash support funcs (aliased from
    /// the relcache partition key; carried as the partcache-owned handle).
    pub partsupfunc: Opaque,
    /// `FmgrInfo *stepcmpfuncs` — per-step, per-key comparison/hash funcs,
    /// lazily looked up (palloc0'd here).
    pub stepcmpfuncs: PgVec<'mcx, FmgrInfo>,
    /// `MemoryContext ppccontext` — context holding subsidiary data.
    pub ppccontext: MemoryContext,
    /// `PlanState *planstate` — parent plan node's PlanState during execution;
    /// NULL in planner. Carried as the executor-owned handle.
    pub planstate: Opaque,
    /// `ExprContext *exprcontext` — context for evaluating pruning exprs (id
    /// into the EState pool).
    pub exprcontext: Option<EcxtId>,
    /// `ExprState **exprstates` — per-step, per-key compiled pruning exprs;
    /// a `None` element is the C `NULL` (Const / not-needed slot).
    pub exprstates: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
}

/* ---------------------------------------------------------------------------
 * execPartition.h — runtime pruning state
 * ------------------------------------------------------------------------- */

/// `PartitionedRelPruningData` (`executor/execPartition.h`) — per-partitioned-
/// table run-time pruning data.
#[derive(Debug)]
pub struct PartitionedRelPruningData<'mcx> {
    /// `Relation partrel` — partitioned table (alias of the EState-owned open).
    pub partrel: Option<rel::Relation<'mcx>>,
    /// `int nparts` — length of the `subplan_map[]`/`subpart_map[]` arrays.
    pub nparts: i32,
    /// `int *subplan_map` — subplan index by partition index, or -1.
    pub subplan_map: PgVec<'mcx, i32>,
    /// `int *subpart_map` — subpart index by partition index, or -1.
    pub subpart_map: PgVec<'mcx, i32>,
    /// `int *leafpart_rti_map` — RT index by partition index, or 0.
    pub leafpart_rti_map: PgVec<'mcx, i32>,
    /// `Bitmapset *present_parts` — partition indexes with subplans/subparts.
    pub present_parts: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *initial_pruning_steps` — startup pruning steps
    /// (`PartitionPruneStep` nodes; aliased from the plan, carried opaque).
    pub initial_pruning_steps: Opaque,
    /// `List *exec_pruning_steps` — per-scan pruning steps.
    pub exec_pruning_steps: Opaque,
    /// `PartitionPruneContext initial_context` — context for the initial steps.
    pub initial_context: PartitionPruneContext<'mcx>,
    /// `PartitionPruneContext exec_context` — context for the per-scan steps.
    pub exec_context: PartitionPruneContext<'mcx>,
}

/// `PartitionPruningData` (`executor/execPartition.h`) — run-time pruning info
/// for one partitioning hierarchy.
#[derive(Debug)]
pub struct PartitionPruningData<'mcx> {
    /// `int num_partrelprunedata` — number of array entries.
    pub num_partrelprunedata: i32,
    /// `PartitionedRelPruningData partrelprunedata[FLEXIBLE_ARRAY_MEMBER]`.
    pub partrelprunedata: PgVec<'mcx, PartitionedRelPruningData<'mcx>>,
}

/// `PartitionPruneState` (`executor/execPartition.h`) — state object for
/// run-time partition pruning.
#[derive(Debug)]
pub struct PartitionPruneState<'mcx> {
    /// `ExprContext *econtext` — standalone context to evaluate pruning step
    /// expressions (id into the EState pool).
    pub econtext: Option<EcxtId>,
    /// `Bitmapset *execparamids` — PARAM_EXEC param IDs within the
    /// partprunedata structs.
    pub execparamids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *other_subplans` — subplan indexes not in any partprunedata.
    pub other_subplans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `MemoryContext prune_context` — short-lived context for the pruning
    /// functions.
    pub prune_context: MemoryContext,
    /// `bool do_initial_prune` — prune during executor startup?
    pub do_initial_prune: bool,
    /// `bool do_exec_prune` — prune during executor run?
    pub do_exec_prune: bool,
    /// `int num_partprunedata` — number of items in the `partprunedata` array.
    pub num_partprunedata: i32,
    /// `PartitionPruningData *partprunedata[FLEXIBLE_ARRAY_MEMBER]`.
    pub partprunedata: PgVec<'mcx, Option<PgBox<'mcx, PartitionPruningData<'mcx>>>>,
}

impl PartitionStrategy {
    /// Construct from the on-disk `char` value (`pg_partitioned_table.partstrat`
    /// / relcache `PartitionKey.strategy`). Panics on an unknown value, which is
    /// the C `elog(ERROR, "unexpected partition strategy")` caller bug.
    pub fn from_char(c: i8) -> Self {
        match c as u8 {
            b'l' => PartitionStrategy::List,
            b'r' => PartitionStrategy::Range,
            b'h' => PartitionStrategy::Hash,
            other => panic!("unexpected partition strategy: {}", other as char),
        }
    }
}

impl<'mcx> PartitionBoundInfoData<'mcx> {
    /// Deep copy into `mcx` (C: the `partition_bounds_copy` deep clone, here a
    /// lifetime-reprojection: by-reference bound datums carry their byte image
    /// in the `Datum` enum and are `datumCopy`'d via [`Datum::clone_in`], so no
    /// `typbyval`/`typlen` is needed). Used by partdesc to materialize the
    /// relcache descriptor into the `PartitionDirectory`'s own context and to
    /// re-project it back into a caller's context on lookup.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> types_error::PgResult<PartitionBoundInfoData<'b>> {
        // datums[][]
        let mut datums: PgVec<'b, PgVec<'b, Datum<'b>>> =
            ::mcx::vec_with_capacity_in(mcx, self.datums.len())?;
        for row in self.datums.iter() {
            let mut nrow: PgVec<'b, Datum<'b>> = ::mcx::vec_with_capacity_in(mcx, row.len())?;
            for d in row.iter() {
                nrow.push(d.clone_in(mcx)?);
            }
            datums.push(nrow);
        }
        // kind[][]
        let kind = match &self.kind {
            None => None,
            Some(rows) => {
                let mut nkind: PgVec<'b, PgVec<'b, PartitionRangeDatumKind>> =
                    ::mcx::vec_with_capacity_in(mcx, rows.len())?;
                for row in rows.iter() {
                    let mut nrow: PgVec<'b, PartitionRangeDatumKind> =
                        ::mcx::vec_with_capacity_in(mcx, row.len())?;
                    for k in row.iter() {
                        nrow.push(*k);
                    }
                    nkind.push(nrow);
                }
                Some(nkind)
            }
        };
        // interleaved_parts
        let interleaved_parts = match &self.interleaved_parts {
            None => None,
            Some(bms) => Some(::mcx::alloc_in(mcx, bms.clone_in(mcx)?)?),
        };
        // indexes[]
        let mut indexes: PgVec<'b, i32> = ::mcx::vec_with_capacity_in(mcx, self.indexes.len())?;
        for v in self.indexes.iter() {
            indexes.push(*v);
        }
        Ok(PartitionBoundInfoData {
            strategy: self.strategy,
            ndatums: self.ndatums,
            datums,
            kind,
            interleaved_parts,
            nindexes: self.nindexes,
            indexes,
            null_index: self.null_index,
            default_index: self.default_index,
        })
    }
}

impl<'mcx> PartitionDescData<'mcx> {
    /// Deep copy into `mcx`. The owned-tree analogue of the C
    /// `partdesc`-into-`new_pdcxt` materialization: the `PartitionDirectory`
    /// stores a clone in its own context, and a lookup re-projects that clone
    /// into the caller's context. `oids`/`is_leaf` are plain scalars; the
    /// `boundinfo` is deep-copied via [`PartitionBoundInfoData::clone_in`].
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> types_error::PgResult<PartitionDescData<'b>> {
        let mut oids: PgVec<'b, Oid> = ::mcx::vec_with_capacity_in(mcx, self.oids.len())?;
        for v in self.oids.iter() {
            oids.push(*v);
        }
        let mut is_leaf: PgVec<'b, bool> = ::mcx::vec_with_capacity_in(mcx, self.is_leaf.len())?;
        for v in self.is_leaf.iter() {
            is_leaf.push(*v);
        }
        let boundinfo = match &self.boundinfo {
            None => None,
            Some(bi) => Some(::mcx::alloc_in(mcx, bi.clone_in(mcx)?)?),
        };
        Ok(PartitionDescData {
            nparts: self.nparts,
            detached_exist: self.detached_exist,
            oids,
            is_leaf,
            boundinfo,
            last_found_datum_index: self.last_found_datum_index,
            last_found_part_index: self.last_found_part_index,
            last_found_count: self.last_found_count,
        })
    }
}

/// `PartitionBoundInfo` — owned alias (`partdefs.h`).
pub type PartitionBoundInfo<'mcx> = Option<PgBox<'mcx, PartitionBoundInfoData<'mcx>>>;
/// `PartitionKey` — owned alias (`partdefs.h`).
pub type PartitionKey<'mcx> = Option<PgBox<'mcx, PartitionKeyData<'mcx>>>;
/// `PartitionDesc` — owned alias (`partdefs.h`).
pub type PartitionDesc<'mcx> = Option<PgBox<'mcx, PartitionDescData<'mcx>>>;

/// Silence unused-import lint until the body phase consumes `Mcx` in helpers.
#[allow(dead_code)]
fn _mcx_anchor(_m: Mcx<'_>) {}
