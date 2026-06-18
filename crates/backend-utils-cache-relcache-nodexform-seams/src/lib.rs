//! Seam declarations for the relcache derived caches whose payload is a raw
//! `rd_indextuple` node-tree transform, owned cross-unit (the node / optimizer /
//! publication / rewrite layers).
//!
//! `RelationGetIndexExpressions` / `RelationGetIndexPredicate` /
//! `RelationGetDummyIndexExpressions` / `RelationGetIndexAttrBitmap` /
//! `RelationBuildPublicationDesc` / `RelationBuildRuleLock` (relcache.c) each
//! read the *raw* `pg_index` HeapTuple (`rd_indextuple` — the un-decoded
//! `indexprs`/`indpred` text datums) and run it through node vocabulary
//! (`stringToNode`, `eval_const_expressions`, `canonicalize_qual`,
//! `make_ands_implicit`, `fix_opfuncids`, `makeConst`/`exprType`/`exprTypmod`/
//! `exprCollation`, `pull_varattnos`) or the publication / rewrite owners. The
//! owned relcache entry carries only the *decoded* `rd_index` form, not the raw
//! tuple, so the whole transform is a genuine cross-unit boundary keyed by the
//! relation's OID. Each owner installs its seam from `init_seams()` when it
//! lands; until then a call panics loudly.
//!
//! The relcache caller resolves the result into its owned entry fields
//! (`rd_indexprs`/`rd_indpred`/`rd_*attr`/`rd_pubdesc`/`rd_rules` presence). The
//! returns are deliberately coarse — the built node trees live on the relcache
//! entry behind the seam, so the consumer only needs the attribute
//! contributions (for the bitmap build) and presence acknowledgements.

use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;

/// One index's attribute contributions for `RelationGetIndexAttrBitmap`,
/// produced by the owner's `index_open` (relation/indexam) + `pull_varattnos`
/// (var) over the index's `indexprs`/`indpred` node trees.
#[derive(Clone, Debug)]
pub struct IndexAttrInfo {
    pub indisunique: bool,
    pub indnkeyatts: i16,
    pub amsummarizing: bool,
    pub has_expressions: bool,
    pub has_predicate: bool,
    /// `rd_index->indkey.values[0..indnatts]` (raw table column numbers).
    pub indkey: Vec<AttrNumber>,
    /// Offset members pulled from the index expressions.
    pub expr_attrs: Vec<i32>,
    /// Offset members pulled from the index predicate.
    pub pred_attrs: Vec<i32>,
}

seam_core::seam!(
    /// `RelationGetIndexExpressions(relation)` (relcache.c): `stringToNode` of
    /// the raw `pg_index.indexprs`, `eval_const_expressions`, `fix_opfuncids`,
    /// then cache the tree into `rd_indexprs` (node + clauses owners).
    /// Identified by the index relation's OID. Returns once the tree is cached
    /// on the entry; can `ereport(ERROR)`, carried on `Err`.
    pub fn index_expressions(index_relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationGetIndexPredicate(relation)` (relcache.c): `stringToNode` of the
    /// raw `pg_index.indpred`, `eval_const_expressions`, `canonicalize_qual`,
    /// `make_ands_implicit`, `fix_opfuncids`, then cache into `rd_indpred`
    /// (node + clauses owners). Can `ereport(ERROR)`, carried on `Err`.
    pub fn index_predicate(index_relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationGetDummyIndexExpressions(relation)` (relcache.c): read the raw
    /// `pg_index.indexprs` datum (`heap_getattr` over `GetPgIndexDescriptor`),
    /// `stringToNode` the expression list, then per sub-tree
    /// `makeConst(exprType, exprTypmod, exprCollation, 1, 0, true, true)`
    /// (makefuncs + nodeFuncs owners). The built dummy-Const list lives on the
    /// entry behind the seam; the consumer only needs the acknowledgement. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn dummy_index_expressions(index_relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationGetIndexAttrBitmap`'s per-index step (relcache.c):
    /// `index_open(indexOid, AccessShareLock)` (indexam) + extract
    /// indkey / `indisunique` / `indnkeyatts` / `amsummarizing` +
    /// `pull_varattnos` (var) over the index's `indexprs`/`indpred`, then
    /// `index_close`. Returns the one index's attribute contributions. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn open_index_attrs(index_oid: Oid) -> PgResult<IndexAttrInfo>
);

seam_core::seam!(
    /// `RelationBuildPublicationDesc(relation)` (relcache.c): traverse
    /// `pg_publication*` to build `rd_pubdesc` (publication owner). The built
    /// descriptor lives on the entry behind the seam; the consumer only needs
    /// the acknowledgement. Can `ereport(ERROR)`, carried on `Err`.
    pub fn publication_desc(relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationBuildPublicationDesc(rel, &pubdesc)` (relcache.c): traverse
    /// `pg_publication*` to build the relation's `rd_pubdesc` and hand it back
    /// by value (the C builds it on the relcache entry and the caller reads its
    /// fields directly). Unlike [`publication_desc`] (which only acknowledges
    /// the cache build), the apply-side executor `CheckCmdReplicaIdentity`
    /// needs the descriptor's row-filter / column-list / generated-column
    /// validity flags. The publication-catalog traversal (`pg_publication.c`'s
    /// validity computation) is the unported owner's; this panics until it
    /// lands. Can `ereport(ERROR)`, carried on `Err`.
    ///
    /// The `mcx` (the caller's `CurrentMemoryContext`) is threaded through
    /// because the publication-catalog traversal and the two REPLICA-IDENTITY
    /// validity checks (`pub_rf_contains_invalid_column` /
    /// `pub_contains_invalid_column`) allocate scan buffers / node trees and
    /// read the `'mcx`-bound relation, exactly as the C runs in
    /// `CurrentMemoryContext`.
    pub fn relation_build_publication_desc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> PgResult<types_catalog::pg_publication::PublicationDesc>
);

seam_core::seam!(
    /// `get_attoptions(relid, attnum)` (lsyscache.c, via
    /// `SearchSysCache2(ATTNUM)` + `SysCacheGetAttr(attoptions)`): the raw
    /// `pg_attribute.attoptions` reloptions text array for one index column, or
    /// `None` (the C `(Datum) 0`) when unset. The relcache caller passes the
    /// returned bytes straight to [`index_opclass_options`]. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn get_attoptions(relid: Oid, attnum: AttrNumber) -> PgResult<Option<Vec<u8>>>
);

// NOTE: the divergent relcache-owned `index_opclass_options(index_oid, attnum,
// attoptions) -> Option<Vec<u8>>` seam was RETIRED: the relcache build now drives
// the canonical `backend-access-index-indexam::index_opclass_options(indrel,
// attnum, attoptions: Datum, validate)` contract directly (the opclass-options
// force runs AFTER cache_insert, so the index entry is cache-resident and the
// canonical OID-resolving seam terminates with normal short borrows — no bridge,
// no recursion). See `derived::force_index_att_options`.

// NOTE: the old `rule_lock(relid) -> ()` acknowledgement seam was RETIRED by the
// full-Query cache-ownership keystone. `RelationBuildRuleLock` now builds the
// real value-typed `rd_rules` (RuleLock/RewriteRule with whole `Query<'static>`
// action trees) in-crate, allocating into the process-lifetime
// `cache_memory_context()` arena and `stringToNode`-ing via the read.c seam +
// the `pg_rewrite` scan via the genam `relcache_scan_pg_rewrite` seam. No
// acknowledgement seam is needed.
