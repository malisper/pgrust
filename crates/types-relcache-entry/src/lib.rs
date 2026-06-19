//! The real, mutable relcache entry: `struct RelationData` (`utils/rel.h`).
//!
//! This crate holds ONLY the owned relcache entry-store type family, relocated
//! out of `backend-utils-cache-relcache` so the relcache **seams** crate
//! (`backend-utils-cache-relcache-seams`) can name `RelationData` in a
//! cross-crate `Rc<RefCell<RelationData>>` seam without a `types-rel` cycle.
//! It deps only the vocabulary the entry embeds (`types-core`/`types-storage`/
//! `types-tableam`/`types-reloptions`) — never `types-rel`.
//!
//! This is the OWN entry-store type — NOT the trimmed cross-unit value-slice
//! `types_rel::RelationData`. It carries the full `rd_*` surface that the C
//! `RelationData` does, field-for-field. Because the `RelationIdCache` owns
//! each descriptor for the whole backend lifetime (the C `CacheMemoryContext`
//! lifetime), the entry stores **owned, lifetime-free** mirrors of the
//! catalog payloads (`String`/`Vec`/owned scalars). The lifetime-bearing
//! cross-unit slice types are only materialized at projection time by the
//! build/derived families; they are never stored on the entry.
//!
//! `Default` produces the all-zero entry the C `AllocateRelationDesc`
//! `palloc0`s before filling; the build family fills it.

use types_core::primitive::{AttrNumber, Oid, ProcNumber, RegProcedure};
use types_core::xact::SubTransactionId;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_storage::lock::LockRelId;
use types_storage::RelFileLocator;

/// `IndexAttrBitmapKind` (`utils/relcache.h`) — which attribute bitmap
/// `RelationGetIndexAttrBitmap` should return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    Keys,
    PrimaryKey,
    Identity,
    HotBlocking,
    Summarized,
}

/// `LockInfoData` (`utils/rel.h`) — the lock-manager info embedded in a
/// relcache entry (`rd_lockInfo`). Just the `LockRelId`.
#[derive(Clone, Debug, Default)]
pub struct LockInfoData {
    /// `LockRelId lockRelId` — `(relId, dbId)` of the relation.
    pub lockRelId: LockRelId,
}

/// `FormData_pg_class` (`catalog/pg_class.h`) — the `rd_rel` payload, owned by
/// the entry (lifetime-free mirror of [`types_rel::FormData_pg_class`]).
#[derive(Clone, Debug, Default)]
pub struct FormPgClass {
    /// `NameData relname`.
    pub relname: String,
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: i8,
    pub relkind: i8,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: i8,
    pub relispartition: bool,
    pub relrewrite: Oid,
    pub relfrozenxid: u32,
    pub relminmxid: u32,
}

/// `FormData_pg_index` (`catalog/pg_index.h`) — the `rd_index` payload, owned
/// by the entry (lifetime-free mirror of [`types_rel::FormData_pg_index`]).
#[derive(Clone, Debug, Default)]
pub struct FormPgIndex {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    /// `int2vector indkey` — table column numbers of the index columns.
    pub indkey: Vec<AttrNumber>,
}

/// An owned `TupleDesc` mirror for the entry (`rd_att`). The lifetime-bearing
/// [`types_tuple::heaptuple::TupleDescData`] is materialized at projection
/// time; the entry stores the owned attribute rows. (Build family fills this.)
#[derive(Clone, Debug, Default)]
pub struct OwnedTupleDesc {
    /// `natts` — number of attributes.
    pub natts: i32,
    /// `tdtypeid` — composite type OID.
    pub tdtypeid: Oid,
    /// `tdtypmod` — composite typmod.
    pub tdtypmod: i32,
    /// The attribute rows, in owned form (filled by the build family).
    pub attrs: Vec<OwnedAttr>,
    /// `TupleConstr *constr` — column defaults/check constraints/not-null
    /// accounting; `None` is the C NULL (no constraints). Filled by
    /// `AttrDefaultFetch`/`CheckNNConstraintFetch` (build family).
    pub constr: Option<OwnedTupleConstr>,
}

impl OwnedTupleDesc {
    /// `natts` (`access/tupdesc.h` `TupleDesc->natts`) — number of attributes.
    pub fn natts(&self) -> i32 {
        self.natts
    }

    /// `TupleDescAttr(rd_att, i)` (`access/tupdesc.h`) over the OWNED attribute
    /// rows — the `i`-th [`OwnedAttr`] (0-based). The entry stores attributes in
    /// owned form; this is the owned-side analog of
    /// [`types_tuple::heaptuple::TupleDescData::attr`].
    pub fn attr(&self, i: usize) -> &OwnedAttr {
        &self.attrs[i]
    }

    /// `rd_att->constr` (`access/tupdesc.h`) — the owned `TupleConstr`, or `None`
    /// (the C NULL: no defaults/check-constraints/not-null accounting).
    pub fn constr(&self) -> Option<&OwnedTupleConstr> {
        self.constr.as_ref()
    }

    /// Materialize the owned tuple descriptor into the cross-unit borrowed
    /// [`types_tuple::heaptuple::TupleDescData`], allocated in `mcx`. This is the
    /// owned->borrowed projection the relcache build family performed for
    /// `rd_att` (`CreateTupleDescCopyConstr(RelationGetDescr(rel))`-shaped): the
    /// full `Form_pg_attribute[]` is built from the owned rows, fed through
    /// `CreateTupleDesc` (which populates the parallel `compact_attrs`), then the
    /// composite type id/typmod/refcount are stamped.
    ///
    /// `relid` is the relation's OID, used to fill each `Form_pg_attribute`'s
    /// `attrelid` (which `populate_compact_attribute` reads via
    /// `IsCatalogRelationOid` to decide the not-null validity of a catalog
    /// column). The C `TupleDescAttr(rd_att, i)->attrelid` carries it; the owned
    /// `OwnedAttr` mirror does not store `attrelid` (it is identical for every
    /// row), so the caller passes the relation OID.
    ///
    /// `rel.borrow().rd_att.project_in(mcx, rel_oid)?` is the call shape
    /// consumers will use once the Deref flip lands.
    pub fn project_in<'mcx>(
        &self,
        mcx: mcx::Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
        let attrs = self.build_form_attrs(relid);
        let mut td = backend_access_common_tupdesc::CreateTupleDesc(mcx, &attrs)?;
        // `CreateTupleDesc` re-derives each `CompactAttribute.attnullability`
        // from `attnotnull` alone (`populate_compact_attribute`), which yields
        // `ATTNULLABLE_UNKNOWN` for a NOT-NULL column on a non-catalog relation.
        // The relcache build (`RelationBuildTupleDesc` / `CheckNNConstraintFetch`)
        // has already resolved that to its final value on the owned `OwnedAttr`
        // rows (UNKNOWN -> VALID for any not-null constraint not marked invalid).
        // Carry that resolved nullability into the projected descriptor so the
        // planner (`get_relation_info`) sees the same per-attr nullability the C
        // relcache stamps onto `rd_att`'s CompactAttribute array.
        for (i, a) in self.attrs.iter().enumerate() {
            td.compact_attrs[i].attnullability = a.attnullability;
        }
        // `CreateTupleDesc` leaves `td.constr = NULL`; carry the owned
        // `rd_att->constr` (defval/check/missing + accounting) into the projected
        // descriptor so consumers (`build_column_default` via
        // `TupleDescGetDefault`, ExecConstraints, the missing-attr deform) see the
        // same `rd_att->constr` the C relcache stamps. This is the owned->borrowed
        // half of the C `CreateTupleDescCopyConstr`.
        if let Some(oc) = self.constr.as_ref() {
            td.constr = Some(oc.project_in(mcx)?);
        }
        td.tdtypeid = self.tdtypeid;
        td.tdtypmod = self.tdtypmod;
        td.tdrefcount = 1;
        mcx::alloc_in(mcx, td)
    }

    /// Build the full `Form_pg_attribute[]` array from the entry's owned
    /// attribute rows, for the tuple-descriptor materialization. The entry
    /// carries the trimmed [`OwnedAttr`] subset; the remaining
    /// `Form_pg_attribute` fields are `Default` (they are not consumed across the
    /// relcache seam). `relid` is copied into each row's `attrelid`.
    pub fn build_form_attrs(
        &self,
        relid: Oid,
    ) -> Vec<types_tuple::heaptuple::FormData_pg_attribute> {
        self.attrs
            .iter()
            .map(|a| types_tuple::heaptuple::FormData_pg_attribute {
                attrelid: relid,
                attname: name_data(&a.attname),
                atttypid: a.atttypid,
                attlen: a.attlen,
                attnum: a.attnum,
                atttypmod: a.atttypmod,
                attbyval: a.attbyval,
                attalign: a.attalign,
                attstorage: a.attstorage,
                attcompression: a.attcompression,
                attnotnull: a.attnotnull,
                atthasdef: a.atthasdef,
                attidentity: a.attidentity,
                attgenerated: a.attgenerated,
                attisdropped: a.attisdropped,
                attcollation: a.attcollation,
                ..types_tuple::heaptuple::FormData_pg_attribute::default()
            })
            .collect()
    }
}

/// `namestrcpy` into a fixed `NameData` (NUL-padded, truncated to NAMEDATALEN).
/// Mirrors the build family's `name_data` helper.
fn name_data(s: &str) -> types_tuple::heaptuple::NameData {
    let mut nd = types_tuple::heaptuple::NameData::default();
    let bytes = s.as_bytes();
    let n = bytes.len().min(nd.data.len() - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// `struct TupleConstr` (`access/tupdesc.h`) — the owned mirror of `rd_att->
/// constr`. Carries the default-expression and check-constraint arrays plus the
/// has_*/num_* accounting the build family fills.
#[derive(Clone, Debug, Default)]
pub struct OwnedTupleConstr {
    /// `AttrDefault *defval` (+ `uint16 num_defval`) — per-column default
    /// expressions, owned cstring node-tree text keyed by attnum, sorted by
    /// adnum. Length is `num_defval`.
    pub defval: Vec<OwnedAttrDefault>,
    /// `ConstrCheck *check` (+ `uint16 num_check`) — check constraints, sorted
    /// by name. Length is `num_check`.
    pub check: Vec<OwnedConstrCheck>,
    /// `AttrMissing *missing` — per-column "missing" values (the `attmissingval`
    /// defaults from fast-default `ALTER TABLE ... ADD COLUMN ... DEFAULT`),
    /// indexed by attnum-1. Empty is the C NULL `constr->missing` (no column has
    /// a missing value); when non-empty it is `relnatts` long, with each
    /// [`OwnedAttrMissing`] present-or-not, exactly as
    /// `RelationBuildTupleDesc` builds `attrmiss[]`. Lifetime-free (the value
    /// image, not a `'mcx`-bound `Datum`), re-materialized at projection time.
    pub missing: Vec<OwnedAttrMissing>,
    /// `bool has_not_null`.
    pub has_not_null: bool,
    /// `bool has_generated_stored`.
    pub has_generated_stored: bool,
    /// `bool has_generated_virtual`.
    pub has_generated_virtual: bool,
}

impl OwnedTupleConstr {
    /// Project the owned `rd_att->constr` into the borrowed cross-unit
    /// [`types_tuple::heaptuple::TupleConstr`] allocated in `mcx`. The
    /// owned->borrowed half of the C `CreateTupleDescCopyConstr`: each
    /// `OwnedAttrDefault.adbin` cstring and `OwnedConstrCheck.ccbin`/`ccname`
    /// becomes a `PgString`; each `OwnedAttrMissing.am_value` image is
    /// re-materialized into a fresh `Datum<'mcx>`; the `num_*`/`has_*` accounting
    /// is carried verbatim.
    pub fn project_in<'mcx>(
        &self,
        mcx: mcx::Mcx<'mcx>,
    ) -> PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleConstr<'mcx>>> {
        use types_tuple::backend_access_common_heaptuple::Datum;
        use types_tuple::heaptuple::{AttrDefault, AttrMissing, ConstrCheck};

        let mut defval = mcx::vec_with_capacity_in(mcx, self.defval.len())?;
        for d in &self.defval {
            defval.push(AttrDefault {
                adnum: d.adnum,
                adbin: Some(mcx::PgString::from_str_in(&d.adbin, mcx)?),
            });
        }

        let mut check = mcx::vec_with_capacity_in(mcx, self.check.len())?;
        for c in &self.check {
            check.push(ConstrCheck {
                ccname: Some(mcx::PgString::from_str_in(&c.ccname, mcx)?),
                ccbin: Some(mcx::PgString::from_str_in(&c.ccbin, mcx)?),
                ccenforced: c.ccenforced,
                ccvalid: c.ccvalid,
                ccnoinherit: c.ccnoinherit,
            });
        }

        let mut missing = mcx::vec_with_capacity_in(mcx, self.missing.len())?;
        for m in &self.missing {
            let am_value = match (m.am_present, m.am_value.as_ref()) {
                (true, Some(img)) => img.to_datum(mcx)?,
                _ => Datum::null(),
            };
            missing.push(AttrMissing {
                am_present: m.am_present,
                am_value,
            });
        }

        let constr = types_tuple::heaptuple::TupleConstr {
            num_defval: defval.len() as u16,
            num_check: check.len() as u16,
            defval,
            check,
            missing,
            has_not_null: self.has_not_null,
            has_generated_stored: self.has_generated_stored,
            has_generated_virtual: self.has_generated_virtual,
        };
        mcx::alloc_in(mcx, constr)
    }
}

/// `struct AttrDefault` (`access/tupdesc.h`) — one column default.
#[derive(Clone, Debug, Default)]
pub struct OwnedAttrDefault {
    /// `AttrNumber adnum`.
    pub adnum: AttrNumber,
    /// `char *adbin` — the serialized default-expression node tree (cstring).
    pub adbin: String,
}

/// `struct ConstrCheck` (`access/tupdesc.h`) — one check constraint.
#[derive(Clone, Debug, Default)]
pub struct OwnedConstrCheck {
    /// `char *ccname`.
    pub ccname: String,
    /// `char *ccbin` — the serialized check-expression node tree (cstring).
    pub ccbin: String,
    /// `bool ccenforced`.
    pub ccenforced: bool,
    /// `bool ccvalid`.
    pub ccvalid: bool,
    /// `bool ccnoinherit`.
    pub ccnoinherit: bool,
}

/// `struct AttrMissing` (`access/tupdesc.h`) — one column's fast-default
/// "missing" value, the owned (lifetime-free) mirror of
/// [`types_tuple::heaptuple::AttrMissing`]. C stores `am_value` as a `Datum`;
/// the owned entry keeps the value's lifetime-free image (or `None` when
/// `am_present` is false), re-materialized into a `Datum<'mcx>` when the entry's
/// tuple descriptor is projected (`project_in`).
#[derive(Clone, Debug, Default)]
pub struct OwnedAttrMissing {
    /// `bool am_present`.
    pub am_present: bool,
    /// `Datum am_value` — the value's lifetime-free image, or `None` when not
    /// present.
    pub am_value: Option<types_tuple::heaptuple::MissingValueImage>,
}

/// The genbki bootstrap schema for one nailed catalog: the hardcoded
/// `Schema_pg_*[]` `FormData_pg_attribute` rows `formrdesc` consumes, plus the
/// catalog relation OID (`FormData_pg_attribute.attrelid`).
///
/// The C `formrdesc(name, reltype, isshared, natts, attrs)` reads
/// `attrs[0]->attrelid` to set `rd_id`; the [`OwnedAttr`] mirror drops
/// `attrelid` (it is identical for every row), so the bootstrap data owner
/// carries it alongside the row vector. This is the carrier the
/// `catalog_schema_attrs` seam hands back.
#[derive(Clone, Debug, Default)]
pub struct BootstrapCatalogSchema {
    /// `FormData_pg_attribute.attrelid` — the catalog relation OID, used for
    /// `rd_id` in `formrdesc` (which the `OwnedAttr` rows cannot carry).
    pub relid: Oid,
    /// The `Schema_pg_*[]` attribute rows.
    pub attrs: Vec<OwnedAttr>,
}

/// One `FormData_pg_attribute` row of [`OwnedTupleDesc`] (owned mirror).
#[derive(Clone, Debug, Default)]
pub struct OwnedAttr {
    pub attname: String,
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: AttrNumber,
    pub atttypmod: i32,
    pub attbyval: bool,
    pub attalign: i8,
    /// `char attstorage` — one of the `TYPSTORAGE_*` constants (`'p'`/`'e'`/
    /// `'m'`/`'x'`). Read by the TOAST machinery (`toast_tuple_init` /
    /// `toast_tuple_find_biggest_attribute`) to decide whether a varlena column
    /// may be compressed/externalized.
    pub attstorage: i8,
    /// `char attcompression` — the per-column compression method
    /// (`InvalidCompressionMethod` `'\0'`, `'p'` pglz, `'l'` lz4). Read by
    /// `toast_tuple_init` to seed `tai_compression`.
    pub attcompression: i8,
    pub attnotnull: bool,
    /// `bool atthasdef` — this column has a default expression in
    /// `pg_attrdef` (and thus an entry in `rd_att->constr->defval`). Read by
    /// `build_column_default` to decide whether to fetch the column default.
    pub atthasdef: bool,
    /// `char attidentity` — one of the `ATTRIBUTE_IDENTITY_*` constants, or
    /// `'\0'`. Copied from the source descriptor by `RelationBuildLocalRelation`.
    pub attidentity: i8,
    /// `char attgenerated` — one of the `ATTRIBUTE_GENERATED_*` constants, or
    /// `'\0'`. Copied from the source descriptor by `RelationBuildLocalRelation`.
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attcollation: Oid,
    /// `CompactAttribute.attnullability` (`access/tupdesc.h`): one of
    /// `ATTNULLABLE_UNRESTRICTED`/`_UNKNOWN`/`_VALID`/`_INVALID`. The not-null
    /// validity state `CheckNNConstraintFetch` maintains.
    pub attnullability: i8,
}

/// `RewriteRule` (`rewrite/prs2lock.h`) — one rewrite rule, the unit the
/// rule-application core of `rewriteHandler.c` reads off `rd_rules->rules[i]`.
///
/// The full-Query cache-ownership keystone: `actions` is the C
/// `List *actions` — a list of whole, post-analysis `Query` trees loaded by
/// `RelationBuildRuleLock` via `stringToNode(ev_action)`. Those trees outlive
/// any single query's `'mcx` arena (they are cached for the relcache entry's —
/// i.e. the backend's — lifetime, exactly as C copies them into
/// `CacheMemoryContext`), so they are carried as the lifetime-free
/// [`types_nodes::copy_query::Query`]`<'static>` allocated in the process-lifetime
/// CacheMemoryContext arena (see `backend-utils-cache-relcache`'s
/// `cache_memory_context()`). This is the faithful rendering of the C
/// `RewriteRule` whose `qual`/`actions` pointers live in `CacheMemoryContext`,
/// not an invented handle/registry.
pub struct RewriteRule {
    /// `Oid ruleId` — the `pg_rewrite` OID.
    pub ruleId: Oid,
    /// `CmdType event` — `CMD_SELECT`/`UPDATE`/`INSERT`/`DELETE` the rule fires on.
    pub event: types_nodes::nodes::CmdType,
    /// `bool enabled` — actually `char ev_enabled` in `pg_rewrite`
    /// (`'O'`/`'D'`/`'R'`/`'A'`); kept as the raw char the relcache stores and
    /// the executor's `check_enable_rule` compares.
    pub enabled: u8,
    /// `bool isInstead` — is this an INSTEAD rule?
    pub isInstead: bool,
    /// `Node *qual` — the rule qualification (`stringToNode(ev_qual)`), or
    /// `None` for an unconditional rule. Cached in the CacheMemoryContext arena.
    pub qual: Option<mcx::PgBox<'static, types_nodes::nodes::Node<'static>>>,
    /// `List *actions` — the rule's action queries (`stringToNode(ev_action)`),
    /// each a whole `Query` tree cached in the CacheMemoryContext arena.
    pub actions: mcx::PgVec<'static, types_nodes::copy_query::Query<'static>>,
}

/// `RuleLock` (`rewrite/prs2lock.h`) — the set of rewrite rules attached to a
/// relation, the C `RuleLock *rd_rules`.
///
/// `numLocks` is implicit in `rules.len()` (the C struct carries both; the Vec
/// length is the single source of truth here). The whole structure lives in the
/// process-lifetime CacheMemoryContext arena alongside the `Query` trees it
/// owns, so it is `'static`-bound — it may borrow nothing from a per-query
/// `'mcx`, exactly the C `CacheMemoryContext` lifetime invariant.
pub struct RuleLock {
    /// `RewriteRule **rules` — the rules, in the order
    /// `RelationBuildRuleLock` read them from `pg_rewrite` (by `rulename`,
    /// the `RewriteRelRulesIndexId` scan order).
    pub rules: mcx::PgVec<'static, RewriteRule>,
}

/// `RowSecurityPolicy` (`rewrite/rowsecurity.h`) — one row-level-security policy
/// attached to a relation, the unit `get_row_security_policies` reads off
/// `rd_rsdesc->policies`.
///
/// Cache-ownership keystone (mirrors [`RewriteRule`]): the `qual` /
/// `with_check_qual` expression trees are loaded by `RelationBuildRowSecurity`
/// via `stringToNode(polqual)` / `stringToNode(polwithcheck)` and live for the
/// relcache entry's (backend's) lifetime in the process-lifetime
/// CacheMemoryContext arena — exactly the C `MemoryContextSetParent(rscxt,
/// CacheMemoryContext)`. So they are carried as `'static`-bound owned trees, not
/// raw pointers. The C `ArrayType *roles` is stored decoded into its element
/// `Oid[]` (the only thing `check_role_for_policy` reads — the `polroles`
/// array's element values).
pub struct RowSecurityPolicy {
    /// `char *policy_name` — name of the policy (`NameStr(polname)`).
    pub policy_name: mcx::PgString<'static>,
    /// `char polcmd` — `pg_policy.polcmd` (`'r'`/`'a'`/`'w'`/`'d'`/`'*'`).
    pub polcmd: i8,
    /// `ArrayType *roles` — the policy's roles, decoded to their `Oid[]`
    /// elements (`DatumGetArrayTypePCopy(polroles)`).
    pub roles: mcx::PgVec<'static, Oid>,
    /// `bool permissive` — restrictive or permissive policy (`polpermissive`).
    pub permissive: bool,
    /// `Expr *qual` — `stringToNode(polqual)`, the row-filter expression, or
    /// `None` for the C `NULL`. Cached in the CacheMemoryContext arena.
    pub qual: Option<mcx::PgBox<'static, types_nodes::nodes::Node<'static>>>,
    /// `Expr *with_check_qual` — `stringToNode(polwithcheck)`, the
    /// WITH CHECK expression, or `None`. Cached in the CacheMemoryContext arena.
    pub with_check_qual: Option<mcx::PgBox<'static, types_nodes::nodes::Node<'static>>>,
    /// `bool hassublinks` — does either expression contain a SubLink?
    /// (`checkExprHasSubLink(qual) || checkExprHasSubLink(with_check_qual)`).
    pub hassublinks: bool,
}

/// `RowSecurityDesc` (`rewrite/rowsecurity.h`) — the set of row-security
/// policies attached to a relation, the C `RowSecurityDesc *rd_rsdesc`.
///
/// The C struct carries `MemoryContext rscxt` (the per-descriptor context that
/// is reparented under `CacheMemoryContext`); here the whole structure lives in
/// the process-lifetime CacheMemoryContext arena, so the context identity is
/// implicit in the `'static` lifetime and is not separately stored.
pub struct RowSecurityDesc {
    /// `List *policies` — the policies, in the reverse name order
    /// `RelationBuildRowSecurity` builds them (C uses `lcons`, prepending).
    pub policies: mcx::PgVec<'static, RowSecurityPolicy>,
}

/// `struct RelationData` (`utils/rel.h`) — the real, mutable relcache entry.
///
/// Field order and names mirror the C struct (see `src/include/utils/rel.h`).
/// The node/rewrite-vocabulary payloads the repo cannot yet represent
/// (`rd_rules`, `rd_rsdesc`, `trigdesc`, `rd_pubdesc`, `rd_indexprs`,
/// `rd_indpred`, the partition descriptors) are carried as presence flags +
/// seam-resolved reads — the build/derived families seam-and-panic on them
/// per "mirror PG and panic"; they are not silently dropped.
// No `Debug` derive: the `rd_tableam`/`rd_indam` vtable types do not implement
// `Debug`. A manual `Debug` (below) prints the entry's identity instead.
#[derive(Default)]
pub struct RelationData {
    /// `RelFileLocator rd_locator` — physical identifier.
    pub rd_locator: RelFileLocator,
    /// `int rd_refcnt` — reference count.
    pub rd_refcnt: i32,
    /// `ProcNumber rd_backend` — owning backend for temp rels.
    pub rd_backend: ProcNumber,
    /// `bool rd_islocaltemp` — temp rel of this session.
    pub rd_islocaltemp: bool,
    /// `bool rd_isnailed` — nailed in cache.
    pub rd_isnailed: bool,
    /// `bool rd_isvalid` — entry is valid.
    pub rd_isvalid: bool,
    /// `bool rd_indexvalid` — `rd_indexlist` is valid.
    pub rd_indexvalid: bool,
    /// `bool rd_statvalid` — `rd_statlist` is valid.
    pub rd_statvalid: bool,

    /// `SubTransactionId rd_createSubid`.
    pub rd_createSubid: SubTransactionId,
    /// `SubTransactionId rd_newRelfilelocatorSubid`.
    pub rd_newRelfilelocatorSubid: SubTransactionId,
    /// `SubTransactionId rd_firstRelfilelocatorSubid`.
    pub rd_firstRelfilelocatorSubid: SubTransactionId,
    /// `SubTransactionId rd_droppedSubid`.
    pub rd_droppedSubid: SubTransactionId,

    /// `Form_pg_class rd_rel` — the pg_class tuple.
    pub rd_rel: FormPgClass,
    /// `TupleDesc rd_att` — the tuple descriptor.
    pub rd_att: OwnedTupleDesc,
    /// `Oid rd_id` — the relation OID.
    pub rd_id: Oid,
    /// `LockInfoData rd_lockInfo`.
    pub rd_lockInfo: LockInfoData,

    /// `RuleLock *rd_rules` — rewrite rules. The full-Query cache-ownership
    /// keystone: this is now the REAL value-typed carrier (no longer a presence
    /// flag). `None` is the C `rd_rules == NULL` (the relation has no rules);
    /// `Some` holds the [`RuleLock`] whose `RewriteRule.actions` are whole
    /// `Query<'static>` trees, all allocated in the process-lifetime
    /// CacheMemoryContext arena so they live for the entry's (backend's)
    /// lifetime, exactly as C copies them into `CacheMemoryContext`.
    /// `RelationBuildRuleLock` builds it; `rewriteHandler.c`'s rule-application
    /// core reads `rd_rules.rules[i].{event,qual,actions,isInstead,enabled}`.
    pub rd_rules: Option<mcx::PgBox<'static, RuleLock>>,
    /// `TriggerDesc *trigdesc` — the relation's triggers. Built by
    /// `RelationBuildTriggers` (commands/trigger.c), which scans `pg_trigger`,
    /// assembles the [`types_trigger::TriggerDesc`] (each `Trigger`'s name /
    /// args / qual / attr arrays), and `CopyTriggerDesc`s it into the
    /// process-lifetime CacheMemoryContext arena, so the descriptor lives for
    /// the entry's (backend's) lifetime exactly as C copies it into
    /// `CacheMemoryContext`. `None` is the C `trigdesc == NULL` (no triggers).
    /// The build family's owned->borrowed projection copies it into the trimmed
    /// `types_rel::RelationData.rd_trigdesc` (`'mcx`) when it serves a consumer.
    pub rd_trigdesc: Option<mcx::PgBox<'static, types_trigger::TriggerDesc<'static>>>,
    /// `bool` mirror of `trigdesc != NULL` — the relcache build family's
    /// presence flag, kept alongside [`Self::rd_trigdesc`] so the seam-level
    /// readers (`with_rel(|r| r.rd_has_trigdesc)`) and the C `relhastriggers`
    /// reconciliation stay cheap.
    pub rd_has_trigdesc: bool,
    /// `RowSecurityDesc *rd_rsdesc` — the relation's row-security policies,
    /// built by `RelationBuildRowSecurity` (commands/policy.c) when
    /// `pg_class.relrowsecurity` is set. The descriptor and its policy
    /// expression trees are allocated in the process-lifetime CacheMemoryContext
    /// arena, so they live for the entry's (backend's) lifetime exactly as C
    /// reparents `rscxt` under `CacheMemoryContext`. `None` is the C
    /// `rd_rsdesc == NULL` (RLS disabled, or no policies were found).
    pub rd_rsdesc: Option<mcx::PgBox<'static, RowSecurityDesc>>,
    /// `bool` mirror of `rd_rsdesc != NULL` — the relcache build family's
    /// presence flag, kept alongside [`Self::rd_rsdesc`] for cheap seam reads.
    pub rd_has_rsdesc: bool,

    /// `List *rd_fkeylist` (managed by `RelationGetFKeyList`); presence flag.
    pub rd_fkeyvalid: bool,

    /// `bool rd_partdesc/rd_partkey` presence (partition payloads via seam).
    pub rd_has_partkey: bool,
    pub rd_has_partdesc: bool,
    /// `List *rd_partcheck` + `rd_partcheckvalid`.
    pub rd_partcheckvalid: bool,

    /// `List *rd_indexlist` — OIDs of indexes on this relation.
    pub rd_indexlist: Vec<Oid>,
    /// `Oid rd_pkindex` — primary-key index OID.
    pub rd_pkindex: Oid,
    /// `bool rd_ispkdeferrable`.
    pub rd_ispkdeferrable: bool,
    /// `Oid rd_replidindex` — replica-identity index OID.
    pub rd_replidindex: Oid,

    /// `List *rd_statlist` — OIDs of extended-statistics objects.
    pub rd_statlist: Vec<Oid>,

    /// `bool rd_attrsvalid` — the `rd_*attr` bitmaps are valid.
    pub rd_attrsvalid: bool,
    /// `Bitmapset *rd_keyattr` — FK-referenceable columns (offset members).
    pub rd_keyattr: Vec<i32>,
    /// `Bitmapset *rd_pkattr` — primary-key columns.
    pub rd_pkattr: Vec<i32>,
    /// `Bitmapset *rd_idattr` — replica-identity columns.
    pub rd_idattr: Vec<i32>,
    /// `Bitmapset *rd_hotblockingattr` — HOT-blocking columns.
    pub rd_hotblockingattr: Vec<i32>,
    /// `Bitmapset *rd_summarizedattr` — summarizing-index columns.
    pub rd_summarizedattr: Vec<i32>,

    /// `PublicationDesc *rd_pubdesc` presence (publication vocabulary, seamed).
    pub rd_has_pubdesc: bool,

    /// `bytea *rd_options` — parsed reloptions; `None` is the C NULL.
    pub rd_options: Option<types_reloptions::StdRdOptions>,

    /// `Oid rd_amhandler` — the AM handler function OID.
    pub rd_amhandler: Oid,

    /// `const TableAmRoutine *rd_tableam` — the table-AM vtable, or `None`.
    pub rd_tableam: Option<types_tableam::TableAmRoutine>,

    /* ---- index-only fields (NULL/empty for a non-index relation) ---- */
    /// `Form_pg_index rd_index` — the pg_index tuple; `None` for a table.
    pub rd_index: Option<FormPgIndex>,
    /// `IndexAmRoutine *rd_indam` — the index-AM vtable.
    pub rd_indam: Option<types_tableam::amapi::IndexAmRoutine>,
    /// `Oid *rd_opfamily` — op-family OID per index column.
    pub rd_opfamily: Vec<Oid>,
    /// `Oid *rd_opcintype` — opclass declared input-type OID per index column.
    pub rd_opcintype: Vec<Oid>,
    /// `RegProcedure *rd_support` — support-procedure OIDs.
    pub rd_support: Vec<RegProcedure>,
    /// `FmgrInfo *rd_supportinfo` — lazily-filled support-proc lookup info.
    pub rd_supportinfo: Vec<types_core::fmgr::FmgrInfo>,
    /// `int16 *rd_indoption` — per-column AM flags.
    pub rd_indoption: Vec<i16>,
    /// `Oid *rd_exclops` / `rd_exclprocs` / `rd_exclstrats` — exclusion info.
    pub rd_exclops: Vec<Oid>,
    pub rd_exclprocs: Vec<Oid>,
    pub rd_exclstrats: Vec<u16>,
    /// `Oid *rd_indcollation` — per-column index collation OIDs.
    pub rd_indcollation: Vec<Oid>,
    /// `bytea **rd_opcoptions` — parsed AM/opclass per-column index options,
    /// cached in `rd_indexcxt`. `None` is the C NULL (not yet computed); when
    /// present it has one entry per attribute, each `None` (the C NULL element)
    /// when that column has no parsed options. The parsed `bytea` payload is
    /// opaque opclass vocabulary, carried as the owned bytes.
    pub rd_opcoptions: Option<Vec<Option<Vec<u8>>>>,

    /// `Oid rd_toastoid` — CLUSTER/rewrite toast-OID hack; `InvalidOid` off.
    pub rd_toastoid: Oid,

    /// `void *rd_amcache` (`utils/rel.h`) — the per-relation access-method-private
    /// cache. In C this is a bare `void *` the index AM (or table AM) fills lazily
    /// the first time it needs derived state for this relation, allocated in
    /// `rd_indexcxt` (a child of `CacheMemoryContext`) so it lives for the whole
    /// backend / cache lifetime and is reused across queries. Each AM stashes its
    /// own struct here and casts it back on every call: SP-GiST caches a
    /// `SpGistCache` (`initSpGistState`), hash a `HashMetaPageData`
    /// (`_hash_getcachedmetap`), GIN a `GinState` (`ginGetCache`), GiST a
    /// `GISTSTATE`.
    ///
    /// The faithful owned rendering is the same erased AM-private payload, made
    /// safe via [`types_tableam::amopaque::AmOpaque`] (the tag-checked downcast
    /// from TOWER-A0 #244 — NOT new opacity, the same `void *` the C carries with
    /// the unsafe cast encapsulated and proven sound by a per-type tag). The slot
    /// is `'static`-bound on purpose: the cache outlives any single query's `'mcx`
    /// arena (it lives for the `CacheMemoryContext` lifetime), so its payload may
    /// borrow nothing from a per-query arena — exactly the C `rd_indexcxt`
    /// lifetime invariant.
    ///
    /// Lifecycle mirrors C `rd_amcache`: filled lazily by each AM, and cleared
    /// (the C `pfree(rd_amcache); rd_amcache = NULL`) on relcache invalidation /
    /// rebuild so the next access refetches. The AM *bodies* that fill it
    /// (`SpGistCache`/`GinState`/...) are NOT ported here — this keystone only
    /// provides the typed slot and the get/set accessors those AM campaigns will
    /// use.
    pub rd_amcache: Option<Box<dyn types_tableam::amopaque::AmOpaque<'static> + 'static>>,

    /// `bool pgstat_enabled` — relation stats should be counted.
    pub pgstat_enabled: bool,

    /// `FdwRoutine *rd_fdwroutine` (`utils/rel.h`) — the cached FDW
    /// callback-presence table for a foreign table, memoized by
    /// `GetFdwRoutineForRelation` (foreign.c) in `CacheMemoryContext` and reused
    /// across queries; `None` is the C NULL (not yet resolved). Cleared on
    /// relcache invalidation / rebuild so the next access refetches. Only the
    /// presence-flag rendering ([`types_nodes::FdwRoutine`]) is cached here; the
    /// extension-owned function pointers live behind the FDW-provider seam.
    pub rd_fdwroutine: Option<types_nodes::FdwRoutine>,
}

impl RelationData {
    /// `palloc0`-equivalent fresh descriptor (`AllocateRelationDesc` start):
    /// every field zero/empty/None, with the sentinel OIDs/proc numbers C uses.
    pub fn new_blank() -> Box<RelationData> {
        let mut rd = RelationData {
            rd_backend: INVALID_PROC_NUMBER,
            rd_toastoid: InvalidOid,
            ..Default::default()
        };
        rd.rd_rel.relfrozenxid = 0;
        Box::new(rd)
    }

    /* ----------------------------------------------------------------------
     * Convenience accessors (the `utils/rel.h` `RelationGet*` macros).
     *
     * F0'' additive: these mirror the methods the trimmed `types_rel::
     * RelationData` exposes, but read the OWNED entry fields. They are placed
     * here so that after the later Deref-target flip (when `types_rel::
     * Relation` derefs to the shared entry cell instead of the projected
     * trimmed copy) every existing `rel.<method>()` call resolves unchanged.
     * The trimmed type keeps its copies until the atomic flip wave; this is a
     * COPY, not a move.
     * ---------------------------------------------------------------------- */

    /// `RelationGetRelationName(relation)` (utils/rel.h):
    /// `NameStr(relation->rd_rel->relname)`.
    pub fn name(&self) -> &str {
        &self.rd_rel.relname
    }

    /// `RelationIsScannable(relation)` (utils/rel.h):
    /// `relation->rd_rel->relispopulated`.
    pub fn is_scannable(&self) -> bool {
        self.rd_rel.relispopulated
    }

    /// `RelationGetFillFactor(relation, defaultff)` (utils/rel.h).
    pub fn get_fillfactor(&self, defaultff: i32) -> i32 {
        match &self.rd_options {
            Some(opts) => opts.fillfactor,
            None => defaultff,
        }
    }

    /// `RelationGetToastTupleTarget(relation, defaulttarg)` (utils/rel.h).
    pub fn get_toast_tuple_target(&self, default_target: i32) -> i32 {
        match &self.rd_options {
            Some(opts) => opts.toast_tuple_target,
            None => default_target,
        }
    }

    /// `RelationUsesLocalBuffers(relation)` (utils/rel.h):
    /// `relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP`. The entry
    /// stores `relpersistence` as `i8`; `RELPERSISTENCE_TEMP` is `u8`.
    pub fn uses_local_buffers(&self) -> bool {
        self.rd_rel.relpersistence == types_tuple::access::RELPERSISTENCE_TEMP as i8
    }

    /// `RelationIsMapped(relation)` (utils/rel.h): true if the relation uses the
    /// relfilenumber map — `RELKIND_HAS_STORAGE(relkind) && relfilenode ==
    /// InvalidRelFileNumber`. The entry stores `relkind` as `i8`; the
    /// `RELKIND_*` constants are `u8`.
    pub fn is_mapped(&self) -> bool {
        use types_tuple::access::{
            RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
        };
        let relkind = self.rd_rel.relkind;
        let has_storage = relkind == RELKIND_RELATION as i8
            || relkind == RELKIND_INDEX as i8
            || relkind == RELKIND_SEQUENCE as i8
            || relkind == RELKIND_TOASTVALUE as i8
            || relkind == RELKIND_MATVIEW as i8;
        has_storage && self.rd_rel.relfilenode == types_core::primitive::InvalidRelFileNumber
    }

    /// `indexRelation->rd_index->indnkeyatts` — the index's number of key
    /// attributes; `0` when this is not an index (`rd_index` is NULL).
    pub fn indnkeyatts(&self) -> i32 {
        self.rd_index
            .as_ref()
            .map(|i| i.indnkeyatts as i32)
            .unwrap_or(0)
    }

    /// `TupleDescAttr(rel->rd_att, attnum)->atttypid == CSTRINGOID &&
    ///  rel->rd_opcintype[attnum] == NAMEOID` (nodeIndexonlyscan.c): does this
    /// index key column store cstrings for a name-type opclass (btree
    /// `name_ops`)? Read over the OWNED attribute rows + `rd_opcintype`.
    pub fn index_attr_is_namecstring(&self, attnum: i32) -> bool {
        let idx = attnum as usize;
        if idx >= self.rd_att.attrs.len() || idx >= self.rd_opcintype.len() {
            return false;
        }
        self.rd_att.attr(idx).atttypid == types_tuple::heaptuple::CSTRINGOID
            && self.rd_opcintype[idx] == types_tuple::heaptuple::NAMEOID
    }

    /// `RelationGetDescr(relation)` deep-copied into `mcx` — the table slot's
    /// descriptor for an index-only scan's recheck slot. Materializes the owned
    /// `rd_att` into the borrowed `TupleDescData` via
    /// [`OwnedTupleDesc::project_in`] (using the entry's own `rd_id` for the
    /// per-attribute `attrelid`).
    pub fn rd_att_clone_in<'b>(
        &self,
        mcx: mcx::Mcx<'b>,
    ) -> PgResult<mcx::PgBox<'b, types_tuple::heaptuple::TupleDescData<'b>>> {
        self.rd_att.project_in(mcx, self.rd_id)
    }
}

impl std::fmt::Debug for RelationData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationData")
            .field("rd_id", &self.rd_id)
            .field("rd_refcnt", &self.rd_refcnt)
            .field("rd_isvalid", &self.rd_isvalid)
            .field("rd_isnailed", &self.rd_isnailed)
            .field("relname", &self.rd_rel.relname)
            .finish_non_exhaustive()
    }
}
