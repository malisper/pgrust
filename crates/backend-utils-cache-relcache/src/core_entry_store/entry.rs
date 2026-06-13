//! The real, mutable relcache entry: `struct RelationData` (`utils/rel.h`).
//!
//! This is the OWN entry-store type — NOT the trimmed cross-unit value-slice
//! [`types_rel::RelationData`]. It carries the full `rd_*` surface that the C
//! `RelationData` does, field-for-field. Because the [`RelationIdCache`] owns
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
use types_storage::lock::LockRelId;
use types_storage::RelFileLocator;

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
    pub attnotnull: bool,
    pub attisdropped: bool,
    pub attcollation: Oid,
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

    /// `RuleLock *rd_rules` — rewrite rules. Node-vocabulary payload, seamed.
    /// Presence only here; the rule tree is built/read via the derived family.
    pub rd_has_rules: bool,
    /// `TriggerDesc *trigdesc`. Presence only (seam vocabulary).
    pub rd_has_trigdesc: bool,
    /// `RowSecurityDesc *rd_rsdesc`. Presence only (seam vocabulary).
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

    /// `Oid rd_toastoid` — CLUSTER/rewrite toast-OID hack; `InvalidOid` off.
    pub rd_toastoid: Oid,

    /// `bool pgstat_enabled` — relation stats should be counted.
    pub pgstat_enabled: bool,
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
