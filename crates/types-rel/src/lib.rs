//! Relation-descriptor vocabulary (`utils/rel.h` / `catalog/pg_class.h`),
//! trimmed to the fields ports consume.
//!
//! C's `Relation` is a typed pointer to a refcounted relcache entry
//! (`RelationData *`). The owned model copies the consumed slice of the entry
//! out of the relcache into the caller's memory context ([`RelationData`])
//! and wraps it in a [`Relation`] handle:
//!
//! - the handle returned by an open (`relation_open`/`table_open`) carries
//!   release authority: dropping it is the C abort-path
//!   `relation_close(rel, NoLock)` (relcache refcount decrement; locks are
//!   released by transaction cleanup), and [`Relation::close`] is the
//!   explicit C `relation_close(rel, lockmode)`;
//! - [`Relation::alias`] is the C pointer alias (e.g. `ri_RelationDesc`
//!   pointing at the relation `es_relations` owns) — it shares the data but
//!   has no release authority.
//!
//! Per AGENTS.md "Locks and held resources" there is no bare-OID close
//! function: release goes through the handle.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use mcx::{PgBox, PgString};
use types_core::primitive::{Oid, ProcNumber};
use types_error::PgResult;
use types_storage::lock::{LOCKMODE, NoLock};
use types_storage::RelFileLocator;
use types_tuple::heaptuple::TupleDescData;

/// `FormData_pg_class` (`catalog/pg_class.h`), trimmed to the fields ports
/// consume (the `rd_rel` payload of a relcache entry).
#[derive(Debug)]
pub struct FormData_pg_class<'mcx> {
    /// `NameData relname` — name of the relation.
    pub relname: PgString<'mcx>,
    /// `Oid relnamespace` — OID of the namespace containing this relation
    /// (`RelationGetNamespace`).
    pub relnamespace: Oid,
    /// `Oid relowner` — the relation's owning role OID.
    pub relowner: Oid,
    /// `bool relrowsecurity` — row-level security is enabled on the relation.
    pub relrowsecurity: bool,
    /// `int32 relpages` — page-count estimate from pg_class.
    pub relpages: i32,
    /// `float4 reltuples` — row-count estimate (negative: never vacuumed).
    pub reltuples: f32,
    /// `int32 relallvisible` — all-visible page count from pg_class.
    pub relallvisible: i32,
    /// `Oid reltoastrelid` — OID of the TOAST table, or `InvalidOid`.
    pub reltoastrelid: Oid,
    /// `Oid reltablespace` — the relation's tablespace, or `InvalidOid` for
    /// the database's default tablespace.
    pub reltablespace: Oid,
    /// `RelFileNumber relfilenode` — the on-disk file OID, or
    /// `InvalidRelFileNumber` for a relation that uses the relfilenumber map.
    pub relfilenode: Oid,
    /// `bool relisshared` — is the relation shared across all databases in
    /// the cluster?
    pub relisshared: bool,
    /// `bool relhasindex` — relation has (or had) any indexes; gates whether
    /// the executor opens the result relation's indexes for maintenance.
    pub relhasindex: bool,
    /// `bool relhassubclass` — has (or once had) inheritance children.
    pub relhassubclass: bool,
    /// `char relpersistence` — `RELPERSISTENCE_*`.
    pub relpersistence: u8,
    /// `char relkind` — `RELKIND_*`.
    pub relkind: u8,
    /// `Oid relam` — the relation's access method (the table/index AM OID).
    /// Read by logical-replication index selection
    /// (`IsIndexUsableForReplicaIdentityFull`: `idxrel->rd_rel->relam`).
    pub relam: Oid,
    /// `bool relispopulated` — matview currently holds query results.
    pub relispopulated: bool,
    /// `char relreplident` — replica identity setting, see
    /// `REPLICA_IDENTITY_*` (types-tuple `access`).
    pub relreplident: u8,
    /// `bool relispartition` — is the relation a partition?
    pub relispartition: bool,
    /// `TransactionId relfrozenxid` — all xids before this are frozen in this
    /// table (`InvalidTransactionId` for relations without storage). Read by
    /// `heap_abort_speculative` to pick a safe prune xid.
    pub relfrozenxid: types_core::primitive::TransactionId,
    /// `MultiXactId relminmxid` — all multixacts before this are frozen in this
    /// table. Read by `rewrite_heap_tuple`'s `heap_freeze_tuple` cutoff.
    pub relminmxid: types_core::primitive::MultiXactId,
}

/// `FormData_pg_index` (`catalog/pg_index.h`), trimmed to the fields ports
/// consume (the `rd_index` payload of an index's relcache entry).
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_index {
    /// `int16 indnatts` — total number of columns in the index (key + INCLUDE;
    /// `IndexRelationGetNumberOfAttributes`). `0` for a non-index relation's
    /// absent `rd_index`. Read by `BuildIndexInfo` (`catalog/index.c`) to size
    /// `ii_IndexAttrNumbers` and copy `indkey.values[0..indnatts]`.
    pub indnatts: i16,
    /// `int16 indnkeyatts` — number of key columns in the index (excludes
    /// INCLUDE columns; `IndexRelationGetNumberOfKeyAttributes`). `0` for a
    /// non-index relation's absent `rd_index`.
    pub indnkeyatts: i16,
    /// `bool indisunique` — is this a unique index?
    pub indisunique: bool,
    /// `bool indisprimary` — is this index for a primary key? Read by
    /// `BuildIndexInfo` (it does not consume this directly, but the relcache
    /// projection exposes it so `index_create`'s callers/`BuildIndexInfo`-side
    /// readers see the full flag set; mirrors `indexStruct->indisprimary`).
    pub indisprimary: bool,
    /// `bool indisexclusion` — is this index for an exclusion constraint?
    /// `BuildIndexInfo` passes `indisexclusion && indisunique` as `makeIndexInfo`'s
    /// `concurrent` argument and gates `RelationGetExclusionInfo` on it.
    pub indisexclusion: bool,
    /// `bool indisready` — is this index ready for inserts? Read by
    /// `BuildIndexInfo` (`indexStruct->indisready`) for `makeIndexInfo`.
    pub indisready: bool,
    /// `bool indimmediate` — is uniqueness enforced immediately?
    pub indimmediate: bool,
    /// `bool indnullsnotdistinct` — for a unique index, do NULL key values
    /// conflict with each other (NULLS NOT DISTINCT)? `false` is the SQL
    /// default (NULLs are distinct, so multiple NULLs never conflict).
    pub indnullsnotdistinct: bool,
    /// `Oid indrelid` — the table this index is for.
    pub indrelid: Oid,
    /// `bool indisvalid` — is the index currently valid for queries? BRIN's
    /// `brin_summarize_range`/`brin_desummarize_range` gate their work on
    /// `indexRel->rd_index->indisvalid` (see `gin_clean_pending_list()`).
    pub indisvalid: bool,
    /// `int2vector indkey.values[0]` — the table column number of the index's
    /// first key column (`InvalidAttrNumber` for an expression key). `pg_nextoid`
    /// reads only this first entry.
    pub indkey0: types_core::primitive::AttrNumber,
}

/// `StdRdOptions` (`utils/rel.h`): the parsed heap reloptions the reloptions
/// parser builds and `RelationData::rd_options` carries. Re-exported from
/// `types-reloptions`, the designated home of the parsed option-struct
/// vocabulary. `None` on [`RelationData::rd_options`] is the C NULL
/// `rd_options` (no reloptions set); when present, the parse filled every
/// field (defaults included), as in C.
pub use types_reloptions::StdRdOptions;

/// `RelationData` (`utils/rel.h`), trimmed: the consumed slice of a relcache
/// entry, copied into the opening caller's memory context. (`rd_tableam` is
/// not carried — the vtable type lives above this crate; it is read through
/// the relcache owner's `relation_rd_tableam` seam.)
#[derive(Debug)]
pub struct RelationData<'mcx> {
    /// `Oid rd_id` — the relation's OID (`RelationGetRelid`).
    pub rd_id: Oid,
    /// `RelFileLocator rd_locator` — physical identity.
    pub rd_locator: RelFileLocator,
    /// `ProcNumber rd_backend` — owning backend for temp relations,
    /// `INVALID_PROC_NUMBER` otherwise.
    pub rd_backend: ProcNumber,
    /// `Form_pg_class rd_rel` — the pg_class row (trimmed).
    pub rd_rel: FormData_pg_class<'mcx>,
    /// `TupleDesc rd_att` — the relation's tuple descriptor
    /// (`RelationGetDescr`). Never NULL in C.
    pub rd_att: PgBox<'mcx, TupleDescData<'mcx>>,
    /// `bytea *rd_options` — parsed reloptions (trimmed), or `None`.
    pub rd_options: Option<StdRdOptions>,
    /// `Form_pg_index rd_index` — the pg_index row (trimmed); `None` (the C
    /// NULL) for non-index relations.
    pub rd_index: Option<FormData_pg_index>,
    /// `Oid *rd_opcintype` — the input type OID of each index column's
    /// operator class (`RelationGetIndexRawAttOptions` cache). Empty for a
    /// non-index relation. Indexed by attribute number (0-based).
    pub rd_opcintype: mcx::PgVec<'mcx, Oid>,
    /// `Oid *rd_opfamily` — the operator family OID of each index column's
    /// operator class. Empty for a non-index relation. Indexed by attribute
    /// number (0-based). Consumed by nbtree's `_bt_mkscankey` /
    /// `_bt_preprocess_keys` for opclass member/proc lookups.
    pub rd_opfamily: mcx::PgVec<'mcx, Oid>,
    /// `int16 *rd_indoption` — the per-column index option flags
    /// (`INDOPTION_DESC` / `INDOPTION_NULLS_FIRST`). Empty for a non-index
    /// relation. Indexed by attribute number (0-based).
    pub rd_indoption: mcx::PgVec<'mcx, i16>,
    /// `Oid *rd_indcollation` — the per-column collation OID used by the index.
    /// Empty for a non-index relation. Indexed by attribute number (0-based).
    pub rd_indcollation: mcx::PgVec<'mcx, Oid>,
    /// `TriggerDesc *rd_trigdesc` (`utils/rel.h`) — the relation's triggers, or
    /// `None` (the C NULL) when the relation has none (`relhastriggers` false).
    /// Built by `RelationBuildTriggers` (commands/trigger.c, F1); until that
    /// lands the relcache builder leaves it `None`.
    pub rd_trigdesc: Option<PgBox<'mcx, types_trigger::TriggerDesc<'mcx>>>,
    /// `bool pgstat_enabled` (`utils/rel.h`) — whether this relation's
    /// cumulative statistics should be counted. Read by the `pgstat_count_*`
    /// seams (they pass it through to the pgstat owner, mirroring the C count
    /// macros' `pgstat_should_count_relation(rel)` gate). The relcache builder
    /// sets it from `pgstat_relation_init`'s rules; the pending-stats link
    /// (C `rd_rel->pgstat_info`) is keyed by OID inside pgstat, not carried.
    pub pgstat_enabled: bool,
}

impl<'mcx> RelationData<'mcx> {
    /// `indexRelation->rd_index->indnkeyatts` — the index's number of key
    /// attributes; `0` when this is not an index (`rd_index` is NULL).
    pub fn indnkeyatts(&self) -> i32 {
        self.rd_index.map(|i| i.indnkeyatts as i32).unwrap_or(0)
    }

    /// `TupleDescAttr(rel->rd_att, attnum)->atttypid == CSTRINGOID &&
    ///  rel->rd_opcintype[attnum] == NAMEOID` (nodeIndexonlyscan.c): does this
    /// index key column store cstrings for a name-type opclass (btree
    /// `name_ops`)?
    pub fn index_attr_is_namecstring(&self, attnum: i32) -> bool {
        let idx = attnum as usize;
        if idx >= self.rd_att.attrs.len() || idx >= self.rd_opcintype.len() {
            return false;
        }
        self.rd_att.attr(idx).atttypid == types_tuple::heaptuple::CSTRINGOID
            && self.rd_opcintype[idx] == types_tuple::heaptuple::NAMEOID
    }

    /// `RelationGetDescr(relation)` deep-copied into `mcx` — the table slot's
    /// descriptor for an index-only scan's recheck slot.
    pub fn rd_att_clone_in<'b>(
        &self,
        mcx: mcx::Mcx<'b>,
    ) -> PgResult<PgBox<'b, TupleDescData<'b>>> {
        mcx::alloc_in(mcx, self.rd_att.clone_in(mcx)?)
    }

    /// `RelationGetRelationName(relation)` (utils/rel.h):
    /// `NameStr(relation->rd_rel->relname)`.
    pub fn name(&self) -> &str {
        self.rd_rel.relname.as_str()
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
    /// `relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP`.
    pub fn uses_local_buffers(&self) -> bool {
        self.rd_rel.relpersistence == types_tuple::access::RELPERSISTENCE_TEMP
    }

    /// `RelationIsMapped(relation)` (utils/rel.h): true if the relation uses
    /// the relfilenumber map —
    /// `RELKIND_HAS_STORAGE(relkind) && relfilenode == InvalidRelFileNumber`.
    pub fn is_mapped(&self) -> bool {
        use types_tuple::access::{
            RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
        };
        let relkind = self.rd_rel.relkind;
        let has_storage = relkind == RELKIND_RELATION
            || relkind == RELKIND_INDEX
            || relkind == RELKIND_SEQUENCE
            || relkind == RELKIND_TOASTVALUE
            || relkind == RELKIND_MATVIEW;
        has_storage && self.rd_rel.relfilenode == types_core::primitive::InvalidRelFileNumber
    }
}

/// The close half of an open: `relation_close(relid, lockmode)` as installed
/// by the relcache/relation owner when it opens the relation.
pub type RelationCloser = fn(Oid, LOCKMODE) -> PgResult<()>;

/// A type-erased CLONE of the relcache's shared entry cell — C's live
/// `RelationData *` into the cache, held as `Rc<dyn Any>`.
///
/// The concrete value is the relcache owner's
/// `Rc<RefCell<types_relcache_entry::RelationData>>`. It is erased to `dyn Any`
/// at this boundary purely to break the crate-dependency cycle: the entry-store
/// crate (`types-relcache-entry`) transitively depends on `types-rel`
/// (`types-relcache-entry → backend-access-common-tupdesc →
/// backend-catalog-catalog-seams → types-rel`), so `types-rel` cannot name the
/// entry type directly. The erasure is LOSSLESS — the `Rc` (and therefore the
/// `strong_count` pin) is preserved, and the concrete cell is recovered by
/// downcast through [`Relation::entry_as`] / [`Relation::borrow_entry_as`] (or
/// the typed convenience accessors the relcache-seams crate re-exports). No
/// opacity is introduced: the real shared pointer flows through unchanged; only
/// its static type is hidden across this one crate edge.
///
/// Holding a clone keeps `Rc::strong_count > 1`, the safe analog of
/// `rd_refcnt > 0` pinning the allocation: the relcache's `strong_count == 1`
/// eviction sees an open relation as a live external holder.
pub type RelcacheCell = Rc<dyn Any>;

/// An open relation (the C `Relation`).
///
/// DUAL-CARRY (F1): the handle carries BOTH representations of the relcache
/// entry:
///
/// - `cell` — a CLONE of the relcache's shared `Rc<RefCell<RelationData>>`
///   (C's live `RelationData *`). While an open `Relation` lives, this clone
///   makes `Rc::strong_count > 1`, so the relcache's `strong_count == 1`
///   eviction is gated on external holders — the user-visible eviction
///   semantic is real *now*. Dropping the `Relation` (or calling
///   [`Relation::close`]) drops the clone and frees the cache for eviction.
///   `None` only for handles built without a cache cell (tests, genuinely
///   transient rels that were never in the cache).
/// - `data` — the trimmed projected copy ([`RelationData`]), copied into the
///   caller's `mcx`. The [`Deref`] target stays the trimmed copy so every
///   existing consumer (the ~48 crates reading fields through `Deref`)
///   compiles UNCHANGED. Consumers migrate off the copy onto
///   [`Relation::entry_as`] / [`Relation::with_entry`] in later gated waves.
///
/// The handle created by the opening function ([`Relation::open`] /
/// [`Relation::open_with_cell`] with a closer) owns the close:
/// [`Relation::close`] is `relation_close(rel, lockmode)`, and `Drop` is the
/// abort path (`relation_close(rel, NoLock)` — refcount release only; lock
/// release belongs to transaction cleanup, as in C). [`Relation::alias`]
/// yields the C pointer alias: same data + a clone of the same cell (a second
/// live `RelationData *`, as C aliasing bumps `rd_refcnt`), but no release
/// authority.
///
/// [`Deref`]: core::ops::Deref
// No `#[derive(Debug)]`: the type-erased `Rc<dyn Any>` cell is not `Debug`. A
// manual impl (below) prints the trimmed copy + whether a cache cell is held.
pub struct Relation<'mcx> {
    /// CLONE of the shared relcache cell (see struct docs); `None` for
    /// cache-less handles.
    cell: Option<RelcacheCell>,
    data: Rc<RelationData<'mcx>>,
    closer: Option<RelationCloser>,
}

impl<'mcx> Relation<'mcx> {
    /// Wrap a freshly opened relation WITHOUT a shared cache cell. The opening
    /// owner passes its close function; `None` builds a handle without release
    /// authority (tests, or relations whose lifecycle someone else owns).
    ///
    /// Prefer [`Relation::open_with_cell`] for relations that came from the
    /// relcache: carrying the cell is what makes eviction gate on open
    /// handles. This `cell`-less constructor is for genuinely transient rels
    /// (bootstrap/dummy/test descriptors never inserted into the cache), where
    /// there is no shared allocation to pin and `strong_count` semantics are
    /// vacuous.
    pub fn open(data: RelationData<'mcx>, closer: Option<RelationCloser>) -> Self {
        Relation {
            cell: None,
            data: Rc::new(data),
            closer,
        }
    }

    /// Wrap a freshly opened relation that came from the relcache, carrying a
    /// CLONE of the shared cache cell alongside the trimmed projected copy.
    ///
    /// `cell` is the relcache's `Rc<RefCell<RelationData>>` for this relation
    /// (from `relation_id_get_relation_shared`); holding it keeps
    /// `Rc::strong_count > 1` so the cache's `strong_count == 1` eviction is
    /// gated on this open handle. `data` is the trimmed copy that the [`Deref`]
    /// target still yields for not-yet-migrated consumers.
    ///
    /// [`Deref`]: core::ops::Deref
    pub fn open_with_cell(
        cell: RelcacheCell,
        data: RelationData<'mcx>,
        closer: Option<RelationCloser>,
    ) -> Self {
        Relation {
            cell: Some(cell),
            data: Rc::new(data),
            closer,
        }
    }

    /// The C pointer alias: shares the relation data, carries no release
    /// authority (dropping an alias releases nothing). It DOES clone the shared
    /// cache cell — an alias is a second live `RelationData *` into the cache,
    /// so it pins the allocation against eviction exactly as C's aliased
    /// pointer does (C bumps `rd_refcnt` for each held pointer).
    pub fn alias(&self) -> Relation<'mcx> {
        Relation {
            cell: self.cell.clone(),
            data: Rc::clone(&self.data),
            closer: None,
        }
    }

    /// The type-erased shared relcache pin this handle carries: `Some` for
    /// relations opened from the cache via [`Relation::open_with_cell`], `None`
    /// for cache-less handles. This is the raw `Rc<dyn Any>`; consumers that
    /// want the concrete entry use [`Relation::entry_as`] /
    /// [`Relation::borrow_entry_as`] (or the relcache-seams typed wrappers).
    pub fn raw_cell(&self) -> Option<&RelcacheCell> {
        self.cell.as_ref()
    }

    /// The shared relcache cell this handle carries, downcast to the concrete
    /// entry-cell type `Rc<RefCell<T>>` (the migration target). `T` is the
    /// relcache owner's entry struct (`types_relcache_entry::RelationData`);
    /// callers that can name it pass it (the relcache-seams crate re-exports a
    /// monomorphized convenience wrapper). `None` for a cache-less handle or
    /// (defensively) a type mismatch — the relcache owner always stores the one
    /// concrete cell type, so a mismatch cannot happen in practice.
    ///
    /// Consumers migrate their field reads off the [`Deref`]-to-copy onto this
    /// shared entry in later gated waves; e.g.
    /// `rel.entry_as::<Entry>().unwrap().borrow().rd_rel.relname`.
    ///
    /// [`Deref`]: core::ops::Deref
    pub fn entry_as<T: 'static>(&self) -> Option<Rc<RefCell<T>>> {
        let cell = self.cell.clone()?;
        cell.downcast::<RefCell<T>>().ok()
    }

    /// Borrow the shared relcache entry downcast to the concrete entry type
    /// `T` and run `f` against it (`f(&*cell.borrow())`), if this handle
    /// carries a cache cell of that type. `f` sees the live entry — a holder
    /// observes the in-place rebuild (`*cell.borrow_mut() = rebuilt`, true C
    /// `RelationData *` semantics). Returns `None` for cache-less handles / a
    /// type mismatch (without calling `f`). This is the borrow helper consumers
    /// use during the off-Deref migration; it matches the relcache owner's
    /// `with`/`with_relation` closure idiom and avoids leaking a self-borrowing
    /// guard across the type-erasure boundary.
    pub fn with_entry<T: 'static, R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let rc = self.entry_as::<T>()?;
        let guard = rc.borrow();
        Some(f(&guard))
    }

    /// Store the `pgstat_init_relation` decision onto this freshly opened
    /// relation's trimmed copy (C's `rel->pgstat_enabled = ...`). Called by the
    /// opener right after `pgstat_init_relation`, while this handle is the sole
    /// holder of the trimmed `RelationData` (it was just `Rc::new`'d and no
    /// alias has been taken), so the in-place mutation is sound. Panics only if
    /// invoked after the copy was shared — which the open path never does.
    pub fn set_pgstat_enabled(&mut self, enabled: bool) {
        Rc::get_mut(&mut self.data)
            .expect("set_pgstat_enabled on a shared relation copy")
            .pgstat_enabled = enabled;
    }

    /// `relation_close(relation, lockmode)` / `table_close(...)`: release
    /// the relcache reference and, if `lockmode` is not `NoLock`, the lock.
    /// On a handle without release authority this is a no-op (the C close of
    /// an alias would be a refcount bug; the type prevents it instead).
    pub fn close(mut self, lockmode: LOCKMODE) -> PgResult<()> {
        match self.closer.take() {
            Some(closer) => closer(self.data.rd_id, lockmode),
            None => Ok(()),
        }
    }
}

impl<'mcx> core::ops::Deref for Relation<'mcx> {
    type Target = RelationData<'mcx>;

    fn deref(&self) -> &RelationData<'mcx> {
        &self.data
    }
}

impl core::fmt::Debug for Relation<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Relation")
            .field("data", &self.data)
            .field("has_cell", &self.cell.is_some())
            .field("has_closer", &self.closer.is_some())
            .finish()
    }
}

impl Drop for Relation<'_> {
    /// The abort path: release the relcache reference, leave any lock to
    /// transaction cleanup (C `relation_close(rel, NoLock)`). The C close
    /// with `NoLock` has no error surface, so a failure here is ignored.
    fn drop(&mut self) {
        if let Some(closer) = self.closer.take() {
            let _ = closer(self.data.rd_id, NoLock);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    /// A standalone `'static` entry-cell stand-in for the cell-semantics tests.
    /// (`types-relcache-entry` is not a dep here — and cannot be, the cycle this
    /// crate's type-erasure works around — so the tests exercise the
    /// `Rc<dyn Any>` pin + downcast over a local `'static` type. The relcache
    /// owner's `RelationData` is the real concrete type in production.)
    #[derive(Debug, PartialEq)]
    struct FakeEntry {
        rd_id: u32,
        name: String,
    }

    fn trimmed<'mcx>(mcx: mcx::Mcx<'mcx>, oid: Oid) -> RelationData<'mcx> {
        let td = TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: mcx::PgVec::new_in(mcx),
            attrs: mcx::PgVec::new_in(mcx),
        };
        RelationData {
            rd_id: oid,
            rd_locator: RelFileLocator {
                spcOid: 0,
                dbOid: 0,
                relNumber: 0,
            },
            rd_backend: types_core::primitive::INVALID_PROC_NUMBER,
            rd_rel: FormData_pg_class {
                relname: PgString::from_str_in("t", mcx).unwrap(),
                relnamespace: 0,
                relowner: 0,
                relrowsecurity: false,
                relpages: 0,
                reltuples: 0.0,
                relallvisible: 0,
                reltoastrelid: 0,
                reltablespace: 0,
                relfilenode: 0,
                relisshared: false,
                relhasindex: false,
                relhassubclass: false,
                relpersistence: b'p',
                relkind: b'r',
                relam: 0,
                relispopulated: true,
                relreplident: b'd',
                relispartition: false,
                relfrozenxid: 0,
                relminmxid: 0,
            },
            rd_att: mcx::alloc_in(mcx, td).unwrap(),
            rd_options: None,
            rd_index: None,
            rd_opcintype: mcx::PgVec::new_in(mcx),
            rd_opfamily: mcx::PgVec::new_in(mcx),
            rd_indoption: mcx::PgVec::new_in(mcx),
            rd_indcollation: mcx::PgVec::new_in(mcx),
            rd_trigdesc: None,
            pgstat_enabled: false,
        }
    }

    /// An open relation holding a cell clone makes `strong_count > 1` (the safe
    /// analog of `rd_refcnt > 0`), and dropping/closing it releases the pin —
    /// so the cache's `strong_count == 1` eviction is now gated on open handles.
    #[test]
    fn open_relation_pins_the_cell_strong_count() {
        let ctx = MemoryContext::new("test");
        let cache_cell: Rc<RefCell<FakeEntry>> = Rc::new(RefCell::new(FakeEntry {
            rd_id: 100,
            name: "t".into(),
        }));
        // Cache holds the only reference: evictable.
        assert_eq!(Rc::strong_count(&cache_cell), 1);

        let erased: RelcacheCell = cache_cell.clone() as RelcacheCell;
        let rel = Relation::open_with_cell(erased, trimmed(ctx.mcx(), 100), None);
        // Now an open relation pins it: NOT evictable.
        assert_eq!(Rc::strong_count(&cache_cell), 2);

        // An alias is a second live pointer (C bumps rd_refcnt per pointer).
        let a = rel.alias();
        assert_eq!(Rc::strong_count(&cache_cell), 3);
        drop(a);
        assert_eq!(Rc::strong_count(&cache_cell), 2);

        // Dropping the relation releases the pin: evictable again.
        drop(rel);
        assert_eq!(Rc::strong_count(&cache_cell), 1);
    }

    /// `entry_as` / `with_entry` recover the concrete cell through the erasure,
    /// and the borrow sees in-place mutation (true `RelationData *` semantics).
    #[test]
    fn entry_accessors_recover_live_cell() {
        let ctx = MemoryContext::new("test");
        let cache_cell: Rc<RefCell<FakeEntry>> = Rc::new(RefCell::new(FakeEntry {
            rd_id: 7,
            name: "before".into(),
        }));
        let rel = Relation::open_with_cell(
            cache_cell.clone() as RelcacheCell,
            trimmed(ctx.mcx(), 7),
            None,
        );

        // entry_as downcasts back to the concrete cell.
        let recovered = rel.entry_as::<FakeEntry>().unwrap();
        assert!(Rc::ptr_eq(&recovered, &cache_cell));

        // In-place rebuild via the cache's handle is observed through the rel.
        cache_cell.borrow_mut().name = "after".into();
        let seen = rel.with_entry::<FakeEntry, String>(|e| e.name.clone()).unwrap();
        assert_eq!(seen, "after");

        // A type mismatch yields None (defensive; cannot happen in production).
        assert!(rel.entry_as::<u32>().is_none());
    }

    /// The `Deref` target stays the trimmed copy, and a cache-less handle has
    /// no cell — both required for the additive (zero-consumer-edit) wave.
    #[test]
    fn deref_to_copy_alive_and_cacheless_handle_has_no_cell() {
        let ctx = MemoryContext::new("test");

        // Cache-backed handle: Deref->copy works, cell present.
        let cell: Rc<RefCell<FakeEntry>> =
            Rc::new(RefCell::new(FakeEntry { rd_id: 9, name: "n".into() }));
        let rel =
            Relation::open_with_cell(cell as RelcacheCell, trimmed(ctx.mcx(), 9), None);
        assert_eq!(rel.rd_id, 9); // Deref to the trimmed copy.
        assert!(rel.raw_cell().is_some());

        // Cache-less handle (transient/test rel): valid, no cell, no panic.
        let transient = Relation::open(trimmed(ctx.mcx(), 11), None);
        assert_eq!(transient.rd_id, 11);
        assert!(transient.raw_cell().is_none());
        assert!(transient.entry_as::<FakeEntry>().is_none());
        assert!(transient.with_entry::<FakeEntry, ()>(|_| ()).is_none());
    }
}
