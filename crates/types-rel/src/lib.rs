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
    /// `int32 relpages` — page-count estimate from pg_class.
    pub relpages: i32,
    /// `float4 reltuples` — row-count estimate (negative: never vacuumed).
    pub reltuples: f32,
    /// `int32 relallvisible` — all-visible page count from pg_class.
    pub relallvisible: i32,
    /// `Oid reltoastrelid` — OID of the TOAST table, or `InvalidOid`.
    pub reltoastrelid: Oid,
    /// `bool relhassubclass` — has (or once had) inheritance children.
    pub relhassubclass: bool,
    /// `char relpersistence` — `RELPERSISTENCE_*`.
    pub relpersistence: u8,
    /// `char relkind` — `RELKIND_*`.
    pub relkind: u8,
    /// `bool relispopulated` — matview currently holds query results.
    pub relispopulated: bool,
    /// `char relreplident` — replica identity setting, see
    /// `REPLICA_IDENTITY_*` (types-tuple `access`).
    pub relreplident: u8,
    /// `bool relispartition` — is the relation a partition?
    pub relispartition: bool,
}

/// `StdRdOptions` (`utils/rel.h`), trimmed: the parsed heap reloptions the
/// ports consume. `None` on [`RelationData::rd_options`] is the C NULL
/// `rd_options` (no reloptions set); when present, the parse filled every
/// field (defaults included), as in C.
#[derive(Clone, Copy, Debug)]
pub struct StdRdOptions {
    /// `int fillfactor`.
    pub fillfactor: i32,
    /// `int toast_tuple_target`.
    pub toast_tuple_target: i32,
}

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
}

impl<'mcx> RelationData<'mcx> {
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
}

/// The close half of an open: `relation_close(relid, lockmode)` as installed
/// by the relcache/relation owner when it opens the relation.
pub type RelationCloser = fn(Oid, LOCKMODE) -> PgResult<()>;

/// An open relation (the C `Relation`).
///
/// The handle created by the opening function ([`Relation::open`] with a
/// closer) owns the close: [`Relation::close`] is `relation_close(rel,
/// lockmode)`, and `Drop` is the abort path (`relation_close(rel, NoLock)` —
/// refcount release only; lock release belongs to transaction cleanup, as in
/// C). [`Relation::alias`] yields the C pointer alias: same data, no release
/// authority.
#[derive(Debug)]
pub struct Relation<'mcx> {
    data: Rc<RelationData<'mcx>>,
    closer: Option<RelationCloser>,
}

impl<'mcx> Relation<'mcx> {
    /// Wrap a freshly opened relation. The opening owner passes its close
    /// function; `None` builds a handle without release authority (tests,
    /// or relations whose lifecycle someone else owns).
    pub fn open(data: RelationData<'mcx>, closer: Option<RelationCloser>) -> Self {
        Relation {
            data: Rc::new(data),
            closer,
        }
    }

    /// The C pointer alias: shares the relation data, carries no release
    /// authority (dropping an alias releases nothing).
    pub fn alias(&self) -> Relation<'mcx> {
        Relation {
            data: Rc::clone(&self.data),
            closer: None,
        }
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
