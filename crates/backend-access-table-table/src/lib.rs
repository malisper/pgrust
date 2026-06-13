//! Port of `src/backend/access/table/table.c` — the generic `table_*`
//! relation open/close routines, independent of any individual table access
//! method.
//!
//! Each `table_open` flavor is the matching `relation_open` flavor (the
//! `access/common/relation.c` unit, reached through its seams) plus
//! `validate_relation_kind`, which rejects opening an index, a partitioned
//! index, or a composite type. The caller should also check that the
//! relation is not a view or foreign table before assuming it has storage.
//!
//! The C `Relation` crosses as a [`types_rel::Relation`] handle (the trimmed
//! relcache-entry copy, allocated in the caller-supplied `mcx`, armed by the
//! relation.c owner with its close function). `table_close` consumes the
//! handle; the C NULL `Relation` of the `try_`/`missing_ok` flavors is
//! `None`.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::access::{
    RangeVar, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
};

/// Install this crate's seam implementations. Every consumer reaches this
/// unit by direct dependency (no cycle exists), so there is no seams crate
/// and nothing to install.
pub fn init_seams() {}

/// `table_open(relationId, lockmode)` — open a table relation by relation
/// OID.
///
/// This is essentially `relation_open` plus a check that the relation is not
/// an index nor a composite type.
pub fn table_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = backend_access_common_relation_seams::relation_open::call(mcx, relationId, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

/// `try_table_open(relationId, lockmode)` — same as [`table_open`], except
/// return `None` instead of failing if the relation does not exist.
pub fn try_table_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    let r =
        backend_access_common_relation_seams::try_relation_open::call(mcx, relationId, lockmode)?;

    // leave if table does not exist
    let Some(r) = r else {
        return Ok(None);
    };

    validate_relation_kind(&r)?;

    Ok(Some(r))
}

/// `table_openrv(relation, lockmode)` — as [`table_open`], but the relation
/// is specified by a `RangeVar`.
pub fn table_openrv<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = backend_access_common_relation_seams::relation_openrv::call(mcx, relation, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

/// `table_openrv_extended(relation, lockmode, missing_ok)` — as
/// [`table_openrv`], but optionally return `None` instead of failing for
/// relation-not-found.
pub fn table_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    let r = backend_access_common_relation_seams::relation_openrv_extended::call(
        mcx, relation, lockmode, missing_ok,
    )?;

    if let Some(r) = &r {
        validate_relation_kind(r)?;
    }

    Ok(r)
}

/// `table_close(relation, lockmode)` — close a table, consuming the carrier.
///
/// If `lockmode` is not `NoLock`, the specified lock is then released. Note
/// that it is often sensible to hold a lock beyond `relation_close`; in that
/// case, the lock is released automatically at xact end.
pub fn table_close(relation: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    relation.close(lockmode)
}

/// `validate_relation_kind(r)` (static inline) — make sure relkind is not
/// index or composite type.
fn validate_relation_kind(r: &Relation<'_>) -> PgResult<()> {
    let relkind = r.rd_rel.relkind;

    if relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX
        || relkind == RELKIND_COMPOSITE_TYPE
    {
        let relname = r.name();
        let detail =
            backend_catalog_pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(
            PgError::error(format!("cannot open relation \"{relname}\""))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        );
    }

    Ok(())
}
