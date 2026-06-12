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
//! The C `Relation` (`struct RelationData *`) crosses as the relation's
//! `Oid`: the relcache store owns the open-relation state and re-resolves
//! the OID; field reads go through the relcache owner's seams. The C NULL
//! `Relation` of the `try_`/`missing_ok` flavors is `None`.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_tuple::access::{
    RangeVar, LOCKMODE, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
};

/// Install this crate's seam implementations into
/// `backend-access-table-table-seams`.
pub fn init_seams() {
    backend_access_table_table_seams::table_open::set(table_open);
    backend_access_table_table_seams::table_close::set(table_close);
}

/// `table_open(relationId, lockmode)` — open a table relation by relation
/// OID.
///
/// This is essentially `relation_open` plus a check that the relation is not
/// an index nor a composite type.
pub fn table_open(relationId: Oid, lockmode: LOCKMODE) -> PgResult<Oid> {
    let r = backend_access_common_relation_seams::relation_open::call(relationId, lockmode)?;

    validate_relation_kind(r)?;

    Ok(r)
}

/// `try_table_open(relationId, lockmode)` — same as [`table_open`], except
/// return `None` instead of failing if the relation does not exist.
pub fn try_table_open(relationId: Oid, lockmode: LOCKMODE) -> PgResult<Option<Oid>> {
    let r = backend_access_common_relation_seams::try_relation_open::call(relationId, lockmode)?;

    // leave if table does not exist
    let Some(r) = r else {
        return Ok(None);
    };

    validate_relation_kind(r)?;

    Ok(Some(r))
}

/// `table_openrv(relation, lockmode)` — as [`table_open`], but the relation
/// is specified by a `RangeVar`.
pub fn table_openrv(relation: &RangeVar, lockmode: LOCKMODE) -> PgResult<Oid> {
    let r = backend_access_common_relation_seams::relation_openrv::call(relation, lockmode)?;

    validate_relation_kind(r)?;

    Ok(r)
}

/// `table_openrv_extended(relation, lockmode, missing_ok)` — as
/// [`table_openrv`], but optionally return `None` instead of failing for
/// relation-not-found.
pub fn table_openrv_extended(
    relation: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Oid>> {
    let r = backend_access_common_relation_seams::relation_openrv_extended::call(
        relation, lockmode, missing_ok,
    )?;

    if let Some(r) = r {
        validate_relation_kind(r)?;
    }

    Ok(r)
}

/// `table_close(relation, lockmode)` — close a table.
///
/// If `lockmode` is not `NoLock`, the specified lock is then released. Note
/// that it is often sensible to hold a lock beyond `relation_close`; in that
/// case, the lock is released automatically at xact end.
pub fn table_close(relation: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    backend_access_common_relation_seams::relation_close::call(relation, lockmode)
}

/// `validate_relation_kind(r)` (static inline) — make sure relkind is not
/// index or composite type.
fn validate_relation_kind(r: Oid) -> PgResult<()> {
    let relkind = backend_utils_cache_relcache_seams::relation_relkind::call(r);

    if relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX
        || relkind == RELKIND_COMPOSITE_TYPE
    {
        let relname = backend_utils_cache_relcache_seams::relation_name::call(r)?;
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
