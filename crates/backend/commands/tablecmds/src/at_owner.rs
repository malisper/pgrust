//! `commands/tablecmds.c` — the `ALTER TABLE ... OWNER TO` executor leg, ported
//! 1:1 from PostgreSQL 18.3:
//!
//! | C function (tablecmds.c line) | Rust |
//! |---|---|
//! | `ATExecChangeOwner` (16072) | [`ATExecChangeOwner`] |
//! | `change_owner_fix_column_acls` (16312) | [`change_owner_fix_column_acls`] |
//! | `change_owner_recurse_to_sequences` (16377) | [`change_owner_recurse_to_sequences`] |
//!
//! These mutate the on-disk `pg_class.relowner`/`relacl`, the per-column
//! `pg_attribute.attacl`, the owner `pg_shdepend` row, and the relation's row
//! type owner (`AlterTypeOwnerInternal`), then recurse into the relation's
//! indexes, owned sequences, and toast table. The catalog reads/writes go
//! through the same substrate the rest of this crate uses:
//!   * `relation_open` + RAII close (the lmgr lock is transaction-scoped).
//!   * `SearchSysCache1(RELOID)` → the live pg_class tuple; the
//!     `relowner`/`relacl` columns are rewritten over it via the
//!     `catalog_tuple_update_relowner_pg_class` write leaf (a `heap_modify_tuple`
//!     that replaces only those columns, preserving the rest).
//!   * `SearchSysCacheAttNum(ATTNUM)` → the live pg_attribute tuple per column;
//!     a non-null `attacl` is owner-rewritten via the `attacl` carrier on
//!     [`PgAttributeUpdateRow`] and `catalog_tuple_update_pg_attribute`.
//!   * `aclnewowner` + the on-disk `aclitem[]` codec cross to aclchk through the
//!     `acl_change_owner_datum` seam (the bare `&[AclItem]` model never leaves
//!     that crate).
//!   * `changeDependencyOnOwner` (pg_shdepend), `AlterTypeOwnerInternal`
//!     (typecmds, via the tablecmds-owned `alter_type_owner_internal` seam that
//!     typecmds installs), `RelationGetIndexList` (relcache),
//!     `sequenceIsOwned` (dependency), all reached through their seams.

#![allow(non_snake_case)]

use ::mcx::Mcx;

use types_acl::{ACLCHECK_OK, ACL_CREATE};
use ::types_catalog::catalog::NAMESPACE_RELATION_ID;
use ::types_catalog::pg_attribute::{
    Anum_pg_attribute_attacl, Anum_pg_attribute_attisdropped, AttributeRelationId,
    PgAttributeUpdateRow,
};
use ::types_catalog::pg_class::{Anum_pg_class_relacl, RelationRelationId};
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use ::types_acl::ACLCHECK_NOT_OWNER;
use ::types_storage::lock::{NoLock, RowExclusiveLock, LOCKMODE};
use ::types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_tuple::heaptuple::Datum;

use ::common_relation::relation_open;
use ::utils_error::ereport;

use aclchk_seams as aclchk_seam;
use dependency_seams as dep_seam;
use indexing_seams as indexing_seam;
use objectaccess_seams as objaccess_seam;
use pg_depend_seams as pg_depend_seam;
use ::pg_shdepend::changeDependencyOnOwner;
use tablecmds_seams as me;
use lsyscache_seams as lsyscache_seam;
use relcache_seams as relcache_seam;
use ::cache_syscache::cacheinfo::RELOID;
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SearchSysCacheAttNum, SysCacheGetAttr, ATTNUM,
};
use objectaddress_seams as objaddr_seam;
use ::miscinit::GetUserId;
use miscinit_seams as miscinit_seam;

use crate::helpers::here;

/// `DEPENDENCY_AUTO` / `DEPENDENCY_INTERNAL` codes (`catalog/dependency.h`),
/// matching the `sequence_is_owned` seam's `deptype` argument.
use ::types_catalog::catalog_dependency::{DEPENDENCY_AUTO, DEPENDENCY_INTERNAL};

/// `SysCacheKey::Value(Int(ObjectIdGetDatum(relid)))` — the by-OID syscache key
/// (the publicationcmds `oid_cache_key` pattern).
fn oid_search_key(value: Oid) -> cache::SysCacheKey<'static> {
    cache::SysCacheKey::Value(datum::Datum::from_oid(value))
}

/// `ATExecChangeOwner(relationOid, newOwnerId, recursing, lockmode)`
/// (tablecmds.c:16072): change a relation's owner (and its dependent objects:
/// indexes, owned sequences, toast tables).
pub fn ATExecChangeOwner<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    mut new_owner_id: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // target_rel = relation_open(relationOid, lockmode);
    let target_rel = relation_open(mcx, relation_oid, lockmode)?;

    // class_rel = table_open(RelationRelationId, RowExclusiveLock);
    let class_rel = relation_open(mcx, RelationRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationOid));
    let Some(tuple) = SearchSysCache1(mcx, RELOID, oid_search_key(relation_oid))? else {
        // elog(ERROR, "cache lookup failed for relation %u", relationOid);
        return ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation {relation_oid}"))
            .finish(here("ATExecChangeOwner"))
            .map(|()| unreachable!());
    };

    // tuple_class = (Form_pg_class) GETSTRUCT(tuple); — the columns we read are
    // available on the relcache rd_rel (relkind / relowner / relnamespace /
    // reltype / reltoastrelid), exactly the GETSTRUCT view C reads.
    let relkind = target_rel.rd_rel.relkind;
    let relowner = target_rel.rd_rel.relowner;
    let relnamespace = target_rel.rd_rel.relnamespace;
    let reltype = target_rel.rd_rel.reltype;
    let reltoastrelid = target_rel.rd_rel.reltoastrelid;
    let relname = target_rel.name().to_string();

    // Can we change the ownership of this tuple?
    match relkind {
        RELKIND_RELATION | RELKIND_VIEW | RELKIND_MATVIEW | RELKIND_FOREIGN_TABLE
        | RELKIND_PARTITIONED_TABLE => { /* ok to change owner */ }
        RELKIND_INDEX => {
            if !recursing {
                // ALTER INDEX OWNER used to be allowed (old pg_dump emits it):
                // warn and do nothing rather than erroring out. Stay silent if
                // it would be a no-op anyway.
                if relowner != new_owner_id {
                    ereport(WARNING)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!("cannot change owner of index \"{relname}\""))
                        .errhint("Change the ownership of the index's table instead.")
                        .finish(here("ATExecChangeOwner"))?;
                }
                // quick hack to exit via the no-op path
                new_owner_id = relowner;
            }
        }
        RELKIND_PARTITIONED_INDEX => {
            if !recursing {
                return ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("cannot change owner of index \"{relname}\""))
                    .errhint("Change the ownership of the index's table instead.")
                    .finish(here("ATExecChangeOwner"))
                    .map(|()| unreachable!());
            }
        }
        RELKIND_SEQUENCE => {
            if !recursing && relowner != new_owner_id {
                // if it's an owned sequence, disallow changing it by itself
                let owned = dep_seam::sequence_is_owned::call(relation_oid, DEPENDENCY_AUTO)?
                    .or(dep_seam::sequence_is_owned::call(relation_oid, DEPENDENCY_INTERNAL)?);
                if let Some((table_id, _col_id)) = owned {
                    let table_name = lsyscache_seam::get_rel_name::call(mcx, table_id)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    return ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!("cannot change owner of sequence \"{relname}\""))
                        .errdetail(format!(
                            "Sequence \"{relname}\" is linked to table \"{table_name}\"."
                        ))
                        .finish(here("ATExecChangeOwner"))
                        .map(|()| unreachable!());
                }
            }
        }
        RELKIND_COMPOSITE_TYPE => {
            if !recursing {
                return ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("\"{relname}\" is a composite type"))
                    // translator: %s is an SQL ALTER command
                    .errhint("Use ALTER TYPE instead.")
                    .finish(here("ATExecChangeOwner"))
                    .map(|()| unreachable!());
            }
        }
        RELKIND_TOASTVALUE => {
            if !recursing {
                return change_owner_wrong_object_type(&relname, relkind);
            }
            // FALL THRU when recursing — ok to change owner.
        }
        _ => {
            return change_owner_wrong_object_type(&relname, relkind);
        }
    }

    // If the new owner is the same as the existing owner, consider the command
    // to have succeeded. This is for dump restoration purposes.
    if relowner != new_owner_id {
        // skip permission checks when recursing to index or toast table
        if !recursing && !miscinit_seam::superuser::call(mcx)? {
            // Must be owner of the existing object.
            if !aclchk_seam::object_ownercheck::call(RelationRelationId, relation_oid, GetUserId())?
            {
                let actual_kind = lsyscache_seam::get_rel_relkind::call(relation_oid)?;
                aclchk_seam::aclcheck_error::call(
                    ACLCHECK_NOT_OWNER,
                    objaddr_seam::get_relkind_objtype::call(actual_kind),
                    Some(relname.clone()),
                )?;
            }

            // Must be able to become new owner.
            acl_seams::check_can_set_role::call(GetUserId(), new_owner_id)?;

            // New owner must have CREATE privilege on namespace.
            let aclresult = aclchk_seam::object_aclcheck::call(
                NAMESPACE_RELATION_ID,
                relnamespace,
                new_owner_id,
                ACL_CREATE,
            )?;
            if aclresult != ACLCHECK_OK {
                let nspname = lsyscache_seam::get_namespace_name::call(mcx, relnamespace)?
                    .map(|s| s.as_str().to_string());
                aclchk_seam::aclcheck_error::call(
                    aclresult,
                    nodes::parsenodes::OBJECT_SCHEMA,
                    nspname,
                )?;
            }
        }

        // Determine the modified ACL for the new owner. This is only necessary
        // when the ACL is non-null.
        //
        // aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl, &isNull);
        let (acl_datum, acl_is_null) =
            SysCacheGetAttr(mcx, RELOID, &tuple, Anum_pg_class_relacl as i32)?;
        let new_acl: Option<Datum<'mcx>> = if acl_is_null {
            None
        } else {
            // newAcl = aclnewowner(DatumGetAclP(aclDatum), relowner, newOwnerId);
            match &acl_datum {
                Datum::ByRef(b) => Some(aclchk_seam::acl_change_owner_datum::call(
                    mcx,
                    &b[..],
                    relowner,
                    new_owner_id,
                )?),
                _ => {
                    return ereport(ERROR)
                        .errmsg_internal("relacl is not a by-ref aclitem[] varlena")
                        .finish(here("ATExecChangeOwner"))
                        .map(|()| unreachable!());
                }
            }
        };

        // repl_repl[relowner] = true; repl_val[relowner] = newOwnerId;
        // (and relacl when non-null) → heap_modify_tuple → CatalogTupleUpdate.
        indexing_seam::catalog_tuple_update_relowner_pg_class::call(
            mcx,
            &class_rel,
            &tuple,
            new_owner_id,
            new_acl,
        )?;

        // We must similarly update any per-column ACLs to reflect the new owner.
        change_owner_fix_column_acls(mcx, relation_oid, relowner, new_owner_id, &target_rel)?;

        // Update owner dependency reference, if any. A composite type has none
        // (tracked on pg_type); indexes and TOAST tables don't have their own
        // entries either.
        if relkind != RELKIND_COMPOSITE_TYPE
            && relkind != RELKIND_INDEX
            && relkind != RELKIND_PARTITIONED_INDEX
            && relkind != RELKIND_TOASTVALUE
        {
            changeDependencyOnOwner(RelationRelationId, relation_oid, new_owner_id)?;
        }

        // Also change the ownership of the table's row type, if it has one.
        if OidIsValid(reltype) {
            me::alter_type_owner_internal::call(reltype, new_owner_id)?;
        }

        // If we are operating on a table or materialized view, also change the
        // ownership of any indexes and sequences that belong to the relation, as
        // well as its toast table (if it has one).
        if relkind == RELKIND_RELATION
            || relkind == RELKIND_PARTITIONED_TABLE
            || relkind == RELKIND_MATVIEW
            || relkind == RELKIND_TOASTVALUE
        {
            // index_oid_list = RelationGetIndexList(target_rel);
            let index_oid_list = relcache_seam::relation_get_index_list::call(mcx, &target_rel)?;
            // foreach(i, index_oid_list) recursively change its ownership.
            for &index_oid in index_oid_list.iter() {
                ATExecChangeOwner(mcx, index_oid, new_owner_id, true, lockmode)?;
            }
        }

        // If it has a toast table, recurse to change its ownership.
        if reltoastrelid != InvalidOid {
            ATExecChangeOwner(mcx, reltoastrelid, new_owner_id, true, lockmode)?;
        }

        // If it has dependent sequences, recurse to change them too.
        change_owner_recurse_to_sequences(mcx, relation_oid, new_owner_id, lockmode)?;
    }

    // InvokeObjectPostAlterHook(RelationRelationId, relationOid, 0);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, relation_oid, 0)?;

    // ReleaseSysCache(tuple);
    ReleaseSysCache(tuple);
    // table_close(class_rel, RowExclusiveLock); relation_close(target_rel, NoLock);
    drop(class_rel);
    drop(target_rel);
    let _ = NoLock; // (RAII drop carries the NoLock semantics for target_rel)

    Ok(())
}

/// `errdetail_relkind_not_supported`-flavored `ERRCODE_WRONG_OBJECT_TYPE` for a
/// relkind that cannot have its owner changed (tablecmds.c:16166-16171).
fn change_owner_wrong_object_type(relname: &str, _relkind: u8) -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("cannot change owner of relation \"{relname}\""))
        .finish(here("ATExecChangeOwner"))
        .map(|()| unreachable!())
}

/// `change_owner_fix_column_acls(relationOid, oldOwnerId, newOwnerId)`
/// (tablecmds.c:16312): scan the columns of the table and fix any non-null
/// column ACLs to reflect the new owner.
///
/// C runs a `systable_beginscan` over `pg_attribute` keyed on `attrelid`; this
/// crate has no systable-scan substrate, so we iterate the relation's live
/// columns by `attnum` (1..=natts) via `SearchSysCacheAttNum(ATTNUM)`, which
/// visits exactly the same rows. The per-row work (skip dropped columns, skip
/// SQL-null ACLs, `aclnewowner`-rewrite the rest, `CatalogTupleUpdate`) matches
/// the C body 1:1.
fn change_owner_fix_column_acls<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    old_owner_id: Oid,
    new_owner_id: Oid,
    target_rel: &rel::Relation<'mcx>,
) -> PgResult<()> {
    // attRelation = table_open(AttributeRelationId, RowExclusiveLock);
    let att_relation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    let natts = target_rel.rd_att.attrs.len() as i16;
    for attnum in 1..=natts {
        let Some(attribute_tuple) = SearchSysCacheAttNum(mcx, relation_oid, attnum)? else {
            continue;
        };

        // Ignore dropped columns.  if (att->attisdropped) continue;
        let (_dropped_datum, _) =
            SysCacheGetAttr(mcx, ATTNUM, &attribute_tuple, Anum_pg_attribute_attisdropped as i32)?;
        let attisdropped = match &_dropped_datum {
            Datum::ByVal(_) => _dropped_datum.as_bool(),
            _ => false,
        };
        if attisdropped {
            ReleaseSysCache(attribute_tuple);
            continue;
        }

        // aclDatum = heap_getattr(attributeTuple, Anum_pg_attribute_attacl, ...);
        // Null ACLs do not require changes.
        let (acl_datum, acl_is_null) =
            SysCacheGetAttr(mcx, ATTNUM, &attribute_tuple, Anum_pg_attribute_attacl as i32)?;
        if acl_is_null {
            ReleaseSysCache(attribute_tuple);
            continue;
        }

        // newAcl = aclnewowner(DatumGetAclP(aclDatum), oldOwnerId, newOwnerId);
        let new_acl_datum = match &acl_datum {
            Datum::ByRef(b) => {
                aclchk_seam::acl_change_owner_datum::call(mcx, &b[..], old_owner_id, new_owner_id)?
            }
            _ => {
                ReleaseSysCache(attribute_tuple);
                return ereport(ERROR)
                    .errmsg_internal("attacl is not a by-ref aclitem[] varlena")
                    .finish(here("change_owner_fix_column_acls"))
                    .map(|()| unreachable!());
            }
        };
        let new_acl_bytes = match new_acl_datum {
            Datum::ByRef(b) => b.to_vec(),
            _ => Vec::new(),
        };

        // repl_repl[attacl] = true; repl_val[attacl] = newAcl;
        // heap_modify_tuple → CatalogTupleUpdate.
        let row = PgAttributeUpdateRow {
            attacl: Some(Some(new_acl_bytes)),
            ..Default::default()
        };
        indexing_seam::catalog_tuple_update_pg_attribute::call(
            mcx,
            &att_relation,
            &attribute_tuple,
            &row,
        )?;

        ReleaseSysCache(attribute_tuple);
    }

    // table_close(attRelation, RowExclusiveLock); — RAII drop.
    drop(att_relation);
    Ok(())
}

/// `change_owner_recurse_to_sequences(relationOid, newOwnerId, lockmode)`
/// (tablecmds.c:16377): examine `pg_depend` for sequences dependent on the
/// table's serial columns (an AUTO/INTERNAL dependency on a column), and recurse
/// to change their ownership.
///
/// C runs a `systable_beginscan` over `pg_depend` keyed on
/// (refclassid=pg_class, refobjid=relationOid), keeping sequences with an
/// AUTO/INTERNAL dependency on a column (`refobjsubid != 0`). `getOwnedSequences`
/// (pg_depend.c) drives exactly that scan with the same filter (it also confirms
/// the dependent relkind is a sequence), returning the dependent sequence OIDs.
fn change_owner_recurse_to_sequences<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    new_owner_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let seq_oids = pg_depend_seam::getOwnedSequences::call(mcx, relation_oid)?;
    for &seq_oid in seq_oids.iter() {
        ATExecChangeOwner(mcx, seq_oid, new_owner_id, true, lockmode)?;
    }
    Ok(())
}
