#![allow(non_snake_case)]
// `CommentObject` faithfully takes/returns the same parameter set as the C
// callee; `PgError` is a large error type shared across the whole tree, so
// boxing it would diverge from every sibling crate's Result shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/comment.c` — `COMMENT ON`.
//!
//! Faithful port of comment.c: add, replace, or remove the `pg_description` /
//! `pg_shdescription` tuple holding an object's comment. Every C function is
//! ported here branch-for-branch:
//!
//!  * [`CommentObject`] — the COMMENT-ON driver: the COMMENT ON DATABASE dump
//!    work-around, `get_object_address` + ownership check, the per-`ObjectType`
//!    integrity check (only `OBJECT_COLUMN` has one), the shared-vs-local
//!    catalog dispatch, finishing with `relation_close`;
//!  * [`CreateComments`] / [`CreateSharedComments`] — upsert/delete a comment;
//!  * [`DeleteComments`] / [`DeleteSharedComments`] — remove comments for an
//!    object (used when the object itself is dropped);
//!  * [`GetComment`] — fetch an object's comment, or `None`.
//!
//! The control flow is identical to the C, including the real catalog reads and
//! writes: `table_open(DescriptionRelationId, …)` over a real
//! [`types_rel::Relation`], the `systable` index scans
//! (`systable_beginscan`/`getnext`/`endscan` on `DescriptionObjIndexId`), and
//! the `CatalogTupleDelete` / `heap_modify_tuple`+`CatalogTupleUpdate` /
//! `heap_form_tuple`+`CatalogTupleInsert` mutations.
//!
//! `get_object_address` / `check_object_ownership` (objectaddress.c) are called
//! through the canonical [`backend_catalog_objectaddress_seams`].
//! `errdetail_relkind_not_supported` is a real ported function
//! (`backend-catalog-pg-class`). `GetUserId` is the canonical miscinit seam.
//! Only the project-wide varlena/`Datum` conversions
//! (`CStringGetTextDatum`/`TextDatumGetCString`) cross the
//! [`backend_commands_comment_seams`] boundary.

use backend_access_common_heaptuple::{heap_form_tuple, heap_getattr, heap_modify_tuple};
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::{table_close, table_open};
use backend_catalog_indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};
use backend_catalog_objectaddress_seams as oaddr;
use backend_commands_comment_seams as seam;
use backend_utils_error::ereport;
use backend_utils_init_miscinit_seams::get_user_id;
use mcx::Mcx;
use types_catalog::catalog::{
    DESCRIPTION_OBJ_INDEX_ID, DESCRIPTION_RELATION_ID, SHARED_DESCRIPTION_OBJ_INDEX_ID,
    SHARED_DESCRIPTION_RELATION_ID,
};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::{Oid, OidIsValid};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::parsenodes::{OBJECT_COLUMN, OBJECT_DATABASE, OBJECT_ROLE, OBJECT_TABLESPACE};
use types_parsenodes::CommentStmt;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::Datum;

/*
 * pg_description / pg_shdescription column counts and 1-based attribute numbers
 * (catalog/pg_description_d.h, catalog/pg_shdescription_d.h). comment.c indexes
 * the values[]/nulls[]/replaces[] arrays by `Anum_* - 1`; the in-crate upsert
 * reproduces that, so these are transcribed verbatim.
 */
const NATTS_PG_DESCRIPTION: usize = 4;
const ANUM_PG_DESCRIPTION_OBJOID: usize = 1;
const ANUM_PG_DESCRIPTION_CLASSOID: usize = 2;
const ANUM_PG_DESCRIPTION_OBJSUBID: usize = 3;
const ANUM_PG_DESCRIPTION_DESCRIPTION: usize = 4;

const NATTS_PG_SHDESCRIPTION: usize = 3;
const ANUM_PG_SHDESCRIPTION_OBJOID: usize = 1;
const ANUM_PG_SHDESCRIPTION_CLASSOID: usize = 2;
const ANUM_PG_SHDESCRIPTION_DESCRIPTION: usize = 3;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/comment.c", 0, funcname)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_INT4EQ,
/// Int32GetDatum(value))`.
fn int4_key<'mcx>(attno: i16, value: i32) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Datum::from_i32(value),
    )?;
    Ok(key)
}

/// `CommentObject` — add the comment in `stmt` into `pg_description` for the
/// object the SQL command names. Returns the resolved [`ObjectAddress`].
///
/// comment.c:39-131.
pub fn CommentObject<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CommentStmt,
) -> PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    // ObjectAddress address = InvalidObjectAddress;
    let mut address = types_catalog::catalog_dependency::InvalidObjectAddress;

    /*
     * When loading a dump, we may see a COMMENT ON DATABASE for the old name
     * of the database. Erroring out would prevent pg_restore from completing
     * (which is really pg_restore's fault, but for now we will work around
     * the problem here). Consensus is that the best fix is to treat wrong
     * database name as a WARNING not an ERROR; hence, the following special
     * case.
     */
    if stmt.objtype == OBJECT_DATABASE {
        // char *database = strVal(stmt->object);
        let database = stmt
            .object
            .as_deref()
            .and_then(|o| o.as_string())
            .and_then(|s| s.sval.as_deref())
            .expect("CommentObject: OBJECT_DATABASE object must be a String value node")
            .to_string();

        // get_database_oid(database, true) — missing_ok, so InvalidOid (not an
        // error) when the database is gone.
        if !OidIsValid(
            backend_commands_dbcommands_seams::get_database_oid::call(&database, true)?,
        ) {
            // ereport(WARNING, errmsg("database \"%s\" does not exist", database))
            ereport(WARNING)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{database}\" does not exist"))
                .finish(here("CommentObject"))?;
            return Ok(address);
        }
    }

    /*
     * Translate the parser representation that identifies this object into an
     * ObjectAddress. get_object_address() will throw an error if the object
     * does not exist, and will also acquire a lock on the target to guard
     * against concurrent DROP operations.
     */
    // get_object_address(stmt->objtype, stmt->object, &relation,
    //                     ShareUpdateExclusiveLock, false);
    let object = stmt
        .object
        .as_deref()
        .expect("CommentObject: stmt->object must be set");
    let resolved = oaddr::get_object_address::call(
        mcx,
        stmt.objtype,
        object,
        ShareUpdateExclusiveLock,
        false,
    )?;
    address = resolved.address;
    let relation = resolved.relation;

    /* Require ownership of the target object. */
    // check_object_ownership(GetUserId(), stmt->objtype, address, stmt->object,
    //                        relation);
    oaddr::check_object_ownership::call(
        get_user_id::call(),
        stmt.objtype,
        address,
        object,
        relation.as_ref(),
    )?;

    /* Perform other integrity checks as needed. */
    #[allow(clippy::single_match)]
    match stmt.objtype {
        OBJECT_COLUMN => {
            /*
             * Allow comments only on columns of tables, views, materialized
             * views, composite types, and foreign tables (which are the only
             * relkinds for which pg_dump will dump per-column comments). In
             * particular we wish to disallow comments on index columns, because
             * the naming of an index's columns may change across PG versions, so
             * dumping per-column comments could create reload failures.
             */
            // The C dereferences `relation` here unconditionally — for an
            // OBJECT_COLUMN, get_object_address always opened the table.
            let rel = relation.as_ref().expect(
                "CommentObject: OBJECT_COLUMN must have opened a relation (get_object_address)",
            );
            // relation->rd_rel->relkind
            let relkind = rel.rd_rel.relkind;
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_VIEW
                && relkind != RELKIND_MATVIEW
                && relkind != RELKIND_COMPOSITE_TYPE
                && relkind != RELKIND_FOREIGN_TABLE
                && relkind != RELKIND_PARTITIONED_TABLE
            {
                // RelationGetRelationName(relation)
                let relname = rel.name().to_string();
                let detail = backend_catalog_pg_class::errdetail_relkind_not_supported(relkind)?;
                ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("cannot set comment on relation \"{relname}\""))
                    .errdetail(detail)
                    .finish(here("CommentObject"))?;
            }
        }
        _ => {}
    }

    /*
     * Databases, tablespaces, and roles are cluster-wide objects, so any
     * comments on those objects are recorded in the shared pg_shdescription
     * catalog. Comments on all other objects are recorded in pg_description.
     */
    if stmt.objtype == OBJECT_DATABASE
        || stmt.objtype == OBJECT_TABLESPACE
        || stmt.objtype == OBJECT_ROLE
    {
        CreateSharedComments(mcx, address.objectId, address.classId, comment_str(stmt))?;
    } else {
        CreateComments(
            mcx,
            address.objectId,
            address.classId,
            address.objectSubId,
            comment_str(stmt),
        )?;
    }

    /*
     * If get_object_address() opened the relation for us, we close it to keep
     * the reference count correct - but we retain any locks acquired by
     * get_object_address() until commit time, to guard against concurrent
     * activity.
     */
    if let Some(rel) = relation {
        // relation_close(relation, NoLock);
        rel.close(NoLock)?;
    }

    Ok(address)
}

/// `CreateComments` — create/replace/delete a `pg_description` comment.
///
/// Inserts a new pg_description tuple, or replaces an existing one with the
/// same key. If `comment` is `None`/empty (the C null/empty-string case),
/// instead delete any existing comment for the specified key.
///
/// comment.c:142-226.
pub fn CreateComments<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    subid: i32,
    comment: Option<&str>,
) -> PgResult<()> {
    /* Reduce empty-string to NULL case (comment.c:156). */
    let comment = reduce_empty(comment);

    /*
     * Prepare to form or update a tuple, if necessary (comment.c:159-171).
     * When `comment` is None this stays unused — like the C, which only fills
     * the arrays in the `comment != NULL` branch.
     */
    let mut values: [Datum<'mcx>; NATTS_PG_DESCRIPTION] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NATTS_PG_DESCRIPTION];
    let mut replaces = [false; NATTS_PG_DESCRIPTION];
    if let Some(comment) = comment {
        for i in 0..NATTS_PG_DESCRIPTION {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[ANUM_PG_DESCRIPTION_OBJOID - 1] = Datum::from_oid(oid);
        values[ANUM_PG_DESCRIPTION_CLASSOID - 1] = Datum::from_oid(classoid);
        values[ANUM_PG_DESCRIPTION_OBJSUBID - 1] = Datum::from_i32(subid);
        values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1] =
            seam::cstring_get_text_datum::call(mcx, comment)?;
    }

    /* Use the index to search for a matching old tuple (comment.c:173-191). */
    let skey = [
        oid_key(ANUM_PG_DESCRIPTION_OBJOID as i16, oid)?,
        oid_key(ANUM_PG_DESCRIPTION_CLASSOID as i16, classoid)?,
        int4_key(ANUM_PG_DESCRIPTION_OBJSUBID as i16, subid)?,
    ];

    let description = table_open(mcx, DESCRIPTION_RELATION_ID, RowExclusiveLock)?;

    let mut scan =
        genam::systable_beginscan::call(&description, DESCRIPTION_OBJ_INDEX_ID, true, None, &skey)?;

    let mut inserted_or_updated = false;
    // while ((oldtuple = systable_getnext(sd)) != NULL)
    if let Some(oldtuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        /* Found the old tuple, so delete or update it (comment.c:193-204). */
        if comment.is_none() {
            CatalogTupleDelete(mcx, &description, oldtuple.tuple.t_self)?;
        } else {
            // newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(description),
            //                              values, nulls, replaces);
            // CatalogTupleUpdate(description, &oldtuple->t_self, newtuple);
            let mut newtuple = heap_modify_tuple(
                mcx,
                &oldtuple,
                &description.rd_att,
                &values,
                &nulls,
                &replaces,
            )?;
            CatalogTupleUpdate(mcx, &description, oldtuple.tuple.t_self, &mut newtuple)?;
            inserted_or_updated = true;
        }
        // break; — Assume there can be only one match.
    }

    scan.end()?;

    /* If we didn't find an old tuple, insert a new one (comment.c:213-218). */
    if !inserted_or_updated && comment.is_some() {
        let mut newtuple = heap_form_tuple(mcx, &description.rd_att, &values, &nulls)?;
        CatalogTupleInsert(mcx, &description, &mut newtuple)?;
    }

    /* Done (comment.c:225). */
    table_close(description, NoLock)
}

/// `CreateSharedComments` — create/replace/delete a `pg_shdescription` comment.
///
/// comment.c:237-316. Same shape as [`CreateComments`] with two scan keys and
/// no objsubid column.
pub fn CreateSharedComments<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    comment: Option<&str>,
) -> PgResult<()> {
    /* Reduce empty-string to NULL case (comment.c:251). */
    let comment = reduce_empty(comment);

    /* Prepare to form or update a tuple, if necessary (comment.c:254-265). */
    let mut values: [Datum<'mcx>; NATTS_PG_SHDESCRIPTION] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NATTS_PG_SHDESCRIPTION];
    let mut replaces = [false; NATTS_PG_SHDESCRIPTION];
    if let Some(comment) = comment {
        for i in 0..NATTS_PG_SHDESCRIPTION {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[ANUM_PG_SHDESCRIPTION_OBJOID - 1] = Datum::from_oid(oid);
        values[ANUM_PG_SHDESCRIPTION_CLASSOID - 1] = Datum::from_oid(classoid);
        values[ANUM_PG_SHDESCRIPTION_DESCRIPTION - 1] =
            seam::cstring_get_text_datum::call(mcx, comment)?;
    }

    /* Use the index to search for a matching old tuple (comment.c:267-281). */
    let skey = [
        oid_key(ANUM_PG_SHDESCRIPTION_OBJOID as i16, oid)?,
        oid_key(ANUM_PG_SHDESCRIPTION_CLASSOID as i16, classoid)?,
    ];

    let shdescription = table_open(mcx, SHARED_DESCRIPTION_RELATION_ID, RowExclusiveLock)?;

    let mut scan = genam::systable_beginscan::call(
        &shdescription,
        SHARED_DESCRIPTION_OBJ_INDEX_ID,
        true,
        None,
        &skey,
    )?;

    let mut inserted_or_updated = false;
    if let Some(oldtuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        /* Found: delete or update it (comment.c:283-294). */
        if comment.is_none() {
            CatalogTupleDelete(mcx, &shdescription, oldtuple.tuple.t_self)?;
        } else {
            let mut newtuple = heap_modify_tuple(
                mcx,
                &oldtuple,
                &shdescription.rd_att,
                &values,
                &nulls,
                &replaces,
            )?;
            CatalogTupleUpdate(mcx, &shdescription, oldtuple.tuple.t_self, &mut newtuple)?;
            inserted_or_updated = true;
        }
        // break;
    }

    scan.end()?;

    /* If we didn't find an old tuple, insert a new one (comment.c:303-308). */
    if !inserted_or_updated && comment.is_some() {
        let mut newtuple = heap_form_tuple(mcx, &shdescription.rd_att, &values, &nulls)?;
        CatalogTupleInsert(mcx, &shdescription, &mut newtuple)?;
    }

    /* Done (comment.c:315). */
    table_close(shdescription, NoLock)
}

/// `DeleteComments` — remove `pg_description` comments for an object.
///
/// If `subid` is nonzero then only comments matching it will be removed. If
/// `subid` is zero, all comments matching the oid/classoid will be removed
/// (this corresponds to deleting a whole object).
///
/// comment.c:325-368.
pub fn DeleteComments(oid: Oid, classoid: Oid, subid: i32) -> PgResult<()> {
    let scratch = mcx::MemoryContext::new("DeleteComments");
    let mcx = scratch.mcx();

    /*
     * Build the scan keys: always {objoid, classoid}; add the objsubid key only
     * when `subid != 0` (comment.c:336-352, where nkeys becomes 3).
     */
    let key_objoid = oid_key(ANUM_PG_DESCRIPTION_OBJOID as i16, oid)?;
    let key_classoid = oid_key(ANUM_PG_DESCRIPTION_CLASSOID as i16, classoid)?;
    let key_objsubid = int4_key(ANUM_PG_DESCRIPTION_OBJSUBID as i16, subid)?;

    let description = table_open(mcx, DESCRIPTION_RELATION_ID, RowExclusiveLock)?;

    // systable_beginscan(description, DescriptionObjIndexId, true, NULL, nkeys, skey)
    let mut scan = if subid != 0 {
        let skey = [key_objoid, key_classoid, key_objsubid];
        genam::systable_beginscan::call(&description, DESCRIPTION_OBJ_INDEX_ID, true, None, &skey)?
    } else {
        let skey = [key_objoid, key_classoid];
        genam::systable_beginscan::call(&description, DESCRIPTION_OBJ_INDEX_ID, true, None, &skey)?
    };

    // while ((oldtuple = systable_getnext(sd)) != NULL)
    //     CatalogTupleDelete(description, &oldtuple->t_self);
    while let Some(oldtuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        CatalogTupleDelete(mcx, &description, oldtuple.tuple.t_self)?;
    }

    /* Done (comment.c:366-367) — closes holding RowExclusiveLock. */
    scan.end()?;
    table_close(description, RowExclusiveLock)
}

/// `DeleteSharedComments` — remove `pg_shdescription` comments for a shared
/// object.
///
/// comment.c:373-404. Always two scan keys `{objoid, classoid}`.
pub fn DeleteSharedComments(oid: Oid, classoid: Oid) -> PgResult<()> {
    let scratch = mcx::MemoryContext::new("DeleteSharedComments");
    let mcx = scratch.mcx();

    let skey = [
        oid_key(ANUM_PG_SHDESCRIPTION_OBJOID as i16, oid)?,
        oid_key(ANUM_PG_SHDESCRIPTION_CLASSOID as i16, classoid)?,
    ];

    let shdescription = table_open(mcx, SHARED_DESCRIPTION_RELATION_ID, RowExclusiveLock)?;

    let mut scan = genam::systable_beginscan::call(
        &shdescription,
        SHARED_DESCRIPTION_OBJ_INDEX_ID,
        true,
        None,
        &skey,
    )?;

    while let Some(oldtuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        CatalogTupleDelete(mcx, &shdescription, oldtuple.tuple.t_self)?;
    }

    /* Done (comment.c:402-403) — closes holding RowExclusiveLock. */
    scan.end()?;
    table_close(shdescription, RowExclusiveLock)
}

/// `GetComment` — get the comment for an object, or `None` if not found.
///
/// comment.c:409-459.
pub fn GetComment<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    subid: i32,
) -> PgResult<Option<String>> {
    /* Use the index to search for a matching old tuple (comment.c:421-432). */
    let skey = [
        oid_key(ANUM_PG_DESCRIPTION_OBJOID as i16, oid)?,
        oid_key(ANUM_PG_DESCRIPTION_CLASSOID as i16, classoid)?,
        int4_key(ANUM_PG_DESCRIPTION_OBJSUBID as i16, subid)?,
    ];

    let description = table_open(mcx, DESCRIPTION_RELATION_ID, AccessShareLock)?;
    // tupdesc = RelationGetDescr(description);
    let tupdesc = &description.rd_att;

    let mut scan =
        genam::systable_beginscan::call(&description, DESCRIPTION_OBJ_INDEX_ID, true, None, &skey)?;

    /* comment = NULL; (comment.c:440) */
    let mut comment: Option<String> = None;

    // while ((tuple = systable_getnext(sd)) != NULL)
    if let Some(tuple) = genam::systable_getnext::call(mcx, scan.desc_mut())? {
        // value = heap_getattr(tuple, Anum_pg_description_description, tupdesc, &isnull);
        let (value, isnull) =
            heap_getattr(mcx, &tuple, ANUM_PG_DESCRIPTION_DESCRIPTION as i32, tupdesc)?;
        if !isnull {
            comment = Some(seam::text_datum_get_cstring::call(value)?);
        }
        // break; — Assume there can be only one match.
    }

    scan.end()?;

    /* Done (comment.c:456). */
    table_close(description, AccessShareLock)?;

    Ok(comment)
}

/// Borrow `stmt.comment` (the C `const char *comment` argument) as
/// `Option<&str>`. The empty-string -> NULL reduction is performed inside
/// [`CreateComments`] / [`CreateSharedComments`], matching the C.
fn comment_str(stmt: &CommentStmt) -> Option<&str> {
    stmt.comment.as_deref()
}

/// `if (comment != NULL && strlen(comment) == 0) comment = NULL;`
/// (comment.c:156, 251) — fold an empty (zero-length) comment to `None`.
fn reduce_empty(comment: Option<&str>) -> Option<&str> {
    match comment {
        Some("") => None,
        other => other,
    }
}

/// Install this crate's seams.
///
/// The catalog read/write control flow runs entirely in-crate over real
/// relations, so the only outward seams are the project-wide varlena/`Datum`
/// conversions (installed by their owners). Here we install the inward
/// [`DeleteComments`] boundary (dependency.c calls it on object drop) and the
/// collationcmds `create_comment` adapter.
pub fn init_seams() {
    backend_commands_comment_seams::DeleteComments::set(DeleteComments);

    // user.c DROP ROLE: `DeleteSharedComments(roleid, AuthIdRelationId)`.
    backend_commands_user_seams::delete_shared_comments::set(|roleid| {
        DeleteSharedComments(roleid, types_core::AUTH_ID_RELATION_ID)
    });

    // collationcmds.c attaches an ICU display-name comment to the collation it
    // imports: `CreateComments(collid, CollationRelationId, 0, comment)`.
    backend_commands_collationcmds_seams::create_comment::set(|collid, comment| {
        let scratch = mcx::MemoryContext::new("create_comment");
        CreateComments(
            scratch.mcx(),
            collid,
            types_catalog::catalog::COLLATION_RELATION_ID,
            0,
            Some(comment),
        )
    });
}

#[cfg(test)]
mod tests;
