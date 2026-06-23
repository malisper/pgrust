//! `src/backend/catalog/pg_attrdef.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_attrdef` relation (column default expressions).
//!
//! Ported 1:1 against the C, name-for-name. Catalog access mirrors the landed
//! `backend-catalog-pg-constraint`/`backend-catalog-pg-database` carrier idiom:
//! `table_open`/`close` guard scopes, `ScanKeyInit` + the genam `systable_*`
//! iterator, `heap_deform_tuple` of the scalar columns. The `pg_attrdef` INSERT
//! (OID allocation + `heap_form_tuple` + `CatalogTupleInsert`) crosses the
//! landed catalog-indexing engine seam `catalog_tuple_insert_pg_attrdef` as a
//! typed [`PgAttrdefInsertRow`]; the `pg_attribute.atthasdef` update/reset
//! crosses the syscache seams (precedent: `decrement_relchecks`). The `adbin`
//! `pg_node_tree` image is produced by `nodeToString` via the
//! `node_to_string_with_locations` seam (owner `outfuncs` not yet ported —
//! mirror-PG-and-panic until then).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, MemoryContext};

use types_catalog::catalog::RELATION_RELATION_ID;
use types_catalog::catalog_dependency::{
    InvalidObjectAddress, ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
};
use types_catalog::pg_attrdef::{
    Anum_pg_attrdef_adnum, Anum_pg_attrdef_adrelid, Anum_pg_attrdef_oid, AttrDefaultIndexId,
    AttrDefaultOidIndexId, AttrDefaultRelationId, FormData_pg_attrdef, PgAttrdefInsertRow,
};
use types_core::fmgr::{F_INT2EQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::{PgError, PgResult};
use ::nodes::nodes::Node;
use ::nodes::parsenodes::DropBehavior;
use rel::RelationData;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock};
use types_tuple::heaptuple::{Datum, FormedTuple};

use heaptuple::heap_deform_tuple;
use common_relation as relation;
use scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use dependency_seams as dependency_seams;
use dependency_seams::PERFORM_DELETION_INTERNAL;
use indexing_seams as indexing_seams;
use objectaccess_seams as objectaccess_seams;
use nodes_core_seams as nodes_seams;
use syscache_seams as syscache_seams;

/* ===========================================================================
 * scan-key builders + the systable scan iterator (mirror pg-constraint)
 * ========================================================================= */

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
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

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_INT2EQ,
/// Int16GetDatum(value))`.
fn int2_key<'mcx>(attno: AttrNumber, value: i16) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_INT2EQ,
        Datum::from_i16(value),
    )?;
    Ok(key)
}

/// `(Form_pg_attrdef) GETSTRUCT(tup)` — the fixed-width scalar columns of one
/// scanned `pg_attrdef` row (`oid`, `adrelid`, `adnum`). The trailing
/// variable-length `adbin` `pg_node_tree` column is not part of this scalar
/// projection.
fn form_pg_attrdef(values: &[Datum<'_>]) -> FormData_pg_attrdef {
    let col = |attno: i16| &values[attno as usize - 1];
    FormData_pg_attrdef {
        oid: col(Anum_pg_attrdef_oid).as_oid(),
        adrelid: col(Anum_pg_attrdef_adrelid).as_oid(),
        adnum: col(Anum_pg_attrdef_adnum).as_i16(),
    }
}

/// One scanned row: the heap TID (`tup->t_self`), the owned full tuple, and the
/// deformed scalar form.
struct AttrdefScanRow<'mcx> {
    htup: FormedTuple<'mcx>,
    form: FormData_pg_attrdef,
}

/// `systable_beginscan` + `while ((tup = systable_getnext(scan)))` loop +
/// `systable_endscan` (the genam iterator). `body` returning `Ok(false)` stops
/// early (the C `break`). Each row's deformed scalar columns + owned tuple land
/// in a per-iteration scratch context.
fn systable_scan_foreach(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&AttrdefScanRow<'_>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("pg_attrdef scan row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: mcx::PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        let form = form_pg_attrdef(&values);
        let row = AttrdefScanRow {
            htup: tup,
            form,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
    }
    scan.end()
}

/* ===========================================================================
 * StoreAttrDefault (pg_attrdef.c)
 * ========================================================================= */

/// Store a default expression for column `attnum` of relation `rel`. Returns
/// the OID of the new `pg_attrdef` tuple.
pub fn StoreAttrDefault(
    mcx: Mcx<'_>,
    rel_id: Oid,
    attnum: AttrNumber,
    expr: &Node<'_>,
    is_internal: bool,
) -> PgResult<Oid> {
    let adrel_ctx = MemoryContext::new("pg_attrdef");
    let adrel = table::table_open(adrel_ctx.mcx(), AttrDefaultRelationId, RowExclusiveLock)?;

    /*
     * Flatten expression to string form for storage.
     */
    let adbin = nodes_seams::node_to_string_with_locations::call(mcx, expr)?;

    /*
     * Make the pg_attrdef entry. The owner allocates the OID
     * (GetNewOidWithIndex), forms the row, and inserts it
     * (CatalogTupleInsert), returning the freshly-allocated attrdefOid.
     */
    let row = PgAttrdefInsertRow {
        adrelid: rel_id,
        adnum: attnum,
        adbin: adbin.as_str().to_string(),
    };
    let attrdefOid =
        indexing_seams::catalog_tuple_insert_pg_attrdef::call(adrel_ctx.mcx(), &adrel, &row)?;

    let defobject = ObjectAddress {
        classId: AttrDefaultRelationId,
        objectId: attrdefOid,
        objectSubId: 0,
    };

    adrel.close(RowExclusiveLock)?;

    /* now can free some of the stuff allocated above (Rust drops it) */

    /*
     * Update the pg_attribute entry for the column to show that a default
     * exists, and recover the column's attgenerated.
     */
    let attgenerated = match syscache_seams::set_attribute_has_default::call(rel_id, attnum)? {
        Some(g) => g,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for attribute {attnum} of relation {rel_id}"
            )));
        }
    };

    /*
     * Make a dependency so that the pg_attrdef entry goes away if the column
     * (or whole table) is deleted.  In the case of a generated column, make it
     * an internal dependency to prevent the default expression from being
     * deleted separately.
     */
    let colobject = ObjectAddress {
        classId: RELATION_RELATION_ID,
        objectId: rel_id,
        objectSubId: attnum as i32,
    };

    dependency_seams::record_dependency_on::call(
        defobject,
        colobject,
        if attgenerated != 0 {
            DEPENDENCY_INTERNAL
        } else {
            DEPENDENCY_AUTO
        },
    )?;

    /*
     * Record dependencies on objects used in the expression, too.
     */
    dependency_seams::record_dependency_on_single_rel_expr::call(
        defobject,
        expr,
        rel_id,
        DEPENDENCY_NORMAL,
        DEPENDENCY_NORMAL,
        false,
    )?;

    /*
     * Post creation hook for attribute defaults.
     */
    objectaccess_seams::invoke_object_post_create_hook_arg::call(
        AttrDefaultRelationId,
        rel_id,
        attnum as i32,
        is_internal,
    )?;

    Ok(attrdefOid)
}

/* ===========================================================================
 * RemoveAttrDefault (pg_attrdef.c)
 * ========================================================================= */

/// If the specified relation/attribute has a default, remove it. (If no
/// default, raise error if `complain` is true, else return quietly.)
pub fn RemoveAttrDefault(
    relid: Oid,
    attnum: AttrNumber,
    behavior: DropBehavior,
    complain: bool,
    internal: bool,
) -> PgResult<()> {
    let mut found = false;

    let attrdef_ctx = MemoryContext::new("pg_attrdef");
    let attrdef_rel = table::table_open(attrdef_ctx.mcx(), AttrDefaultRelationId, RowExclusiveLock)?;

    let scankeys = [
        oid_key(Anum_pg_attrdef_adrelid, relid)?,
        int2_key(Anum_pg_attrdef_adnum, attnum)?,
    ];

    /* There should be at most one matching tuple, but we loop anyway */
    let mut pending: Vec<Oid> = Vec::new();
    systable_scan_foreach(&attrdef_rel, AttrDefaultIndexId, &scankeys, |row| {
        /*
         * performDeletion can recurse into RemoveAttrDefaultById, which re-opens
         * pg_attrdef with RowExclusiveLock; defer it until the scan over
         * attrdef_rel is closed (the C calls performDeletion inside the scan
         * loop — there can be at most one matching tuple, so deferral is
         * behaviour-equivalent).
         */
        pending.push(row.form.oid);
        found = true;
        Ok(true)
    })?;

    for objectId in pending {
        dependency_seams::perform_deletion::call(
            AttrDefaultRelationId,
            objectId,
            0,
            behavior,
            if internal { PERFORM_DELETION_INTERNAL } else { 0 },
        )?;
    }

    attrdef_rel.close(RowExclusiveLock)?;

    if complain && !found {
        return Err(PgError::error(format!(
            "could not find attrdef tuple for relation {relid} attnum {attnum}"
        )));
    }

    Ok(())
}

/* ===========================================================================
 * RemoveAttrDefaultById (pg_attrdef.c)
 * ========================================================================= */

/// Remove a `pg_attrdef` entry specified by OID. This is the guts of
/// attribute-default removal. Note it should be called via `performDeletion`,
/// not directly.
pub fn RemoveAttrDefaultById(attrdefId: Oid) -> PgResult<()> {
    /* Grab an appropriate lock on the pg_attrdef relation */
    let attrdef_ctx = MemoryContext::new("pg_attrdef");
    let attrdef_rel = table::table_open(attrdef_ctx.mcx(), AttrDefaultRelationId, RowExclusiveLock)?;

    /* Find the pg_attrdef tuple */
    let scankeys = [oid_key(Anum_pg_attrdef_oid, attrdefId)?];

    let mut found: Option<(Oid, i16, types_tuple::heaptuple::ItemPointerData)> = None;
    systable_scan_foreach(&attrdef_rel, AttrDefaultOidIndexId, &scankeys, |row| {
        found = Some((row.form.adrelid, row.form.adnum, row.htup.tuple.t_self));
        Ok(false)
    })?;

    let (myrelid, myattnum, tid) = match found {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "could not find tuple for attrdef {attrdefId}"
            )));
        }
    };

    /* Get an exclusive lock on the relation owning the attribute */
    let myrel_ctx = MemoryContext::new("pg_attrdef myrel");
    let myrel = relation::relation_open(myrel_ctx.mcx(), myrelid, AccessExclusiveLock)?;

    /* Now we can delete the pg_attrdef row */
    indexing_seams::catalog_tuple_delete::call(&attrdef_rel, tid)?;

    attrdef_rel.close(RowExclusiveLock)?;

    /* Fix the pg_attribute row */
    if !syscache_seams::clear_attribute_has_default::call(myrelid, myattnum)? {
        /* shouldn't happen */
        return Err(PgError::error(format!(
            "cache lookup failed for attribute {myattnum} of relation {myrelid}"
        )));
    }

    /*
     * Our update of the pg_attribute row will force a relcache rebuild, so
     * there's nothing else to do here.
     */

    /* Keep lock on attribute's rel until end of xact */
    myrel.close(NoLock)?;

    Ok(())
}

/* ===========================================================================
 * GetAttrDefaultOid (pg_attrdef.c)
 * ========================================================================= */

/// Get the `pg_attrdef` OID of the default expression for a column identified
/// by relation OID and column number. Returns `InvalidOid` if there is no such
/// `pg_attrdef` entry.
pub fn GetAttrDefaultOid(mcx: Mcx<'_>, relid: Oid, attnum: AttrNumber) -> PgResult<Oid> {
    let mut result = InvalidOid;

    let attrdef = table::table_open(mcx, AttrDefaultRelationId, AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_attrdef_adrelid, relid)?,
        int2_key(Anum_pg_attrdef_adnum, attnum)?,
    ];

    systable_scan_foreach(&attrdef, AttrDefaultIndexId, &keys, |row| {
        result = row.form.oid;
        Ok(false)
    })?;

    attrdef.close(AccessShareLock)?;

    Ok(result)
}

/* ===========================================================================
 * GetAttrDefaultColumnAddress (pg_attrdef.c)
 * ========================================================================= */

/// Given a `pg_attrdef` OID, return the relation OID and column number of the
/// owning column (represented as an `ObjectAddress` for convenience). Returns
/// `InvalidObjectAddress` if there is no such `pg_attrdef` entry.
pub fn GetAttrDefaultColumnAddress(mcx: Mcx<'_>, attrdefoid: Oid) -> PgResult<ObjectAddress> {
    let mut result = InvalidObjectAddress;

    let attrdef = table::table_open(mcx, AttrDefaultRelationId, AccessShareLock)?;
    let skey = [oid_key(Anum_pg_attrdef_oid, attrdefoid)?];

    systable_scan_foreach(&attrdef, AttrDefaultOidIndexId, &skey, |row| {
        result.classId = RELATION_RELATION_ID;
        result.objectId = row.form.adrelid;
        result.objectSubId = row.form.adnum as i32;
        Ok(false)
    })?;

    attrdef.close(AccessShareLock)?;

    Ok(result)
}

/* ===========================================================================
 * Inward seam installation
 * ========================================================================= */

/// Inward adapter for the `attr_default_column` syscache seam: a scratch-`Mcx`
/// wrapper over [`GetAttrDefaultColumnAddress`] projected to the owning column's
/// `(adrelid, adnum)`. `Ok(None)` mirrors the C `InvalidObjectAddress` return.
fn attr_default_column(attrdefoid: Oid) -> PgResult<Option<(Oid, i16)>> {
    let scratch = MemoryContext::new("syscache attr default column");
    let addr = GetAttrDefaultColumnAddress(scratch.mcx(), attrdefoid)?;
    if addr.classId == InvalidOid {
        Ok(None)
    } else {
        Ok(Some((addr.objectId, addr.objectSubId as i16)))
    }
}

/// Inward adapter for the `get_attr_default_oid` heap seam: a scratch-`Mcx`
/// wrapper over [`GetAttrDefaultOid`] (the function lives in `pg_attrdef.c`,
/// the seam is declared in `backend-catalog-heap-seams` because its sole caller
/// is `get_object_address`'s `OBJECT_DEFAULT` arm).
fn get_attr_default_oid(relid: Oid, attnum: AttrNumber) -> PgResult<Oid> {
    let scratch = MemoryContext::new("heap get_attr_default_oid");
    GetAttrDefaultOid(scratch.mcx(), relid, attnum)
}

pub fn init_seams() {
    use pg_attrdef_seams as seams;

    seams::RemoveAttrDefaultById::set(RemoveAttrDefaultById);
    syscache_seams::attr_default_column::set(attr_default_column);
    heap_seams::get_attr_default_oid::set(get_attr_default_oid);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_oids_match_postgres() {
        assert_eq!(AttrDefaultRelationId, 2604);
        assert_eq!(AttrDefaultIndexId, 2656);
        assert_eq!(AttrDefaultOidIndexId, 2657);
        assert_eq!(Anum_pg_attrdef_oid, 1);
        assert_eq!(Anum_pg_attrdef_adrelid, 2);
        assert_eq!(Anum_pg_attrdef_adnum, 3);
        assert_eq!(F_OIDEQ, 184);
        assert_eq!(F_INT2EQ, 63);
    }
}
