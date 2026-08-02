//! ALTER EXTENSION ADD/DROP (extension.c ExecAlterExtensionContentsStmt +
//! ExecAlterExtensionContentsRecurse), incl. the dependent-object recursion
//! (array/multirange/rowtype members) and extconfig removal. pg_init_privs
//! record/remove is a no-op repo-wide (see aclchk grant.rs).

use mcx::Mcx;
use types_core::{
    Oid, OidIsValid, EXTENSION_RELATION_ID, NAMESPACE_RELATION_ID, OIDOID, RELATION_RELATION_ID,
    TEXTOID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT,
};
use types_nodes::parsenodes::ObjectType;
use types_nodes::rawnodes::AlterExtensionContentsStmt;
use types_nodes::Node;
use types_rel::{AccessShareLock, RowExclusiveLock, ShareUpdateExclusiveLock};

use datum::Datum;
use pg_depend::{DependencyType, ObjectAddress};

use crate::alter::oid_key;
use crate::{
    Anum_pg_extension_extcondition, Anum_pg_extension_extconfig, Anum_pg_extension_oid,
    ExtensionOidIndexId, Natts_pg_extension,
};

/// `ExecAlterExtensionContentsStmt`.
pub fn ExecAlterExtensionContentsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterExtensionContentsStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let extname = stmt.extname.expect("grammar always supplies extname");

    match stmt.objtype {
        ObjectType::OBJECT_DATABASE
        | ObjectType::OBJECT_EXTENSION
        | ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_PUBLICATION
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABLESPACE => {
            return Err(Box::new(
                PgError::error("cannot add an object of this type to an extension")
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
        _ => {}
    }

    // Shared lock on the extension so it can't be dropped concurrently.
    let (extension, ext_rel) = objectaddress_seams::get_object_address::call(
        mcx,
        ObjectType::OBJECT_EXTENSION,
        Node::mk_string(mcx, extname)?,
        AccessShareLock,
        false,
    )?;
    debug_assert!(ext_rel.is_none());

    if !aclchk::object_ownercheck(EXTENSION_RELATION_ID, extension.objectId, miscinit::GetUserId())?
    {
        return Err(Box::new(
            PgError::error(format!("must be owner of extension {extname}"))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    let object_node = stmt.object.expect("grammar always supplies the member object");
    let (object, relation) = objectaddress_seams::get_object_address::call(
        mcx,
        stmt.objtype,
        object_node,
        ShareUpdateExclusiveLock,
        false,
    )?;
    debug_assert!(object.objectSubId == 0);

    objectaddress_seams::check_object_ownership::call(
        mcx,
        miscinit::GetUserId(),
        stmt.objtype,
        object,
        object_node,
        relation.as_ref(),
    )?;

    let extension =
        ObjectAddress { classId: extension.classId, objectId: extension.objectId, objectSubId: 0 };
    let object =
        ObjectAddress { classId: object.classId, objectId: object.objectId, objectSubId: 0 };
    alter_contents_recurse(mcx, stmt, extname, &extension, &object)?;

    // C: InvokeObjectPostAlterHook (no object_access hooks in this port),
    // then relation_close(NoLock) — the Relation guard drop is that close.
    drop(relation);

    Ok(extension)
}

/// `ExecAlterExtensionContentsRecurse`.
fn alter_contents_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterExtensionContentsStmt<'mcx>,
    extname: &str,
    extension: &ObjectAddress,
    object: &ObjectAddress,
) -> PgResult<()> {
    let old_extension = pg_depend::getExtensionOfObject(mcx, object.classId, object.objectId)?;

    if stmt.action > 0 {
        if old_extension != 0 {
            let desc = objectaddress_seams::get_object_description::call(
                mcx,
                object.classId,
                object.objectId,
                0,
                false,
            )?
            .unwrap_or_else(|| "object".into());
            let owner = crate::get_extension_name(mcx, old_extension)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!("{desc} is already a member of extension \"{owner}\""))
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if object.classId == NAMESPACE_RELATION_ID
            && object.objectId == crate::get_extension_schema(extension.objectId)?
        {
            let nsp = lsyscache::get_namespace_name(mcx, object.objectId)?
                .map(|s| s.to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "cannot add schema \"{nsp}\" to extension \"{extname}\" because the schema contains the extension"
                ))
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        pg_depend::recordDependencyOn(mcx, object, extension, DependencyType::Extension)?;
        // recordExtObjInitPriv: pg_init_privs is a repo-wide no-op.
    } else {
        if old_extension != extension.objectId {
            let desc = objectaddress_seams::get_object_description::call(
                mcx,
                object.classId,
                object.objectId,
                0,
                false,
            )?
            .unwrap_or_else(|| "object".into());
            return Err(Box::new(
                PgError::error(format!("{desc} is not a member of extension \"{extname}\""))
                    .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        if pg_depend::deleteDependencyRecordsForClass(
            mcx,
            object.classId,
            object.objectId,
            EXTENSION_RELATION_ID,
            DependencyType::Extension,
        )? != 1
        {
            return Err(PgError::error("unexpected number of extension dependency records").into());
        }
        if object.classId == RELATION_RELATION_ID {
            extension_config_remove(mcx, extension.objectId, object.objectId)?;
        }
        // removeExtObjInitPriv: pg_init_privs is a repo-wide no-op.
    }

    // Recurse to dependent objects: the array type of a base type, the
    // multirange type associated with a range type, and the rowtype of a
    // table.
    if object.classId == TYPE_RELATION_ID {
        let arrtype = lsyscache::get_array_type(object.objectId)?;
        if OidIsValid(arrtype) {
            let depobject =
                ObjectAddress { classId: TYPE_RELATION_ID, objectId: arrtype, objectSubId: 0 };
            alter_contents_recurse(mcx, stmt, extname, extension, &depobject)?;
        }
        if lsyscache::type_is_range(object.objectId)? {
            let multirange = lsyscache::get_range_multirange(object.objectId)?;
            if !OidIsValid(multirange) {
                return Err(Box::new(
                    PgError::error(format!(
                        "could not find multirange type for data type {}",
                        format_type::format_type_be(object.objectId)?
                    ))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            let depobject =
                ObjectAddress { classId: TYPE_RELATION_ID, objectId: multirange, objectSubId: 0 };
            alter_contents_recurse(mcx, stmt, extname, extension, &depobject)?;
        }
    }
    if object.classId == RELATION_RELATION_ID {
        // It might not have a rowtype, but if it does, update that too.
        let rowtype = lsyscache::get_rel_type_id(object.objectId)?;
        if OidIsValid(rowtype) {
            let depobject =
                ObjectAddress { classId: TYPE_RELATION_ID, objectId: rowtype, objectSubId: 0 };
            alter_contents_recurse(mcx, stmt, extname, extension, &depobject)?;
        }
    }
    Ok(())
}

// construct_array_builtin element metadata (arrayfuncs.c): (elmlen, elmbyval,
// elmalign) for the two extconfig/extcondition element types.
fn builtin_array_meta(elmtype: Oid) -> (i32, bool, u8) {
    match elmtype {
        OIDOID => (4, true, b'i'),
        TEXTOID => (-1, false, b'i'),
        other => panic!("type {other} not supported by builtin_array_meta()"),
    }
}

// DatumGetArrayTypeP: the full (detoasted) array image behind a datum.
fn detoast_array_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast::detoast_attr(mcx, raw)
}

// Squeeze dvalues[array_index] out and rebuild a 1-D array (extension.c's
// "squeeze out the target element" blocks).
fn squeeze_array<'mcx>(
    mcx: Mcx<'mcx>,
    dvalues: &[Datum],
    array_index: usize,
    elmtype: Oid,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let mut squeezed: Vec<Datum> = Vec::with_capacity(dvalues.len() - 1);
    squeezed.extend_from_slice(&dvalues[..array_index]);
    squeezed.extend_from_slice(&dvalues[array_index + 1..]);
    let (elmlen, elmbyval, elmalign) = builtin_array_meta(elmtype);
    arrayfuncs::construct_array(mcx, &squeezed, elmtype, elmlen, elmbyval, elmalign)
}

/// `extension_config_remove` (extension.c): remove the table OID from the
/// extension's extconfig array (and the matching extcondition entry), if
/// present.
fn extension_config_remove(mcx: Mcx<'_>, extension_oid: Oid, table_oid: Oid) -> PgResult<()> {
    // Find the pg_extension tuple.
    let ext_rel = table::table_open(mcx, EXTENSION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_key(Anum_pg_extension_oid, extension_oid);
    let mut scan =
        genam::systable_beginscan(mcx, &ext_rel, ExtensionOidIndexId, true, None, &[key])?;
    let Some(ext_tup) = genam::systable_getnext(mcx, &mut scan)? else {
        // C: should not happen.
        panic!("could not find tuple for extension {extension_oid}");
    };
    let desc = ext_rel.descr();

    // Search extconfig for the tableoid.
    let mut isnull = false;
    // SAFETY: extconfig attno is within the pg_extension descriptor.
    let config_datum = unsafe {
        types_tuple::heap_getattr(ext_tup, Anum_pg_extension_extconfig, desc, &mut isnull)
    };
    let mut array_index: Option<usize> = None;
    let mut array_length: i32 = 0;
    let mut config_values = None;
    if !isnull {
        let a = detoast_array_datum(mcx, config_datum)?;
        array_length = arrayfuncs::arr_dim(&a, 0);
        if arrayfuncs::arr_ndim(&a) != 1
            || arrayfuncs::arr_lbound(&a, 0) != 1
            || array_length < 0
            || arrayfuncs::arr_hasnull(&a)
            || arrayfuncs::arr_elemtype(&a) != OIDOID
        {
            return Err(PgError::error("extconfig is not a 1-D Oid array").into());
        }
        // We already checked there are no nulls.
        let (dvalues, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &a, OIDOID, false)?;
        array_index = dvalues.iter().position(|d| d.as_oid() == table_oid);
        config_values = Some(dvalues);
    }

    // If tableoid is not in extconfig, nothing to do.
    let Some(array_index) = array_index else {
        genam::systable_endscan(mcx, scan)?;
        return ext_rel.close(RowExclusiveLock);
    };

    // Modify or delete the extconfig value.
    let mut repl_val = [Datum::null(); Natts_pg_extension];
    let mut repl_null = [false; Natts_pg_extension];
    let mut repl_repl = [false; Natts_pg_extension];

    let new_config;
    if array_length <= 1 {
        // Removing the only element: set the array to null.
        repl_null[Anum_pg_extension_extconfig as usize - 1] = true;
    } else {
        let dvalues = config_values.as_ref().expect("populated extconfig deconstructed");
        new_config = squeeze_array(mcx, dvalues, array_index, OIDOID)?;
        repl_val[Anum_pg_extension_extconfig as usize - 1] =
            Datum::from_usize(new_config.as_ptr() as usize);
    }
    repl_repl[Anum_pg_extension_extconfig as usize - 1] = true;

    // Modify or delete the extcondition value.
    let mut cond_isnull = false;
    // SAFETY: extcondition attno is within the pg_extension descriptor.
    let cond_datum = unsafe {
        types_tuple::heap_getattr(ext_tup, Anum_pg_extension_extcondition, desc, &mut cond_isnull)
    };
    if cond_isnull {
        return Err(PgError::error("extconfig and extcondition arrays do not match").into());
    }
    let cond_img = detoast_array_datum(mcx, cond_datum)?;
    if arrayfuncs::arr_ndim(&cond_img) != 1
        || arrayfuncs::arr_lbound(&cond_img, 0) != 1
        || arrayfuncs::arr_hasnull(&cond_img)
        || arrayfuncs::arr_elemtype(&cond_img) != TEXTOID
    {
        return Err(PgError::error("extcondition is not a 1-D text array").into());
    }
    if arrayfuncs::arr_dim(&cond_img, 0) != array_length {
        return Err(PgError::error("extconfig and extcondition arrays do not match").into());
    }

    let new_condition;
    if array_length <= 1 {
        repl_null[Anum_pg_extension_extcondition as usize - 1] = true;
    } else {
        // We already checked there are no nulls.
        let (dvalues, _nulls) =
            arrayfuncs::deconstruct_array_builtin(mcx, &cond_img, TEXTOID, false)?;
        new_condition = squeeze_array(mcx, &dvalues, array_index, TEXTOID)?;
        repl_val[Anum_pg_extension_extcondition as usize - 1] =
            Datum::from_usize(new_condition.as_ptr() as usize);
    }
    repl_repl[Anum_pg_extension_extcondition as usize - 1] = true;

    let tid = ext_tup.t_self;
    let mut new_tup =
        heaptuple::heap_modify_tuple(mcx, ext_tup, desc, &repl_val, &repl_null, &repl_repl)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &ext_rel, &tid, &mut new_tup)?;

    genam::systable_endscan(mcx, scan)?;
    ext_rel.close(RowExclusiveLock)
}

#[cfg(test)]
mod tests {
    use super::*;

    // extension_config_remove's array surgery: squeeze one element out of a
    // 1-D array, exactly C's deconstruct/shift/construct_array_builtin
    // sequence (previously fenced for any populated extconfig).
    #[test]
    fn squeeze_array_matches_c() {
        let ctx = mcx::MemoryContext::new("contents-test");
        let mcx = ctx.mcx();

        // Oid arm (extconfig).
        let oids = [Datum::from_oid(50001), Datum::from_oid(50002), Datum::from_oid(50003)];
        let (elmlen, elmbyval, elmalign) = builtin_array_meta(OIDOID);
        let a = arrayfuncs::construct_array(mcx, &oids, OIDOID, elmlen, elmbyval, elmalign)
            .unwrap();
        let squeezed = squeeze_array(mcx, &oids, 1, OIDOID).unwrap();
        assert_eq!(arrayfuncs::arr_ndim(&squeezed), 1);
        assert_eq!(arrayfuncs::arr_dim(&squeezed, 0), 2);
        assert_eq!(arrayfuncs::arr_lbound(&squeezed, 0), 1);
        assert_eq!(arrayfuncs::arr_elemtype(&squeezed), OIDOID);
        let (vals, _) =
            arrayfuncs::deconstruct_array_builtin(mcx, &squeezed, OIDOID, false).unwrap();
        assert_eq!(vals.iter().map(|d| d.as_oid()).collect::<Vec<_>>(), [50001, 50003]);
        // The original 3-element image still deconstructs to the input.
        let (vals, _) = arrayfuncs::deconstruct_array_builtin(mcx, &a, OIDOID, false).unwrap();
        assert_eq!(vals.len(), 3);

        // text arm (extcondition), removing the first element.
        let t1 = varlena::cstring_to_text(mcx, b"WHERE true").unwrap();
        let t2 = varlena::cstring_to_text(mcx, b"WHERE id > 0").unwrap();
        let texts = [
            Datum::from_usize(t1.as_bytes().as_ptr() as usize),
            Datum::from_usize(t2.as_bytes().as_ptr() as usize),
        ];
        let squeezed = squeeze_array(mcx, &texts, 0, TEXTOID).unwrap();
        assert_eq!(arrayfuncs::arr_dim(&squeezed, 0), 1);
        assert_eq!(arrayfuncs::arr_elemtype(&squeezed), TEXTOID);
    }
}
