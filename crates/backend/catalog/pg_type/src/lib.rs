// pg_type.c TypeCreate insert-arm slice (+ AssignTypeArrayOid from
// typecmds.c and makeObjectName from indexcmds.c). Loud: shell-type replace,
// type defaults, non-dependent types (ACL/owner deps), RenameTypeInternal.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, NAMEDATALEN, PROCEDURE_RELATION_ID,
    RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_rel::{AccessShareLock, RowExclusiveLock, RELKIND_COMPOSITE_TYPE};
use types_tuple::NameData;

pub use pg_depend::{DependencyType, ObjectAddress};

pub const TypeOidIndexId: Oid = 2703;
pub const TypeNameNspIndexId: Oid = 2704;
pub const Anum_pg_type_oid: AttrNumber = 1;
use catalog::CollationRelationId;
const Natts_pg_type: usize = 32;

pub const TYPTYPE_BASE: i8 = b'b' as i8;
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
pub const TYPCATEGORY_ARRAY: i8 = b'A' as i8;
pub const TYPCATEGORY_COMPOSITE: i8 = b'C' as i8;
pub const DEFAULT_TYPDELIM: i8 = b',' as i8;

pub const F_RECORD_IN: Oid = 2290;
pub const F_RECORD_OUT: Oid = 2291;
pub const F_RECORD_RECV: Oid = 2402;
pub const F_RECORD_SEND: Oid = 2403;
pub const F_ARRAY_IN: Oid = 750;
pub const F_ARRAY_OUT: Oid = 751;
pub const F_ARRAY_RECV: Oid = 2400;
pub const F_ARRAY_SEND: Oid = 2401;
pub const F_ARRAY_TYPANALYZE: Oid = 3816;
pub const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

pub struct TypeCreateParams<'a> {
    pub newTypeOid: Oid,
    pub typeName: &'a str,
    pub typeNamespace: Oid,
    pub relationOid: Oid,
    pub relationKind: u8,
    pub ownerId: Oid,
    pub internalSize: i16,
    pub typeType: i8,
    pub typeCategory: i8,
    pub typePreferred: bool,
    pub typDelim: i8,
    pub inputProcedure: Oid,
    pub outputProcedure: Oid,
    pub receiveProcedure: Oid,
    pub sendProcedure: Oid,
    pub typmodinProcedure: Oid,
    pub typmodoutProcedure: Oid,
    pub analyzeProcedure: Oid,
    pub subscriptProcedure: Oid,
    pub elementType: Oid,
    pub isImplicitArray: bool,
    pub arrayType: Oid,
    pub baseType: Oid,
    pub passedByValue: bool,
    pub alignment: i8,
    pub storage: i8,
    pub typeMod: i32,
    pub typNDims: i32,
    pub typeNotNull: bool,
    pub typeCollation: Oid,
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

fn validate_shape(p: &TypeCreateParams<'_>) -> PgResult<()> {
    let (size, align) = (p.internalSize, p.alignment as u8 as char);
    if !(size > 0 || size == -1 || size == -2) {
        return Err(err(
            format!("invalid type internal size {size}"),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    if p.passedByValue {
        let ok = match size {
            1 => align == 'c',
            2 => align == 's',
            4 => align == 'i',
            8 => align == 'd',
            _ => {
                return Err(err(
                    format!("internal size {size} is invalid for passed-by-value type"),
                    ERRCODE_INVALID_OBJECT_DEFINITION,
                ))
            }
        };
        if !ok {
            return Err(err(
                format!("alignment \"{align}\" is invalid for passed-by-value type of size {size}"),
                ERRCODE_INVALID_OBJECT_DEFINITION,
            ));
        }
    } else if (size == -1 && !(align == 'i' || align == 'd')) || (size == -2 && align != 'c') {
        return Err(err(
            format!("alignment \"{align}\" is invalid for variable-length type"),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    if p.storage as u8 != b'p' && size != -1 {
        return Err(err(
            "fixed-size types must have storage PLAIN".into(),
            ERRCODE_INVALID_OBJECT_DEFINITION,
        ));
    }
    Ok(())
}

pub fn TypeCreate<'mcx>(mcx: Mcx<'mcx>, p: &TypeCreateParams<'_>) -> PgResult<ObjectAddress> {
    validate_shape(p)?;

    let isDependentType = p.isImplicitArray
        || p.typeType == TYPTYPE_MULTIRANGE
        || (p.relationOid != InvalidOid && p.relationKind != RELKIND_COMPOSITE_TYPE);
    if !isDependentType {
        panic!(
            "TypeCreate (pg_type.c): non-dependent type \"{}\" unported \
             (get_user_default_acl / pg_shdepend owner recording)",
            p.typeName
        );
    }

    let mut name = NameData::default();
    name.namestrcpy(p.typeName);
    let mut values = [Datum::null(); Natts_pg_type];
    let mut nulls = [false; Natts_pg_type];
    values[1] = Datum::from_usize(name.data.as_ptr() as usize);
    values[2] = Datum::from_oid(p.typeNamespace);
    values[3] = Datum::from_oid(p.ownerId);
    values[4] = Datum::from_i16(p.internalSize);
    values[5] = Datum::from_bool(p.passedByValue);
    values[6] = Datum::from_char(p.typeType);
    values[7] = Datum::from_char(p.typeCategory);
    values[8] = Datum::from_bool(p.typePreferred);
    values[9] = Datum::from_bool(true); // typisdefined
    values[10] = Datum::from_char(p.typDelim);
    values[11] = Datum::from_oid(p.relationOid);
    values[12] = Datum::from_oid(p.subscriptProcedure);
    values[13] = Datum::from_oid(p.elementType);
    values[14] = Datum::from_oid(p.arrayType);
    values[15] = Datum::from_oid(p.inputProcedure);
    values[16] = Datum::from_oid(p.outputProcedure);
    values[17] = Datum::from_oid(p.receiveProcedure);
    values[18] = Datum::from_oid(p.sendProcedure);
    values[19] = Datum::from_oid(p.typmodinProcedure);
    values[20] = Datum::from_oid(p.typmodoutProcedure);
    values[21] = Datum::from_oid(p.analyzeProcedure);
    values[22] = Datum::from_char(p.alignment);
    values[23] = Datum::from_char(p.storage);
    values[24] = Datum::from_bool(p.typeNotNull);
    values[25] = Datum::from_oid(p.baseType);
    values[26] = Datum::from_i32(p.typeMod);
    values[27] = Datum::from_i32(p.typNDims);
    values[28] = Datum::from_oid(p.typeCollation);
    nulls[29] = true; // typdefaultbin
    nulls[30] = true; // typdefault
    nulls[31] = true; // typacl (dependent types never get one)

    let pg_type_desc = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;

    let old_oid = syscache_seams::lookup_pg_type_oid_by_name::call(p.typeName, p.typeNamespace)?;
    if old_oid != InvalidOid {
        if syscache_seams::pg_type_isdefined::call(old_oid)?.unwrap_or(false) {
            return Err(err(
                format!("type \"{}\" already exists", p.typeName),
                ERRCODE_DUPLICATE_OBJECT,
            ));
        }
        panic!(
            "TypeCreate (pg_type.c): shell type replacement unported (type \"{}\")",
            p.typeName
        );
    }

    let typeObjectId = if p.newTypeOid != InvalidOid {
        p.newTypeOid
    } else {
        catalog::GetNewOidWithIndex(mcx, &pg_type_desc, TypeOidIndexId, Anum_pg_type_oid)?
    };
    values[0] = Datum::from_oid(typeObjectId);

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_type_desc.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_type_desc, &mut tup)?;

    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        GenerateTypeDependencies(mcx, typeObjectId, p, isDependentType)?;
    }

    pg_type_desc.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(TYPE_RELATION_ID, typeObjectId))
}

fn GenerateTypeDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    typeObjectId: Oid,
    p: &TypeCreateParams<'_>,
    isDependentType: bool,
) -> PgResult<()> {
    let myself = ObjectAddress::set(TYPE_RELATION_ID, typeObjectId);
    let mut addrs_normal = [ObjectAddress::set(InvalidOid, InvalidOid); 11];
    let mut n = 0;

    if !isDependentType || p.typeType == TYPTYPE_MULTIRANGE {
        addrs_normal[n] = ObjectAddress::set(catalog::NamespaceRelationId, p.typeNamespace);
        n += 1;
    }
    if !isDependentType {
        pg_depend::recordDependencyOnOwner(TYPE_RELATION_ID, typeObjectId, p.ownerId);
        panic!(
            "GenerateTypeDependencies (pg_type.c): recordDependencyOnNewAcl unported \
             for non-dependent type {typeObjectId}"
        );
    }
    // recordDependencyOnCurrentExtension: no-op — CREATE EXTENSION scripts
    // (extension.c creating_extension) are unported, so it can never fire.
    for proc in [
        p.inputProcedure,
        p.outputProcedure,
        p.receiveProcedure,
        p.sendProcedure,
        p.typmodinProcedure,
        p.typmodoutProcedure,
        p.analyzeProcedure,
        p.subscriptProcedure,
    ] {
        if proc != InvalidOid {
            addrs_normal[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, proc);
            n += 1;
        }
    }
    if p.baseType != InvalidOid {
        addrs_normal[n] = ObjectAddress::set(TYPE_RELATION_ID, p.baseType);
        n += 1;
    }
    if p.typeCollation != InvalidOid && p.typeCollation != DEFAULT_COLLATION_OID {
        addrs_normal[n] = ObjectAddress::set(CollationRelationId, p.typeCollation);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut addrs_normal[..n],
        DependencyType::Normal,
    )?;

    if p.relationOid != InvalidOid {
        let referenced = ObjectAddress::set(RELATION_RELATION_ID, p.relationOid);
        if p.relationKind != RELKIND_COMPOSITE_TYPE {
            pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Internal)?;
        } else {
            pg_depend::recordDependencyOn(mcx, &referenced, &myself, DependencyType::Internal)?;
        }
    }

    if p.elementType != InvalidOid {
        let referenced = ObjectAddress::set(TYPE_RELATION_ID, p.elementType);
        let behavior = if p.isImplicitArray {
            DependencyType::Internal
        } else {
            DependencyType::Normal
        };
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, behavior)?;
    }
    Ok(())
}

// AssignTypeArrayOid (typecmds.c); IsBinaryUpgrade arm out of scope.
pub fn AssignTypeArrayOid<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Oid> {
    let pg_type = table::table_open(mcx, TYPE_RELATION_ID, AccessShareLock)?;
    let oid = catalog::GetNewOidWithIndex(mcx, &pg_type, TypeOidIndexId, Anum_pg_type_oid)?;
    pg_type.close(AccessShareLock)?;
    Ok(oid)
}

// makeObjectName (indexcmds.c) specialized to name1 = "" (the only pg_type
// caller shape): "_<name2 truncated>[_<label>]", NAMEDATALEN-bounded.
fn make_array_object_name(typeName: &str, label: Option<&str>) -> NameData {
    let overhead = 1 + label.map_or(0, |l| l.len() + 1);
    let availchars = NAMEDATALEN as usize - 1 - overhead;
    let mut name2chars = typeName.len().min(availchars);
    name2chars = mbutils_seams::pg_mbcliplen::call(
        typeName.as_bytes(),
        name2chars as i32,
        name2chars as i32,
    ) as usize;

    let mut out = NameData::default();
    let mut pos = 0;
    out.data[pos] = b'_';
    pos += 1;
    out.data[pos..pos + name2chars].copy_from_slice(&typeName.as_bytes()[..name2chars]);
    pos += name2chars;
    if let Some(l) = label {
        out.data[pos] = b'_';
        pos += 1;
        out.data[pos..pos + l.len()].copy_from_slice(l.as_bytes());
    }
    out
}

pub fn makeArrayTypeName(typeName: &str, typeNamespace: Oid) -> PgResult<NameData> {
    let mut pass = 0u32;
    let mut arr_name = make_array_object_name(typeName, None);
    loop {
        let candidate = core::str::from_utf8(arr_name.name_str()).expect("non-UTF-8 type name");
        if syscache_seams::lookup_pg_type_oid_by_name::call(candidate, typeNamespace)? == InvalidOid
        {
            return Ok(arr_name);
        }
        pass += 1;
        let suffix = pass.to_string();
        arr_name = make_array_object_name(typeName, Some(&suffix));
    }
}

pub fn moveArrayTypeName(typeOid: Oid, typeName: &str, typeNamespace: Oid) -> PgResult<bool> {
    if !syscache_seams::pg_type_isdefined::call(typeOid)?.unwrap_or(false) {
        return Ok(true);
    }
    let elemOid =
        syscache_seams::pg_type_element_shape::call(typeOid)?.map_or(InvalidOid, |s| s.typelem);
    if elemOid == InvalidOid
        || syscache_seams::pg_type_typarray::call(elemOid)?.unwrap_or(InvalidOid) != typeOid
    {
        return Ok(false);
    }
    let _ = typeNamespace;
    panic!(
        "moveArrayTypeName (pg_type.c): RenameTypeInternal lane unported \
         (autogenerated array type {typeOid} shadows \"{typeName}\")"
    );
}
