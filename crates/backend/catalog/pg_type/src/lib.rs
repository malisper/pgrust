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
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
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
    if !isDependentType && has_default_acl_for_type(mcx, p.ownerId, p.typeNamespace)? {
        panic!(
            "TypeCreate (pg_type.c): get_user_default_acl found a pg_default_acl entry \
             for type \"{}\" — non-NULL typacl lane unported",
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
        pg_depend::recordDependencyOnOwner(mcx, TYPE_RELATION_ID, typeObjectId, p.ownerId)?;
    }
    // C: makeExtensionDep is true on every TypeCreate path (dependent types
    // get explicit membership too); rebuild is false for fresh rows.
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;
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

// AssignTypeArrayOid (typecmds.c); IsBinaryUpgrade arm loud below.
pub fn AssignTypeArrayOid<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Oid> {
    if init_small::globals::IsBinaryUpgrade() {
        panic!("pg_type: binary-upgrade array-OID override (typecmds.c AssignTypeArrayOid) unported");
    }
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

// RenameTypeInternal (pg_type.c): rename the type row, then chase the array
// type with a fresh makeArrayTypeName.
pub fn RenameTypeInternal<'mcx>(
    mcx: Mcx<'mcx>,
    typeOid: Oid,
    newTypeName: &str,
    typeNamespace: Oid,
) -> PgResult<()> {
    const Anum_pg_type_typname: usize = 2;
    const Anum_pg_type_typarray: i32 = 15;
    if syscache_seams::lookup_pg_type_oid_by_name::call(newTypeName, typeNamespace)? != InvalidOid
    {
        return Err(Box::new(
            PgError::new(ERROR, format!("type \"{newTypeName}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    let pg_type_rel = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_type_oid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(typeOid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_type_rel, TypeOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeOid}"));
    let desc = pg_type_rel.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_type column under its descriptor.
    let arrayOid = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_type_typarray, desc, &mut isnull)
    }
    .as_oid();

    assert!(newTypeName.len() < NAMEDATALEN as usize, "type name truncation unported");
    let mut namebuf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
    mcx::vec_append_bytes(&mut namebuf, newTypeName.as_bytes())?;
    mcx::vec_append_bytes(&mut namebuf, &[0u8; 64][..64 - newTypeName.len()])?;
    let n = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    values.resize(n, Datum::null());
    nulls.resize(n, false);
    replace.resize(n, false);
    values[Anum_pg_type_typname - 1] = Datum::from_usize(namebuf.as_ptr() as usize);
    replace[Anum_pg_type_typname - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_type_rel, &otid, &mut newtup)?;
    pg_type_rel.close(RowExclusiveLock)?;

    if arrayOid != InvalidOid {
        let arr = makeArrayTypeName(newTypeName, typeNamespace)?;
        let arr_str = core::str::from_utf8(arr.name_str()).expect("array type name UTF-8");
        RenameTypeInternal(mcx, arrayOid, arr_str, typeNamespace)?;
    }
    Ok(())
}

// RemoveTypeById (typecmds.c); enum/range cleanup arms are loud.
pub fn RemoveTypeById<'mcx>(mcx: Mcx<'mcx>, typeOid: Oid) -> PgResult<()> {
    const Anum_pg_type_typtype: i32 = 7;
    let relation = table::table_open(mcx, TYPE_RELATION_ID, RowExclusiveLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_type_oid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(typeOid);
    let mut scan = genam::systable_beginscan(mcx, &relation, TypeOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typeOid}"));
    let tid = tup.t_self;
    let mut isnull = false;
    // SAFETY: typtype is a fixed NOT NULL pg_type column.
    let typtype = unsafe {
        types_tuple::heap_getattr(tup, Anum_pg_type_typtype, relation.descr(), &mut isnull)
    }
    .as_i8();
    catalog_indexing::CatalogTupleDelete(&relation, &tid)?;
    const TYPTYPE_ENUM: i8 = b'e' as i8;
    const TYPTYPE_RANGE: i8 = b'r' as i8;
    if typtype == TYPTYPE_ENUM {
        pg_enum::EnumValuesDelete(mcx, typeOid)?;
    }
    if typtype == TYPTYPE_RANGE {
        panic!("unported: RemoveTypeById RangeDelete (pg_range.c)");
    }
    genam::systable_endscan(mcx, scan)?;
    relation.close(RowExclusiveLock)
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

// get_user_default_acl (aclchk.c) reduced to the fresh-initdb decision: any
// matching pg_default_acl row (per-schema or global, DEFACLOBJ_TYPE) means a
// non-NULL typacl, which is unported; no row means typacl NULL.
fn has_default_acl_for_type<'mcx>(mcx: Mcx<'mcx>, ownerId: Oid, nsp: Oid) -> PgResult<bool> {
    const DEFAULT_ACL_RELATION_ID: Oid = 826;
    const DEFAULT_ACL_ROLE_NSP_OBJ_INDEX_ID: Oid = 827;
    const Anum_pg_default_acl_defaclrole: AttrNumber = 2;
    const Anum_pg_default_acl_defaclnamespace: AttrNumber = 3;
    const Anum_pg_default_acl_defaclobjtype: AttrNumber = 4;
    const DEFACLOBJ_TYPE: i8 = b'T' as i8;

    let rel = table::table_open(mcx, DEFAULT_ACL_RELATION_ID, AccessShareLock)?;
    let oideq = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    let chareq = fmgr_seams::fmgr_info::call(types_core::fmgr::F_CHAREQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_CHAREQ) failed: {e:?}"));
    let mut found = false;
    for probe_nsp in [nsp, InvalidOid] {
        let mut key = |attno: AttrNumber, func, arg: Datum| {
            let mut k = types_scan::scankey::ScanKeyData::empty();
            k.sk_attno = attno;
            k.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
            k.sk_collation = 0;
            k.sk_func = func;
            k.sk_argument = arg;
            k
        };
        let keys = [
            key(Anum_pg_default_acl_defaclrole, oideq.clone(), Datum::from_oid(ownerId)),
            key(Anum_pg_default_acl_defaclnamespace, oideq.clone(), Datum::from_oid(probe_nsp)),
            key(Anum_pg_default_acl_defaclobjtype, chareq.clone(), Datum::from_i8(DEFACLOBJ_TYPE)),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel,
            DEFAULT_ACL_ROLE_NSP_OBJ_INDEX_ID,
            true,
            None,
            &keys,
        )?;
        found |= genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if found {
            break;
        }
    }
    rel.close(AccessShareLock)?;
    Ok(found)
}
