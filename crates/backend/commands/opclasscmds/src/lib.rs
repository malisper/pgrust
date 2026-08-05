//! opclasscmds.c — CREATE OPERATOR CLASS/FAMILY and ALTER OPERATOR FAMILY.
#![allow(non_snake_case, non_upper_case_globals)]

use amapi::{am_adjust_members, GetIndexAmRoutineByAmId};
use cache_syscache::cacheinfo::{AMNAME, AMOID, AMOPSTRATEGY, AMPROCNUM};
use cache_syscache::{
    GetSysCacheOid, SearchSysCache1, SearchSysCacheExists, SysCacheGetAttrNotNull, SysCacheKey,
};
use datum::Datum;
use mcx::Mcx;
use pg_depend::{recordDependencyOn, recordDependencyOnOwner, DependencyType, ObjectAddress};
use types_core::{
    InvalidOid, Oid, ACCESS_METHOD_OPERATOR_OID_INDEX_ID, ACCESS_METHOD_OPERATOR_RELATION_ID,
    ACCESS_METHOD_PROCEDURE_OID_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID,
    ACCESS_METHOD_RELATION_ID, BOOLOID, BTREE_AM_OID, INT4OID, INT8OID, INTERNALOID,
    NAMESPACE_RELATION_ID, OPCLASS_OID_INDEX_ID, OPERATOR_CLASS_RELATION_ID,
    OPERATOR_FAMILY_RELATION_ID, OPERATOR_RELATION_ID, OPFAMILY_OID_INDEX_ID,
    PROCEDURE_RELATION_ID, TYPE_RELATION_ID, VOIDOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
};
use types_nbtree::page::{
    BTEQUALIMAGE_PROC, BTINRANGE_PROC, BTORDER_PROC, BTSKIPSUPPORT_PROC, BTSORTSUPPORT_PROC,
};
use types_nodes::parsenodes::{
    AlterOpFamilyStmt, CreateOpClassItem, CreateOpClassStmt, CreateOpFamilyStmt, ObjectWithArgs,
    OPCLASS_ITEM_FUNCTION, OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_STORAGETYPE,
};
use types_nodes::rawnodes::TypeName;
use types_nodes::{Node, NodeList};
use types_rel::RowExclusiveLock;
use types_relscan::{IndexAmKind, OpFamilyMember};
use types_tuple::NameData;

pub mod builtins;

pub fn init_seams() {
    opclasscmds_seams::get_index_am_oid::set(get_index_am_oid);
    opclasscmds_seams::get_opclass_oid::set(get_opclass_oid);
    opclasscmds_seams::get_opfamily_oid::set(get_opfamily_oid);
}

const AMOP_SEARCH: i8 = b's' as i8;
const AMOP_ORDER: i8 = b'o' as i8;
const SHRT_MAX: i32 = 32767;
const HASHSTANDARD_PROC: u16 = 1;
const HASHEXTENDED_PROC: u16 = 2;

const Natts_pg_opfamily: usize = 5;
const Anum_pg_opfamily_oid: i32 = 1;
const Natts_pg_opclass: usize = 9;
const Anum_pg_opclass_oid: i32 = 1;
const Natts_pg_amop: usize = 9;
const Anum_pg_amop_oid: i32 = 1;
const Natts_pg_amproc: usize = 6;
const Anum_pg_amproc_oid: i32 = 1;
const Anum_pg_am_oid: i32 = 1;
const Anum_pg_am_amtype: i32 = 4;
const AMTYPE_INDEX: i8 = b'i' as i8;

fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn name_parts<'a, 'mcx>(names: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = names.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = names.nth(i).as_string().expect("name list holds String nodes").sval;
    }
    &buf[..n]
}

fn get_am_name(amoid: Oid) -> PgResult<String> {
    let tup = SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(amoid)))?
        .unwrap_or_else(|| panic!("cache lookup failed for access method {amoid}"));
    const ANUM_PG_AM_AMNAME: i32 = 2;
    let d = SysCacheGetAttrNotNull(AMOID, &tup, ANUM_PG_AM_AMNAME)?;
    // SAFETY: amname is the row's inline NameData column.
    let name = unsafe { *(d.as_usize() as *const NameData) };
    cache_syscache::ReleaseSysCache(tup);
    Ok(core::str::from_utf8(name.name_str()).unwrap_or("").to_string())
}

// pg_am AMNAME probe: (oid, amtype); None if no such access method.
fn get_am_by_name(amname: &str) -> PgResult<Option<(Oid, i8)>> {
    let Some(tup) = SearchSysCache1(AMNAME, SysCacheKey::Str(amname))? else {
        return Ok(None);
    };
    let oid = SysCacheGetAttrNotNull(AMNAME, &tup, Anum_pg_am_oid)?.as_oid();
    let amtype = SysCacheGetAttrNotNull(AMNAME, &tup, Anum_pg_am_amtype)?.as_i8();
    cache_syscache::ReleaseSysCache(tup);
    Ok(Some((oid, amtype)))
}

#[track_caller]
#[cold]
fn no_such_am(amname: &str) -> Box<PgError> {
    err(
        ERRCODE_UNDEFINED_OBJECT,
        format!("access method \"{amname}\" does not exist"),
    )
}

// get_index_am_oid (amcmds.c).
fn get_index_am_oid(amname: &str) -> PgResult<Oid> {
    match get_am_by_name(amname)? {
        Some((oid, amtype)) if amtype == AMTYPE_INDEX => Ok(oid),
        Some((_, _)) => Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("access method \"{amname}\" is not of type {}", "INDEX"),
        )),
        None => Err(no_such_am(amname)),
    }
}

// OpFamilyCacheLookup + get_opfamily_oid (opclasscmds.c): resolve a possibly
// qualified opfamily name for an AM.
pub fn get_opfamily_oid(amID: Oid, opfamilyname: &NodeList<'_>, missing_ok: bool) -> PgResult<Oid> {
    let mut buf = [""; 4];
    let parts = name_parts(opfamilyname, &mut buf);
    let (schemaname, opfname) = catalog_namespace::DeconstructQualifiedName(parts)?;
    let opfID = match schemaname {
        Some(schemaname) => {
            let namespaceId = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
            if !OidIsValid(namespaceId) {
                InvalidOid
            } else {
                syscache_seams::lookup_pg_opfamily_oid_exact::call(amID, opfname, namespaceId)?
            }
        }
        None => catalog_namespace::OpfamilynameGetOpfid(amID, opfname)?,
    };
    if !OidIsValid(opfID) && !missing_ok {
        let amname = get_am_name(amID)?;
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "operator family \"{}\" does not exist for access method \"{amname}\"",
                parts.join(".")
            ),
        ));
    }
    Ok(opfID)
}

// OpClassCacheLookup + get_opclass_oid (opclasscmds.c).
pub fn get_opclass_oid(amID: Oid, opclassname: &NodeList<'_>, missing_ok: bool) -> PgResult<Oid> {
    let mut buf = [""; 4];
    let parts = name_parts(opclassname, &mut buf);
    let (schemaname, opcname) = catalog_namespace::DeconstructQualifiedName(parts)?;
    let opcID = match schemaname {
        Some(schemaname) => {
            let namespaceId = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
            if !OidIsValid(namespaceId) {
                InvalidOid
            } else {
                syscache_seams::lookup_pg_opclass_oid_by_name::call(amID, opcname, namespaceId)?
            }
        }
        None => catalog_namespace::OpclassnameGetOpcid(amID, opcname)?,
    };
    if !OidIsValid(opcID) && !missing_ok {
        let amname = get_am_name(amID)?;
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "operator class \"{}\" does not exist for access method \"{amname}\"",
                parts.join(".")
            ),
        ));
    }
    Ok(opcID)
}

// CreateOpFamily: catalog entry for a new operator family (permissions
// checked by callers).
fn CreateOpFamily(
    mcx: Mcx<'_>,
    amname: &str,
    opfname: &str,
    namespaceoid: Oid,
    amoid: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, OPERATOR_FAMILY_RELATION_ID, RowExclusiveLock)?;

    if OidIsValid(syscache_seams::lookup_pg_opfamily_oid_exact::call(
        amoid,
        opfname,
        namespaceoid,
    )?) {
        return Err(err(
            ERRCODE_DUPLICATE_OBJECT,
            format!(
                "operator family \"{opfname}\" for access method \"{amname}\" already exists"
            ),
        ));
    }

    let opfamilyoid =
        catalog::GetNewOidWithIndex(mcx, &rel, OPFAMILY_OID_INDEX_ID, Anum_pg_opfamily_oid as i16)?;
    let mut opfName = NameData::default();
    opfName.namestrcpy(opfname);
    let values = [
        Datum::from_oid(opfamilyoid),
        Datum::from_oid(amoid),
        Datum::from_usize(opfName.data.as_ptr() as usize),
        Datum::from_oid(namespaceoid),
        Datum::from_oid(miscinit::GetUserId()),
    ];
    let nulls = [false; Natts_pg_opfamily];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(OPERATOR_FAMILY_RELATION_ID, opfamilyoid);

    // dependency on access method
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(ACCESS_METHOD_RELATION_ID, amoid),
        DependencyType::Auto,
    )?;
    // dependency on namespace
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid),
        DependencyType::Normal,
    )?;
    // dependency on owner
    recordDependencyOnOwner(mcx, OPERATOR_FAMILY_RELATION_ID, opfamilyoid, miscinit::GetUserId())?;
    // dependency on extension
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    // C: EventTriggerCollectSimpleCommand(myself, Invalid, stmt) — the tag is
    // the opfamily statement's, also when called from DefineOpClass.
    event_trigger::EventTriggerCollectSimpleCommand(
        myself,
        ObjectAddress::set(types_core::InvalidOid, types_core::InvalidOid),
        cmdtag::GetCommandTagEnum(b"CREATE OPERATOR FAMILY"),
    );

    rel.close(RowExclusiveLock)?;
    Ok(myself)
}

struct AmInfo {
    amoid: Oid,
    kind: IndexAmKind,
    max_op_number: i32,
    max_proc_number: i32,
    opts_proc_number: i32,
    amstorage: bool,
}

fn get_am_info(amname: &str) -> PgResult<AmInfo> {
    let Some((amoid, _amtype)) = get_am_by_name(amname)? else {
        return Err(no_such_am(amname));
    };
    let kind = GetIndexAmRoutineByAmId(amoid, false)?.expect("noerror=false");
    let mut max_op_number = kind.amstrategies();
    if max_op_number <= 0 {
        max_op_number = SHRT_MAX;
    }
    Ok(AmInfo {
        amoid,
        kind,
        max_op_number,
        max_proc_number: kind.amsupport(),
        opts_proc_number: kind.amoptsprocnum(),
        amstorage: kind.amstorage(),
    })
}

fn require_superuser(what: &str) -> PgResult<()> {
    if !superuser::superuser()? {
        return Err(err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("must be superuser to {what}"),
        ));
    }
    Ok(())
}

fn namespace_create_check(mcx: Mcx<'_>, namespaceoid: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespaceoid,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, namespaceoid)?;
        aclchk::aclcheck_error(
            aclresult,
            types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA,
            nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }
    Ok(())
}

fn item_owa<'mcx>(item: &CreateOpClassItem<'mcx>) -> &'mcx ObjectWithArgs<'mcx> {
    item.name
        .and_then(|n| n.as_variant::<ObjectWithArgs>())
        .expect("CreateOpClassItem.name is ObjectWithArgs")
}

fn typename_type_id(mcx: Mcx<'_>, n: Node<'_>) -> PgResult<Oid> {
    let tn = n.as_variant::<TypeName>().expect("TypeName node");
    parse_utilcmd::LookupTypeNameOid(mcx, tn)
}

// DefineOpClass: define a new index operator class.
pub fn DefineOpClass<'mcx>(mcx: Mcx<'mcx>, stmt: &CreateOpClassStmt<'mcx>) -> PgResult<ObjectAddress> {
    let mut buf = [""; 4];
    let parts = name_parts(&stmt.opclassname, &mut buf);
    let (namespaceoid, opcname) = catalog_namespace::QualifiedNameGetCreationNamespace(mcx, parts)?;

    namespace_create_check(mcx, namespaceoid)?;

    let amname = stmt.amname.expect("CreateOpClassStmt.amname");
    let am = get_am_info(amname)?;

    // Creating an opclass is tantamount to granting public execute on its
    // functions, so require superuser.
    require_superuser("create an operator class")?;

    let typeoid = typename_type_id(mcx, stmt.datatype.expect("CreateOpClassStmt.datatype"))?;

    // Containing operator family: explicit, same-name existing, or created.
    let opfamilyoid = if !stmt.opfamilyname.is_nil() {
        get_opfamily_oid(am.amoid, &stmt.opfamilyname, false)?
    } else {
        let existing =
            syscache_seams::lookup_pg_opfamily_oid_exact::call(am.amoid, opcname, namespaceoid)?;
        if OidIsValid(existing) {
            existing
        } else {
            CreateOpFamily(mcx, amname, opcname, namespaceoid, am.amoid)?.objectId
        }
    };

    let mut operators: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);
    let mut procedures: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);
    let mut storageoid = InvalidOid;

    for n in stmt.items.iter() {
        let item = n.as_variant::<CreateOpClassItem>().expect("CreateOpClassItem");
        match item.itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if item.number <= 0 || item.number > am.max_op_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid operator number {}, must be between 1 and {}",
                            item.number, am.max_op_number
                        ),
                    ));
                }
                let owa = item_owa(item);
                let operOid = if !owa.objargs.is_nil() {
                    parse_oper::LookupOperWithArgs(&owa.objname, &owa.objargs, false)?
                } else {
                    // Default to binary op on the input datatype.
                    parse_oper::LookupOperName(&owa.objname, typeoid, typeoid, false)?
                };
                let sortfamilyOid = if !item.order_family.is_nil() {
                    get_opfamily_oid(BTREE_AM_OID, &item.order_family, false)?
                } else {
                    InvalidOid
                };
                let mut member = OpFamilyMember {
                    is_func: false,
                    object: operOid,
                    number: item.number as i16,
                    sortfamily: sortfamilyOid,
                    ..Default::default()
                };
                assignOperTypes(&mut member, am.kind, amname, typeoid)?;
                addFamilyMember(&mut operators, member)?;
            }
            OPCLASS_ITEM_FUNCTION => {
                if item.number <= 0 || item.number > am.max_proc_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid function number {}, must be between 1 and {}",
                            item.number, am.max_proc_number
                        ),
                    ));
                }
                let owa = item_owa(item);
                let funcOid = parse_func::LookupFuncWithArgs(
                    types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION,
                    owa,
                    false,
                )?;
                let mut member = OpFamilyMember {
                    is_func: true,
                    object: funcOid,
                    number: item.number as i16,
                    ..Default::default()
                };
                // allow overriding of the function's actual arg types
                if !item.class_args.is_nil() {
                    let (l, r) = processTypesSpec(mcx, &item.class_args)?;
                    member.lefttype = l;
                    member.righttype = r;
                }
                assignProcTypes(&mut member, am.kind, typeoid, am.opts_proc_number)?;
                addFamilyMember(&mut procedures, member)?;
            }
            OPCLASS_ITEM_STORAGETYPE => {
                if OidIsValid(storageoid) {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        "storage type specified more than once".into(),
                    ));
                }
                storageoid =
                    typename_type_id(mcx, item.storedtype.expect("STORAGE Typename"))?;
            }
            other => panic!("unrecognized item type: {other}"),
        }
    }

    if OidIsValid(storageoid) {
        if storageoid == typeoid {
            // Just drop the spec if same as column datatype.
            storageoid = InvalidOid;
        } else if !am.amstorage {
            return Err(err(
                ERRCODE_INVALID_OBJECT_DEFINITION,
                format!(
                    "storage type cannot be different from data type for access method \"{amname}\""
                ),
            ));
        }
    }

    let rel = table::table_open(mcx, OPERATOR_CLASS_RELATION_ID, RowExclusiveLock)?;

    if OidIsValid(syscache_seams::lookup_pg_opclass_oid_by_name::call(
        am.amoid,
        opcname,
        namespaceoid,
    )?) {
        return Err(err(
            ERRCODE_DUPLICATE_OBJECT,
            format!(
                "operator class \"{opcname}\" for access method \"{amname}\" already exists"
            ),
        ));
    }

    // A default opclass must be the only default for its type (visibility
    // ignored so typcache answers stay unique).
    if stmt.isDefault {
        let scratch = mcx::MemoryContext::new("DefineOpClass default check");
        let rows =
            syscache_seams::lookup_pg_opclass_rows_by_am::call(scratch.mcx(), am.amoid)?;
        for &(_oid, _fam, opcintype, opcdefault, name) in rows.iter() {
            if opcintype == typeoid && opcdefault {
                return Err(Box::new(
                    (*err(
                        ERRCODE_DUPLICATE_OBJECT,
                        format!(
                            "could not make operator class \"{opcname}\" be default for type {}",
                            commands_define::TypeNameToString(
                                mcx,
                                stmt.datatype
                                    .and_then(|n| n.as_variant::<TypeName>())
                                    .expect("CreateOpClassStmt.datatype"),
                            )?
                            .as_str()
                        ),
                    ))
                    .with_detail(format!(
                        "Operator class \"{}\" already is the default.",
                        core::str::from_utf8(name.name_str()).unwrap_or("")
                    )),
                ));
            }
        }
    }

    let opclassoid =
        catalog::GetNewOidWithIndex(mcx, &rel, OPCLASS_OID_INDEX_ID, Anum_pg_opclass_oid as i16)?;
    let mut opcName = NameData::default();
    opcName.namestrcpy(opcname);
    let values = [
        Datum::from_oid(opclassoid),
        Datum::from_oid(am.amoid),
        Datum::from_usize(opcName.data.as_ptr() as usize),
        Datum::from_oid(namespaceoid),
        Datum::from_oid(miscinit::GetUserId()),
        Datum::from_oid(opfamilyoid),
        Datum::from_oid(typeoid),
        Datum::from_bool(stmt.isDefault),
        Datum::from_oid(storageoid),
    ];
    let nulls = [false; Natts_pg_opclass];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    // Default dependency choices: CREATE OPERATOR CLASS historically makes
    // hard dependencies on the opclass.
    for op in operators.iter_mut().chain(procedures.iter_mut()) {
        op.ref_is_hard = true;
        op.ref_is_family = false;
        op.refobjid = opclassoid;
    }

    // Let the index AM editorialize on the dependency choices.
    am_adjust_members(am.kind, opfamilyoid, opclassoid, &mut operators, &mut procedures)?;

    storeOperators(mcx, &stmt.opfamilyname, am.amoid, opfamilyoid, &operators, false)?;
    storeProcedures(mcx, &stmt.opfamilyname, am.amoid, opfamilyoid, &procedures, false)?;

    let myself = ObjectAddress::set(OPERATOR_CLASS_RELATION_ID, opclassoid);

    // No explicit AM dependency: it exists through the opfamily.
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid),
        DependencyType::Normal,
    )?;
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(OPERATOR_FAMILY_RELATION_ID, opfamilyoid),
        DependencyType::Auto,
    )?;
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(TYPE_RELATION_ID, typeoid),
        DependencyType::Normal,
    )?;
    if OidIsValid(storageoid) {
        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(TYPE_RELATION_ID, storageoid),
            DependencyType::Normal,
        )?;
    }
    recordDependencyOnOwner(mcx, OPERATOR_CLASS_RELATION_ID, opclassoid, miscinit::GetUserId())?;
    // dependency on extension (C opclasscmds.c: ONE recordDependencyOnCurrentExtension;
    // t31 fold fix: the extowner x surgery-isn merge keep-both'd identical arms,
    // double-inserting DEPENDENCY_EXTENSION pg_depend rows for CREATE OPERATOR
    // CLASS inside extension scripts)
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    // C: EventTriggerCollectCreateOpClass (SCT_CreateOpClass) also retains the
    // operators/procedures lists — extension-deparse-only surface; the SRF
    // rows (command_tag/object_type/identity) are identical via Simple.
    event_trigger::EventTriggerCollectSimpleCommand(
        myself,
        ObjectAddress::set(types_core::InvalidOid, types_core::InvalidOid),
        cmdtag::GetCommandTagEnum(b"CREATE OPERATOR CLASS"),
    );

    rel.close(RowExclusiveLock)?;
    Ok(myself)
}

// DefineOpFamily: define a new index operator family.
pub fn DefineOpFamily<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateOpFamilyStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let mut buf = [""; 4];
    let parts = name_parts(&stmt.opfamilyname, &mut buf);
    let (namespaceoid, opfname) = catalog_namespace::QualifiedNameGetCreationNamespace(mcx, parts)?;

    namespace_create_check(mcx, namespaceoid)?;

    let amname = stmt.amname.expect("CreateOpFamilyStmt.amname");
    let amoid = get_index_am_oid(amname)?;

    require_superuser("create an operator family")?;

    CreateOpFamily(mcx, amname, opfname, namespaceoid, amoid)
}

// AlterOpFamily: ALTER OPERATOR FAMILY ... ADD/DROP.
pub fn AlterOpFamily<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterOpFamilyStmt<'mcx>) -> PgResult<Oid> {
    let amname = stmt.amname.expect("AlterOpFamilyStmt.amname");
    let am = get_am_info(amname)?;

    let opfamilyoid = get_opfamily_oid(am.amoid, &stmt.opfamilyname, false)?;

    require_superuser("alter an operator family")?;

    if stmt.isDrop {
        AlterOpFamilyDrop(mcx, stmt, &am, opfamilyoid)?;
    } else {
        AlterOpFamilyAdd(mcx, stmt, &am, opfamilyoid)?;
    }
    Ok(opfamilyoid)
}

fn AlterOpFamilyAdd<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterOpFamilyStmt<'mcx>,
    am: &AmInfo,
    opfamilyoid: Oid,
) -> PgResult<()> {
    let mut operators: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);
    let mut procedures: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);

    for n in stmt.items.iter() {
        let item = n.as_variant::<CreateOpClassItem>().expect("CreateOpClassItem");
        match item.itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if item.number <= 0 || item.number > am.max_op_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid operator number {}, must be between 1 and {}",
                            item.number, am.max_op_number
                        ),
                    ));
                }
                let owa = item_owa(item);
                if owa.objargs.is_nil() {
                    return Err(err(
                        ERRCODE_SYNTAX_ERROR,
                        "operator argument types must be specified in ALTER OPERATOR FAMILY"
                            .into(),
                    ));
                }
                let operOid = parse_oper::LookupOperWithArgs(&owa.objname, &owa.objargs, false)?;
                let sortfamilyOid = if !item.order_family.is_nil() {
                    get_opfamily_oid(BTREE_AM_OID, &item.order_family, false)?
                } else {
                    InvalidOid
                };
                // Historically, ALTER ADD creates soft dependencies.
                let mut member = OpFamilyMember {
                    is_func: false,
                    object: operOid,
                    number: item.number as i16,
                    sortfamily: sortfamilyOid,
                    ref_is_hard: false,
                    ref_is_family: true,
                    refobjid: opfamilyoid,
                    ..Default::default()
                };
                assignOperTypes(&mut member, am.kind, stmt.amname.expect("amname"), InvalidOid)?;
                addFamilyMember(&mut operators, member)?;
            }
            OPCLASS_ITEM_FUNCTION => {
                if item.number <= 0 || item.number > am.max_proc_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid function number {}, must be between 1 and {}",
                            item.number, am.max_proc_number
                        ),
                    ));
                }
                let owa = item_owa(item);
                let funcOid = parse_func::LookupFuncWithArgs(
                    types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION,
                    owa,
                    false,
                )?;
                let mut member = OpFamilyMember {
                    is_func: true,
                    object: funcOid,
                    number: item.number as i16,
                    ref_is_hard: false,
                    ref_is_family: true,
                    refobjid: opfamilyoid,
                    ..Default::default()
                };
                if !item.class_args.is_nil() {
                    let (l, r) = processTypesSpec(mcx, &item.class_args)?;
                    member.lefttype = l;
                    member.righttype = r;
                }
                assignProcTypes(&mut member, am.kind, InvalidOid, am.opts_proc_number)?;
                addFamilyMember(&mut procedures, member)?;
            }
            OPCLASS_ITEM_STORAGETYPE => {
                return Err(err(
                    ERRCODE_SYNTAX_ERROR,
                    "STORAGE cannot be specified in ALTER OPERATOR FAMILY".into(),
                ));
            }
            other => panic!("unrecognized item type: {other}"),
        }
    }

    am_adjust_members(am.kind, opfamilyoid, InvalidOid, &mut operators, &mut procedures)?;

    storeOperators(mcx, &stmt.opfamilyname, am.amoid, opfamilyoid, &operators, true)?;
    storeProcedures(mcx, &stmt.opfamilyname, am.amoid, opfamilyoid, &procedures, true)?;
    Ok(())
}

fn AlterOpFamilyDrop<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterOpFamilyStmt<'mcx>,
    am: &AmInfo,
    opfamilyoid: Oid,
) -> PgResult<()> {
    let mut operators: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);
    let mut procedures: mcx::PgVec<'_, OpFamilyMember> = mcx::PgVec::new_in(mcx);

    for n in stmt.items.iter() {
        let item = n.as_variant::<CreateOpClassItem>().expect("CreateOpClassItem");
        match item.itemtype {
            OPCLASS_ITEM_OPERATOR => {
                if item.number <= 0 || item.number > am.max_op_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid operator number {}, must be between 1 and {}",
                            item.number, am.max_op_number
                        ),
                    ));
                }
                let (lefttype, righttype) = processTypesSpec(mcx, &item.class_args)?;
                let member = OpFamilyMember {
                    is_func: false,
                    number: item.number as i16,
                    lefttype,
                    righttype,
                    ..Default::default()
                };
                addFamilyMember(&mut operators, member)?;
            }
            OPCLASS_ITEM_FUNCTION => {
                if item.number <= 0 || item.number > am.max_proc_number {
                    return Err(err(
                        ERRCODE_INVALID_OBJECT_DEFINITION,
                        format!(
                            "invalid function number {}, must be between 1 and {}",
                            item.number, am.max_proc_number
                        ),
                    ));
                }
                let (lefttype, righttype) = processTypesSpec(mcx, &item.class_args)?;
                let member = OpFamilyMember {
                    is_func: true,
                    number: item.number as i16,
                    lefttype,
                    righttype,
                    ..Default::default()
                };
                addFamilyMember(&mut procedures, member)?;
            }
            other => panic!("unrecognized item type: {other}"),
        }
    }

    dropOperators(mcx, &stmt.opfamilyname, opfamilyoid, &operators)?;
    dropProcedures(mcx, &stmt.opfamilyname, opfamilyoid, &procedures)?;
    Ok(())
}

// Explicit arg types used in ALTER ADD/DROP.
fn processTypesSpec(mcx: Mcx<'_>, args: &NodeList<'_>) -> PgResult<(Oid, Oid)> {
    assert!(!args.is_nil());
    let lefttype = typename_type_id(mcx, args.nth(0))?;
    let righttype = if args.len() > 1 {
        typename_type_id(mcx, args.nth(1))?
    } else {
        lefttype
    };
    if args.len() > 2 {
        return Err(err(
            ERRCODE_SYNTAX_ERROR,
            "one or two argument types must be specified".into(),
        ));
    }
    Ok((lefttype, righttype))
}

// Determine lefttype/righttype for an operator member and validate.
fn assignOperTypes(
    member: &mut OpFamilyMember,
    amkind: IndexAmKind,
    amname: &str,
    _typeoid: Oid,
) -> PgResult<()> {
    let opform = syscache_seams::lookup_pg_operator_shape::call(member.object)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator {}", member.object));

    // Opfamily operators must be binary.
    if opform.oprleft == InvalidOid || opform.oprright == InvalidOid {
        return Err(err(
            ERRCODE_INVALID_OBJECT_DEFINITION,
            "index operators must be binary".into(),
        ));
    }

    if OidIsValid(member.sortfamily) {
        // Ordering op: check the index supports that.
        if !amkind.amcanorderbyop() {
            return Err(err(
                ERRCODE_INVALID_OBJECT_DEFINITION,
                format!(
                    "access method \"{amname}\" does not support ordering operators"
                ),
            ));
        }
    } else {
        // Search operators must return boolean.
        if opform.oprresult != BOOLOID {
            return Err(err(
                ERRCODE_INVALID_OBJECT_DEFINITION,
                "index search operators must return boolean".into(),
            ));
        }
    }

    if !OidIsValid(member.lefttype) {
        member.lefttype = opform.oprleft;
    }
    if !OidIsValid(member.righttype) {
        member.righttype = opform.oprright;
    }
    Ok(())
}

// Determine lefttype/righttype for a support procedure and validate.
fn assignProcTypes(
    member: &mut OpFamilyMember,
    amkind: IndexAmKind,
    typeoid: Oid,
    opclassOptsProcNum: i32,
) -> PgResult<()> {
    let scratch = mcx::MemoryContext::new("assignProcTypes");
    let (prorettype, proargtypes) = lsyscache::get_func_signature(scratch.mcx(), member.object)?;
    let pronargs = proargtypes.len();
    let def_err = |msg: &str| err(ERRCODE_INVALID_OBJECT_DEFINITION, msg.into());

    if member.number as i32 == opclassOptsProcNum {
        if OidIsValid(typeoid) {
            if (OidIsValid(member.lefttype) && member.lefttype != typeoid)
                || (OidIsValid(member.righttype) && member.righttype != typeoid)
            {
                return Err(def_err(
                    "associated data types for operator class options parsing functions must match opclass input type",
                ));
            }
        } else if member.lefttype != member.righttype {
            return Err(def_err(
                "left and right associated data types for operator class options parsing functions must match",
            ));
        }
        if prorettype != VOIDOID || pronargs != 1 || proargtypes[0] != INTERNALOID {
            return Err(Box::new(
                (*def_err("invalid operator class options parsing function")).with_hint(
                    format!(
                        "Valid signature of operator class options parsing function is {}.",
                        "(internal) RETURNS void"
                    ),
                ),
            ));
        }
    } else if amkind.amcanorder() {
        if member.number as u16 == BTORDER_PROC {
            if pronargs != 2 {
                return Err(def_err("ordering comparison functions must have two arguments"));
            }
            if prorettype != INT4OID {
                return Err(def_err("ordering comparison functions must return integer"));
            }
            if !OidIsValid(member.lefttype) {
                member.lefttype = proargtypes[0];
            }
            if !OidIsValid(member.righttype) {
                member.righttype = proargtypes[1];
            }
        } else if member.number as u16 == BTSORTSUPPORT_PROC {
            if pronargs != 1 || proargtypes[0] != INTERNALOID {
                return Err(def_err(
                    "ordering sort support functions must accept type \"internal\"",
                ));
            }
            if prorettype != VOIDOID {
                return Err(def_err("ordering sort support functions must return void"));
            }
        } else if member.number as u16 == BTINRANGE_PROC {
            if pronargs != 5 {
                return Err(def_err("ordering in_range functions must have five arguments"));
            }
            if prorettype != BOOLOID {
                return Err(def_err("ordering in_range functions must return boolean"));
            }
            if !OidIsValid(member.lefttype) {
                member.lefttype = proargtypes[0];
            }
            if !OidIsValid(member.righttype) {
                member.righttype = proargtypes[2];
            }
        } else if member.number as u16 == BTEQUALIMAGE_PROC {
            if pronargs != 1 {
                return Err(def_err("ordering equal image functions must have one argument"));
            }
            if prorettype != BOOLOID {
                return Err(def_err("ordering equal image functions must return boolean"));
            }
            // equalimage is only called at CREATE INDEX time with the
            // opclass opcintype for both sides; cross-type is nonsense.
            if member.lefttype != member.righttype {
                return Err(def_err("ordering equal image functions must not be cross-type"));
            }
        } else if member.number as u16 == BTSKIPSUPPORT_PROC {
            if pronargs != 1 || proargtypes[0] != INTERNALOID {
                return Err(def_err(
                    "btree skip support functions must accept type \"internal\"",
                ));
            }
            if prorettype != VOIDOID {
                return Err(def_err("btree skip support functions must return void"));
            }
            if member.lefttype != member.righttype {
                return Err(def_err("btree skip support functions must not be cross-type"));
            }
        }
    } else if amkind.amcanhash() {
        if member.number as u16 == HASHSTANDARD_PROC {
            if pronargs != 1 {
                return Err(def_err("hash function 1 must have one argument"));
            }
            if prorettype != INT4OID {
                return Err(def_err("hash function 1 must return integer"));
            }
        } else if member.number as u16 == HASHEXTENDED_PROC {
            if pronargs != 2 {
                return Err(def_err("hash function 2 must have two arguments"));
            }
            if prorettype != INT8OID {
                return Err(def_err("hash function 2 must return bigint"));
            }
        }
        if !OidIsValid(member.lefttype) {
            member.lefttype = proargtypes[0];
        }
        if !OidIsValid(member.righttype) {
            member.righttype = proargtypes[0];
        }
    }

    // CREATE OPERATOR CLASS defaults to opcintype; CREATE/ALTER FAMILY has
    // no opcintype, so the user must specify the types.
    if !OidIsValid(member.lefttype) {
        member.lefttype = typeoid;
    }
    if !OidIsValid(member.righttype) {
        member.righttype = typeoid;
    }
    if !OidIsValid(member.lefttype) || !OidIsValid(member.righttype) {
        return Err(def_err(
            "associated data types must be specified for index support function",
        ));
    }
    Ok(())
}

// Reject duplicated strategy/proc numbers, then append.
fn addFamilyMember(list: &mut mcx::PgVec<'_, OpFamilyMember>, member: OpFamilyMember) -> PgResult<()> {
    for old in list.iter() {
        if old.number == member.number
            && old.lefttype == member.lefttype
            && old.righttype == member.righttype
        {
            let what = if member.is_func { "function" } else { "operator" };
            return Err(err(
                ERRCODE_INVALID_OBJECT_DEFINITION,
                format!(
                    "{what} number {} for ({},{}) appears more than once",
                    member.number,
                    format_type::format_type_be(member.lefttype)?,
                    format_type::format_type_be(member.righttype)?
                ),
            ));
        }
    }
    list.push(member);
    Ok(())
}

fn opfamily_display(opfamilyname: &NodeList<'_>) -> String {
    let mut buf = [""; 4];
    name_parts(opfamilyname, &mut buf).join(".")
}

// Dump the operators to pg_amop, with their pg_depend entries.
fn storeOperators(
    mcx: Mcx<'_>,
    opfamilyname: &NodeList<'_>,
    amoid: Oid,
    opfamilyoid: Oid,
    operators: &[OpFamilyMember],
    isAdd: bool,
) -> PgResult<()> {
    let rel = table::table_open(mcx, ACCESS_METHOD_OPERATOR_RELATION_ID, RowExclusiveLock)?;

    for op in operators {
        // Nicer message than "duplicate key" when adding to a family.
        if isAdd
            && SearchSysCacheExists(
                AMOPSTRATEGY,
                SysCacheKey::Value(Datum::from_oid(opfamilyoid)),
                SysCacheKey::Value(Datum::from_oid(op.lefttype)),
                SysCacheKey::Value(Datum::from_oid(op.righttype)),
                SysCacheKey::Value(Datum::from_i16(op.number)),
            )?
        {
            return Err(err(
                ERRCODE_DUPLICATE_OBJECT,
                format!(
                    "operator {}({},{}) already exists in operator family \"{}\"",
                    op.number,
                    format_type::format_type_be(op.lefttype)?,
                    format_type::format_type_be(op.righttype)?,
                    opfamily_display(opfamilyname)
                ),
            ));
        }

        let oppurpose = if OidIsValid(op.sortfamily) { AMOP_ORDER } else { AMOP_SEARCH };

        let entryoid = catalog::GetNewOidWithIndex(
            mcx,
            &rel,
            ACCESS_METHOD_OPERATOR_OID_INDEX_ID,
            Anum_pg_amop_oid as i16,
        )?;
        let values = [
            Datum::from_oid(entryoid),
            Datum::from_oid(opfamilyoid),
            Datum::from_oid(op.lefttype),
            Datum::from_oid(op.righttype),
            Datum::from_i16(op.number),
            Datum::from_char(oppurpose),
            Datum::from_oid(op.object),
            Datum::from_oid(amoid),
            Datum::from_oid(op.sortfamily),
        ];
        let nulls = [false; Natts_pg_amop];
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

        let myself = ObjectAddress::set(ACCESS_METHOD_OPERATOR_RELATION_ID, entryoid);

        // See amapi.h for the dependency-strength rules.
        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(OPERATOR_RELATION_ID, op.object),
            if op.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
        )?;
        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(
                if op.ref_is_family {
                    OPERATOR_FAMILY_RELATION_ID
                } else {
                    OPERATOR_CLASS_RELATION_ID
                },
                op.refobjid,
            ),
            if op.ref_is_hard { DependencyType::Internal } else { DependencyType::Auto },
        )?;
        if typeDepNeeded(op.lefttype, op)? {
            recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(TYPE_RELATION_ID, op.lefttype),
                if op.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
            )?;
        }
        if op.lefttype != op.righttype && typeDepNeeded(op.righttype, op)? {
            recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(TYPE_RELATION_ID, op.righttype),
                if op.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
            )?;
        }
        // An ordering operator also depends on its referenced opfamily.
        if OidIsValid(op.sortfamily) {
            recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(OPERATOR_FAMILY_RELATION_ID, op.sortfamily),
                if op.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
            )?;
        }
    }

    rel.close(RowExclusiveLock)
}

// Dump the support procedures to pg_amproc, with their pg_depend entries.
fn storeProcedures(
    mcx: Mcx<'_>,
    opfamilyname: &NodeList<'_>,
    _amoid: Oid,
    opfamilyoid: Oid,
    procedures: &[OpFamilyMember],
    isAdd: bool,
) -> PgResult<()> {
    let rel = table::table_open(mcx, ACCESS_METHOD_PROCEDURE_RELATION_ID, RowExclusiveLock)?;

    for proc in procedures {
        if isAdd
            && SearchSysCacheExists(
                AMPROCNUM,
                SysCacheKey::Value(Datum::from_oid(opfamilyoid)),
                SysCacheKey::Value(Datum::from_oid(proc.lefttype)),
                SysCacheKey::Value(Datum::from_oid(proc.righttype)),
                SysCacheKey::Value(Datum::from_i16(proc.number)),
            )?
        {
            return Err(err(
                ERRCODE_DUPLICATE_OBJECT,
                format!(
                    "function {}({},{}) already exists in operator family \"{}\"",
                    proc.number,
                    format_type::format_type_be(proc.lefttype)?,
                    format_type::format_type_be(proc.righttype)?,
                    opfamily_display(opfamilyname)
                ),
            ));
        }

        let entryoid = catalog::GetNewOidWithIndex(
            mcx,
            &rel,
            ACCESS_METHOD_PROCEDURE_OID_INDEX_ID,
            Anum_pg_amproc_oid as i16,
        )?;
        let values = [
            Datum::from_oid(entryoid),
            Datum::from_oid(opfamilyoid),
            Datum::from_oid(proc.lefttype),
            Datum::from_oid(proc.righttype),
            Datum::from_i16(proc.number),
            Datum::from_oid(proc.object),
        ];
        let nulls = [false; Natts_pg_amproc];
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

        let myself = ObjectAddress::set(ACCESS_METHOD_PROCEDURE_RELATION_ID, entryoid);

        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(PROCEDURE_RELATION_ID, proc.object),
            if proc.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
        )?;
        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::set(
                if proc.ref_is_family {
                    OPERATOR_FAMILY_RELATION_ID
                } else {
                    OPERATOR_CLASS_RELATION_ID
                },
                proc.refobjid,
            ),
            if proc.ref_is_hard { DependencyType::Internal } else { DependencyType::Auto },
        )?;
        if typeDepNeeded(proc.lefttype, proc)? {
            recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(TYPE_RELATION_ID, proc.lefttype),
                if proc.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
            )?;
        }
        if proc.lefttype != proc.righttype && typeDepNeeded(proc.righttype, proc)? {
            recordDependencyOn(
                mcx,
                &myself,
                &ObjectAddress::set(TYPE_RELATION_ID, proc.righttype),
                if proc.ref_is_hard { DependencyType::Normal } else { DependencyType::Auto },
            )?;
        }
    }

    rel.close(RowExclusiveLock)
}

// A member needs an explicit type dependency unless its operator/function
// already carries one through its input types.
fn typeDepNeeded(typid: Oid, member: &OpFamilyMember) -> PgResult<bool> {
    if catalog::IsPinnedObject(TYPE_RELATION_ID, typid) {
        return Ok(false);
    }
    if member.is_func {
        let scratch = mcx::MemoryContext::new("typeDepNeeded");
        let (_ret, argtypes) = lsyscache::get_func_signature(scratch.mcx(), member.object)?;
        Ok(!argtypes.contains(&typid))
    } else {
        let (lefttype, righttype) = lsyscache::op_input_types(member.object)?;
        Ok(typid != lefttype && typid != righttype)
    }
}

// Loose-member removal is always RESTRICT.
fn dropOperators(
    mcx: Mcx<'_>,
    opfamilyname: &NodeList<'_>,
    opfamilyoid: Oid,
    operators: &[OpFamilyMember],
) -> PgResult<()> {
    for op in operators {
        let amopid = GetSysCacheOid(
            AMOPSTRATEGY,
            Anum_pg_amop_oid,
            SysCacheKey::Value(Datum::from_oid(opfamilyoid)),
            SysCacheKey::Value(Datum::from_oid(op.lefttype)),
            SysCacheKey::Value(Datum::from_oid(op.righttype)),
            SysCacheKey::Value(Datum::from_i16(op.number)),
        )?;
        if !OidIsValid(amopid) {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "operator {}({},{}) does not exist in operator family \"{}\"",
                    op.number,
                    format_type::format_type_be(op.lefttype)?,
                    format_type::format_type_be(op.righttype)?,
                    opfamily_display(opfamilyname)
                ),
            ));
        }
        let object = ObjectAddress::set(ACCESS_METHOD_OPERATOR_RELATION_ID, amopid);
        catalog_dependency::performDeletion(
            mcx,
            &object,
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            0,
        )?;
    }
    Ok(())
}

fn dropProcedures(
    mcx: Mcx<'_>,
    opfamilyname: &NodeList<'_>,
    opfamilyoid: Oid,
    procedures: &[OpFamilyMember],
) -> PgResult<()> {
    for proc in procedures {
        let amprocid = GetSysCacheOid(
            AMPROCNUM,
            Anum_pg_amproc_oid,
            SysCacheKey::Value(Datum::from_oid(opfamilyoid)),
            SysCacheKey::Value(Datum::from_oid(proc.lefttype)),
            SysCacheKey::Value(Datum::from_oid(proc.righttype)),
            SysCacheKey::Value(Datum::from_i16(proc.number)),
        )?;
        if !OidIsValid(amprocid) {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "function {}({},{}) does not exist in operator family \"{}\"",
                    proc.number,
                    format_type::format_type_be(proc.lefttype)?,
                    format_type::format_type_be(proc.righttype)?,
                    opfamily_display(opfamilyname)
                ),
            ));
        }
        let object = ObjectAddress::set(ACCESS_METHOD_PROCEDURE_RELATION_ID, amprocid);
        catalog_dependency::performDeletion(
            mcx,
            &object,
            types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
            0,
        )?;
    }
    Ok(())
}

// IsThereOpClassInNamespace / IsThereOpFamilyInNamespace (opclasscmds.c):
// friendliness checks ahead of the unique-index failure.
pub fn IsThereOpClassInNamespace(
    mcx: Mcx<'_>,
    opcname: &str,
    opcmethod: Oid,
    opcnamespace: Oid,
) -> PgResult<()> {
    if SearchSysCacheExists(
        cache_syscache::cacheinfo::CLAAMNAMENSP,
        SysCacheKey::Value(Datum::from_oid(opcmethod)),
        SysCacheKey::Str(opcname),
        SysCacheKey::Value(Datum::from_oid(opcnamespace)),
        SysCacheKey::UNUSED,
    )? {
        let nspname = lsyscache::get_namespace_name(mcx, opcnamespace)?
            .map(|n| n.to_string())
            .unwrap_or_default();
        return Err(Box::new(
            types_error::PgError::error(format!(
                "operator class \"{opcname}\" for access method \"{}\" already exists in schema \"{nspname}\"",
                get_am_name(opcmethod)?
            ))
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    Ok(())
}

pub fn IsThereOpFamilyInNamespace(
    mcx: Mcx<'_>,
    opfname: &str,
    opfmethod: Oid,
    opfnamespace: Oid,
) -> PgResult<()> {
    if SearchSysCacheExists(
        cache_syscache::cacheinfo::OPFAMILYAMNAMENSP,
        SysCacheKey::Value(Datum::from_oid(opfmethod)),
        SysCacheKey::Str(opfname),
        SysCacheKey::Value(Datum::from_oid(opfnamespace)),
        SysCacheKey::UNUSED,
    )? {
        let nspname = lsyscache::get_namespace_name(mcx, opfnamespace)?
            .map(|n| n.to_string())
            .unwrap_or_default();
        return Err(Box::new(
            types_error::PgError::error(format!(
                "operator family \"{opfname}\" for access method \"{}\" already exists in schema \"{nspname}\"",
                get_am_name(opfmethod)?
            ))
            .with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT),
        ));
    }
    Ok(())
}
