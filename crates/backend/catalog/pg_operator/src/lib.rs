//! pg_operator.c — pg_operator row construction, shell operators, and
//! commutator/negator back-linking.
#![allow(non_snake_case, non_upper_case_globals)]

use cache_syscache::{SearchSysCacheCopy, SysCacheKey, OPEROID};
use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{
    InvalidOid, Oid, BOOLOID, NAMESPACE_RELATION_ID, OPERATOR_OID_INDEX_ID, OPERATOR_RELATION_ID,
    PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_INVALID_NAME,
};
use types_nodes::NodeList;
use types_rel::{Relation, RowExclusiveLock};
use types_tuple::NameData;

pub const Natts_pg_operator: usize = 15;
pub const Anum_pg_operator_oid: i32 = 1;
pub const Anum_pg_operator_oprname: i32 = 2;
pub const Anum_pg_operator_oprnamespace: i32 = 3;
pub const Anum_pg_operator_oprowner: i32 = 4;
pub const Anum_pg_operator_oprkind: i32 = 5;
pub const Anum_pg_operator_oprcanmerge: i32 = 6;
pub const Anum_pg_operator_oprcanhash: i32 = 7;
pub const Anum_pg_operator_oprleft: i32 = 8;
pub const Anum_pg_operator_oprright: i32 = 9;
pub const Anum_pg_operator_oprresult: i32 = 10;
pub const Anum_pg_operator_oprcom: i32 = 11;
pub const Anum_pg_operator_oprnegate: i32 = 12;
pub const Anum_pg_operator_oprcode: i32 = 13;
pub const Anum_pg_operator_oprrest: i32 = 14;
pub const Anum_pg_operator_oprjoin: i32 = 15;

#[derive(Clone, Copy)]
pub struct FormPgOperator {
    pub oid: Oid,
    pub oprname: NameData,
    pub oprnamespace: Oid,
    pub oprowner: Oid,
    pub oprkind: i8,
    pub oprcanmerge: bool,
    pub oprcanhash: bool,
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
    pub oprcom: Oid,
    pub oprnegate: Oid,
    pub oprcode: Oid,
    pub oprrest: Oid,
    pub oprjoin: Oid,
}

fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[track_caller]
#[cold]
fn err(sqlstate: types_error::SqlState, msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

// All 15 pg_operator columns are fixed-width NOT NULL (GETSTRUCT invariant).
pub fn form_of_tuple(rel: &Relation<'_>, tup: &types_tuple::HeapTupleData<'_>) -> FormPgOperator {
    let get = |attnum: i32| -> Datum {
        // SAFETY: fixed-width NOT NULL column of a pg_operator row.
        unsafe { types_tuple::fastgetattr_fixed(tup, attnum, rel.descr()) }
    };
    // SAFETY: oprname datum points at the row's inline 64-byte name column.
    let oprname = unsafe { *(get(Anum_pg_operator_oprname).as_usize() as *const NameData) };
    FormPgOperator {
        oid: get(Anum_pg_operator_oid).as_oid(),
        oprname,
        oprnamespace: get(Anum_pg_operator_oprnamespace).as_oid(),
        oprowner: get(Anum_pg_operator_oprowner).as_oid(),
        oprkind: get(Anum_pg_operator_oprkind).as_i8(),
        oprcanmerge: get(Anum_pg_operator_oprcanmerge).as_bool(),
        oprcanhash: get(Anum_pg_operator_oprcanhash).as_bool(),
        oprleft: get(Anum_pg_operator_oprleft).as_oid(),
        oprright: get(Anum_pg_operator_oprright).as_oid(),
        oprresult: get(Anum_pg_operator_oprresult).as_oid(),
        oprcom: get(Anum_pg_operator_oprcom).as_oid(),
        oprnegate: get(Anum_pg_operator_oprnegate).as_oid(),
        oprcode: get(Anum_pg_operator_oprcode).as_oid(),
        oprrest: get(Anum_pg_operator_oprrest).as_oid(),
        oprjoin: get(Anum_pg_operator_oprjoin).as_oid(),
    }
}

fn operator_values(form: &FormPgOperator) -> [Datum; Natts_pg_operator] {
    [
        Datum::from_oid(form.oid),
        Datum::from_usize(form.oprname.data.as_ptr() as usize),
        Datum::from_oid(form.oprnamespace),
        Datum::from_oid(form.oprowner),
        Datum::from_char(form.oprkind),
        Datum::from_bool(form.oprcanmerge),
        Datum::from_bool(form.oprcanhash),
        Datum::from_oid(form.oprleft),
        Datum::from_oid(form.oprright),
        Datum::from_oid(form.oprresult),
        Datum::from_oid(form.oprcom),
        Datum::from_oid(form.oprnegate),
        Datum::from_oid(form.oprcode),
        Datum::from_oid(form.oprrest),
        Datum::from_oid(form.oprjoin),
    ]
}

// Must match op_chars in scan.l.
pub fn validOperatorName(name: &str) -> bool {
    let bytes = name.as_bytes();
    let len = bytes.len();
    if len == 0 || len >= 64 {
        return false;
    }
    const OP_CHARS: &[u8] = b"~!@#^&|`?+-*/%<>=";
    if !bytes.iter().all(|b| OP_CHARS.contains(b)) {
        return false;
    }
    if bytes.windows(2).any(|w| w == b"/*" || w == b"--") {
        return false;
    }
    if len > 1 && (bytes[len - 1] == b'+' || bytes[len - 1] == b'-') {
        const SPECIAL: &[u8] = b"~!@#^&|`?%";
        if !bytes[..len - 1].iter().any(|b| SPECIAL.contains(b)) {
            return false;
        }
    }
    bytes != b"!="
}

#[track_caller]
#[cold]
fn invalid_operator_name(name: &str) -> Box<PgError> {
    err(ERRCODE_INVALID_NAME, format!("\"{name}\" is not a valid operator name"))
}

fn OperatorGet(
    operatorName: &str,
    operatorNamespace: Oid,
    leftObjectId: Oid,
    rightObjectId: Oid,
) -> PgResult<(Oid, bool)> {
    let oid = syscache_seams::lookup_pg_operator_oid_exact::call(
        operatorName,
        leftObjectId,
        rightObjectId,
        operatorNamespace,
    )?;
    if !OidIsValid(oid) {
        return Ok((InvalidOid, false));
    }
    let shape = syscache_seams::lookup_pg_operator_shape::call(oid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator {oid}"));
    Ok((oid, OidIsValid(shape.oprcode)))
}

pub fn OperatorLookup(
    operatorName: &NodeList<'_>,
    leftObjectId: Oid,
    rightObjectId: Oid,
) -> PgResult<(Oid, bool)> {
    let oid = parse_oper::LookupOperName(operatorName, leftObjectId, rightObjectId, true)?;
    if !OidIsValid(oid) {
        return Ok((InvalidOid, false));
    }
    let oprcode = lsyscache::get_opcode(oid)?;
    Ok((oid, OidIsValid(oprcode)))
}

fn OperatorShellMake(
    mcx: Mcx<'_>,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> PgResult<Oid> {
    if !validOperatorName(operatorName) {
        return Err(invalid_operator_name(operatorName));
    }

    let rel = table::table_open(mcx, OPERATOR_RELATION_ID, RowExclusiveLock)?;
    let operatorObjectId =
        catalog::GetNewOidWithIndex(mcx, &rel, OPERATOR_OID_INDEX_ID, Anum_pg_operator_oid as i16)?;

    let mut oprname = NameData::default();
    oprname.namestrcpy(operatorName);
    let form = FormPgOperator {
        oid: operatorObjectId,
        oprname,
        oprnamespace: operatorNamespace,
        oprowner: miscinit::GetUserId(),
        oprkind: if OidIsValid(leftTypeId) { b'b' as i8 } else { b'l' as i8 },
        oprcanmerge: false,
        oprcanhash: false,
        oprleft: leftTypeId,
        oprright: rightTypeId,
        oprresult: InvalidOid,
        oprcom: InvalidOid,
        oprnegate: InvalidOid,
        oprcode: InvalidOid,
        oprrest: InvalidOid,
        oprjoin: InvalidOid,
    };
    let values = operator_values(&form);
    let nulls = [false; Natts_pg_operator];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    makeOperatorDependencies(mcx, &form, true, false)?;

    xact::CommandCounterIncrement()?;
    rel.close(RowExclusiveLock)?;
    Ok(operatorObjectId)
}

pub fn OperatorCreate(
    mcx: Mcx<'_>,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
    procedureId: Oid,
    commutatorName: Option<&NodeList<'_>>,
    negatorName: Option<&NodeList<'_>>,
    restrictionId: Oid,
    joinId: Oid,
    canMerge: bool,
    canHash: bool,
) -> PgResult<ObjectAddress> {
    let mut selfCommutator = false;

    if !validOperatorName(operatorName) {
        return Err(invalid_operator_name(operatorName));
    }

    let operResultType = lsyscache::get_func_rettype(procedureId)?;

    OperatorValidateParams(
        leftTypeId,
        rightTypeId,
        operResultType,
        commutatorName.is_some(),
        negatorName.is_some(),
        OidIsValid(restrictionId),
        OidIsValid(joinId),
        canMerge,
        canHash,
    )?;

    let (mut operatorObjectId, operatorAlreadyDefined) =
        OperatorGet(operatorName, operatorNamespace, leftTypeId, rightTypeId)?;

    if operatorAlreadyDefined {
        return Err(err(
            ERRCODE_DUPLICATE_FUNCTION,
            format!("operator {operatorName} already exists"),
        ));
    }

    // Filling in a previously-created shell: insist the user own it.
    if OidIsValid(operatorObjectId) {
        must_own_operator(operatorObjectId, operatorName)?;
    }

    let mut commutatorId = InvalidOid;
    if let Some(commutatorName) = commutatorName {
        // commutator has reversed arg types
        commutatorId = get_other_operator(
            mcx,
            commutatorName,
            rightTypeId,
            leftTypeId,
            operatorName,
            operatorNamespace,
            leftTypeId,
            rightTypeId,
        )?;
        if OidIsValid(commutatorId) {
            let name = commands_define::NameListToString(mcx, commutatorName)?;
            must_own_operator(commutatorId, name.as_str())?;
        }
        // Self-linkage to the new operator is fixed below.
        if !OidIsValid(commutatorId) {
            selfCommutator = true;
        }
    }

    let negatorId;
    if let Some(negatorName) = negatorName {
        // negator has same arg types
        negatorId = get_other_operator(
            mcx,
            negatorName,
            leftTypeId,
            rightTypeId,
            operatorName,
            operatorNamespace,
            leftTypeId,
            rightTypeId,
        )?;
        if OidIsValid(negatorId) {
            let name = commands_define::NameListToString(mcx, negatorName)?;
            must_own_operator(negatorId, name.as_str())?;
        }
        if !OidIsValid(negatorId) || negatorId == operatorObjectId {
            return Err(err(
                ERRCODE_INVALID_FUNCTION_DEFINITION,
                "operator cannot be its own negator".into(),
            ));
        }
    } else {
        negatorId = InvalidOid;
    }

    let mut oprname = NameData::default();
    oprname.namestrcpy(operatorName);
    let mut form = FormPgOperator {
        oid: InvalidOid,
        oprname,
        oprnamespace: operatorNamespace,
        oprowner: miscinit::GetUserId(),
        oprkind: if OidIsValid(leftTypeId) { b'b' as i8 } else { b'l' as i8 },
        oprcanmerge: canMerge,
        oprcanhash: canHash,
        oprleft: leftTypeId,
        oprright: rightTypeId,
        oprresult: operResultType,
        oprcom: commutatorId,
        oprnegate: negatorId,
        oprcode: procedureId,
        oprrest: restrictionId,
        oprjoin: joinId,
    };

    let rel = table::table_open(mcx, OPERATOR_RELATION_ID, RowExclusiveLock)?;

    let isUpdate;
    if OidIsValid(operatorObjectId) {
        isUpdate = true;
        form.oid = operatorObjectId;

        let oldtup = SearchSysCacheCopy(
            mcx,
            OPEROID,
            SysCacheKey::Value(Datum::from_oid(operatorObjectId)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?
        .unwrap_or_else(|| panic!("cache lookup failed for operator {operatorObjectId}"));

        let values = operator_values(&form);
        let nulls = [false; Natts_pg_operator];
        let mut replaces = [true; Natts_pg_operator];
        replaces[Anum_pg_operator_oid as usize - 1] = false;
        let mut tup = heaptuple::heap_modify_tuple(
            mcx,
            oldtup.as_tuple(),
            rel.descr(),
            &values,
            &nulls,
            &replaces,
        )?;
        let otid = oldtup.as_tuple().t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut tup)?;
    } else {
        isUpdate = false;
        operatorObjectId = catalog::GetNewOidWithIndex(
            mcx,
            &rel,
            OPERATOR_OID_INDEX_ID,
            Anum_pg_operator_oid as i16,
        )?;
        form.oid = operatorObjectId;

        let values = operator_values(&form);
        let nulls = [false; Natts_pg_operator];
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
    }

    let address = makeOperatorDependencies(mcx, &form, true, isUpdate)?;

    if selfCommutator {
        commutatorId = operatorObjectId;
    }

    if OidIsValid(commutatorId) || OidIsValid(negatorId) {
        OperatorUpd(mcx, operatorObjectId, commutatorId, negatorId, false)?;
    }

    rel.close(RowExclusiveLock)?;
    Ok(address)
}

fn must_own_operator(oper_oid: Oid, name: &str) -> PgResult<()> {
    if !aclchk::object_ownercheck(OPERATOR_RELATION_ID, oper_oid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            types_nodes::parsenodes::ObjectType::OBJECT_OPERATOR,
            name,
        )?;
    }
    Ok(())
}

pub fn OperatorValidateParams(
    leftTypeId: Oid,
    rightTypeId: Oid,
    operResultType: Oid,
    hasCommutator: bool,
    hasNegator: bool,
    hasRestrictionSelectivity: bool,
    hasJoinSelectivity: bool,
    canMerge: bool,
    canHash: bool,
) -> PgResult<()> {
    fn def_err(msg: &str) -> Box<PgError> {
        err(ERRCODE_INVALID_FUNCTION_DEFINITION, msg.into())
    }
    if !(OidIsValid(leftTypeId) && OidIsValid(rightTypeId)) {
        if hasCommutator {
            return Err(def_err("only binary operators can have commutators"));
        }
        if hasJoinSelectivity {
            return Err(def_err("only binary operators can have join selectivity"));
        }
        if canMerge {
            return Err(def_err("only binary operators can merge join"));
        }
        if canHash {
            return Err(def_err("only binary operators can hash"));
        }
    }
    if operResultType != BOOLOID {
        if hasNegator {
            return Err(def_err("only boolean operators can have negators"));
        }
        if hasRestrictionSelectivity {
            return Err(def_err("only boolean operators can have restriction selectivity"));
        }
        if hasJoinSelectivity {
            return Err(def_err("only boolean operators can have join selectivity"));
        }
        if canMerge {
            return Err(def_err("only boolean operators can merge join"));
        }
        if canHash {
            return Err(def_err("only boolean operators can hash"));
        }
    }
    Ok(())
}

fn get_other_operator(
    mcx: Mcx<'_>,
    otherOp: &NodeList<'_>,
    otherLeftTypeId: Oid,
    otherRightTypeId: Oid,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> PgResult<Oid> {
    let (other_oid, _defined) = OperatorLookup(otherOp, otherLeftTypeId, otherRightTypeId)?;
    if OidIsValid(other_oid) {
        return Ok(other_oid);
    }

    let mut buf = [""; 4];
    let parts = name_parts(otherOp, &mut buf);
    let (otherNamespace, otherName) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, parts)?;

    if otherName == operatorName
        && otherNamespace == operatorNamespace
        && otherLeftTypeId == leftTypeId
        && otherRightTypeId == rightTypeId
    {
        // self-linkage to new operator; caller must handle this
        return Ok(InvalidOid);
    }

    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        otherNamespace,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, otherNamespace)?;
        aclchk::aclcheck_error(
            aclresult,
            types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA,
            nspname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
    }

    OperatorShellMake(mcx, otherName, otherNamespace, otherLeftTypeId, otherRightTypeId)
}

fn name_parts<'a, 'mcx>(names: &NodeList<'mcx>, buf: &'a mut [&'mcx str; 4]) -> &'a [&'mcx str] {
    let n = names.len().min(buf.len());
    for (i, slot) in buf.iter_mut().enumerate().take(n) {
        *slot = names.nth(i).as_string().expect("name list holds String nodes").sval;
    }
    &buf[..n]
}

pub fn OperatorUpd(
    mcx: Mcx<'_>,
    baseId: Oid,
    commId: Oid,
    negId: Oid,
    isDelete: bool,
) -> PgResult<()> {
    // Self-commutator fill-in needs the just-inserted tuple visible; drops
    // start at command begin and can skip it.
    if !isDelete {
        xact::CommandCounterIncrement()?;
    }

    let rel = table::table_open(mcx, OPERATOR_RELATION_ID, RowExclusiveLock)?;

    let fetch = |oid: Oid| {
        if !OidIsValid(oid) {
            return Ok(None);
        }
        SearchSysCacheCopy(
            mcx,
            OPEROID,
            SysCacheKey::Value(Datum::from_oid(oid)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )
    };

    if let Some(tup) = fetch(commId)? {
        let mut form = form_of_tuple(&rel, tup.as_tuple());
        let mut update_commutator = false;
        if isDelete && OidIsValid(form.oprcom) {
            form.oprcom = InvalidOid;
            update_commutator = true;
        } else if !isDelete && form.oprcom != baseId {
            // A link to some third operator is an error, not overwritten.
            if OidIsValid(form.oprcom) {
                return Err(third_op_error(mcx, "commutator", &form.oprname, form.oprcom)?);
            }
            form.oprcom = baseId;
            update_commutator = true;
        }
        if update_commutator {
            update_row(mcx, &rel, &tup, &form)?;
            // CCI in case the commutator is also the negator.
            xact::CommandCounterIncrement()?;
        }
    }

    if let Some(tup) = fetch(negId)? {
        let mut form = form_of_tuple(&rel, tup.as_tuple());
        let mut update_negator = false;
        if isDelete && OidIsValid(form.oprnegate) {
            form.oprnegate = InvalidOid;
            update_negator = true;
        } else if !isDelete && form.oprnegate != baseId {
            if OidIsValid(form.oprnegate) {
                return Err(third_op_error(mcx, "negator", &form.oprname, form.oprnegate)?);
            }
            form.oprnegate = baseId;
            update_negator = true;
        }
        if update_negator {
            update_row(mcx, &rel, &tup, &form)?;
            if isDelete {
                xact::CommandCounterIncrement()?;
            }
        }
    }

    rel.close(RowExclusiveLock)
}

#[cold]
fn third_op_error(
    mcx: Mcx<'_>,
    what: &str,
    oprname: &NameData,
    third: Oid,
) -> PgResult<Box<PgError>> {
    let name = core::str::from_utf8(oprname.name_str()).unwrap_or("");
    let thirdop = lsyscache::get_opname(mcx, third)?;
    let third_s = match &thirdop {
        Some(s) => s.as_str().to_string(),
        None => third.to_string(),
    };
    Ok(err(
        ERRCODE_INVALID_FUNCTION_DEFINITION,
        format!("{what} operator {name} is already the {what} of operator {third_s}"),
    ))
}

fn update_row<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &heaptuple::HeapTuple<'_>,
    form: &FormPgOperator,
) -> PgResult<()> {
    let values = operator_values(form);
    let nulls = [false; Natts_pg_operator];
    let mut replaces = [true; Natts_pg_operator];
    replaces[Anum_pg_operator_oid as usize - 1] = false;
    let mut tup = heaptuple::heap_modify_tuple(
        mcx,
        oldtup.as_tuple(),
        rel.descr(),
        &values,
        &nulls,
        &replaces,
    )?;
    let otid = oldtup.as_tuple().t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut tup)
}

pub fn makeOperatorDependencies(
    mcx: Mcx<'_>,
    oper: &FormPgOperator,
    makeExtensionDep: bool,
    isUpdate: bool,
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddress {
        classId: OPERATOR_RELATION_ID,
        objectId: oper.oid,
        objectSubId: 0,
    };

    if isUpdate {
        catalog_dependency::deleteDependencyRecordsFor(mcx, myself.classId, myself.objectId, true)?;
        pg_shdepend::deleteSharedDependencyRecordsFor(
            mcx,
            myself.classId,
            myself.objectId,
            0,
        )?;
    }

    let mut refs: [ObjectAddress; 7] = [myself; 7];
    let mut n = 0;
    let mut add = |class_id: Oid, object_id: Oid| {
        if OidIsValid(object_id) {
            refs[n] = ObjectAddress { classId: class_id, objectId: object_id, objectSubId: 0 };
            n += 1;
        }
    };
    add(NAMESPACE_RELATION_ID, oper.oprnamespace);
    add(TYPE_RELATION_ID, oper.oprleft);
    add(TYPE_RELATION_ID, oper.oprright);
    add(TYPE_RELATION_ID, oper.oprresult);
    // oprcom/oprnegate links are reset manually by RemoveOperatorById, never
    // recorded as dependencies.
    add(PROCEDURE_RELATION_ID, oper.oprcode);
    add(PROCEDURE_RELATION_ID, oper.oprrest);
    add(PROCEDURE_RELATION_ID, oper.oprjoin);
    let count = n;
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut refs[..count],
        DependencyType::Normal,
    )?;

    pg_depend::recordDependencyOnOwner(mcx, OPERATOR_RELATION_ID, oper.oid, oper.oprowner)?;

    // Dependency on extension (membership when created by an extension
    // script; lets DROP EXTENSION / cascade reporting treat the operator as
    // an implementation detail).
    if makeExtensionDep {
        pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, isUpdate)?;
    }

    Ok(myself)
}

#[cfg(test)]
mod tests {
    use super::validOperatorName;

    #[test]
    fn valid_operator_names() {
        for ok in ["===", "<%", "!==", "@#@", "<<<", "?-", "-", "+", "~", "<@>"] {
            assert!(validOperatorName(ok), "{ok}");
        }
        for bad in [
            "", "!=", "a<", "<a", "=-", "<>-", "/*", "a--", "--", "<-/*", "++",
            &"~".repeat(64),
        ] {
            assert!(!validOperatorName(bad), "{bad}");
        }
        assert!(validOperatorName(&"~".repeat(63)));
        assert!(validOperatorName("?-"));
        assert!(validOperatorName("@-"));
        assert!(!validOperatorName("*-"));
    }
}
