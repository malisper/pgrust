//! `src/backend/catalog/pg_operator.c` (PostgreSQL 18.3) — routines to support
//! manipulation of the `pg_operator` relation.
//!
//! Ported 1:1 against the C, name-for-name, in this repo's catalog
//! carrier model (the `pg_conversion` / `pg_type` precedent): `table_open` /
//! `close` guard the relation, the tuple is built with real `heap_form_tuple` /
//! `heap_modify_tuple` over `RelationGetDescr(rel)` and inserted/updated with
//! the `catalog-indexing` keystone (`CatalogTupleInsert` / `CatalogTupleUpdate`
//! / `CatalogTupleDelete`), the OID column is allocated by `GetNewOidWithIndex`,
//! and the `OperatorGet` exact-spec probe and the `SearchSysCacheCopy1(OPEROID)`
//! read-modify-write fetches are `systable` index scans (mirroring
//! pg_conversion's owner-crate scan idiom).
//!
//! Every C function (public and file-static) is present with its original name,
//! branch order, error codes/messages/SQLSTATE, tuple field-formation order, and
//! dependency-recording order preserved.
//!
//! This crate OWNS and installs the inward seams consumed by `operatorcmds.c`
//! (`backend-catalog-pg-operator-seams`): `operator_create`,
//! `operator_validate_params`, `operator_upd`, `fetch_operator_form`,
//! `operator_lookup`, `remove_operator_tuple`, `alter_operator_apply`,
//! `invoke_object_post_alter_hook`. (`RemoveOperatorById` lives in
//! operatorcmds.c and is owned/installed there.)

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, MemoryContext, PgVec};

use types_acl::acl::{ACL_CREATE, ACLCHECK_NOT_OWNER, ACLCHECK_OK};
use types_catalog::catalog::{NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_operator::{
    Anum_pg_operator_oid, Anum_pg_operator_oprcanhash, Anum_pg_operator_oprcanmerge,
    Anum_pg_operator_oprcode, Anum_pg_operator_oprcom, Anum_pg_operator_oprjoin,
    Anum_pg_operator_oprkind, Anum_pg_operator_oprleft, Anum_pg_operator_oprname,
    Anum_pg_operator_oprnamespace, Anum_pg_operator_oprnegate, Anum_pg_operator_oprowner,
    Anum_pg_operator_oprrest, Anum_pg_operator_oprresult, Anum_pg_operator_oprright,
    FormPgOperator, Natts_pg_operator, OperatorNameNspIndexId, OperatorOidIndexId,
    OperatorRelationId,
};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::primitive::{InvalidOid, Oid, RegProcedure};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_INVALID_NAME, ERROR,
};
use ::nodes::parsenodes::{OBJECT_OPERATOR, OBJECT_SCHEMA};

use utils_error::ereport;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::{Datum, FormedTuple};

use heaptuple::{heap_deform_tuple, heap_form_tuple, heap_modify_tuple};
use scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use catalog_catalog::GetNewOidWithIndex;
use indexing::keystone::{CatalogTupleDelete, CatalogTupleInsert, CatalogTupleUpdate};
use objectaccess_seams as objectaccess_seams;

use pg_operator_seams::{
    OperatorAttrUpdate, OperatorCreateArgs, OperatorValidateParamsArgs,
};

/// `NAMEDATALEN`.
const NAMEDATALEN: usize = 64;

/// `OidIsValid(objectId)` — `((objectId) != InvalidOid)`.
#[inline]
fn OidIsValid(object_id: Oid) -> bool {
    object_id != InvalidOid
}

/// `RegProcedureIsValid(p)` — `((RegProcedure) (p) != InvalidOid)`.
#[inline]
fn RegProcedureIsValid(p: RegProcedure) -> bool {
    p != InvalidOid
}

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled `NameData` image,
/// truncated to `NAMEDATALEN - 1` bytes with a guaranteed trailing NUL.
fn namestrcpy(src: &str) -> [u8; NAMEDATALEN] {
    let mut name = [0u8; NAMEDATALEN];
    let bytes = src.as_bytes();
    let n = core::cmp::min(bytes.len(), NAMEDATALEN - 1);
    name[..n].copy_from_slice(&bytes[..n]);
    name
}

/// `NameStr(name)` — read a `NameData` column image back to a `String`
/// (NUL-terminated, for the `%s` slots in `OperatorUpd`'s error messages).
fn name_str(image: &[u8]) -> String {
    let end = image.iter().position(|&b| b == 0).unwrap_or(image.len());
    String::from_utf8_lossy(&image[..end]).into_owned()
}

/// `ObjectAddressSet(object, class_id, object_id)` (objectaddress.h): set
/// `classId`/`objectId` and zero `objectSubId`.
#[inline]
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `NameGetDatum(&name)` for a 64-byte `NameData` image: a by-reference Datum
/// over the column's on-disk bytes (the `name` type is fixed-length 64, stored
/// inline).
fn name_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8; NAMEDATALEN]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &image[..])?))
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

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_NAMEEQ,
/// CStringGetDatum(value))`. The name crosses as a NUL-terminated byte image
/// (the genam owner's `nameeq` comparator interprets it).
fn name_key<'mcx>(mcx: Mcx<'mcx>, attno: i16, value: &str) -> PgResult<ScanKeyData<'mcx>> {
    let mut bytes: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, value.len() + 1)?;
    for &b in value.as_bytes() {
        bytes.push(b);
    }
    bytes.push(0);
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        Datum::ByRef(bytes),
    )?;
    Ok(key)
}

/// Deform a scanned/copied `pg_operator` row into the owned [`FormPgOperator`].
/// This is the idiomatic `(Form_pg_operator) GETSTRUCT(tup)`.
fn form_pg_operator_of_tuple(
    mcx: Mcx<'_>,
    rel: &rel::Relation<'_>,
    tup: &FormedTuple<'_>,
) -> PgResult<FormPgOperator> {
    let cols = heap_deform_tuple(mcx, &tup.tuple, &rel.rd_att, &tup.data)?;
    Ok(FormPgOperator {
        oid: cols[Anum_pg_operator_oid as usize - 1].0.as_oid(),
        oprname: name_str(cols[Anum_pg_operator_oprname as usize - 1].0.as_ref_bytes()),
        oprnamespace: cols[Anum_pg_operator_oprnamespace as usize - 1].0.as_oid(),
        oprowner: cols[Anum_pg_operator_oprowner as usize - 1].0.as_oid(),
        oprkind: cols[Anum_pg_operator_oprkind as usize - 1].0.as_char(),
        oprcanmerge: cols[Anum_pg_operator_oprcanmerge as usize - 1].0.as_bool(),
        oprcanhash: cols[Anum_pg_operator_oprcanhash as usize - 1].0.as_bool(),
        oprleft: cols[Anum_pg_operator_oprleft as usize - 1].0.as_oid(),
        oprright: cols[Anum_pg_operator_oprright as usize - 1].0.as_oid(),
        oprresult: cols[Anum_pg_operator_oprresult as usize - 1].0.as_oid(),
        oprcom: cols[Anum_pg_operator_oprcom as usize - 1].0.as_oid(),
        oprnegate: cols[Anum_pg_operator_oprnegate as usize - 1].0.as_oid(),
        oprcode: cols[Anum_pg_operator_oprcode as usize - 1].0.as_oid(),
        oprrest: cols[Anum_pg_operator_oprrest as usize - 1].0.as_oid(),
        oprjoin: cols[Anum_pg_operator_oprjoin as usize - 1].0.as_oid(),
    })
}

/// Build the `Natts_pg_operator` `values` array (all non-null) for
/// `heap_form_tuple` / `heap_modify_tuple` from a [`FormPgOperator`], in genbki
/// field order. The `oid` column is filled by the caller (it sets it after
/// `GetNewOidWithIndex`, or `replaces[oid]=false` keeps the existing one on
/// update).
fn operator_values<'mcx>(
    mcx: Mcx<'mcx>,
    form: &FormPgOperator,
) -> PgResult<PgVec<'mcx, Datum<'mcx>>> {
    let mut values: PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, Natts_pg_operator as usize)?;
    let oname = namestrcpy(&form.oprname);
    values.push(Datum::from_oid(form.oid)); /* oid */
    values.push(name_datum(mcx, &oname)?); /* oprname */
    values.push(Datum::from_oid(form.oprnamespace)); /* oprnamespace */
    values.push(Datum::from_oid(form.oprowner)); /* oprowner */
    values.push(Datum::from_char(form.oprkind)); /* oprkind */
    values.push(Datum::from_bool(form.oprcanmerge)); /* oprcanmerge */
    values.push(Datum::from_bool(form.oprcanhash)); /* oprcanhash */
    values.push(Datum::from_oid(form.oprleft)); /* oprleft */
    values.push(Datum::from_oid(form.oprright)); /* oprright */
    values.push(Datum::from_oid(form.oprresult)); /* oprresult */
    values.push(Datum::from_oid(form.oprcom)); /* oprcom */
    values.push(Datum::from_oid(form.oprnegate)); /* oprnegate */
    values.push(Datum::from_oid(form.oprcode)); /* oprcode */
    values.push(Datum::from_oid(form.oprrest)); /* oprrest */
    values.push(Datum::from_oid(form.oprjoin)); /* oprjoin */
    Ok(values)
}

/* ===========================================================================
 * validOperatorName (pg_operator.c:67-112)
 * ========================================================================= */

/// Check whether a proposed operator name is legal.
///
/// This had better match the behavior of parser/scan.l! We need this because the
/// parser is not smart enough to check that the arguments of CREATE OPERATOR's
/// COMMUTATOR, NEGATOR, etc clauses are operator names rather than some other
/// lexical entity.
fn validOperatorName(name: &str) -> bool {
    let bytes = name.as_bytes();
    let len = bytes.len(); /* strlen(name) */

    /* Can't be empty or too long */
    if len == 0 || len >= NAMEDATALEN {
        return false;
    }

    /* Can't contain any invalid characters */
    /* Test string here should match op_chars in scan.l */
    const OP_CHARS: &[u8] = b"~!@#^&|`?+-*/%<>=";
    if !bytes.iter().all(|b| OP_CHARS.contains(b)) {
        /* strspn(name, op_chars) != len */
        return false;
    }

    /* Can't contain slash-star or dash-dash (comment starts) */
    if contains_sub(bytes, b"/*") || contains_sub(bytes, b"--") {
        return false;
    }

    /*
     * For SQL standard compatibility, '+' and '-' cannot be the last char of a
     * multi-char operator unless the operator contains chars that are not in
     * SQL operators. The idea is to lex '=-' as two operators, but not to
     * forbid operator names like '?-' that could not be sequences of standard
     * SQL operators.
     */
    if len > 1 && (bytes[len - 1] == b'+' || bytes[len - 1] == b'-') {
        const SPECIAL: &[u8] = b"~!@#^&|`?%";
        let mut ic: isize = len as isize - 2;
        let mut found = false;
        while ic >= 0 {
            if SPECIAL.contains(&bytes[ic as usize]) {
                found = true;
                break;
            }
            ic -= 1;
        }
        if !found {
            /* ic < 0 */
            return false; /* nope, not valid */
        }
    }

    /* != isn't valid either, because parser will convert it to <> */
    if bytes == b"!=" {
        return false;
    }

    true
}

/// `strstr(haystack, needle) != NULL` for byte slices.
fn contains_sub(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/* ===========================================================================
 * OperatorGet (pg_operator.c:123-153)
 * ========================================================================= */

/// `OperatorGet` — finds an operator given an exact specification (name,
/// namespace, left and right type IDs).
///
/// The C `SearchSysCache4(OPERNAMENSP, name, left, right, namespace)` is a probe
/// of the unique `OperatorNameNspIndexId` index `(oprname, oprleft, oprright,
/// oprnamespace)`. Returns `(operatorObjectId, defined)`, where `defined` is
/// true if the row exists and is not a shell
/// (`RegProcedureIsValid(oprform->oprcode)`).
fn OperatorGet(
    mcx: Mcx<'_>,
    operatorName: &str,
    operatorNamespace: Oid,
    leftObjectId: Oid,
    rightObjectId: Oid,
) -> PgResult<(Oid, bool)> {
    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;
    let skey = [
        name_key(mcx, Anum_pg_operator_oprname, operatorName)?,
        oid_key(Anum_pg_operator_oprleft, leftObjectId)?,
        oid_key(Anum_pg_operator_oprright, rightObjectId)?,
        oid_key(Anum_pg_operator_oprnamespace, operatorNamespace)?,
    ];

    let mut result = (InvalidOid, false);
    {
        let mut scan = genam_seams::systable_beginscan::call(
            &rel,
            OperatorNameNspIndexId,
            true,
            None,
            &skey,
        )?;
        let scratch = MemoryContext::new("OperatorGet");
        let smcx = scratch.mcx();
        if let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? {
            let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
            // oprform->oid; *defined = RegProcedureIsValid(oprform->oprcode);
            let operatorObjectId = cols[Anum_pg_operator_oid as usize - 1].0.as_oid();
            let oprcode = cols[Anum_pg_operator_oprcode as usize - 1].0.as_oid();
            result = (operatorObjectId, RegProcedureIsValid(oprcode));
        }
        scan.end()?;
    }
    rel.close(RowExclusiveLock)?;
    Ok(result)
}

/* ===========================================================================
 * OperatorLookup (pg_operator.c:163-185)
 * ========================================================================= */

/// `OperatorLookup` — looks up an operator given a possibly-qualified name and
/// left and right type IDs. Returns `(operatorObjectId, defined)`, where
/// `defined` is set true if the operator is defined (not a shell).
pub fn OperatorLookup(
    operatorName: &[String],
    leftObjectId: Oid,
    rightObjectId: Oid,
) -> PgResult<(Oid, bool)> {
    // LookupOperName(NULL, operatorName, leftObjectId, rightObjectId, true, -1);
    let operatorObjectId = parse_oper::LookupOperName(
        None,
        operatorName,
        leftObjectId,
        rightObjectId,
        true,
        -1,
    )?;
    if !OidIsValid(operatorObjectId) {
        /* *defined = false; */
        return Ok((InvalidOid, false));
    }

    let oprcode = lsyscache::opfamily_operator::get_opcode(operatorObjectId)?;
    let defined = RegProcedureIsValid(oprcode);

    Ok((operatorObjectId, defined))
}

/* ===========================================================================
 * OperatorShellMake (pg_operator.c:192-283)
 * ========================================================================= */

/// `OperatorShellMake` — make a "shell" entry for a not-yet-existing operator.
fn OperatorShellMake(
    mcx: Mcx<'_>,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> PgResult<Oid> {
    /* validate operator name */
    if !validOperatorName(operatorName) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg(format!("\"{operatorName}\" is not a valid operator name"))
            .into_error());
    }

    /* open pg_operator */
    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;

    /*
     * initialize values[] with the operator name and input data types. Note
     * that oprcode is set to InvalidOid, indicating it's a shell.
     */
    let operatorObjectId =
        GetNewOidWithIndex(&rel, OperatorOidIndexId, Anum_pg_operator_oid)?;

    let form = FormPgOperator {
        oid: operatorObjectId,
        oprname: operatorName.to_string(),
        oprnamespace: operatorNamespace,
        oprowner: miscinit::GetUserId(),
        oprkind: if leftTypeId != InvalidOid { b'b' as i8 } else { b'l' as i8 },
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

    /* create a new operator tuple */
    let values = operator_values(mcx, &form)?;
    let nulls = [false; Natts_pg_operator as usize];
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;

    /* insert our "shell" operator tuple */
    CatalogTupleInsert(mcx, &rel, &mut tup)?;

    /* Add dependencies for the entry */
    makeOperatorDependencies(mcx, &form, true, false)?;

    /* heap_freetuple(tup) — `tup` is owned and dropped here. */

    /* Post creation hook for new shell operator */
    objectaccess_seams::invoke_object_post_create_hook::call(
        OperatorRelationId,
        operatorObjectId,
        0,
    )?;

    /* Make sure the tuple is visible for subsequent lookups/updates. */
    backend_access_transam_xact_cci()?;

    /* close the operator relation and return the oid. */
    rel.close(RowExclusiveLock)?;

    Ok(operatorObjectId)
}

/// `CommandCounterIncrement()` (xact.c) via the transaction seam.
fn backend_access_transam_xact_cci() -> PgResult<()> {
    transam_xact_seams::command_counter_increment::call()
}

/* ===========================================================================
 * OperatorCreate (pg_operator.c:320-542)
 * ========================================================================= */

/// `OperatorCreate`
///
/// "X" indicates an optional argument (i.e. one that can be NULL or 0). The
/// caller should have validated properties and permissions for the objects
/// passed as OID references. We must handle the commutator and negator operator
/// references specially, however, since those need not exist beforehand.
///
/// `commutatorName` / `negatorName` are possibly-qualified name lists, empty for
/// `NIL`.
pub fn OperatorCreate(
    mcx: Mcx<'_>,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
    procedureId: Oid,
    commutatorName: &[String],
    negatorName: &[String],
    restrictionId: Oid,
    joinId: Oid,
    canMerge: bool,
    canHash: bool,
) -> PgResult<ObjectAddress> {
    let isUpdate: bool;
    let mut commutatorId: Oid;
    let negatorId: Oid;
    let mut selfCommutator = false;

    /* Sanity checks */
    if !validOperatorName(operatorName) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg(format!("\"{operatorName}\" is not a valid operator name"))
            .into_error());
    }

    let operResultType = lsyscache::function::get_func_rettype(procedureId)?;

    OperatorValidateParams(
        leftTypeId,
        rightTypeId,
        operResultType,
        !commutatorName.is_empty(),
        !negatorName.is_empty(),
        OidIsValid(restrictionId),
        OidIsValid(joinId),
        canMerge,
        canHash,
    )?;

    let (mut operatorObjectId, operatorAlreadyDefined) =
        OperatorGet(mcx, operatorName, operatorNamespace, leftTypeId, rightTypeId)?;

    if operatorAlreadyDefined {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_FUNCTION)
            .errmsg(format!("operator {operatorName} already exists"))
            .into_error());
    }

    /*
     * At this point, if operatorObjectId is not InvalidOid then we are filling
     * in a previously-created shell.  Insist that the user own any such shell.
     */
    if OidIsValid(operatorObjectId)
        && !aclchk::object_ownercheck(
            mcx,
            OperatorRelationId,
            operatorObjectId,
            miscinit::GetUserId(),
        )?
    {
        aclchk::aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_OPERATOR,
            Some(operatorName.to_string()),
        )?;
    }

    /*
     * Set up the other operators.  If they do not currently exist, create
     * shells in order to get ObjectId's.
     */

    if !commutatorName.is_empty() {
        /* commutator has reversed arg types */
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

        /* Permission check: must own other operator */
        if OidIsValid(commutatorId)
            && !aclchk::object_ownercheck(
                mcx,
                OperatorRelationId,
                commutatorId,
                miscinit::GetUserId(),
            )?
        {
            aclchk::aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_OPERATOR,
                Some(NameListToString(mcx, commutatorName)?),
            )?;
        }

        /*
         * If self-linkage to the new operator is requested, we'll fix it below.
         * (In case of self-linkage to an existing shell operator, we need do
         * nothing special.)
         */
        if !OidIsValid(commutatorId) {
            selfCommutator = true;
        }
    } else {
        commutatorId = InvalidOid;
    }

    if !negatorName.is_empty() {
        /* negator has same arg types */
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

        /* Permission check: must own other operator */
        if OidIsValid(negatorId)
            && !aclchk::object_ownercheck(
                mcx,
                OperatorRelationId,
                negatorId,
                miscinit::GetUserId(),
            )?
        {
            aclchk::aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_OPERATOR,
                Some(NameListToString(mcx, negatorName)?),
            )?;
        }

        /*
         * Prevent self negation, as it doesn't make sense.  It's self negation
         * if result is InvalidOid (negator would be the same operator but it
         * doesn't exist yet) or operatorObjectId (we are replacing a shell that
         * would need to be its own negator).
         */
        if !OidIsValid(negatorId) || negatorId == operatorObjectId {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("operator cannot be its own negator")
                .into_error());
        }
    } else {
        negatorId = InvalidOid;
    }

    /* set up values in the operator tuple */
    let mut form = FormPgOperator {
        oid: InvalidOid,
        oprname: operatorName.to_string(),
        oprnamespace: operatorNamespace,
        oprowner: miscinit::GetUserId(),
        oprkind: if leftTypeId != InvalidOid { b'b' as i8 } else { b'l' as i8 },
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

    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;

    /* If we are replacing an operator shell, update; else insert */
    if operatorObjectId != InvalidOid {
        isUpdate = true;

        // tup = SearchSysCacheCopy1(OPEROID, operatorObjectId);
        let existing = match search_operator_by_oid(mcx, &rel, operatorObjectId)? {
            Some(t) => t,
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "cache lookup failed for operator {operatorObjectId}"
                    ))
                    .into_error());
            }
        };

        /*
         * replaces[Anum_pg_operator_oid - 1] = false; the existing oid is
         * preserved (== operatorObjectId from OperatorGet), so it carries
         * through into the row we form.
         */
        form.oid = operatorObjectId;

        let mut replaces = [true; Natts_pg_operator as usize];
        replaces[Anum_pg_operator_oid as usize - 1] = false;
        let values = operator_values(mcx, &form)?;
        let nulls = [false; Natts_pg_operator as usize];
        let tupdesc = rel.rd_att_clone_in(mcx)?;
        let mut tup =
            heap_modify_tuple(mcx, &existing, &tupdesc, &values, &nulls, &replaces)?;

        CatalogTupleUpdate(mcx, &rel, existing.tuple.t_self, &mut tup)?;
    } else {
        isUpdate = false;

        operatorObjectId =
            GetNewOidWithIndex(&rel, OperatorOidIndexId, Anum_pg_operator_oid)?;
        form.oid = operatorObjectId;

        let values = operator_values(mcx, &form)?;
        let nulls = [false; Natts_pg_operator as usize];
        let tupdesc = rel.rd_att_clone_in(mcx)?;
        let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;

        CatalogTupleInsert(mcx, &rel, &mut tup)?;
    }

    /* Add dependencies for the entry */
    let address = makeOperatorDependencies(mcx, &form, true, isUpdate)?;

    /*
     * If a commutator and/or negator link is provided, update the other
     * operator(s) to point at this one, if they don't already have a link.
     */
    if selfCommutator {
        commutatorId = operatorObjectId;
    }

    if OidIsValid(commutatorId) || OidIsValid(negatorId) {
        OperatorUpd(mcx, operatorObjectId, commutatorId, negatorId, false)?;
    }

    /* Post creation hook for new operator */
    objectaccess_seams::invoke_object_post_create_hook::call(
        OperatorRelationId,
        operatorObjectId,
        0,
    )?;

    rel.close(RowExclusiveLock)?;

    Ok(address)
}

/* ===========================================================================
 * OperatorValidateParams (pg_operator.c:555-611)
 * ========================================================================= */

/// `OperatorValidateParams`
///
/// Check that an operator with argument types leftTypeId and rightTypeId,
/// returning operResultType, can have the attributes that are set to true. Raise
/// an error for any disallowed attribute.
///
/// Note: in ALTER OPERATOR, we only bother to pass "true" for attributes the
/// command is trying to set, not those that may already be set. This is OK as
/// long as the attribute checks are independent.
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
    if !(OidIsValid(leftTypeId) && OidIsValid(rightTypeId)) {
        /* If it's not a binary op, these things mustn't be set: */
        if hasCommutator {
            return Err(invalid_func_def("only binary operators can have commutators"));
        }
        if hasJoinSelectivity {
            return Err(invalid_func_def(
                "only binary operators can have join selectivity",
            ));
        }
        if canMerge {
            return Err(invalid_func_def("only binary operators can merge join"));
        }
        if canHash {
            return Err(invalid_func_def("only binary operators can hash"));
        }
    }

    if operResultType != types_core::catalog::BOOLOID {
        /* If it's not a boolean op, these things mustn't be set: */
        if hasNegator {
            return Err(invalid_func_def("only boolean operators can have negators"));
        }
        if hasRestrictionSelectivity {
            return Err(invalid_func_def(
                "only boolean operators can have restriction selectivity",
            ));
        }
        if hasJoinSelectivity {
            return Err(invalid_func_def(
                "only boolean operators can have join selectivity",
            ));
        }
        if canMerge {
            return Err(invalid_func_def("only boolean operators can merge join"));
        }
        if canHash {
            return Err(invalid_func_def("only boolean operators can hash"));
        }
    }

    Ok(())
}

/// `ereport(ERROR, errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg(msg))`.
fn invalid_func_def(msg: &'static str) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg(msg)
        .into_error()
}

/* ===========================================================================
 * get_other_operator (pg_operator.c:621-668)
 * ========================================================================= */

/// Try to lookup another operator (commutator, etc); return its OID.
///
/// If not found, check to see if it would be the same operator we are trying to
/// define; if so, return InvalidOid. (Caller must decide whether that is
/// sensible.) If it is not the same operator, create a shell operator.
fn get_other_operator(
    mcx: Mcx<'_>,
    otherOp: &[String],
    otherLeftTypeId: Oid,
    otherRightTypeId: Oid,
    operatorName: &str,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> PgResult<Oid> {
    let (other_oid, _otherDefined) =
        OperatorLookup(otherOp, otherLeftTypeId, otherRightTypeId)?;

    if OidIsValid(other_oid) {
        /* other op already in catalogs */
        return Ok(other_oid);
    }

    let other_nl = name_list(otherOp);
    let (otherNamespace, otherName) =
        catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &other_nl)?;

    if otherName == operatorName
        && otherNamespace == operatorNamespace
        && otherLeftTypeId == leftTypeId
        && otherRightTypeId == rightTypeId
    {
        /* self-linkage to new operator; caller must handle this */
        return Ok(InvalidOid);
    }

    /* not in catalogs, different from operator, so make shell */

    let aclresult = aclchk::object_aclcheck(
        mcx,
        NAMESPACE_RELATION_ID,
        otherNamespace,
        miscinit::GetUserId(),
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            OBJECT_SCHEMA,
            lsyscache::namespace_range_index_pubsub::get_namespace_name(mcx, otherNamespace)?
                .map(|s| s.as_str().to_string()),
        )?;
    }

    let other_name_owned = otherName.to_string();
    let other_oid = OperatorShellMake(
        mcx,
        &other_name_owned,
        otherNamespace,
        otherLeftTypeId,
        otherRightTypeId,
    )?;
    Ok(other_oid)
}

/// Adapt a `&[String]` operator-name list to the `NameList = &[Option<String>]`
/// the namespace helpers consume (a qualified operator name has no NULL
/// components).
fn name_list(names: &[String]) -> Vec<Option<String>> {
    names.iter().map(|s| Some(s.clone())).collect()
}

/// `NameListToString(names)` (namespace.c) over a bare-component operator name.
fn NameListToString(mcx: Mcx<'_>, names: &[String]) -> PgResult<String> {
    let nl = name_list(names);
    Ok(catalog_namespace::NameListToString(mcx, &nl)?
        .as_str()
        .to_string())
}

/* ===========================================================================
 * OperatorUpd (pg_operator.c:683-838)
 * ========================================================================= */

/// `OperatorUpd`
///
/// For a given operator, look up its negator and commutator operators. When
/// isDelete is false, update their negator and commutator fields to point back
/// to the given operator; when isDelete is true, update those fields to be
/// InvalidOid.
///
/// The !isDelete case solves a problem for users who need to insert two new
/// operators that are the negator or commutator of each other, while the
/// isDelete case is needed so as not to leave dangling OID links behind after
/// dropping an operator.
pub fn OperatorUpd(
    mcx: Mcx<'_>,
    baseId: Oid,
    commId: Oid,
    negId: Oid,
    isDelete: bool,
) -> PgResult<()> {
    /*
     * If we're making an operator into its own commutator, then we need a
     * command-counter increment here, since we've just inserted the tuple we're
     * about to update.  But when we're dropping an operator, we can skip this
     * because we're at the beginning of the command.
     */
    if !isDelete {
        backend_access_transam_xact_cci()?;
    }

    /* Open the relation. */
    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;

    /* Get a writable copy of the commutator's tuple. */
    let tup = if OidIsValid(commId) {
        search_operator_by_oid(mcx, &rel, commId)?
    } else {
        None
    };

    /* Update the commutator's tuple if need be. */
    if let Some(tuple) = tup {
        let mut form = form_pg_operator_of_tuple(mcx, &rel, &tuple)?;
        let mut update_commutator = false;

        /*
         * We can skip doing anything if the commutator's oprcom field is already
         * what we want.  While that's not expected in the isDelete case, it's
         * perfectly possible when filling in a shell operator.
         */
        if isDelete && OidIsValid(form.oprcom) {
            form.oprcom = InvalidOid;
            update_commutator = true;
        } else if !isDelete && form.oprcom != baseId {
            /*
             * If commutator's oprcom field is already set to point to some third
             * operator, it's an error.  Changing its link would be unsafe, and
             * letting the inconsistency stand would not be good either.  This
             * might be indicative of catalog corruption, so don't assume
             * t->oprcom is necessarily a valid operator.
             */
            if OidIsValid(form.oprcom) {
                let thirdop = lsyscache::opfamily_operator::get_opname(mcx, form.oprcom)?;
                let third = thirdop
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_else(|| form.oprcom.to_string());
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "commutator operator {} is already the commutator of operator {}",
                        form.oprname, third
                    ))
                    .into_error());
            }

            form.oprcom = baseId;
            update_commutator = true;
        }

        /* If any columns were found to need modification, update tuple. */
        if update_commutator {
            update_operator_tuple(mcx, &rel, &tuple, &form)?;

            /*
             * Do CCI to make the updated tuple visible.  We must do this in case
             * the commutator is also the negator.  (Which would be a logic error
             * on the operator definer's part, but that's not a good reason to
             * fail here.)  We would need a CCI anyway in the deletion case for a
             * self-commutator with no negator.
             */
            backend_access_transam_xact_cci()?;
        }
    }

    /* Similarly find and update the negator, if any. */
    let tup = if OidIsValid(negId) {
        search_operator_by_oid(mcx, &rel, negId)?
    } else {
        None
    };

    if let Some(tuple) = tup {
        let mut form = form_pg_operator_of_tuple(mcx, &rel, &tuple)?;
        let mut update_negator = false;

        /*
         * We can skip doing anything if the negator's oprnegate field is already
         * what we want.  While that's not expected in the isDelete case, it's
         * perfectly possible when filling in a shell operator.
         */
        if isDelete && OidIsValid(form.oprnegate) {
            form.oprnegate = InvalidOid;
            update_negator = true;
        } else if !isDelete && form.oprnegate != baseId {
            /*
             * If negator's oprnegate field is already set to point to some third
             * operator, it's an error.  Changing its link would be unsafe, and
             * letting the inconsistency stand would not be good either.  This
             * might be indicative of catalog corruption, so don't assume
             * t->oprnegate is necessarily a valid operator.
             */
            if OidIsValid(form.oprnegate) {
                let thirdop = lsyscache::opfamily_operator::get_opname(mcx, form.oprnegate)?;
                let third = thirdop
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_else(|| form.oprnegate.to_string());
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "negator operator {} is already the negator of operator {}",
                        form.oprname, third
                    ))
                    .into_error());
            }

            form.oprnegate = baseId;
            update_negator = true;
        }

        /* If any columns were found to need modification, update tuple. */
        if update_negator {
            update_operator_tuple(mcx, &rel, &tuple, &form)?;

            /*
             * In the deletion case, do CCI to make the updated tuple visible. We
             * must do this in case the operator is its own negator. (Which would
             * be a logic error on the operator definer's part, but that's not a
             * good reason to fail here.)
             */
            if isDelete {
                backend_access_transam_xact_cci()?;
            }
        }
    }

    /* Close relation and release catalog lock. */
    rel.close(RowExclusiveLock)?;

    Ok(())
}

/// `heap_modify_tuple` over the full form (the C mutates `t->oprcom` /
/// `t->oprnegate` in place on the `GETSTRUCT` view and `CatalogTupleUpdate`s it).
/// We rebuild from the owned form (every column replaced, `oid` preserved).
fn update_operator_tuple(
    mcx: Mcx<'_>,
    rel: &rel::Relation<'_>,
    oldtup: &FormedTuple<'_>,
    form: &FormPgOperator,
) -> PgResult<()> {
    let mut replaces = [true; Natts_pg_operator as usize];
    replaces[Anum_pg_operator_oid as usize - 1] = false;
    let values = operator_values(mcx, form)?;
    let nulls = [false; Natts_pg_operator as usize];
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &replaces)?;
    CatalogTupleUpdate(mcx, rel, oldtup.tuple.t_self, &mut tup)
}

/// `SearchSysCacheCopy1(OPEROID, oid)` — a writable copy of the operator's heap
/// tuple, fetched via an index scan on `OperatorOidIndexId`. Returns the raw
/// `FormedTuple` (the caller deforms it / reads its `t_self`).
fn search_operator_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel::Relation<'mcx>,
    oid: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let skey = [oid_key(Anum_pg_operator_oid, oid)?];
    let found;
    {
        let mut scan =
            genam_seams::systable_beginscan::call(rel, OperatorOidIndexId, true, None, &skey)?;
        found = genam_seams::systable_getnext::call(mcx, scan.desc_mut())?;
        scan.end()?;
    }
    Ok(found)
}

/* ===========================================================================
 * makeOperatorDependencies (pg_operator.c:852-945)
 * ========================================================================= */

/// Create dependencies for an operator (either a freshly inserted complete
/// operator, a new shell operator, a just-updated shell, or an operator that's
/// being modified by ALTER OPERATOR).
///
/// `makeExtensionDep` should be true when making a new operator or replacing a
/// shell, false for ALTER OPERATOR. Passing false will prevent any change in the
/// operator's extension membership.
///
/// NB: the OidIsValid tests in this routine are necessary, in case the given
/// operator is a shell. In C the argument is the `HeapTuple` and `oper` is
/// `GETSTRUCT(tuple)`; here the caller passes the owned `FormPgOperator`.
pub fn makeOperatorDependencies(
    mcx: Mcx<'_>,
    oper: &FormPgOperator,
    makeExtensionDep: bool,
    isUpdate: bool,
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddressSet(OperatorRelationId, oper.oid);

    /*
     * If we are updating the operator, delete any existing entries, except for
     * extension membership which should remain the same.
     */
    if isUpdate {
        pg_depend::deleteDependencyRecordsFor(myself.classId, myself.objectId, true)?;
        pg_shdepend::deleteSharedDependencyRecordsFor(
            myself.classId,
            myself.objectId,
            0,
        )?;
    }

    let mut addrs = dependency::new_object_addresses();

    /* Dependency on namespace */
    if OidIsValid(oper.oprnamespace) {
        let referenced = ObjectAddressSet(NAMESPACE_RELATION_ID, oper.oprnamespace);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /* Dependency on left type */
    if OidIsValid(oper.oprleft) {
        let referenced = ObjectAddressSet(TYPE_RELATION_ID, oper.oprleft);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /* Dependency on right type */
    if OidIsValid(oper.oprright) {
        let referenced = ObjectAddressSet(TYPE_RELATION_ID, oper.oprright);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /* Dependency on result type */
    if OidIsValid(oper.oprresult) {
        let referenced = ObjectAddressSet(TYPE_RELATION_ID, oper.oprresult);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /*
     * NOTE: we do not consider the operator to depend on the associated
     * operators oprcom and oprnegate.  We do not want to delete this operator if
     * those go away, but only reset the link fields; which is not a function
     * that the dependency logic can handle.  (It's taken care of manually within
     * RemoveOperatorById, instead.)
     */

    /* Dependency on implementation function */
    if OidIsValid(oper.oprcode) {
        let referenced = ObjectAddressSet(PROCEDURE_RELATION_ID, oper.oprcode);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /* Dependency on restriction selectivity function */
    if OidIsValid(oper.oprrest) {
        let referenced = ObjectAddressSet(PROCEDURE_RELATION_ID, oper.oprrest);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    /* Dependency on join selectivity function */
    if OidIsValid(oper.oprjoin) {
        let referenced = ObjectAddressSet(PROCEDURE_RELATION_ID, oper.oprjoin);
        dependency::add_exact_object_address(&referenced, &mut addrs);
    }

    dependency::record_object_address_dependencies(
        &myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;
    /* free_object_addresses(addrs) — `addrs` is owned and dropped here. */
    drop(addrs);

    /* Dependency on owner */
    pg_shdepend::recordDependencyOnOwner(OperatorRelationId, oper.oid, oper.oprowner)?;

    /* Dependency on extension */
    if makeExtensionDep {
        pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, isUpdate)?;
    }

    Ok(myself)
}

/* ===========================================================================
 * remove_operator_tuple — the tuple-touching body of RemoveOperatorById
 * (operatorcmds.c:446-482), the catalog half that pg_operator.c owns.
 * ========================================================================= */

/// The tuple-touching guts of `RemoveOperatorById`: under `RowExclusiveLock`,
/// optionally `OperatorUpd(operOid, oprcom, oprnegate, true)` (re-fetching on a
/// self-commutator/self-negator) and then `CatalogTupleDelete(relation,
/// &tup->t_self)`.
fn remove_operator_tuple(
    operOid: Oid,
    oprcom: Oid,
    oprnegate: Oid,
    do_operator_upd: bool,
) -> PgResult<()> {
    let ctx = MemoryContext::new("RemoveOperatorById");
    let mcx = ctx.mcx();

    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;

    /*
     * Reset links from commutator and negator, if any.  In case of a
     * self-commutator or self-negator, this means we have to re-fetch the
     * updated tuple.
     */
    if do_operator_upd {
        OperatorUpd(mcx, operOid, oprcom, oprnegate, true)?;
    }

    // tup = SearchSysCache1(OPEROID, operOid); re-fetched here (after the
    // possible self-link update) so its t_self is current.
    let tup = match search_operator_by_oid(mcx, &rel, operOid)? {
        Some(t) => t,
        None => {
            /* should not happen */
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {operOid}"))
                .into_error());
        }
    };

    CatalogTupleDelete(mcx, &rel, tup.tuple.t_self)?;

    rel.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * alter_operator_apply — the held-tuple update path of AlterOperator
 * (operatorcmds.c:540-590), the catalog half that pg_operator.c owns.
 * ========================================================================= */

/// The held-tuple update path of `AlterOperator`: `heap_modify_tuple(tup,
/// RelationGetDescr(catalog), values, nulls, replaces)`, `CatalogTupleUpdate`,
/// then `makeOperatorDependencies(tup, false, true)`.
fn alter_operator_apply(operOid: Oid, updates: Vec<OperatorAttrUpdate>) -> PgResult<ObjectAddress> {
    let ctx = MemoryContext::new("AlterOperator");
    let mcx = ctx.mcx();

    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;

    let oldtup = match search_operator_by_oid(mcx, &rel, operOid)? {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {operOid}"))
                .into_error());
        }
    };

    // Build values/replaces, mirroring AlterOperator's per-attribute packing.
    let mut values: [Datum<'_>; Natts_pg_operator as usize] =
        core::array::from_fn(|_| Datum::null());
    let mut replaces = [false; Natts_pg_operator as usize];
    let nulls = [false; Natts_pg_operator as usize];

    for upd in &updates {
        let (attnum, value): (i16, Datum<'_>) = match *upd {
            OperatorAttrUpdate::Restriction(oid) => {
                (Anum_pg_operator_oprrest, Datum::from_oid(oid))
            }
            OperatorAttrUpdate::Join(oid) => (Anum_pg_operator_oprjoin, Datum::from_oid(oid)),
            OperatorAttrUpdate::Commutator(oid) => {
                (Anum_pg_operator_oprcom, Datum::from_oid(oid))
            }
            OperatorAttrUpdate::Negator(oid) => {
                (Anum_pg_operator_oprnegate, Datum::from_oid(oid))
            }
            OperatorAttrUpdate::Merges(b) => (Anum_pg_operator_oprcanmerge, Datum::from_bool(b)),
            OperatorAttrUpdate::Hashes(b) => (Anum_pg_operator_oprcanhash, Datum::from_bool(b)),
        };
        values[attnum as usize - 1] = value;
        replaces[attnum as usize - 1] = true;
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, &oldtup, &tupdesc, &values, &nulls, &replaces)?;

    CatalogTupleUpdate(mcx, &rel, oldtup.tuple.t_self, &mut tup)?;

    /* Make dependencies (makeExtensionDep=false, isUpdate=true). */
    let form = form_pg_operator_of_tuple(mcx, &rel, &tup)?;
    let address = makeOperatorDependencies(mcx, &form, false, true)?;

    rel.close(RowExclusiveLock)?;

    Ok(address)
}

/* ===========================================================================
 * Inward-seam adapters + install
 * ========================================================================= */

/// `operator_create` seam: open a private context and forward to
/// [`OperatorCreate`] (which opens/closes `pg_operator` itself).
fn operator_create_seam(args: OperatorCreateArgs) -> PgResult<ObjectAddress> {
    let ctx = MemoryContext::new("OperatorCreate");
    OperatorCreate(
        ctx.mcx(),
        &args.operator_name,
        args.operator_namespace,
        args.left_type,
        args.right_type,
        args.proc,
        &args.commutator_name,
        &args.negator_name,
        args.restriction_oid,
        args.join_oid,
        args.can_merge,
        args.can_hash,
    )
}

/// `operator_validate_params` seam: forward the bundle to
/// [`OperatorValidateParams`].
fn operator_validate_params_seam(args: OperatorValidateParamsArgs) -> PgResult<()> {
    OperatorValidateParams(
        args.oprleft,
        args.oprright,
        args.oprresult,
        args.has_commutator,
        args.has_negator,
        args.has_restriction_selectivity,
        args.has_join_selectivity,
        args.can_merge,
        args.can_hash,
    )
}

/// `operator_upd` seam: open a private context and forward to [`OperatorUpd`].
fn operator_upd_seam(base_id: Oid, comm_id: Oid, neg_id: Oid, is_delete: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("OperatorUpd");
    OperatorUpd(ctx.mcx(), base_id, comm_id, neg_id, is_delete)
}

/// `operator_lookup` seam: forward to [`OperatorLookup`].
fn operator_lookup_seam(
    operator_name: Vec<String>,
    left_type_id: Oid,
    right_type_id: Oid,
) -> PgResult<(Oid, bool)> {
    OperatorLookup(&operator_name, left_type_id, right_type_id)
}

/// `fetch_operator_form` seam: `SearchSysCache1(OPEROID, operOid)` + `GETSTRUCT`,
/// returning the owned [`FormPgOperator`] or `None` on a miss.
fn fetch_operator_form_seam(oper_oid: Oid) -> PgResult<Option<FormPgOperator>> {
    let ctx = MemoryContext::new("fetch_operator_form");
    let mcx = ctx.mcx();
    let rel = table::table_open(mcx, OperatorRelationId, RowExclusiveLock)?;
    let result = match search_operator_by_oid(mcx, &rel, oper_oid)? {
        Some(tup) => Some(form_pg_operator_of_tuple(mcx, &rel, &tup)?),
        None => None,
    };
    rel.close(RowExclusiveLock)?;
    Ok(result)
}

/// `(oprcanhash, oprcode)` of `pg_operator` row `opno` — the
/// `SearchSysCache1(OPEROID, opno)` projection `hash_ok_operator`
/// (optimizer/plan/subselect.c) reads. C `elog(ERROR)`s on a cache miss; we
/// surface the absent row as an error, mirroring that.
fn oper_canhash_code_seam(opno: Oid) -> PgResult<(bool, Oid)> {
    match fetch_operator_form_seam(opno)? {
        Some(form) => Ok((form.oprcanhash, form.oprcode)),
        None => Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for operator {opno}"))
            .into_error()),
    }
}

/// `invoke_object_post_alter_hook` seam:
/// `InvokeObjectPostAlterHook(OperatorRelationId, operOid, 0)`.
fn invoke_object_post_alter_hook_seam(oper_oid: Oid) -> PgResult<()> {
    objectaccess_seams::invoke_object_post_alter_hook::call(OperatorRelationId, oper_oid, 0)
}

/// Install this crate's implementations into
/// `backend-catalog-pg-operator-seams`.
pub fn init_seams() {
    use pg_operator_seams as seams;

    seams::operator_create::set(operator_create_seam);
    seams::operator_validate_params::set(operator_validate_params_seam);
    seams::operator_upd::set(operator_upd_seam);
    seams::operator_lookup::set(operator_lookup_seam);
    seams::fetch_operator_form::set(fetch_operator_form_seam);
    seams::remove_operator_tuple::set(remove_operator_tuple);
    seams::alter_operator_apply::set(alter_operator_apply);
    seams::invoke_object_post_alter_hook::set(invoke_object_post_alter_hook_seam);

    // `(oprcanhash, oprcode)` projection of `pg_operator` consumed by
    // `hash_ok_operator` (optimizer/plan/subselect.c, in init-subselect). Homed
    // here because `OperRow` does not project `oprcanhash`; pg_operator is owned
    // by this crate.
    init_subselect_ext_seams::oper_canhash_code::set(
        oper_canhash_code_seam,
    );
}
