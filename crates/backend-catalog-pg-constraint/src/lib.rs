//! `src/backend/catalog/pg_constraint.c` (PostgreSQL 18.3) — routines to
//! support manipulation of the `pg_constraint` relation.
//!
//! Ported 1:1 against the C, name-for-name; catalog access is done directly
//! (mirroring `backend-catalog-pg-depend`): `table_open`/`table_close` guard
//! scopes, `ScanKeyInit` + the genam `systable_*` iterator, `heap_deform_tuple`
//! of the scalar columns. Output parameters become returns; `Bitmapset *` is an
//! owned `PgBox<Bitmapset>` (`None` == the C NULL set); the array columns
//! (conkey/confkey/…) cross the syscache/heap detoast seams as validated carrier
//! structs, with the 1-D/elemtype/hasnull validation + error strings kept here.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

use mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgString, PgVec};

use types_amapi::COMPARE_CONTAINED_BY;
use types_catalog::catalog::{
    CONSTRAINT_RELATION_ID, OPERATOR_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_NORMAL, DEPENDENCY_PARTITION_PRI,
    DEPENDENCY_PARTITION_SEC,
};
use types_catalog::pg_constraint::{
    Anum_pg_constraint_condeferrable, Anum_pg_constraint_condeferred,
    Anum_pg_constraint_conenforced, Anum_pg_constraint_confdeltype, Anum_pg_constraint_confmatchtype,
    Anum_pg_constraint_confrelid, Anum_pg_constraint_confupdtype, Anum_pg_constraint_conindid,
    Anum_pg_constraint_coninhcount, Anum_pg_constraint_conislocal, Anum_pg_constraint_conname,
    Anum_pg_constraint_connamespace, Anum_pg_constraint_connoinherit, Anum_pg_constraint_conparentid,
    Anum_pg_constraint_conperiod, Anum_pg_constraint_conrelid, Anum_pg_constraint_contype,
    Anum_pg_constraint_contypid, Anum_pg_constraint_convalidated, Anum_pg_constraint_oid,
    ConKeyArray, ConstraintCategory, ConstraintFieldUpdate, FormData_pg_constraint, OidArray,
    PgConstraintInsertRow, ConstraintNameNspIndexId, ConstraintRelidTypidNameIndexId,
    CONSTRAINT_CHECK, CONSTRAINT_EXCLUSION, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
    CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP,
    OID_RANGE_INTERSECT_RANGE_OP,
};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, INDEX_MAX_KEYS};
use types_core::primitive::{AttrNumber, InvalidAttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::primnodes::Expr;
use types_rel::RelationData;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber, ItemPointerData, INT2OID,
};

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;
use backend_commands_indexcmds_seams as indexcmds_seams;
use backend_nodes_core::bitmapset::{bms_add_member, bms_is_subset};
use backend_utils_adt_format_type_seams as format_type_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;

/// `NAMEDATALEN`.
const NAMEDATALEN: usize = 64;

/* ===========================================================================
 * small helpers (mirror src-idiomatic)
 * ========================================================================= */

/// `Max(a, b)`.
#[inline]
fn Max(a: i32, b: i32) -> i32 {
    if a > b {
        a
    } else {
        b
    }
}

/// `pg_add_s16_overflow(a, b, result)` (common/int.h): returns `true` on
/// overflow, else writes `a + b` into `*result`.
#[inline]
fn pg_add_s16_overflow(a: i16, b: i16, result: &mut i16) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *result = v;
            false
        }
        None => true,
    }
}

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled `NameData`,
/// truncated to `NAMEDATALEN`, force-terminated at the last slot.
fn namestrcpy(src: &str) -> [u8; NAMEDATALEN] {
    let mut name = [0u8; NAMEDATALEN];
    for (i, &byte) in src.as_bytes().iter().take(NAMEDATALEN).enumerate() {
        name[i] = byte;
    }
    name[NAMEDATALEN - 1] = 0;
    name
}

/// `NameStr(name)` → a `&str` (read up to the NUL).
fn name_str(name: &[u8; NAMEDATALEN]) -> &str {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    core::str::from_utf8(&name[..end]).unwrap_or("")
}

/// `get_rel_name(relid)` for a `%s` slot — a NULL pointer renders as `(null)`.
fn rel_name_for_msg(mcx: Mcx<'_>, relid: Oid) -> PgResult<String> {
    Ok(lsyscache_seams::get_rel_name::call(mcx, relid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "(null)".to_string()))
}

/// `strlcpy`/`snprintf` into a `NAMEDATALEN` buffer: truncate to
/// `NAMEDATALEN - 1` bytes on a char boundary.
fn truncate_namedatalen(src: &str) -> String {
    let max = NAMEDATALEN - 1;
    if src.len() <= max {
        src.to_string()
    } else {
        let mut end = max;
        while end > 0 && !src.is_char_boundary(end) {
            end -= 1;
        }
        src[..end].to_string()
    }
}

/// OOM-safe copy of the leading `n` elements of `src` (the C `memcpy` into a
/// fixed-size caller array); validates `n` against availability first.
fn take_checked<T: Copy>(mcx: Mcx<'_>, src: &[T], n: i32, what: &str) -> PgResult<Vec<T>> {
    if n < 0 || n as usize > src.len() {
        return Err(PgError::error(format!(
            "{what}: requested {n} elements but only {} available",
            src.len()
        )));
    }
    let n = n as usize;
    let mut out: Vec<T> = Vec::new();
    out.try_reserve_exact(n)
        .map_err(|_| mcx.oom(n * core::mem::size_of::<T>()))?;
    out.extend_from_slice(&src[..n]);
    Ok(out)
}

/* ===========================================================================
 * scan-key builders + the systable scan iterator (mirror pg-depend)
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

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_NAMEEQ,
/// CStringGetDatum(conname))`. The name crosses as a NUL-terminated byte image
/// (the genam owner's `nameeq` comparator interprets it).
fn name_key<'mcx>(mcx: Mcx<'mcx>, attno: AttrNumber, value: &str) -> PgResult<ScanKeyData<'mcx>> {
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

/// `(Form_pg_constraint) GETSTRUCT(tup)` — the fixed-width scalar columns of one
/// scanned `pg_constraint` row (Anum 1..=20; the trailing variable-length array
/// columns are read separately via the detoast seams).
fn form_pg_constraint(values: &[Datum<'_>]) -> FormData_pg_constraint {
    let col = |attno: i16| &values[attno as usize - 1];
    // conname (NameData) is the 64-byte by-value image at Anum 2.
    let conname = {
        let mut n = [0u8; NAMEDATALEN];
        if let Datum::ByRef(b) = col(Anum_pg_constraint_conname) {
            let take = core::cmp::min(NAMEDATALEN, b.len());
            n[..take].copy_from_slice(&b[..take]);
        }
        n
    };
    FormData_pg_constraint {
        oid: col(Anum_pg_constraint_oid).as_oid(),
        conname,
        connamespace: col(Anum_pg_constraint_connamespace).as_oid(),
        contype: col(Anum_pg_constraint_contype).as_char(),
        condeferrable: col(Anum_pg_constraint_condeferrable).as_i32() != 0,
        condeferred: col(Anum_pg_constraint_condeferred).as_i32() != 0,
        conenforced: col(Anum_pg_constraint_conenforced).as_i32() != 0,
        convalidated: col(Anum_pg_constraint_convalidated).as_i32() != 0,
        conrelid: col(Anum_pg_constraint_conrelid).as_oid(),
        contypid: col(Anum_pg_constraint_contypid).as_oid(),
        conindid: col(Anum_pg_constraint_conindid).as_oid(),
        conparentid: col(Anum_pg_constraint_conparentid).as_oid(),
        confrelid: col(Anum_pg_constraint_confrelid).as_oid(),
        confupdtype: col(Anum_pg_constraint_confupdtype).as_char(),
        confdeltype: col(Anum_pg_constraint_confdeltype).as_char(),
        confmatchtype: col(Anum_pg_constraint_confmatchtype).as_char(),
        conislocal: col(Anum_pg_constraint_conislocal).as_i32() != 0,
        coninhcount: col(Anum_pg_constraint_coninhcount).as_i16(),
        connoinherit: col(Anum_pg_constraint_connoinherit).as_i32() != 0,
        conperiod: col(Anum_pg_constraint_conperiod).as_i32() != 0,
    }
}

/// One scanned row: the heap TID (`tup->t_self`) plus the owned full tuple (for
/// the array-column reads / `heap_copytuple`) plus the deformed scalar form.
struct ConScanRow<'mcx> {
    tid: ItemPointerData,
    htup: FormedTuple<'mcx>,
    form: FormData_pg_constraint,
}

/// `systable_beginscan` + `while ((tup = systable_getnext(scan)))` loop +
/// `systable_endscan` (the genam iterator). `body` returning `Ok(false)` stops
/// early (the C `break`). Each row's deformed scalar columns + owned tuple land
/// in a per-iteration scratch context.
fn systable_scan_foreach(
    rel: &RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&ConScanRow<'_>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("pg_constraint scan row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        let form = form_pg_constraint(&values);
        let tid = tup.tuple.t_self;
        let row = ConScanRow {
            tid,
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
 * CreateConstraintEntry (pg_constraint.c:50-397)
 * ========================================================================= */

/// CreateConstraintEntry — create a constraint table entry. Subsidiary records
/// are *not* created here, but we make the dependency links. Returns the new
/// constraint's OID.
///
/// `conExpr` is `Some(node)` for a CHECK expression `Node *`, `None` otherwise.
pub fn CreateConstraintEntry(
    mcx: Mcx<'_>,
    constraintName: &str,
    constraintNamespace: Oid,
    constraintType: i8,
    isDeferrable: bool,
    isDeferred: bool,
    isEnforced: bool,
    isValidated: bool,
    parentConstrId: Oid,
    relId: Oid,
    constraintKey: &[i16],
    constraintNKeys: i32,
    constraintNTotalKeys: i32,
    domainId: Oid,
    indexRelId: Oid,
    foreignRelId: Oid,
    foreignKey: &[i16],
    pfEqOp: &[Oid],
    ppEqOp: &[Oid],
    ffEqOp: &[Oid],
    foreignNKeys: i32,
    foreignUpdateType: i8,
    foreignDeleteType: i8,
    fkDeleteSetCols: &[i16],
    numFkDeleteSetCols: i32,
    foreignMatchType: i8,
    exclOp: Option<&[Oid]>,
    conExpr: Option<&Node<'_>>,
    conBin: Option<&str>,
    conIsLocal: bool,
    conInhCount: i16,
    conNoInherit: bool,
    conPeriod: bool,
    is_internal: bool,
) -> PgResult<Oid> {
    let conkeyArray: Option<Vec<i16>>;
    let confkeyArray: Option<Vec<i16>>;
    let conpfeqopArray: Option<Vec<Oid>>;
    let conppeqopArray: Option<Vec<Oid>>;
    let conffeqopArray: Option<Vec<Oid>>;
    let conexclopArray: Option<Vec<Oid>>;
    let confdelsetcolsArray: Option<Vec<i16>>;

    /* Only CHECK or FOREIGN KEY constraint can be not enforced */
    debug_assert!(
        isEnforced || constraintType == CONSTRAINT_CHECK || constraintType == CONSTRAINT_FOREIGN
    );
    /* NOT ENFORCED constraint must be NOT VALID */
    debug_assert!(isEnforced || !isValidated);

    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let cname = namestrcpy(constraintName);

    /*
     * Convert C arrays into Postgres arrays. Each count is validated against the
     * supplied slice (mirroring `construct_array_builtin(arr, n, ELEMTYPE)`).
     */
    if constraintNKeys > 0 {
        conkeyArray = Some(take_checked(mcx, constraintKey, constraintNKeys, "conkey")?);
    } else {
        conkeyArray = None;
    }

    if foreignNKeys > 0 {
        let _nkeys = Max(foreignNKeys, numFkDeleteSetCols); // shared fkdatums buffer sizing in C
        confkeyArray = Some(take_checked(mcx, foreignKey, foreignNKeys, "confkey")?);
        conpfeqopArray = Some(take_checked(mcx, pfEqOp, foreignNKeys, "conpfeqop")?);
        conppeqopArray = Some(take_checked(mcx, ppEqOp, foreignNKeys, "conppeqop")?);
        conffeqopArray = Some(take_checked(mcx, ffEqOp, foreignNKeys, "conffeqop")?);

        if numFkDeleteSetCols > 0 {
            confdelsetcolsArray = Some(take_checked(
                mcx,
                fkDeleteSetCols,
                numFkDeleteSetCols,
                "confdelsetcols",
            )?);
        } else {
            confdelsetcolsArray = None;
        }
    } else {
        confkeyArray = None;
        conpfeqopArray = None;
        conppeqopArray = None;
        conffeqopArray = None;
        confdelsetcolsArray = None;
    }

    if let Some(excl) = exclOp {
        conexclopArray = Some(take_checked(mcx, excl, constraintNKeys, "conexclop")?);
    } else {
        conexclopArray = None;
    }

    /* The owner allocates the OID, forms the row, and inserts it. */
    let row = PgConstraintInsertRow {
        conname: cname,
        connamespace: constraintNamespace,
        contype: constraintType,
        condeferrable: isDeferrable,
        condeferred: isDeferred,
        conenforced: isEnforced,
        convalidated: isValidated,
        conrelid: relId,
        contypid: domainId,
        conindid: indexRelId,
        conparentid: parentConstrId,
        confrelid: foreignRelId,
        confupdtype: foreignUpdateType,
        confdeltype: foreignDeleteType,
        confmatchtype: foreignMatchType,
        conislocal: conIsLocal,
        coninhcount: conInhCount,
        connoinherit: conNoInherit,
        conperiod: conPeriod,
        conkey: conkeyArray,
        confkey: confkeyArray,
        conpfeqop: conpfeqopArray,
        conppeqop: conppeqopArray,
        conffeqop: conffeqopArray,
        confdelsetcols: confdelsetcolsArray,
        conexclop: conexclopArray,
        conbin: conBin.map(|s| s.to_string()),
    };

    let conOid = indexing_seams::catalog_tuple_insert_pg_constraint::call(&conDesc, &row)?;

    /* ObjectAddressSet(conobject, ConstraintRelationId, conOid); */
    let conobject = ObjectAddress {
        classId: CONSTRAINT_RELATION_ID,
        objectId: conOid,
        objectSubId: 0,
    };

    conDesc.close(RowExclusiveLock)?;

    /* Handle set of auto dependencies */
    let mut addrs_auto = dependency_seams::new_object_addresses::call()?;

    if OidIsValid(relId) {
        /*
         * Register auto dependency from constraint to owning relation, or to
         * specific column(s) if any are mentioned.
         */
        if constraintNTotalKeys > 0 {
            for i in 0..constraintNTotalKeys {
                let relobject = ObjectAddress {
                    classId: RELATION_RELATION_ID,
                    objectId: relId,
                    objectSubId: constraintKey[i as usize] as i32,
                };
                dependency_seams::add_exact_object_address::call(relobject, &mut addrs_auto)?;
            }
        } else {
            let relobject = ObjectAddress {
                classId: RELATION_RELATION_ID,
                objectId: relId,
                objectSubId: 0,
            };
            dependency_seams::add_exact_object_address::call(relobject, &mut addrs_auto)?;
        }
    }

    if OidIsValid(domainId) {
        /* Register auto dependency from constraint to owning domain */
        let domobject = ObjectAddress {
            classId: TYPE_RELATION_ID,
            objectId: domainId,
            objectSubId: 0,
        };
        dependency_seams::add_exact_object_address::call(domobject, &mut addrs_auto)?;
    }

    dependency_seams::record_object_address_dependencies::call(conobject, &mut addrs_auto, DEPENDENCY_AUTO)?;
    dependency_seams::free_object_addresses::call(addrs_auto)?;

    /* Handle set of normal dependencies */
    let mut addrs_normal = dependency_seams::new_object_addresses::call()?;

    if OidIsValid(foreignRelId) {
        /*
         * Register normal dependency from constraint to foreign relation, or to
         * specific column(s) if any are mentioned.
         */
        if foreignNKeys > 0 {
            for i in 0..foreignNKeys {
                let relobject = ObjectAddress {
                    classId: RELATION_RELATION_ID,
                    objectId: foreignRelId,
                    objectSubId: foreignKey[i as usize] as i32,
                };
                dependency_seams::add_exact_object_address::call(relobject, &mut addrs_normal)?;
            }
        } else {
            let relobject = ObjectAddress {
                classId: RELATION_RELATION_ID,
                objectId: foreignRelId,
                objectSubId: 0,
            };
            dependency_seams::add_exact_object_address::call(relobject, &mut addrs_normal)?;
        }
    }

    if OidIsValid(indexRelId) && constraintType == CONSTRAINT_FOREIGN {
        /*
         * Register normal dependency on the unique index that supports a
         * foreign-key constraint. (For indexes associated with unique or
         * primary-key constraints, the dependency runs the other way, and is not
         * made here.)
         */
        let relobject = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: indexRelId,
            objectSubId: 0,
        };
        dependency_seams::add_exact_object_address::call(relobject, &mut addrs_normal)?;
    }

    if foreignNKeys > 0 {
        /*
         * Register normal dependencies on the equality operators that support a
         * foreign-key constraint. If the PK and FK types are the same then all
         * three operators for a column are the same; otherwise they are
         * different.
         */
        let mut oprobject = ObjectAddress {
            classId: OPERATOR_RELATION_ID,
            objectId: InvalidOid,
            objectSubId: 0,
        };

        for i in 0..foreignNKeys {
            let pf = pfEqOp[i as usize];
            let pp = ppEqOp[i as usize];
            let ff = ffEqOp[i as usize];
            oprobject.objectId = pf;
            dependency_seams::add_exact_object_address::call(oprobject, &mut addrs_normal)?;
            if pp != pf {
                oprobject.objectId = pp;
                dependency_seams::add_exact_object_address::call(oprobject, &mut addrs_normal)?;
            }
            if ff != pf {
                oprobject.objectId = ff;
                dependency_seams::add_exact_object_address::call(oprobject, &mut addrs_normal)?;
            }
        }
    }

    dependency_seams::record_object_address_dependencies::call(
        conobject,
        &mut addrs_normal,
        DEPENDENCY_NORMAL,
    )?;
    dependency_seams::free_object_addresses::call(addrs_normal)?;

    /*
     * We don't bother to register dependencies on the exclusion operators of an
     * exclusion constraint. We assume they are members of the opclass supporting
     * the index, so there's an indirect dependency via that.
     */

    if let Some(expr) = conExpr {
        /*
         * Register dependencies from constraint to objects mentioned in CHECK
         * expression.
         */
        dependency_seams::record_dependency_on_single_rel_expr::call(
            conobject,
            expr,
            relId,
            DEPENDENCY_NORMAL,
            DEPENDENCY_NORMAL,
            false,
        )?;
    }

    /* Post creation hook for new constraint */
    objectaccess_seams::invoke_object_post_create_hook_arg::call(
        CONSTRAINT_RELATION_ID,
        conOid,
        0,
        is_internal,
    )?;

    Ok(conOid)
}

/* ===========================================================================
 * ConstraintNameIsUsed (pg_constraint.c:411-447)
 * ========================================================================= */

/// Test whether `conname` is currently used as a constraint name for the given
/// object (relation or domain).
pub fn ConstraintNameIsUsed(
    mcx: Mcx<'_>,
    conCat: ConstraintCategory,
    objId: Oid,
    conname: &str,
) -> PgResult<bool> {
    let mut found = false;

    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let conrelid = if conCat == ConstraintCategory::Relation {
        objId
    } else {
        InvalidOid
    };
    let contypid = if conCat == ConstraintCategory::Domain {
        objId
    } else {
        InvalidOid
    };

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, conrelid)?,
        oid_key(Anum_pg_constraint_contypid, contypid)?,
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
    ];

    /* There can be at most one matching row */
    systable_scan_foreach(&conDesc, ConstraintRelidTypidNameIndexId, &skey, |_row| {
        found = true;
        Ok(false)
    })?;

    conDesc.close(AccessShareLock)?;

    Ok(found)
}

/* ===========================================================================
 * ConstraintNameExists (pg_constraint.c:456-485)
 * ========================================================================= */

/// Does any constraint of the given name exist in the given namespace?
pub fn ConstraintNameExists(mcx: Mcx<'_>, conname: &str, namespaceid: Oid) -> PgResult<bool> {
    let mut found = false;

    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
        oid_key(Anum_pg_constraint_connamespace, namespaceid)?,
    ];

    systable_scan_foreach(&conDesc, ConstraintNameNspIndexId, &skey, |_row| {
        found = true;
        Ok(false)
    })?;

    conDesc.close(AccessShareLock)?;

    Ok(found)
}

/* ===========================================================================
 * ChooseConstraintName (pg_constraint.c:512-580)
 * ========================================================================= */

/// Select a nonconflicting name for a new constraint. `others` is the C
/// `List *` of already-chosen names.
pub fn ChooseConstraintName(
    mcx: Mcx<'_>,
    name1: &str,
    name2: &str,
    label: &str,
    namespaceid: Oid,
    others: &[String],
) -> PgResult<String> {
    let mut pass: i32 = 0;
    let mut conname: String;
    let mut modlabel: String;

    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    /* try the unmodified label first, unless it's empty */
    if !label.is_empty() {
        modlabel = truncate_namedatalen(label);
    } else {
        pass += 1;
        modlabel = truncate_namedatalen(&format!("{label}{pass}"));
    }

    loop {
        conname = indexcmds_seams::make_object_name::call(name1, name2, &modlabel)?;

        let mut found = false;

        for s in others {
            if *s == conname {
                found = true;
                break;
            }
        }

        if !found {
            let skey = [
                name_key(mcx, Anum_pg_constraint_conname, &conname)?,
                oid_key(Anum_pg_constraint_connamespace, namespaceid)?,
            ];
            systable_scan_foreach(&conDesc, ConstraintNameNspIndexId, &skey, |_row| {
                found = true;
                Ok(false)
            })?;
        }

        if !found {
            break;
        }

        /* found a conflict, so try a new name component */
        pass += 1;
        modlabel = truncate_namedatalen(&format!("{label}{pass}"));
    }

    conDesc.close(AccessShareLock)?;

    Ok(conname)
}

/* ===========================================================================
 * findNotNullConstraintAttnum (pg_constraint.c:591-633)
 * ========================================================================= */

/// Find and return a copy of the `pg_constraint` tuple that implements a
/// (possibly not valid) not-null constraint for the given column of the given
/// relation, or `None`.
pub fn findNotNullConstraintAttnum<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let mut retval: Option<FormedTuple<'mcx>> = None;

    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let key = [oid_key(Anum_pg_constraint_conrelid, relid)?];

    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &key, |row| {
        /*
         * We're looking for a NOTNULL constraint with the column we're looking
         * for as the sole element in conkey.
         */
        if row.form.contype != CONSTRAINT_NOTNULL {
            return Ok(true);
        }

        let conkey = extractNotNullColumn(&row.htup)?;
        if conkey != attnum {
            return Ok(true);
        }

        /* Found it */
        retval = Some(row.htup.clone_in(mcx)?);
        Ok(false)
    })?;

    pg_constraint.close(AccessShareLock)?;

    Ok(retval)
}

/* ===========================================================================
 * findNotNullConstraint (pg_constraint.c:641-651)
 * ========================================================================= */

/// Find the not-null constraint tuple for a named column, or `None`.
pub fn findNotNullConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    colname: &str,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let attnum = lsyscache_seams::get_attnum::call(relid, colname)?;
    if attnum <= InvalidAttrNumber {
        return Ok(None);
    }

    findNotNullConstraintAttnum(mcx, relid, attnum)
}

/* ===========================================================================
 * findDomainNotNullConstraint (pg_constraint.c:657-695)
 * ========================================================================= */

/// Find the validated not-null constraint tuple for a domain, or `None`.
pub fn findDomainNotNullConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    typid: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let mut retval: Option<FormedTuple<'mcx>> = None;

    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let key = [oid_key(Anum_pg_constraint_contypid, typid)?];

    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &key, |row| {
        /* We're looking for a NOTNULL constraint that's marked validated. */
        if row.form.contype != CONSTRAINT_NOTNULL {
            return Ok(true);
        }
        if !row.form.convalidated {
            return Ok(true);
        }

        /* Found it */
        retval = Some(row.htup.clone_in(mcx)?);
        Ok(false)
    })?;

    pg_constraint.close(AccessShareLock)?;

    Ok(retval)
}

/* ===========================================================================
 * extractNotNullColumn (pg_constraint.c:701-722)
 * ========================================================================= */

/// Given a `pg_constraint` tuple for a not-null constraint, return the column
/// number it is for.
pub fn extractNotNullColumn(constrTup: &FormedTuple<'_>) -> PgResult<AttrNumber> {
    /* only tuples for not-null constraints should be given (the C Assert) */

    let arr: ConKeyArray = syscache_seams::get_conkey_array::call(constrTup)?;
    if arr.ndim != 1 || arr.hasnull || arr.elemtype != INT2OID || arr.dim0 != 1 {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    }

    /* We leak the detoasted datum, but we don't care */

    Ok(arr.data[0])
}

/* ===========================================================================
 * AdjustNotNullInheritance (pg_constraint.c:741-821)
 * ========================================================================= */

/// AdjustNotNullInheritance — adjust inheritance status for a single not-null
/// constraint. `None` (`false`) if no such constraint is found.
pub fn AdjustNotNullInheritance(
    mcx: Mcx<'_>,
    relid: Oid,
    attnum: AttrNumber,
    new_conname: Option<&str>,
    is_local: bool,
    is_no_inherit: bool,
    is_notvalid: bool,
) -> PgResult<bool> {
    let tup = findNotNullConstraintAttnum(mcx, relid, attnum)?;
    if let Some(tup) = tup {
        let mut changed = false;

        let con_ctx = MemoryContext::new("pg_constraint");
        let pg_constraint =
            table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
        /* conform = (Form_pg_constraint) GETSTRUCT(tup); */
        let mut conform = syscache_seams::read_constraint_form::call(&tup)?;

        /*
         * If the NO INHERIT flag we're asked for doesn't match what the existing
         * constraint has, throw an error.
         */
        if is_no_inherit != conform.connoinherit {
            return Err(PgError::new(
                ERROR,
                format!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                    name_str(&conform.conname),
                    rel_name_for_msg(mcx, relid)?
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.",
            ));
        }

        /*
         * Throw an error if the existing constraint is NOT VALID and caller wants
         * a valid one.
         */
        if !is_notvalid && !conform.convalidated {
            return Err(PgError::new(
                ERROR,
                format!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                    name_str(&conform.conname),
                    rel_name_for_msg(mcx, relid)?
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.",
            ));
        }

        /*
         * If, for a new constraint that is being defined locally, a name was
         * specified, verify that the existing constraint has the same name.
         */
        if is_local && new_conname.is_some() && new_conname != Some(name_str(&conform.conname)) {
            let attname = lsyscache_seams::get_attname::call(mcx, relid, attnum, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(PgError::new(
                ERROR,
                format!(
                    "cannot create not-null constraint \"{}\" on column \"{}\" of table \"{}\"",
                    new_conname.unwrap_or(""),
                    attname,
                    rel_name_for_msg(mcx, relid)?
                ),
            )
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_detail(format!(
                "A not-null constraint named \"{}\" already exists for this column.",
                name_str(&conform.conname)
            )));
        }

        if !is_local {
            let mut newcount = conform.coninhcount;
            if pg_add_s16_overflow(conform.coninhcount, 1, &mut newcount) {
                return Err(PgError::new(ERROR, "too many inheritance parents".to_string())
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
            }
            conform.coninhcount = newcount;
            changed = true;
        } else if !conform.conislocal {
            conform.conislocal = true;
            changed = true;
        }

        if changed {
            let fields = ConstraintFieldUpdate {
                conname: conform.conname,
                connamespace: conform.connamespace,
                conislocal: conform.conislocal,
                coninhcount: conform.coninhcount,
                conparentid: conform.conparentid,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(
                &pg_constraint,
                tup.tuple.t_self,
                &fields,
            )?;
        }

        pg_constraint.close(RowExclusiveLock)?;

        return Ok(true);
    }

    Ok(false)
}

/* ===========================================================================
 * RelationGetNotNullConstraints (pg_constraint.c:833-905)
 * ========================================================================= */

/// One element of the list `RelationGetNotNullConstraints` returns. C returns a
/// `List *` whose elements are `Constraint *` nodes when `cooked == false`, or
/// `CookedConstraint *` nodes when `cooked == true`. We mirror both node shapes
/// 1:1 here (rather than a trimmed carrier), so the caller gets the exact node
/// the C built.
#[derive(Debug)]
pub enum NotNullConstraint<'mcx> {
    /// `cooked == false`: a `makeNode(Constraint)` of type `CONSTR_NOTNULL`.
    Raw(types_nodes::ddlnodes::Constraint<'mcx>),
    /// `cooked == true`: a `palloc(sizeof(CookedConstraint))`.
    Cooked(types_nodes::ddlnodes::CookedConstraint<'mcx>),
}

/// RelationGetNotNullConstraints — return the list of not-null constraints for
/// the given rel. `include_noinh` chooses whether to include NO INHERIT ones.
/// `cooked` selects which node shape the caller wants.
pub fn RelationGetNotNullConstraints(
    mcx: Mcx<'_>,
    relid: Oid,
    cooked: bool,
    include_noinh: bool,
) -> PgResult<Vec<NotNullConstraint<'_>>> {
    use types_nodes::ddlnodes::ConstrType::CONSTR_NOTNULL as CONSTR_NOTNULL_TYPE;
    use types_nodes::ddlnodes::{Constraint, CookedConstraint};

    let mut notnulls: Vec<NotNullConstraint<'_>> = Vec::new(); // NIL

    let constrRel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let skey = [oid_key(Anum_pg_constraint_conrelid, relid)?];

    systable_scan_foreach(&constrRel, ConstraintRelidTypidNameIndexId, &skey, |row| {
        let con_form = &row.form;

        if con_form.contype != CONSTRAINT_NOTNULL {
            return Ok(true);
        }
        if con_form.connoinherit && !include_noinh {
            return Ok(true);
        }

        let colnum = extractNotNullColumn(&row.htup)?;

        if cooked {
            /*
             * cooked->contype = CONSTR_NOTNULL;
             * cooked->conoid = conForm->oid;
             * cooked->name = pstrdup(NameStr(conForm->conname));
             * cooked->attnum = colnum;
             * cooked->expr = NULL;
             * cooked->is_enforced = true;
             * cooked->skip_validation = !conForm->convalidated;
             * cooked->is_local = true;
             * cooked->inhcount = 0;
             * cooked->is_no_inherit = conForm->connoinherit;
             */
            let cookedc = CookedConstraint {
                contype: CONSTR_NOTNULL_TYPE,
                conoid: con_form.oid,
                name: Some(PgString::from_str_in(name_str(&con_form.conname), mcx)?),
                attnum: colnum,
                expr: None,
                is_enforced: true,
                skip_validation: !con_form.convalidated,
                is_local: true,
                inhcount: 0,
                is_no_inherit: con_form.connoinherit,
            };
            notnulls.push(NotNullConstraint::Cooked(cookedc));
        } else {
            /*
             * constr = makeNode(Constraint);
             * constr->contype = CONSTR_NOTNULL;
             * constr->conname = pstrdup(NameStr(conForm->conname));
             * constr->deferrable = false;
             * constr->initdeferred = false;
             * constr->location = -1;
             * constr->keys = list_make1(makeString(get_attname(relid, colnum, false)));
             * constr->is_enforced = true;
             * constr->skip_validation = !conForm->convalidated;
             * constr->initially_valid = true;
             * constr->is_no_inherit = conForm->connoinherit;
             */
            /* get_attname(relid, colnum, false) — false == not missing_ok */
            let attname = lsyscache_seams::get_attname::call(mcx, relid, colnum, false)?
                .ok_or_else(|| {
                    PgError::error(format!(
                        "cache lookup failed for attribute {colnum} of relation {relid}"
                    ))
                })?;
            let mut keys: PgVec<'_, NodePtr<'_>> = PgVec::new_in(mcx);
            keys.push(alloc_in(
                mcx,
                Node::String(types_nodes::value::StringNode { sval: attname }),
            )?);

            let constr = Constraint {
                contype: CONSTR_NOTNULL_TYPE,
                conname: Some(PgString::from_str_in(name_str(&con_form.conname), mcx)?),
                deferrable: false,
                initdeferred: false,
                is_enforced: true,
                skip_validation: !con_form.convalidated,
                initially_valid: true,
                is_no_inherit: con_form.connoinherit,
                raw_expr: None,
                cooked_expr: None,
                generated_when: 0,
                generated_kind: 0,
                nulls_not_distinct: false,
                keys,
                without_overlaps: false,
                including: PgVec::new_in(mcx),
                exclusions: PgVec::new_in(mcx),
                options: PgVec::new_in(mcx),
                indexname: None,
                indexspace: None,
                reset_default_tblspc: false,
                access_method: None,
                where_clause: None,
                pktable: None,
                fk_attrs: PgVec::new_in(mcx),
                pk_attrs: PgVec::new_in(mcx),
                fk_with_period: false,
                pk_with_period: false,
                fk_matchtype: 0,
                fk_upd_action: 0,
                fk_del_action: 0,
                fk_del_set_cols: PgVec::new_in(mcx),
                old_conpfeqop: PgVec::new_in(mcx),
                old_pktable_oid: InvalidOid,
                location: -1,
            };
            notnulls.push(NotNullConstraint::Raw(constr));
        }
        Ok(true)
    })?;

    constrRel.close(AccessShareLock)?;

    Ok(notnulls)
}

/* ===========================================================================
 * RemoveConstraintById (pg_constraint.c:911-990)
 * ========================================================================= */

/// Delete a single constraint record.
pub fn RemoveConstraintById(conId: Oid) -> PgResult<()> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    /* tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conId)); */
    let con = syscache_seams::search_constraint_form_by_oid::call(conId)?;
    let con = match con {
        Some(c) => c,
        None => {
            /* should not happen */
            return Err(PgError::error(format!(
                "cache lookup failed for constraint {conId}"
            )));
        }
    };
    let conform = con.form;

    /* Special processing depending on what the constraint is for. */
    if OidIsValid(conform.conrelid) {
        /*
         * If the constraint is for a relation, open and exclusive-lock the
         * relation it's for.
         */
        let rel_ctx = MemoryContext::new("pg_constraint rel");
        let rel = table::table_open(rel_ctx.mcx(), conform.conrelid, AccessExclusiveLock)?;

        /*
         * We need to update the relchecks count if it is a check constraint being
         * dropped. This update will force backends to rebuild relcache entries
         * when we commit.
         */
        if conform.contype == CONSTRAINT_CHECK {
            let relchecks = syscache_seams::fetch_relchecks::call(conform.conrelid)?;
            let relchecks = match relchecks {
                Some(rc) => rc,
                None => {
                    return Err(PgError::error(format!(
                        "cache lookup failed for relation {}",
                        conform.conrelid
                    )));
                }
            };

            if relchecks == 0 {
                /* should not happen */
                return Err(PgError::error(format!(
                    "relation \"{}\" has relchecks = 0",
                    rel_name_for_msg(rel_ctx.mcx(), conform.conrelid)?
                )));
            }

            /* classForm->relchecks--; CatalogTupleUpdate; ... table_close */
            syscache_seams::decrement_relchecks::call(conform.conrelid)?;
        }

        /* Keep lock on constraint's rel until end of xact */
        rel.close(NoLock)?;
    } else if OidIsValid(conform.contypid) {
        /*
         * XXX for now, do nothing special when dropping a domain constraint.
         */
    } else {
        return Err(PgError::error(format!(
            "constraint {conId} is not of a known type"
        )));
    }

    /* Fry the constraint itself */
    indexing_seams::catalog_tuple_delete::call(&conDesc, con.tid)?;

    /* Clean up (ReleaseSysCache) */
    conDesc.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * RenameConstraintById (pg_constraint.c:1002-1045)
 * ========================================================================= */

/// RenameConstraintById — rename a constraint (not user-exposed; no permission
/// checks).
pub fn RenameConstraintById(mcx: Mcx<'_>, conId: Oid, newname: &str) -> PgResult<()> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    /* tuple = SearchSysCacheCopy1(CONSTROID, ObjectIdGetDatum(conId)); */
    let con = syscache_seams::search_constraint_form_by_oid::call(conId)?;
    let con = match con {
        Some(c) => c,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for constraint {conId}"
            )));
        }
    };
    let conform = con.form;

    /* For user-friendliness, check whether the name is already in use. */
    if OidIsValid(conform.conrelid)
        && ConstraintNameIsUsed(mcx, ConstraintCategory::Relation, conform.conrelid, newname)?
    {
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{}\" for relation \"{}\" already exists",
                newname,
                rel_name_for_msg(mcx, conform.conrelid)?
            ),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }
    if OidIsValid(conform.contypid)
        && ConstraintNameIsUsed(mcx, ConstraintCategory::Domain, conform.contypid, newname)?
    {
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{}\" for domain {} already exists",
                newname,
                format_type_seams::format_type_be_str::call(conform.contypid)?
            ),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /* OK, do the rename --- namestrcpy(&(con->conname), newname); */
    let new_conname = namestrcpy(newname);
    let fields = ConstraintFieldUpdate {
        conname: new_conname,
        connamespace: conform.connamespace,
        conislocal: conform.conislocal,
        coninhcount: conform.coninhcount,
        conparentid: conform.conparentid,
    };
    indexing_seams::catalog_tuple_update_pg_constraint::call(&conDesc, con.tid, &fields)?;

    objectaccess_seams::invoke_object_post_alter_hook::call(CONSTRAINT_RELATION_ID, conId, 0)?;

    conDesc.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * AlterConstraintNamespaces (pg_constraint.c:1054-1112)
 * ========================================================================= */

/// AlterConstraintNamespaces — move constraints belonging to the specified
/// object to a new namespace. `objsMoved` is the runtime `ObjectAddresses *`.
pub fn AlterConstraintNamespaces(
    ownerId: Oid,
    oldNspId: Oid,
    newNspId: Oid,
    isType: bool,
    objsMoved: &mut types_catalog::catalog_dependency::ObjectAddresses,
) -> PgResult<()> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conRel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let conrelid = if isType { InvalidOid } else { ownerId };
    let contypid = if isType { ownerId } else { InvalidOid };

    let key = [
        oid_key(Anum_pg_constraint_conrelid, conrelid)?,
        oid_key(Anum_pg_constraint_contypid, contypid)?,
    ];

    systable_scan_foreach(&conRel, ConstraintRelidTypidNameIndexId, &key, |row| {
        let conform = &row.form;
        let thisobj = ObjectAddress {
            classId: CONSTRAINT_RELATION_ID,
            objectId: conform.oid,
            objectSubId: 0,
        };

        if dependency_seams::object_address_present::call(thisobj, &*objsMoved)? {
            return Ok(true);
        }

        /* Don't update if the object is already part of the namespace */
        if conform.connamespace == oldNspId && oldNspId != newNspId {
            let fields = ConstraintFieldUpdate {
                conname: conform.conname,
                connamespace: newNspId,
                conislocal: conform.conislocal,
                coninhcount: conform.coninhcount,
                conparentid: conform.conparentid,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(&conRel, row.tid, &fields)?;

            /*
             * Note: currently, the constraint will not have its own dependency on
             * the namespace, so we don't need to do changeDependencyFor().
             */
        }

        objectaccess_seams::invoke_object_post_alter_hook::call(
            CONSTRAINT_RELATION_ID,
            thisobj.objectId,
            0,
        )?;

        dependency_seams::add_exact_object_address::call(thisobj, objsMoved)?;
        Ok(true)
    })?;

    conRel.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * ConstraintSetParentConstraint (pg_constraint.c:1123-1189)
 * ========================================================================= */

/// ConstraintSetParentConstraint — set a partition's constraint as child of its
/// parent constraint, or remove the linkage if `parentConstrId` is `InvalidOid`.
pub fn ConstraintSetParentConstraint(
    mcx: Mcx<'_>,
    childConstrId: Oid,
    parentConstrId: Oid,
    childTableId: Oid,
) -> PgResult<()> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let constrRel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let tuple = syscache_seams::search_constraint_form_by_oid::call(childConstrId)?;
    let tuple = match tuple {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for constraint {childConstrId}"
            )));
        }
    };
    let tid = tuple.tid;
    let mut constrForm = tuple.form;

    if OidIsValid(parentConstrId) {
        /* don't allow setting parent for a constraint that already has one */
        debug_assert!(constrForm.coninhcount == 0);
        if constrForm.conparentid != InvalidOid {
            return Err(PgError::error(format!(
                "constraint {childConstrId} already has a parent constraint"
            )));
        }

        constrForm.conislocal = false;
        let mut newcount = constrForm.coninhcount;
        if pg_add_s16_overflow(constrForm.coninhcount, 1, &mut newcount) {
            return Err(PgError::new(ERROR, "too many inheritance parents".to_string())
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        constrForm.coninhcount = newcount;

        constrForm.conparentid = parentConstrId;

        let fields = ConstraintFieldUpdate {
            conname: constrForm.conname,
            connamespace: constrForm.connamespace,
            conislocal: constrForm.conislocal,
            coninhcount: constrForm.coninhcount,
            conparentid: constrForm.conparentid,
        };
        indexing_seams::catalog_tuple_update_pg_constraint::call(&constrRel, tid, &fields)?;

        let depender = ObjectAddress {
            classId: CONSTRAINT_RELATION_ID,
            objectId: childConstrId,
            objectSubId: 0,
        };

        let referenced = ObjectAddress {
            classId: CONSTRAINT_RELATION_ID,
            objectId: parentConstrId,
            objectSubId: 0,
        };
        pg_depend_seams::recordDependencyOn::call(mcx, &depender, &referenced, DEPENDENCY_PARTITION_PRI)?;

        let referenced = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: childTableId,
            objectSubId: 0,
        };
        pg_depend_seams::recordDependencyOn::call(mcx, &depender, &referenced, DEPENDENCY_PARTITION_SEC)?;
    } else {
        constrForm.coninhcount -= 1;
        constrForm.conislocal = true;
        constrForm.conparentid = InvalidOid;

        /* Make sure there's no further inheritance. */
        debug_assert!(constrForm.coninhcount == 0);

        let fields = ConstraintFieldUpdate {
            conname: constrForm.conname,
            connamespace: constrForm.connamespace,
            conislocal: constrForm.conislocal,
            coninhcount: constrForm.coninhcount,
            conparentid: constrForm.conparentid,
        };
        indexing_seams::catalog_tuple_update_pg_constraint::call(&constrRel, tid, &fields)?;

        pg_depend_seams::deleteDependencyRecordsForClass::call(
            CONSTRAINT_RELATION_ID,
            childConstrId,
            CONSTRAINT_RELATION_ID,
            DEPENDENCY_PARTITION_PRI.0,
        )?;
        pg_depend_seams::deleteDependencyRecordsForClass::call(
            CONSTRAINT_RELATION_ID,
            childConstrId,
            RELATION_RELATION_ID,
            DEPENDENCY_PARTITION_SEC.0,
        )?;
    }

    constrRel.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * get_relation_constraint_oid (pg_constraint.c:1197-1240)
 * ========================================================================= */

/// get_relation_constraint_oid — find a constraint on a relation by name.
pub fn get_relation_constraint_oid(
    mcx: Mcx<'_>,
    relid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut conOid = InvalidOid;

    let con_ctx = MemoryContext::new("pg_constraint");
    let pg_constraint = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, relid)?,
        oid_key(Anum_pg_constraint_contypid, InvalidOid)?,
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
    ];

    /* There can be at most one matching row */
    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &skey, |row| {
        conOid = row.form.oid;
        Ok(false)
    })?;

    /* If no such constraint exists, complain */
    if !OidIsValid(conOid) && !missing_ok {
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{}\" for table \"{}\" does not exist",
                conname,
                rel_name_for_msg(mcx, relid)?
            ),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }

    pg_constraint.close(AccessShareLock)?;

    Ok(conOid)
}

/* ===========================================================================
 * get_relation_constraint_attnos (pg_constraint.c:1254-1333)
 * ========================================================================= */

/// get_relation_constraint_attnos — find a constraint by name and return its
/// constrained columns as a Bitmapset (offset by
/// `FirstLowInvalidHeapAttributeNumber`), plus the constraint OID.
pub fn get_relation_constraint_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<(Oid, Option<PgBox<'mcx, Bitmapset<'mcx>>>)> {
    let mut conattnos: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None; // NULL

    /* Set *constraintOid, to avoid complaints about uninitialized vars */
    let mut constraintOid = InvalidOid;

    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, relid)?,
        oid_key(Anum_pg_constraint_contypid, InvalidOid)?,
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
    ];

    let mut scan_err: Option<PgError> = None;
    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &skey, |row| {
        constraintOid = row.form.oid;

        /* Extract the conkey array, ie, attnums of constrained columns */
        let adatum = syscache_seams::heap_get_conkey::call(&pg_constraint, &row.htup)?;
        if let Some(arr) = adatum {
            let numcols = arr.dim0;
            if arr.ndim != 1 || numcols < 0 || arr.hasnull || arr.elemtype != INT2OID {
                scan_err = Some(PgError::error("conkey is not a 1-D smallint array"));
                return Ok(false);
            }
            let attnums = &arr.data;
            for i in 0..numcols {
                conattnos = Some(bms_add_member(
                    mcx,
                    conattnos.take(),
                    attnums[i as usize] as i32 - FirstLowInvalidHeapAttributeNumber as i32,
                )?);
            }
        }
        Ok(false)
    })?;
    if let Some(e) = scan_err {
        return Err(e);
    }

    /* If no such constraint exists, complain */
    if !OidIsValid(constraintOid) && !missing_ok {
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{}\" for table \"{}\" does not exist",
                conname,
                rel_name_for_msg(mcx, relid)?
            ),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }

    pg_constraint.close(AccessShareLock)?;

    Ok((constraintOid, conattnos))
}

/* ===========================================================================
 * get_relation_idx_constraint_oid (pg_constraint.c:1343-1383)
 * ========================================================================= */

/// Return the OID of the constraint enforced by the given index in the given
/// relation; or `InvalidOid`.
pub fn get_relation_idx_constraint_oid(relationId: Oid, indexId: Oid) -> PgResult<Oid> {
    let mut constraintId = InvalidOid;

    let con_ctx = MemoryContext::new("pg_constraint");
    let pg_constraint = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let key = [oid_key(Anum_pg_constraint_conrelid, relationId)?];

    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &key, |row| {
        let constr_form = &row.form;

        /* See above */
        if constr_form.contype != CONSTRAINT_PRIMARY
            && constr_form.contype != CONSTRAINT_UNIQUE
            && constr_form.contype != CONSTRAINT_EXCLUSION
        {
            return Ok(true);
        }

        if constr_form.conindid == indexId {
            constraintId = constr_form.oid;
            return Ok(false);
        }
        Ok(true)
    })?;

    pg_constraint.close(AccessShareLock)?;
    Ok(constraintId)
}

/* ===========================================================================
 * get_domain_constraint_oid (pg_constraint.c:1390-1433)
 * ========================================================================= */

/// get_domain_constraint_oid — find a constraint on a domain by name.
pub fn get_domain_constraint_oid(
    mcx: Mcx<'_>,
    typid: Oid,
    conname: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut conOid = InvalidOid;

    let con_ctx = MemoryContext::new("pg_constraint");
    let pg_constraint = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, InvalidOid)?,
        oid_key(Anum_pg_constraint_contypid, typid)?,
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
    ];

    /* There can be at most one matching row */
    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &skey, |row| {
        conOid = row.form.oid;
        Ok(false)
    })?;

    /* If no such constraint exists, complain */
    if !OidIsValid(conOid) && !missing_ok {
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{}\" for domain {} does not exist",
                conname,
                format_type_seams::format_type_be_str::call(typid)?
            ),
        )
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }

    pg_constraint.close(AccessShareLock)?;

    Ok(conOid)
}

/* ===========================================================================
 * get_primary_key_attnos (pg_constraint.c:1449-1526)
 * ========================================================================= */

/// get_primary_key_attnos — identify the columns in a relation's primary key.
/// Returns the Bitmapset (offset by `FirstLowInvalidHeapAttributeNumber`;
/// `None` if no PK or a deferrable PK with `deferrableOk = false`) plus the PK
/// constraint OID.
pub fn get_primary_key_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    deferrableOk: bool,
) -> PgResult<(Option<PgBox<'mcx, Bitmapset<'mcx>>>, Oid)> {
    let mut pkattnos: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None; // NULL

    /* Set *constraintOid, to avoid complaints about uninitialized vars */
    let mut constraintOid = InvalidOid;

    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [oid_key(Anum_pg_constraint_conrelid, relid)?];

    let mut scan_err: Option<PgError> = None;
    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &skey, |row| {
        let con = &row.form;

        /* Skip constraints that are not PRIMARY KEYs */
        if con.contype != CONSTRAINT_PRIMARY {
            return Ok(true);
        }

        /*
         * If the primary key is deferrable, but we've been instructed to ignore
         * deferrable constraints, then we might as well give up searching, since
         * there can only be a single primary key on a table.
         */
        if con.condeferrable && !deferrableOk {
            return Ok(false);
        }

        /* Extract the conkey array, ie, attnums of PK's columns */
        let adatum = syscache_seams::heap_get_conkey::call(&pg_constraint, &row.htup)?;
        let arr = match adatum {
            Some(a) => a,
            None => {
                scan_err = Some(PgError::error(format!(
                    "null conkey for constraint {}",
                    con.oid
                )));
                return Ok(false);
            }
        };
        let numkeys = arr.dim0;
        if arr.ndim != 1 || numkeys < 0 || arr.hasnull || arr.elemtype != INT2OID {
            scan_err = Some(PgError::error("conkey is not a 1-D smallint array"));
            return Ok(false);
        }
        let attnums = &arr.data;

        for i in 0..numkeys {
            pkattnos = Some(bms_add_member(
                mcx,
                pkattnos.take(),
                attnums[i as usize] as i32 - FirstLowInvalidHeapAttributeNumber as i32,
            )?);
        }
        constraintOid = con.oid;

        /* No need to search further */
        Ok(false)
    })?;
    if let Some(e) = scan_err {
        return Err(e);
    }

    pg_constraint.close(AccessShareLock)?;

    Ok((pkattnos, constraintOid))
}

/* ===========================================================================
 * DeconstructFkConstraintRow (pg_constraint.c:1535-1650)
 * ========================================================================= */

/// The data `DeconstructFkConstraintRow` extracts from a FK `pg_constraint`
/// tuple (the C output parameters, returned as a struct). `pf_eq_oprs` /
/// `pp_eq_oprs` / `ff_eq_oprs` / `fk_del_set_cols` are `None` when the C caller
/// passed NULL (`want_*` false).
#[derive(Clone, Debug, Default)]
pub struct FkArrays {
    pub numfks: i32,
    pub conkey: Vec<AttrNumber>,
    pub confkey: Vec<AttrNumber>,
    pub pf_eq_oprs: Option<Vec<Oid>>,
    pub pp_eq_oprs: Option<Vec<Oid>>,
    pub ff_eq_oprs: Option<Vec<Oid>>,
    pub num_fk_del_set_cols: i32,
    pub fk_del_set_cols: Option<Vec<AttrNumber>>,
}

/// Extract data from the `pg_constraint` tuple of a foreign-key constraint.
/// `want_pf` / `want_pp` / `want_ff` / `want_del_set_cols` select which optional
/// output groups to fill (the C passes a NULL pointer to skip a group).
pub fn DeconstructFkConstraintRow(
    mcx: Mcx<'_>,
    tuple: &FormedTuple<'_>,
    want_pf: bool,
    want_pp: bool,
    want_ff: bool,
    want_del_set_cols: bool,
) -> PgResult<FkArrays> {
    let mut out = FkArrays::default();

    /*
     * We expect the arrays to be 1-D arrays of the right types; verify that.
     */
    let arrays = syscache_seams::deconstruct_fk_arrays::call(tuple)?;

    // conkey
    let arr: &ConKeyArray = &arrays.conkey;
    if arr.ndim != 1 || arr.hasnull || arr.elemtype != INT2OID {
        return Err(PgError::error("conkey is not a 1-D smallint array"));
    }
    let numkeys = arr.dim0;
    if numkeys <= 0 || numkeys > INDEX_MAX_KEYS {
        return Err(PgError::error(format!(
            "foreign key constraint cannot have {numkeys} columns"
        )));
    }
    out.conkey = take_checked(mcx, &arr.data, numkeys, "conkey")?;

    // confkey
    let arr: &ConKeyArray = &arrays.confkey;
    if arr.ndim != 1 || arr.dim0 != numkeys || arr.hasnull || arr.elemtype != INT2OID {
        return Err(PgError::error("confkey is not a 1-D smallint array"));
    }
    out.confkey = take_checked(mcx, &arr.data, numkeys, "confkey")?;

    if want_pf {
        let arr: &OidArray = &arrays.conpfeqop;
        /* see TryReuseForeignKey if you change the test below */
        if arr.ndim != 1 || arr.dim0 != numkeys || arr.hasnull || arr.elemtype != OIDOID {
            return Err(PgError::error("conpfeqop is not a 1-D Oid array"));
        }
        out.pf_eq_oprs = Some(take_checked(mcx, &arr.data, numkeys, "conpfeqop")?);
    }

    if want_pp {
        let arr: &OidArray = &arrays.conppeqop;
        if arr.ndim != 1 || arr.dim0 != numkeys || arr.hasnull || arr.elemtype != OIDOID {
            return Err(PgError::error("conppeqop is not a 1-D Oid array"));
        }
        out.pp_eq_oprs = Some(take_checked(mcx, &arr.data, numkeys, "conppeqop")?);
    }

    if want_ff {
        let arr: &OidArray = &arrays.conffeqop;
        if arr.ndim != 1 || arr.dim0 != numkeys || arr.hasnull || arr.elemtype != OIDOID {
            return Err(PgError::error("conffeqop is not a 1-D Oid array"));
        }
        out.ff_eq_oprs = Some(take_checked(mcx, &arr.data, numkeys, "conffeqop")?);
    }

    if want_del_set_cols {
        match &arrays.confdelsetcols {
            None => {
                out.num_fk_del_set_cols = 0;
                out.fk_del_set_cols = Some(Vec::new());
            }
            Some(arr) => {
                if arr.ndim != 1 || arr.hasnull || arr.elemtype != INT2OID {
                    return Err(PgError::error("confdelsetcols is not a 1-D smallint array"));
                }
                let num_delete_cols = arr.dim0;
                out.fk_del_set_cols =
                    Some(take_checked(mcx, &arr.data, num_delete_cols, "confdelsetcols")?);
                out.num_fk_del_set_cols = num_delete_cols;
            }
        }
    }

    out.numfks = numkeys;

    Ok(out)
}

/* ===========================================================================
 * FindFKPeriodOpers (pg_constraint.c:1665-1722)
 * ========================================================================= */

/// The operator OIDs `FindFKPeriodOpers` looks up for the PERIOD part of a
/// temporal foreign key.
#[derive(Clone, Copy, Debug, Default)]
pub struct FkPeriodOpers {
    pub containedbyoperoid: Oid,
    pub aggedcontainedbyoperoid: Oid,
    pub intersectoperoid: Oid,
}

/// FindFKPeriodOpers — look up the operator oids used for the PERIOD part of a
/// temporal foreign key. `opclass` is the opclass of that PERIOD element.
pub fn FindFKPeriodOpers(opclass: Oid) -> PgResult<FkPeriodOpers> {
    let opcintype: Oid;
    let mut out = FkPeriodOpers::default();

    /* Make sure we have a range or multirange. */
    if let Some((_opfamily, oct)) =
        lsyscache_seams::get_opclass_opfamily_and_input_type::call(opclass)?
    {
        opcintype = oct;
        if opcintype != ANYRANGEOID && opcintype != ANYMULTIRANGEOID {
            return Err(PgError::new(
                ERROR,
                "invalid type for PERIOD part of foreign key".to_string(),
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail("Only range and multirange are supported."));
        }
    } else {
        return Err(PgError::error(format!(
            "cache lookup failed for opclass {opclass}"
        )));
    }

    /*
     * Look up the ContainedBy operator whose lhs and rhs are the opclass's type.
     */
    let (cb, _strat) = indexcmds_seams::get_operator_from_compare_type::call(
        opclass,
        InvalidOid,
        COMPARE_CONTAINED_BY,
    )?;
    out.containedbyoperoid = cb;

    /*
     * Now look up the ContainedBy operator. Its right arg must be a multirange.
     */
    let (acb, _strat) = indexcmds_seams::get_operator_from_compare_type::call(
        opclass,
        ANYMULTIRANGEOID,
        COMPARE_CONTAINED_BY,
    )?;
    out.aggedcontainedbyoperoid = acb;

    if opcintype == ANYRANGEOID {
        out.intersectoperoid = OID_RANGE_INTERSECT_RANGE_OP;
    } else if opcintype == ANYMULTIRANGEOID {
        out.intersectoperoid = OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP;
    } else {
        return Err(PgError::error(format!("unexpected opcintype: {opcintype}")));
    }

    Ok(out)
}

/* ===========================================================================
 * check_functional_grouping (pg_constraint.c:1739-1776)
 * ========================================================================= */

/// check_functional_grouping — determine whether a relation can be proven
/// functionally dependent on a set of grouping columns. Returns `(proven,
/// constraintDeps)` (the `*constraintDeps` accumulator).
pub fn check_functional_grouping(
    mcx: Mcx<'_>,
    relid: Oid,
    varno: u32,
    varlevelsup: u32,
    grouping_columns: &[Node<'_>],
    mut constraintDeps: Vec<Oid>,
) -> PgResult<(bool, Vec<Oid>)> {
    /* If the rel has no PK, then we can't prove functional dependency */
    let (pkattnos, constraintOid) = get_primary_key_attnos(mcx, relid, false)?;
    if pkattnos.is_none() {
        return Ok((false, constraintDeps));
    }

    /* Identify all the rel's columns that appear in grouping_columns */
    let mut groupbyattnos: Option<PgBox<Bitmapset>> = None;
    for gvar in grouping_columns {
        if let Node::Expr(Expr::Var(gvar)) = gvar {
            if gvar.varno == varno as i32 && gvar.varlevelsup == varlevelsup {
                groupbyattnos = Some(bms_add_member(
                    mcx,
                    groupbyattnos.take(),
                    gvar.varattno as i32 - FirstLowInvalidHeapAttributeNumber as i32,
                )?);
            }
        }
    }

    if bms_is_subset(pkattnos.as_deref(), groupbyattnos.as_deref()) {
        /* The PK is a subset of grouping_columns, so we win */
        constraintDeps
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        constraintDeps.push(constraintOid);
        return Ok((true, constraintDeps));
    }

    Ok((false, constraintDeps))
}

/* ===========================================================================
 * Inward seam bodies (the 5 this crate owns + installs) + init_seams
 * ========================================================================= */

use types_ri_triggers::{FkConstraintRow, PeriodOpers};

/// `find_fk_period_opers(conindid, nkeys)` — resolve the PERIOD opers from the
/// supporting unique index's last-column opclass.
fn find_fk_period_opers(conindid: Oid, nkeys: i32) -> PgResult<PeriodOpers> {
    let opclass = lsyscache_seams::get_index_column_opclass::call(conindid, nkeys)?;
    let o = FindFKPeriodOpers(opclass)?;
    Ok(PeriodOpers {
        period_contained_by_oper: o.containedbyoperoid,
        agged_period_contained_by_oper: o.aggedcontainedbyoperoid,
        period_intersect_oper: o.intersectoperoid,
    })
}

/// `constraint_hash_value(oid)` — `GetSysCacheHashValue1(CONSTROID, oid)`.
fn constraint_hash_value(oid: Oid) -> PgResult<u32> {
    syscache_seams::get_syscache_hash_value_constroid::call(oid)
}

/// `constraint_type_oids(constroid)` — the `(conrelid, contypid, oid)`
/// projection objectaddress.c's `getConstraintTypeDescription` /
/// `getConstraintIdentity` read off the `pg_constraint` row to disambiguate a
/// table constraint from a domain constraint. The C reads it via
/// `get_catalog_object_by_oid(pg_constraint, ...)`; here the equivalent
/// `Form_pg_constraint` projection rides on [`search_constraint_form_by_oid`]
/// (same row, same fields). `Ok(None)` when no such constraint row exists.
fn constraint_type_oids(constroid: Oid) -> PgResult<Option<(Oid, Oid, Oid)>> {
    match syscache_seams::search_constraint_form_by_oid::call(constroid)? {
        Some(c) => Ok(Some((c.form.conrelid, c.form.contypid, c.form.oid))),
        None => Ok(None),
    }
}

/// `get_ri_constraint_root(constr_oid)` — walk `conparentid` to the root.
fn get_ri_constraint_root(constr_oid: Oid) -> PgResult<Oid> {
    let mut constr_oid = constr_oid;
    loop {
        let form = syscache_seams::search_constraint_form_by_oid::call(constr_oid)?;
        let form = match form {
            Some(f) => f.form,
            None => {
                return Err(PgError::error(format!(
                    "cache lookup failed for constraint {constr_oid}"
                )));
            }
        };
        if !OidIsValid(form.conparentid) {
            return Ok(constr_oid);
        }
        constr_oid = form.conparentid;
    }
}

/// `load_fk_constraint(mcx, constraint_oid)` — `SearchSysCache1(CONSTROID)` +
/// the FK-row projection `ri_LoadConstraintInfo` performs (lines 2294-2331).
fn load_fk_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    constraint_oid: Oid,
) -> PgResult<Option<FkConstraintRow<'mcx>>> {
    let tuple = syscache_seams::search_constraint_tuple_by_oid::call(mcx, constraint_oid)?;
    let Some(tuple) = tuple else {
        return Ok(None);
    };

    let form = syscache_seams::search_constraint_form_by_oid::call(constraint_oid)?;
    let form = match form {
        Some(f) => f.form,
        None => return Ok(None),
    };

    if form.contype != CONSTRAINT_FOREIGN {
        return Err(PgError::error(format!(
            "constraint {constraint_oid} is not a foreign key constraint"
        )));
    }

    let oid_hash_value = syscache_seams::get_syscache_hash_value_constroid::call(constraint_oid)?;

    let fk = DeconstructFkConstraintRow(mcx, &tuple, true, true, true, true)?;

    /* conname bytes copied into mcx */
    let conname_bytes = form.conname_str().as_bytes();
    let mut conname: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, conname_bytes.len())?;
    for &b in conname_bytes {
        conname.push(b);
    }

    let to_pgvec_i16 = |mcx: Mcx<'mcx>, v: &[i16]| -> PgResult<PgVec<'mcx, i16>> {
        let mut out: PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, v.len())?;
        for &x in v {
            out.push(x);
        }
        Ok(out)
    };
    let to_pgvec_oid = |mcx: Mcx<'mcx>, v: &[Oid]| -> PgResult<PgVec<'mcx, Oid>> {
        let mut out: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, v.len())?;
        for &x in v {
            out.push(x);
        }
        Ok(out)
    };

    let row = FkConstraintRow {
        conname,
        pk_relid: form.confrelid,
        fk_relid: form.conrelid,
        confupdtype: form.confupdtype,
        confdeltype: form.confdeltype,
        confmatchtype: form.confmatchtype,
        hasperiod: form.conperiod,
        conparentid: form.conparentid,
        conindid: form.conindid,
        oid_hash_value,
        nkeys: fk.numfks,
        fk_attnums: to_pgvec_i16(mcx, &fk.conkey)?,
        pk_attnums: to_pgvec_i16(mcx, &fk.confkey)?,
        pf_eq_oprs: to_pgvec_oid(mcx, &fk.pf_eq_oprs.unwrap_or_default())?,
        pp_eq_oprs: to_pgvec_oid(mcx, &fk.pp_eq_oprs.unwrap_or_default())?,
        ff_eq_oprs: to_pgvec_oid(mcx, &fk.ff_eq_oprs.unwrap_or_default())?,
        ndelsetcols: fk.num_fk_del_set_cols,
        confdelsetcols: to_pgvec_i16(mcx, &fk.fk_del_set_cols.unwrap_or_default())?,
    };

    Ok(Some(row))
}

/// `register_constraint_inval_callback()` — register ri_triggers'
/// `InvalidateConstraintCacheCallBack` for `CONSTROID`.
fn register_constraint_inval_callback() -> PgResult<()> {
    backend_utils_cache_inval_pc_seams::register_syscache_callback::call(
        types_syscache::syscache_ids::CONSTROID,
        constraint_inval_callback_adapter,
    )
}

/// Adapter exposing ri_triggers' value-seam `invalidate_constraint_cache_callback`
/// as a `SyscacheCallbackFn` (`fn(cacheid, hashvalue)`). The C callback ignores
/// the cacheid and takes only the hashvalue.
fn constraint_inval_callback_adapter(_cacheid: i32, hashvalue: u32) {
    backend_utils_adt_ri_triggers_seams::invalidate_constraint_cache_callback::call(hashvalue)
}

/// Install this crate's implementations into `backend-catalog-pg-constraint-seams`.
pub fn init_seams() {
    use backend_catalog_pg_constraint_seams as seams;

    seams::register_constraint_inval_callback::set(register_constraint_inval_callback);
    seams::load_fk_constraint::set(load_fk_constraint);
    seams::constraint_hash_value::set(constraint_hash_value);
    seams::get_ri_constraint_root::set(get_ri_constraint_root);
    seams::find_fk_period_opers::set(find_fk_period_opers);
    seams::constraint_type_oids::set(constraint_type_oids);
    seams::get_relation_constraint_oid::set(get_relation_constraint_oid);
    seams::get_domain_constraint_oid::set(get_domain_constraint_oid);
    seams::RemoveConstraintById::set(RemoveConstraintById);
}

/* ===========================================================================
 * tuple/form helpers for the SearchSysCache form-search projection
 * ========================================================================= */

/// `OIDOID` (`pg_type.h`).
const OIDOID: Oid = 26;
/// `ANYRANGEOID` / `ANYMULTIRANGEOID` (`pg_type.h`).
const ANYRANGEOID: Oid = 3831;
const ANYMULTIRANGEOID: Oid = 4537;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namestrcpy_roundtrip() {
        let n = namestrcpy("pkey");
        assert_eq!(&n[..4], b"pkey");
        assert_eq!(n[4], 0);
        assert_eq!(n[NAMEDATALEN - 1], 0);
        assert_eq!(name_str(&n), "pkey");
    }

    #[test]
    fn namestrcpy_empty() {
        let n = namestrcpy("");
        assert_eq!(name_str(&n), "");
        assert!(n.iter().all(|&b| b == 0));
    }

    #[test]
    fn namestrcpy_truncates_and_terminates() {
        let long = "a".repeat(100);
        let n = namestrcpy(&long);
        assert_eq!(n[NAMEDATALEN - 1], 0);
        assert_eq!(name_str(&n).len(), NAMEDATALEN - 1);
        assert!(name_str(&n).chars().all(|c| c == 'a'));
    }

    #[test]
    fn truncate_namedatalen_caps_length() {
        assert_eq!(truncate_namedatalen("short"), "short");
        let long = "x".repeat(200);
        assert_eq!(truncate_namedatalen(&long).len(), NAMEDATALEN - 1);
    }

    #[test]
    fn truncate_namedatalen_char_boundary() {
        let s = "é".repeat(32);
        let out = truncate_namedatalen(&s);
        assert!(out.len() <= NAMEDATALEN - 1);
        assert!(out.chars().all(|c| c == 'é'));
    }

    #[test]
    fn max_helper() {
        assert_eq!(Max(3, 7), 7);
        assert_eq!(Max(7, 3), 7);
        assert_eq!(Max(-1, 0), 0);
    }

    #[test]
    fn add_s16_overflow() {
        let mut out = 0i16;
        assert!(!pg_add_s16_overflow(10, 5, &mut out));
        assert_eq!(out, 15);
        assert!(pg_add_s16_overflow(i16::MAX, 1, &mut out));
    }

    #[test]
    fn constants_match_postgres() {
        assert_eq!(CONSTRAINT_RELATION_ID, 2606);
        assert_eq!(ConstraintRelidTypidNameIndexId, 2665);
        assert_eq!(ConstraintNameNspIndexId, 2664);
        assert_eq!(CONSTRAINT_CHECK, b'c' as i8);
        assert_eq!(CONSTRAINT_FOREIGN, b'f' as i8);
        assert_eq!(CONSTRAINT_NOTNULL, b'n' as i8);
        assert_eq!(CONSTRAINT_PRIMARY, b'p' as i8);
        assert_eq!(CONSTRAINT_UNIQUE, b'u' as i8);
        assert_eq!(CONSTRAINT_EXCLUSION, b'x' as i8);
        assert_eq!(F_NAMEEQ, 62);
        assert_eq!(types_catalog::pg_constraint::Natts_pg_constraint, 28);
        assert_eq!(OID_RANGE_INTERSECT_RANGE_OP, 3900);
        assert_eq!(OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP, 4394);
    }
}
