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
    Anum_pg_constraint_conbin, Anum_pg_constraint_conperiod, Anum_pg_constraint_conrelid,
    Anum_pg_constraint_contype,
    Anum_pg_constraint_contypid, Anum_pg_constraint_convalidated, Anum_pg_constraint_oid,
    ConKeyArray, ConstraintCategory, ConstraintFieldUpdate, FormData_pg_constraint, OidArray,
    PgConstraintInsertRow, ConstraintNameNspIndexId, ConstraintOidIndexId,
    ConstraintRelidTypidNameIndexId,
    ConstraintTypidIndexId,
    CONSTRAINT_CHECK, CONSTRAINT_EXCLUSION, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
    CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE, OID_MULTIRANGE_INTERSECT_MULTIRANGE_OP,
    OID_RANGE_INTERSECT_RANGE_OP,
};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, INDEX_MAX_KEYS};
use types_core::primitive::{AttrNumber, InvalidAttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR, NOTICE,
};
use types_error::pg_error::ErrorLocation;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::parsenodes::DropBehavior;
use types_nodes::nodes::{Node, NodePtr};
use types_rel::RelationData;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber, ItemPointerData, INT2OID,
};

use backend_access_common_heaptuple::heap_deform_tuple;
use types_cache::typcache::DomainCheckConstraintRow;
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
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_utils_adt_ruleutils_seams as ruleutils_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;

/// `NAMEDATALEN`.
const NAMEDATALEN: usize = 64;

use backend_nodes_equalfuncs_seams as equalfuncs_seams;
use backend_nodes_read_seams as read_seams;
use backend_utils_error::ereport;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module (the C source
/// is `src/backend/catalog/heap.c` for `MergeWithExistingConstraint`).
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/catalog/heap.c", 0, funcname)
}

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

/// The `pg_constraint` scan of `ATPrepChangePersistence` (tablecmds.c:18871-18927):
/// to preserve the invariant that permanent tables cannot reference unlogged
/// ones (and vice-versa), find this relation's foreign-key constraints and
/// return, for each, the *opposite* endpoint relation OID plus the constraint
/// name. When changing to LOGGED we scan `conrelid == relid` (this rel is the
/// referencing table) and report `confrelid`; otherwise we scan
/// `confrelid == relid` (this rel is referenced) and report `conrelid`.
/// Self-referencing FKs (`foreignrelid == relid`) are skipped. The caller opens
/// each foreign rel and checks `RelationIsPermanent` to raise the right error.
///
/// Lives here because the catalog scan + `Form_pg_constraint` deform substrate
/// is owned by this crate (the C `systable_beginscan` on `pg_constraint`).
pub fn fk_constraints_for_persistence_check<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    to_logged: bool,
) -> PgResult<Vec<(Oid, String)>> {
    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;

    // Scan conrelid if changing to permanent, else confrelid. The conrelid
    // scan can use ConstraintRelidTypidNameIndexId; the confrelid scan has no
    // useful index (InvalidOid → heap scan + key filter), matching the C.
    let (scan_attno, index_id) = if to_logged {
        (Anum_pg_constraint_conrelid, ConstraintRelidTypidNameIndexId)
    } else {
        (Anum_pg_constraint_confrelid, InvalidOid)
    };
    let key = [oid_key(scan_attno, relid)?];

    let mut result: Vec<(Oid, String)> = Vec::new();
    systable_scan_foreach(&pg_constraint, index_id, &key, |row| {
        if row.form.contype == CONSTRAINT_FOREIGN {
            // The opposite end of what we used as scankey.
            let foreignrelid = if to_logged {
                row.form.confrelid
            } else {
                row.form.conrelid
            };
            // Ignore if self-referencing.
            if foreignrelid != relid {
                result.push((foreignrelid, row.form.conname_str().to_string()));
            }
        }
        Ok(true)
    })?;

    pg_constraint.close(AccessShareLock)?;

    Ok(result)
}

/* ===========================================================================
 * disinherit_constraints — the pg_constraint half of RemoveInheritance
 * (tablecmds.c:18025-18138)
 * ========================================================================= */

/// The constraint-disinheriting leg of `RemoveInheritance` (tablecmds.c:18025).
///
/// Find the parent's inheritable CHECK / NOT NULL constraints, match them to the
/// child (CHECK by name, NOT NULL by column number mapped through `attmap`), and
/// decrement each matched child constraint's `coninhcount` (flipping
/// `conislocal` to true when it reaches zero). Errors if a matched child
/// constraint is non-inherited, or if any parent constraint goes unmatched.
///
/// Lives here (not in tablecmds) because the `Form_pg_constraint` deform
/// substrate and the catalog scan machinery are owned by this crate, mirroring
/// `verifyNotNullPKCompatible` / `merge_constraints_into_existing`.
///
/// `attmap.attnums[parent_attno - 1]` maps a parent attribute number to the
/// child's.
pub fn disinherit_constraints(
    mcx: Mcx<'_>,
    child_rel: &RelationData<'_>,
    parent_rel: &RelationData<'_>,
    attmap: &types_tuple::attmap::AttrMap<'_>,
) -> PgResult<()> {
    let child_relid = child_rel.rd_id;
    let parent_relid = parent_rel.rd_id;

    let catalog = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    // First scan: collect the parent's inheritable CHECK names + NOT NULL
    // columns (mapped to the child's attnos).
    let mut connames: Vec<String> = Vec::new();
    let mut nncolumns: Vec<AttrNumber> = Vec::new();

    let parent_key = [oid_key(Anum_pg_constraint_conrelid, parent_relid)?];
    systable_scan_foreach(&catalog, ConstraintRelidTypidNameIndexId, &parent_key, |row| {
        if row.form.connoinherit {
            return Ok(true);
        }
        if row.form.contype == CONSTRAINT_CHECK {
            connames.push(name_str(&row.form.conname).to_string());
        }
        if row.form.contype == CONSTRAINT_NOTNULL {
            let parent_attno = extractNotNullColumn(&row.htup)?;
            // nncolumns = lappend_int(nncolumns, attmap->attnums[parent_attno - 1]);
            let mapped = attmap.attnums[(parent_attno - 1) as usize];
            nncolumns.push(mapped);
        }
        Ok(true)
    })?;

    // Second scan: the child's constraints; match and decrement.
    let child_key = [oid_key(Anum_pg_constraint_conrelid, child_relid)?];
    systable_scan_foreach(&catalog, ConstraintRelidTypidNameIndexId, &child_key, |row| {
        let mut matched = false;

        if row.form.contype == CONSTRAINT_CHECK {
            let chkname = name_str(&row.form.conname);
            if let Some(pos) = connames.iter().position(|c| c == chkname) {
                matched = true;
                connames.remove(pos);
            }
        } else if row.form.contype == CONSTRAINT_NOTNULL {
            let child_attno = extractNotNullColumn(&row.htup)?;
            if let Some(pos) = nncolumns.iter().position(|&c| c == child_attno) {
                matched = true;
                nncolumns.remove(pos);
            }
        } else {
            return Ok(true);
        }

        if matched {
            let mut con = row.form.clone();
            if con.coninhcount <= 0 {
                // shouldn't happen
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "relation {} has non-inherited constraint \"{}\"",
                        child_relid,
                        name_str(&con.conname)
                    ),
                ));
            }
            con.coninhcount -= 1;
            if con.coninhcount == 0 {
                con.conislocal = true;
            }
            let fields = ConstraintFieldUpdate {
                conname: con.conname,
                connamespace: con.connamespace,
                conislocal: con.conislocal,
                coninhcount: con.coninhcount,
                conparentid: con.conparentid,
                convalidated: con.convalidated,
                connoinherit: con.connoinherit,
                conenforced: con.conenforced,
                condeferrable: con.condeferrable,
                condeferred: con.condeferred,
                conindid: con.conindid,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(
                &catalog,
                row.tid,
                &fields,
            )?;
        }
        Ok(true)
    })?;

    // We should have matched all constraints.
    if !connames.is_empty() || !nncolumns.is_empty() {
        return Err(PgError::new(
            ERROR,
            format!(
                "{} unmatched constraints while removing inheritance from \"{}\" to \"{}\"",
                connames.len() + nncolumns.len(),
                name_str_relation(child_rel),
                name_str_relation(parent_rel)
            ),
        ));
    }

    catalog.close(RowExclusiveLock)?;
    Ok(())
}

/// Helper: the relation's name as an owned `String` (mirrors
/// `RelationGetRelationName`).
fn name_str_relation(rel: &RelationData<'_>) -> String {
    rel.rd_rel.relname.as_str().to_string()
}

/* ===========================================================================
 * verifyNotNullPKCompatible (tablecmds.c:9576)
 * ========================================================================= */

/// `verifyNotNullPKCompatible(tuple, colname)` (tablecmds.c:9576) — verify
/// whether the given not-null constraint tuple is compatible with a primary
/// key.  If not, an error is thrown.  Lives here (rather than in tablecmds)
/// because the `Form_pg_constraint` deform substrate is owned by this crate.
pub fn verifyNotNullPKCompatible(
    mcx: Mcx<'_>,
    tuple: &FormedTuple<'_>,
    colname: &str,
) -> PgResult<()> {
    /* conForm = (Form_pg_constraint) GETSTRUCT(tuple); */
    let con_form = syscache_seams::read_constraint_form::call(tuple)?;

    if con_form.contype != CONSTRAINT_NOTNULL {
        /* elog(ERROR, "constraint %u is not a not-null constraint", conForm->oid) */
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint {} is not a not-null constraint",
                con_form.oid
            ),
        ));
    }

    /* a NO INHERIT constraint is no good */
    if con_form.connoinherit {
        return Err(PgError::new(
            ERROR,
            format!("cannot create primary key on column \"{colname}\""),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_detail(format!(
            "The constraint \"{}\" on column \"{}\" of table \"{}\", marked {}, is incompatible with a primary key.",
            name_str(&con_form.conname),
            colname,
            rel_name_for_msg(mcx, con_form.conrelid)?,
            "NO INHERIT"
        ))
        .with_hint(
            "You might need to make the existing constraint inheritable using ALTER TABLE ... ALTER CONSTRAINT ... INHERIT.",
        ));
    }

    /* an unvalidated constraint is no good */
    if !con_form.convalidated {
        return Err(PgError::new(
            ERROR,
            format!("cannot create primary key on column \"{colname}\""),
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_detail(format!(
            "The constraint \"{}\" on column \"{}\" of table \"{}\", marked {}, is incompatible with a primary key.",
            name_str(&con_form.conname),
            colname,
            rel_name_for_msg(mcx, con_form.conrelid)?,
            "NOT VALID"
        ))
        .with_hint(
            "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT.",
        ));
    }

    Ok(())
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
                convalidated: conform.convalidated,
                connoinherit: conform.connoinherit,
                conenforced: conform.conenforced,
                condeferrable: conform.condeferrable,
                condeferred: conform.condeferred,
                conindid: conform.conindid,
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
 * MergeWithExistingConstraint (catalog/heap.c:2451-2611)
 * ========================================================================= */

/// `MergeWithExistingConstraint(rel, ccname, expr, ...)` (heap.c) — check for a
/// pre-existing CHECK constraint conflicting with a proposed new one of the same
/// name. Returns `true` if the new constraint was merged into an identical
/// existing one (a duplicate), `false` if no matching row exists. Raises on a
/// genuine conflict.
///
/// `expr` is the cooked (transformed) CHECK expression node; it is compared
/// against the stored `conbin` via `equal(expr, stringToNode(conbin))`.
/// `rel_name` / `relispartition` come from the target relation.
#[allow(clippy::too_many_arguments)]
pub fn MergeWithExistingConstraint(
    mcx: Mcx<'_>,
    relid: Oid,
    rel_name: &str,
    relispartition: bool,
    ccname: &str,
    expr: &Node<'_>,
    mut allow_merge: bool,
    is_local: bool,
    is_enforced: bool,
    is_initially_valid: bool,
    is_no_inherit: bool,
) -> PgResult<bool> {
    let con_ctx = MemoryContext::new("pg_constraint");
    /* conDesc = table_open(ConstraintRelationId, RowExclusiveLock); */
    let conDesc = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, relid)?,
        oid_key(Anum_pg_constraint_contypid, InvalidOid)?,
        name_key(mcx, Anum_pg_constraint_conname, ccname)?,
    ];

    /*
     * There can be at most one matching row. We re-implement the scan loop
     * (rather than `systable_scan_foreach`) so the deformed `conbin` Datum
     * survives long enough to detoast — the shared helper discards `values`.
     */
    let mut scan = genam_seams::systable_beginscan::call(
        &conDesc,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &skey,
    )?;
    let scratch = MemoryContext::new("pg_constraint scan row");
    let smcx = scratch.mcx();

    let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
        /* No matching row; nothing to merge. */
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Ok(false);
    };

    let cols = heap_deform_tuple(smcx, &tup.tuple, &conDesc.rd_att, &tup.data)?;
    let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
    for (value, _null) in cols.iter() {
        values.push(value.clone());
    }
    let mut con = form_pg_constraint(&values);
    let tid = tup.tuple.t_self;

    /* Found it.  Conflicts if not identical check constraint */
    let mut found = false;
    if con.contype == CONSTRAINT_CHECK {
        /* val = fastgetattr(tup, Anum_pg_constraint_conbin, ...); */
        let conbin_is_null = cols
            .get(Anum_pg_constraint_conbin as usize - 1)
            .map(|(_, n)| *n)
            .unwrap_or(true);
        if conbin_is_null {
            scan.end()?;
            conDesc.close(RowExclusiveLock)?;
            return Err(PgError::new(
                ERROR,
                format!("null conbin for rel {rel_name}"),
            ));
        }
        let conbin_datum = &values[Anum_pg_constraint_conbin as usize - 1];
        let conbin = varlena_seams::text_to_cstring_v::call(smcx, conbin_datum)?
            .as_str()
            .to_string();
        /* if (equal(expr, stringToNode(TextDatumGetCString(val)))) found = true; */
        let stored = read_seams::string_to_node::call(smcx, &conbin)?;
        if equalfuncs_seams::equal_node::call(expr, &stored) {
            found = true;
        }
    }

    /*
     * If the existing constraint is purely inherited (no local definition) then
     * interpret addition of a local constraint as a legal merge.  This allows
     * ALTER ADD CONSTRAINT on parent and child tables to be given in either
     * order with same end state.  However if the relation is a partition, all
     * inherited constraints are always non-local, including those that were
     * merged.
     */
    if is_local && !con.conislocal && !relispartition {
        allow_merge = true;
    }

    if !found || !allow_merge {
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Err(PgError::new(
            ERROR,
            format!("constraint \"{ccname}\" for relation \"{rel_name}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
    }

    /* If the child constraint is "no inherit" then cannot merge */
    if con.connoinherit {
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{ccname}\" conflicts with non-inherited constraint on relation \"{rel_name}\""
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
    }

    /*
     * Must not change an existing inherited constraint to "no inherit" status.
     * That's because inherited constraints should be able to propagate to
     * lower-level children.
     */
    if con.coninhcount > 0 && is_no_inherit {
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{ccname}\" conflicts with inherited constraint on relation \"{rel_name}\""
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
    }

    /*
     * If the child constraint is "not valid" then cannot merge with a valid
     * parent constraint.
     */
    if is_initially_valid && con.conenforced && !con.convalidated {
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{ccname}\" conflicts with NOT VALID constraint on relation \"{rel_name}\""
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
    }

    /*
     * A non-enforced child constraint cannot be merged with an enforced parent
     * constraint. However, the reverse is allowed, where the child constraint
     * is enforced.
     */
    if (!is_local && is_enforced && !con.conenforced)
        || (is_local && !is_enforced && con.conenforced)
    {
        scan.end()?;
        conDesc.close(RowExclusiveLock)?;
        return Err(PgError::new(
            ERROR,
            format!(
                "constraint \"{ccname}\" conflicts with NOT ENFORCED constraint on relation \"{rel_name}\""
            ),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION));
    }

    /* OK to update the tuple */
    ereport(NOTICE)
        .errmsg(format!(
            "merging constraint \"{ccname}\" with inherited definition"
        ))
        .finish(here("MergeWithExistingConstraint"))?;

    /*
     * In case of partitions, an inherited constraint must be inherited only once
     * since it cannot have multiple parents and it is never considered local.
     */
    if relispartition {
        con.coninhcount = 1;
        con.conislocal = false;
    } else if is_local {
        con.conislocal = true;
    } else {
        let mut newcount = con.coninhcount;
        if pg_add_s16_overflow(con.coninhcount, 1, &mut newcount) {
            scan.end()?;
            conDesc.close(RowExclusiveLock)?;
            return Err(
                PgError::new(ERROR, "too many inheritance parents".to_string())
                    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            );
        }
        con.coninhcount = newcount;
    }

    if is_no_inherit {
        debug_assert!(is_local);
        con.connoinherit = true;
    }

    /*
     * If the child constraint is required to be enforced while the parent
     * constraint is not, this should be allowed by marking the child constraint
     * as enforced. In the reverse case, an error would have already been thrown
     * before reaching this point.
     */
    if is_enforced && !con.conenforced {
        debug_assert!(is_local);
        con.conenforced = true;
        con.convalidated = true;
    }

    /* CatalogTupleUpdate(conDesc, &tup->t_self, tup); */
    let fields = ConstraintFieldUpdate {
        conname: con.conname,
        connamespace: con.connamespace,
        conislocal: con.conislocal,
        coninhcount: con.coninhcount,
        conparentid: con.conparentid,
        convalidated: con.convalidated,
        connoinherit: con.connoinherit,
        conenforced: con.conenforced,
        condeferrable: con.condeferrable,
        condeferred: con.condeferred,
        conindid: con.conindid,
    };
    indexing_seams::catalog_tuple_update_pg_constraint::call(&conDesc, tid, &fields)?;

    scan.end()?;
    conDesc.close(RowExclusiveLock)?;

    Ok(found)
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
                Node::mk_string(mcx, types_nodes::value::StringNode { sval: attname })?,
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

/// RemoveConstraintById's relchecks-decrement leg (pg_constraint.c:945-966):
/// `pgrel = table_open(RelationRelationId, RowExclusiveLock); relTup =
/// SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(conrelid)); classForm->relchecks--;
/// CatalogTupleUpdate(pgrel, &relTup->t_self, relTup); heap_freetuple(relTup);
/// table_close(pgrel, RowExclusiveLock)`. The `relchecks == 0` guard is the
/// caller's (it read the value via `fetch_relchecks` and raised the
/// "should not happen" error). This installs the `decrement_relchecks` seam
/// declared in syscache-seams; the catalog-write leaf is the
/// relchecks-preserving pg_class update seam (it `heap_modify_tuple`s only the
/// `relchecks` column over the held copy, preserving all 33 other columns).
fn decrement_relchecks(relid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("decrement_relchecks");
    let mcx = ctx.mcx();

    /* pgrel = table_open(RelationRelationId, RowExclusiveLock); */
    let pgrel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    /* relTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(conrelid)); */
    let relchecks = syscache_seams::fetch_relchecks::call(relid)?;
    let relchecks = match relchecks {
        Some(rc) => rc,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relid}"
            )));
        }
    };
    let class_tuple = syscache_seams::search_syscache_copy_pg_class_tuple::call(mcx, relid)?;
    let class_tuple = match class_tuple {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relid}"
            )));
        }
    };

    /* classForm->relchecks--; CatalogTupleUpdate(pgrel, &relTup->t_self, relTup); */
    indexing_seams::catalog_tuple_update_relchecks_pg_class::call(
        mcx,
        &pgrel,
        &class_tuple,
        relchecks - 1,
    )?;

    /* table_close(pgrel, RowExclusiveLock); */
    pgrel.close(RowExclusiveLock)?;

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
        convalidated: conform.convalidated,
        connoinherit: conform.connoinherit,
        conenforced: conform.conenforced,
        condeferrable: conform.condeferrable,
        condeferred: conform.condeferred,
        conindid: conform.conindid,
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
                convalidated: conform.convalidated,
                connoinherit: conform.connoinherit,
                conenforced: conform.conenforced,
                condeferrable: conform.condeferrable,
                condeferred: conform.condeferred,
                conindid: conform.conindid,
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
            convalidated: constrForm.convalidated,
            connoinherit: constrForm.connoinherit,
            conenforced: constrForm.conenforced,
            condeferrable: constrForm.condeferrable,
            condeferred: constrForm.condeferred,
            conindid: constrForm.conindid,
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
            convalidated: constrForm.convalidated,
            connoinherit: constrForm.connoinherit,
            conenforced: constrForm.conenforced,
            condeferrable: constrForm.condeferrable,
            condeferred: constrForm.condeferred,
            conindid: constrForm.conindid,
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
        if let Some(gvar) = gvar.as_var() {
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

/// `find_fk_period_opers(opclass)` — resolve the PERIOD opers from the PERIOD
/// element's `opclass` (matching the C `FindFKPeriodOpers` signature).
fn find_fk_period_opers(opclass: Oid) -> PgResult<PeriodOpers> {
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
/// `findDomainNotNullConstraint(typid)` reduced to the OID the typecmds caller
/// reads (`((Form_pg_constraint) GETSTRUCT(conTup))->oid`): the domain's
/// validated NOT NULL constraint OID, or `InvalidOid`. Consumed by
/// `AlterDomainNotNull` (typecmds.c).
fn find_domain_not_null_constraint_oid(mcx: Mcx<'_>, typid: Oid) -> PgResult<Oid> {
    let mut conoid = InvalidOid;

    let con_ctx = MemoryContext::new("pg_constraint");
    let pg_constraint = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let key = [oid_key(Anum_pg_constraint_contypid, typid)?];

    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &key, |row| {
        /* We're looking for a NOTNULL constraint that's marked validated. */
        if row.form.contype != CONSTRAINT_NOTNULL {
            return Ok(true);
        }
        if !row.form.convalidated {
            return Ok(true);
        }
        conoid = row.form.oid;
        Ok(false)
    })?;

    pg_constraint.close(AccessShareLock)?;
    let _ = mcx;
    Ok(conoid)
}

/// The pg_constraint half of `AlterDomainDropConstraint` (typecmds.c:2860):
/// `systable_beginscan(ConstraintRelidTypidNameIndexId, conrelid=Invalid,
/// contypid=domainoid, conname=constrName)` finds the at-most-one matching row;
/// for a `CONSTRAINT_NOTNULL` row the caller must clear `pg_type.typnotnull`
/// (reported via the `was_notnull` flag), then `performDeletion(conobj,
/// behavior, 0)`. Returns `(found, was_notnull)`. The caller renders the
/// `missing_ok` NOTICE/ERROR (it owns the `TypeName`).
fn drop_domain_constraint(
    mcx: Mcx<'_>,
    domainoid: Oid,
    constr_name: String,
    behavior: DropBehavior,
) -> PgResult<(bool, bool)> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, InvalidOid)?,
        oid_key(Anum_pg_constraint_contypid, domainoid)?,
        name_key(mcx, Anum_pg_constraint_conname, &constr_name)?,
    ];

    /* There can be at most one matching row. */
    let mut target: Option<(Oid, bool)> = None;
    systable_scan_foreach(&conrel, ConstraintRelidTypidNameIndexId, &skey, |row| {
        target = Some((row.form.oid, row.form.contype == CONSTRAINT_NOTNULL));
        Ok(false)
    })?;

    let (found, was_notnull) = match target {
        Some((conoid, was_notnull)) => {
            /* performDeletion(&conobj, behavior, 0). */
            dependency_seams::perform_deletion::call(
                CONSTRAINT_RELATION_ID,
                conoid,
                0,
                behavior,
                0,
            )?;
            (true, was_notnull)
        }
        None => (false, false),
    };

    conrel.close(RowExclusiveLock)?;

    Ok((found, was_notnull))
}

/// The pg_constraint catalog half of `AlterDomainValidateConstraint`
/// (typecmds.c:3031): locate the CHECK constraint of `domainoid` named
/// `constr_name`, returning its OID and cooked `conbin` text for the executor
/// VALIDATE pass. Errors if the constraint does not exist or is not a CHECK
/// constraint.
fn find_domain_check_constraint(
    mcx: Mcx<'_>,
    domainoid: Oid,
    constr_name: String,
) -> PgResult<(Oid, String)> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, InvalidOid)?,
        oid_key(Anum_pg_constraint_contypid, domainoid)?,
        name_key(mcx, Anum_pg_constraint_conname, &constr_name)?,
    ];

    /*
     * There can be at most one matching row. We re-implement the scan loop
     * (rather than `systable_scan_foreach`) so the deformed `conbin` Datum
     * survives long enough to detoast — the shared helper discards `values`.
     */
    let mut scan =
        genam_seams::systable_beginscan::call(&conrel, ConstraintRelidTypidNameIndexId, true, None, &skey)?;
    let scratch = MemoryContext::new("pg_constraint scan row");
    let smcx = scratch.mcx();
    let result = match genam_seams::systable_getnext::call(smcx, scan.desc_mut())? {
        None => {
            scan.end()?;
            conrel.close(RowExclusiveLock)?;
            return Err(PgError::new(
                ERROR,
                format!(
                    "constraint \"{}\" of domain \"{}\" does not exist",
                    constr_name,
                    format_type_seams::format_type_be_str::call(domainoid)?
                ),
            )
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
        Some(tup) => {
            let cols = heap_deform_tuple(smcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
            let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
            for (value, _null) in cols.iter() {
                values.push(value.clone());
            }
            let form = form_pg_constraint(&values);

            if form.contype != CONSTRAINT_CHECK {
                scan.end()?;
                conrel.close(RowExclusiveLock)?;
                return Err(PgError::new(
                    ERROR,
                    format!(
                        "constraint \"{}\" of domain \"{}\" is not a check constraint",
                        constr_name,
                        format_type_seams::format_type_be_str::call(domainoid)?
                    ),
                )
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
            }

            /* val = SysCacheGetAttrNotNull(CONSTROID, tuple, conbin);
             * conbin = TextDatumGetCString(val); */
            let conbin_datum = &values[Anum_pg_constraint_conbin as usize - 1];
            let conbin = varlena_seams::text_to_cstring_v::call(mcx, conbin_datum)?
                .as_str()
                .to_string();
            (form.oid, conbin)
        }
    };

    scan.end()?;
    conrel.close(RowExclusiveLock)?;

    Ok(result)
}

/// Read the cooked `conbin` (`pg_node_tree`) text of a table CHECK constraint,
/// the `SysCacheGetAttrNotNull(CONSTROID, contuple, Anum_pg_constraint_conbin)`
/// + `TextDatumGetCString(val)` of `QueueCheckConstraintValidation`
/// (tablecmds.c:13185-13187). Keyed by `(conrelid = relid, contypid = Invalid,
/// conname = constr_name)` — the at-most-one matching row. Errors if the
/// constraint does not exist or its `conbin` is NULL (the C `NotNull` getter
/// `elog`s on NULL).
pub fn get_check_constraint_conbin(
    mcx: Mcx<'_>,
    relid: Oid,
    constr_name: &str,
) -> PgResult<String> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, relid)?,
        oid_key(Anum_pg_constraint_contypid, InvalidOid)?,
        name_key(mcx, Anum_pg_constraint_conname, constr_name)?,
    ];

    /*
     * There can be at most one matching row. We re-implement the scan loop
     * (rather than `systable_scan_foreach`) so the deformed `conbin` Datum
     * survives long enough to detoast — the shared helper discards `values`.
     */
    let mut scan = genam_seams::systable_beginscan::call(
        &conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        None,
        &skey,
    )?;
    let scratch = MemoryContext::new("pg_constraint scan row");
    let smcx = scratch.mcx();

    let result = match genam_seams::systable_getnext::call(smcx, scan.desc_mut())? {
        None => {
            scan.end()?;
            conrel.close(AccessShareLock)?;
            return Err(PgError::error(format!(
                "could not find tuple for constraint \"{constr_name}\""
            )));
        }
        Some(tup) => {
            let cols = heap_deform_tuple(smcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
            let conbin_is_null = cols
                .get(Anum_pg_constraint_conbin as usize - 1)
                .map(|(_, n)| *n)
                .unwrap_or(true);
            if conbin_is_null {
                scan.end()?;
                conrel.close(AccessShareLock)?;
                return Err(PgError::error(format!(
                    "null conbin for constraint \"{constr_name}\""
                )));
            }
            let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
            for (value, _null) in cols.iter() {
                values.push(value.clone());
            }
            let conbin_datum = &values[Anum_pg_constraint_conbin as usize - 1];
            varlena_seams::text_to_cstring_v::call(mcx, conbin_datum)?
                .as_str()
                .to_string()
        }
    };

    scan.end()?;
    conrel.close(AccessShareLock)?;

    Ok(result)
}

/// `scan_domain_check_constraints` — the per-level `pg_constraint` CHECK scan of
/// `load_domaintype_info` (typcache.c:1145-1167). Opens `pg_constraint`,
/// `ScanKeyInit(Anum_pg_constraint_contypid == type_id)`,
/// `systable_beginscan(conRel, ConstraintTypidIndexId, ...)`, and for every
/// matching row keeps only CHECK constraints (`c->contype == CONSTRAINT_CHECK`),
/// raising on a NULL `conbin` (`elog(ERROR, "... has NULL conbin")`), and
/// returning each CHECK's `conname` plus its `conbin` node-string
/// (`TextDatumGetCString(val)`) in scan (index) order. The typcache plans each
/// (`stringToNode` + `expression_planner`), sorts by name, and orders
/// parent-first; this seam only does the catalog read. Owned here because
/// `pg_constraint.c` can do the systable scan without a cycle (the typcache
/// reaches it through `backend-utils-adt-domains-seams`).
fn scan_domain_check_constraints(type_id: Oid) -> PgResult<Vec<DomainCheckConstraintRow>> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [oid_key(Anum_pg_constraint_contypid, type_id)?];

    let mut rows: Vec<DomainCheckConstraintRow> = Vec::new();

    // One scratch context for the whole scan: the genam scan's internal tuple
    // slot materializes each fetched tuple into the context passed to
    // `systable_getnext`, and that slot is only dropped at `scan.end()`. A
    // per-iteration context (dropped each loop turn) would free the slot's last
    // tuple out from under the end-of-scan slot teardown (use-after-free in
    // ExecDropSingleTupleTableSlot). The single context lives until after
    // `scan.end()` returns, so the slot's storage is valid through teardown.
    let scratch = MemoryContext::new("pg_constraint scan row");
    let smcx = scratch.mcx();
    let mut scan =
        genam_seams::systable_beginscan::call(&conrel, ConstraintTypidIndexId, true, None, &skey)?;
    let mut null_conbin_name: Option<String> = None;
    loop {
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(smcx, cols.len())?;
        let mut conbin_is_null = true;
        for (i, (value, null)) in cols.iter().enumerate() {
            values.push(value.clone());
            if i == Anum_pg_constraint_conbin as usize - 1 {
                conbin_is_null = *null;
            }
        }
        let form = form_pg_constraint(&values);

        /* Ignore non-CHECK constraints. */
        if form.contype != CONSTRAINT_CHECK {
            continue;
        }

        /* Not expecting conbin to be NULL, but we'll test for it anyway. */
        if conbin_is_null {
            null_conbin_name = Some(name_str(&form.conname).to_string());
            break;
        }

        /* Convert conbin to a C string (TextDatumGetCString). */
        let conbin_datum = &values[Anum_pg_constraint_conbin as usize - 1];
        let conbin = varlena_seams::text_to_cstring_v::call(smcx, conbin_datum)?
            .as_str()
            .to_string();

        rows.push(DomainCheckConstraintRow {
            conname: name_str(&form.conname).to_string(),
            conbin,
        });
    }

    scan.end()?;
    conrel.close(AccessShareLock)?;

    if let Some(conname) = null_conbin_name {
        return Err(PgError::error(format!(
            "domain \"{}\" constraint \"{}\" has NULL conbin",
            format_type_seams::format_type_be_str::call(type_id)?,
            conname,
        )));
    }

    Ok(rows)
}

/// `copy_con->convalidated = true; CatalogTupleUpdate` for the constraint OID
/// (the catalog-write half of `AlterDomainValidateConstraint`,
/// typecmds.c:3106, and the `Queue{FK,Check,NN}ConstraintValidation` tails in
/// tablecmds.c). Reads the existing row by OID and re-stores it with
/// `convalidated` flipped on, then fires `InvokeObjectPostAlterHook`.
pub fn set_constraint_validated(_mcx: Mcx<'_>, con_oid: Oid) -> PgResult<()> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let con = match syscache_seams::search_constraint_form_by_oid::call(con_oid)? {
        Some(c) => c,
        None => {
            conrel.close(RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for constraint {con_oid}"
            )));
        }
    };
    let conform = con.form;

    let fields = ConstraintFieldUpdate {
        conname: conform.conname,
        connamespace: conform.connamespace,
        conislocal: conform.conislocal,
        coninhcount: conform.coninhcount,
        conparentid: conform.conparentid,
        convalidated: true,
        connoinherit: conform.connoinherit,
        conenforced: conform.conenforced,
        condeferrable: conform.condeferrable,
        condeferred: conform.condeferred,
        conindid: conform.conindid,
    };
    indexing_seams::catalog_tuple_update_pg_constraint::call(&conrel, con.tid, &fields)?;

    /* InvokeObjectPostAlterHook(ConstraintRelationId, con->oid, 0); */
    objectaccess_seams::invoke_object_post_alter_hook::call(CONSTRAINT_RELATION_ID, con_oid, 0)?;

    conrel.close(RowExclusiveLock)?;

    Ok(())
}

/// The set of `ATAlterConstraint` flags `AlterConstrUpdateConstraintEntry`
/// applies (tablecmds.c:12854). Each `alter_*` selector gates whether the
/// matching column(s) are overwritten; the values are carried in the same
/// struct.
#[derive(Clone, Copy, Debug)]
pub struct AlterConstrFlags {
    pub alter_enforceability: bool,
    pub is_enforced: bool,
    pub alter_deferrability: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub alter_inheritability: bool,
    pub noinherit: bool,
}

/// `AlterConstrUpdateConstraintEntry(cmdcon, conrel, contuple)`
/// (tablecmds.c:12854). Re-reads the `pg_constraint` row by OID, overwrites the
/// enforceability / deferrability / inheritability columns selected by `flags`,
/// re-stores it, and fires `InvokeObjectPostAlterHook`. Returns the row's
/// `conrelid` so the caller can `CacheInvalidateRelcacheByRelid` (the C does
/// that invalidation inside this function; here the relcache facet lives in the
/// caller's crate).
pub fn AlterConstrUpdateConstraintEntry(
    _mcx: Mcx<'_>,
    con_oid: Oid,
    flags: &AlterConstrFlags,
) -> PgResult<Oid> {
    debug_assert!(
        flags.alter_enforceability || flags.alter_deferrability || flags.alter_inheritability
    );

    let con_ctx = MemoryContext::new("pg_constraint");
    let conrel = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    let con = match syscache_seams::search_constraint_form_by_oid::call(con_oid)? {
        Some(c) => c,
        None => {
            conrel.close(RowExclusiveLock)?;
            return Err(PgError::error(format!(
                "cache lookup failed for constraint {con_oid}"
            )));
        }
    };
    let conform = con.form;

    let mut convalidated = conform.convalidated;
    let mut conenforced = conform.conenforced;
    let mut condeferrable = conform.condeferrable;
    let mut condeferred = conform.condeferred;
    let mut connoinherit = conform.connoinherit;

    if flags.alter_enforceability {
        conenforced = flags.is_enforced;
        // The convalidated status is irrelevant when NOT ENFORCED, but for
        // consistency it is set to match; when ENFORCED, phase 3 validation
        // makes it valid.
        convalidated = flags.is_enforced;
    }
    if flags.alter_deferrability {
        condeferrable = flags.deferrable;
        condeferred = flags.initdeferred;
    }
    if flags.alter_inheritability {
        connoinherit = flags.noinherit;
    }

    let fields = ConstraintFieldUpdate {
        conname: conform.conname,
        connamespace: conform.connamespace,
        conislocal: conform.conislocal,
        coninhcount: conform.coninhcount,
        conparentid: conform.conparentid,
        convalidated,
        connoinherit,
        conenforced,
        condeferrable,
        condeferred,
        conindid: conform.conindid,
    };
    indexing_seams::catalog_tuple_update_pg_constraint::call(&conrel, con.tid, &fields)?;

    objectaccess_seams::invoke_object_post_alter_hook::call(CONSTRAINT_RELATION_ID, con_oid, 0)?;

    conrel.close(RowExclusiveLock)?;

    Ok(conform.conrelid)
}

/// Scan `pg_constraint` for a relation-level constraint by name
/// (`(conrelid, contypid = InvalidOid, conname)` over
/// `ConstraintRelidTypidNameIndexId`); returns the form copy + TID, or `None`
/// when no row matches. The by-name lookup the ATExecAlterConstraint entry
/// performs; the caller raises the "does not exist" error so it can use the
/// ALTER-specific wording.
pub fn find_relation_constraint_by_name(
    mcx: Mcx<'_>,
    relid: Oid,
    conname: &str,
) -> PgResult<Option<types_catalog::pg_constraint::ConstraintFormCopy>> {
    let con_ctx = MemoryContext::new("pg_constraint");
    let pg_constraint = table::table_open(con_ctx.mcx(), CONSTRAINT_RELATION_ID, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_constraint_conrelid, relid)?,
        oid_key(Anum_pg_constraint_contypid, InvalidOid)?,
        name_key(mcx, Anum_pg_constraint_conname, conname)?,
    ];

    let mut found: Option<types_catalog::pg_constraint::ConstraintFormCopy> = None;
    /* There can be at most one matching row */
    systable_scan_foreach(&pg_constraint, ConstraintRelidTypidNameIndexId, &skey, |row| {
        found = Some(types_catalog::pg_constraint::ConstraintFormCopy {
            form: row.form.clone(),
            conkey: None,
            tid: row.tid,
        });
        Ok(false)
    })?;

    pg_constraint.close(AccessShareLock)?;

    Ok(found)
}

/* ===========================================================================
 * index_concurrently_swap's constraint+trigger move leg (catalog/index.c:1656-1724)
 * ========================================================================= */

/// `swap_index_constraints_and_triggers(constraintOids, oldIndexId, newIndexId)`
/// — the "Move constraints and triggers over to the new index" block of
/// `index_concurrently_swap` (catalog/index.c:1656-1724).
///
/// For each constraint whose `conindid == oldIndexId`, set `conindid =
/// newIndexId` and `CatalogTupleUpdate`. Then scan `pg_trigger` by
/// `tgconstraint` and move every `tgconstrindid == oldIndexId` to `newIndexId`.
/// Homed here because the `pg_constraint` unit owns the constraint catalog and
/// reaches `pg_trigger` through genam.
fn swap_index_constraints_and_triggers(
    constraint_oids: &[Oid],
    old_index_id: Oid,
    new_index_id: Oid,
) -> PgResult<()> {
    use types_catalog::pg_trigger::{
        Anum_pg_trigger_tgconstraint, Anum_pg_trigger_tgconstrindid, TriggerConstraintIndexId,
        TriggerFieldUpdate, TriggerRelationId,
    };

    let pg_constraint_ctx = MemoryContext::new("swap_index_constraints");
    let cmcx = pg_constraint_ctx.mcx();
    let pg_constraint = table::table_open(cmcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;
    let pg_trigger = table::table_open(cmcx, TriggerRelationId, RowExclusiveLock)?;

    for &constraint_oid in constraint_oids {
        /*
         * Move the constraint from the old to the new index. The C reads the
         * constraint row via SearchSysCacheCopy1(CONSTROID); here we scan the
         * pg_constraint OID index for the row, deform it, and (when it points at
         * the old index) rewrite conindid via the indexing update seam.
         */
        let key = [oid_key(Anum_pg_constraint_oid, constraint_oid)?];
        let mut con_row: Option<(ItemPointerData, FormData_pg_constraint)> = None;
        systable_scan_foreach(
            &pg_constraint,
            ConstraintOidIndexId,
            &key,
            |row| {
                con_row = Some((row.tid, row.form.clone()));
                Ok(false)
            },
        )?;
        let Some((tid, con)) = con_row else {
            return Err(PgError::error(format!(
                "could not find tuple for constraint {constraint_oid}"
            )));
        };

        if con.conindid == old_index_id {
            let fields = ConstraintFieldUpdate {
                conname: con.conname,
                connamespace: con.connamespace,
                conislocal: con.conislocal,
                coninhcount: con.coninhcount,
                conparentid: con.conparentid,
                convalidated: con.convalidated,
                connoinherit: con.connoinherit,
                conenforced: con.conenforced,
                condeferrable: con.condeferrable,
                condeferred: con.condeferred,
                conindid: new_index_id,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(&pg_constraint, tid, &fields)?;
        }

        /*
         * Search for trigger records whose tgconstraint matches, and move the
         * referenced tgconstrindid from the old index to the new one.
         */
        let tkey = [oid_key(Anum_pg_trigger_tgconstraint, constraint_oid)?];
        let mut scan = genam_seams::systable_beginscan::call(
            &pg_trigger,
            TriggerConstraintIndexId,
            true,
            None,
            &tkey,
        )?;
        loop {
            let scratch = MemoryContext::new("pg_trigger scan row");
            let smcx = scratch.mcx();
            let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
                break;
            };
            let cols = heap_deform_tuple(smcx, &tup.tuple, &pg_trigger.rd_att, &tup.data)?;
            let tgconstrindid =
                cols[(Anum_pg_trigger_tgconstrindid as usize) - 1].0.as_oid();
            if tgconstrindid != old_index_id {
                continue;
            }
            let tid = tup.tuple.t_self;
            let fields = TriggerFieldUpdate {
                tgname: None,
                tgparentid: None,
                tgdeferrable: None,
                tginitdeferred: None,
                tgenabled: None,
                tgconstrindid: Some(new_index_id),
            };
            indexing_seams::catalog_tuple_update_pg_trigger::call(&pg_trigger, tid, &fields)?;
        }
        scan.end()?;
    }

    pg_trigger.close(RowExclusiveLock)?;
    pg_constraint.close(RowExclusiveLock)?;
    Ok(())
}

pub fn init_seams() {
    use backend_catalog_pg_constraint_seams as seams;
    seams::swap_index_constraints_and_triggers::set(swap_index_constraints_and_triggers);

    seams::find_domain_not_null_constraint_oid::set(|mcx, typid| {
        find_domain_not_null_constraint_oid(mcx, typid)
    });
    seams::drop_domain_constraint::set(|mcx, domainoid, constr_name, behavior| {
        drop_domain_constraint(mcx, domainoid, constr_name, behavior)
    });
    seams::find_domain_check_constraint::set(|mcx, domainoid, constr_name| {
        find_domain_check_constraint(mcx, domainoid, constr_name)
    });
    // typcache's load_domaintype_info domain-stack CHECK scan (the
    // ConstraintTypidIndexId systable scan over contypid). The typcache reaches
    // it through backend-utils-adt-domains-seams.
    backend_utils_adt_domains_seams::scan_domain_check_constraints::set(
        scan_domain_check_constraints,
    );
    seams::set_constraint_validated::set(set_constraint_validated);
    seams::alter_constraint_namespaces::set(
        |_mcx, owner_id, old_nsp, new_nsp, is_type, objs_moved| {
            AlterConstraintNamespaces(owner_id, old_nsp, new_nsp, is_type, objs_moved)
        },
    );
    seams::register_constraint_inval_callback::set(register_constraint_inval_callback);
    seams::load_fk_constraint::set(load_fk_constraint);
    seams::constraint_hash_value::set(constraint_hash_value);
    seams::get_ri_constraint_root::set(get_ri_constraint_root);
    seams::find_fk_period_opers::set(find_fk_period_opers);
    seams::constraint_type_oids::set(constraint_type_oids);
    seams::get_relation_constraint_oid::set(get_relation_constraint_oid);
    seams::get_domain_constraint_oid::set(get_domain_constraint_oid);
    seams::RemoveConstraintById::set(RemoveConstraintById);
    syscache_seams::decrement_relchecks::set(decrement_relchecks);

    // index_create / index_constraint_create (catalog/index.c) constraint legs.
    seams::constraint_name_is_used::set(|mcx, con_cat, obj_id, conname| {
        ConstraintNameIsUsed(mcx, con_cat, obj_id, conname)
    });
    seams::create_constraint_entry::set(|mcx, args| {
        // The full CreateConstraintEntry takes the ~30-parameter list; the
        // index-constraint call site supplies only the PK/UNIQUE/EXCLUDE subset
        // (no foreign key, no CHECK expression, no domain), and isEnforced /
        // isValidated are both true. Fill the remaining C constants here.
        CreateConstraintEntry(
            mcx,
            args.constraint_name,
            args.constraint_namespace,
            args.constraint_type,
            args.is_deferrable,
            args.is_deferred,
            true,  /* isEnforced */
            true,  /* isValidated */
            args.parent_constr_id,
            args.rel_id,
            args.constraint_key,
            args.constraint_n_keys, /* constraintNKeys */
            args.constraint_n_total_keys,
            InvalidOid, /* domainId — no domain */
            args.index_rel_id,
            InvalidOid, /* foreignRelId — no FK */
            &[],        /* foreignKey */
            &[],        /* pfEqOp */
            &[],        /* ppEqOp */
            &[],        /* ffEqOp */
            0,          /* foreignNKeys */
            b' ' as i8, /* foreignUpdateType */
            b' ' as i8, /* foreignDeleteType */
            &[],        /* fkDeleteSetCols */
            0,          /* numFkDeleteSetCols */
            b' ' as i8, /* foreignMatchType */
            args.excl_op,
            None, /* conExpr — no CHECK */
            None, /* conBin */
            args.con_is_local,
            args.con_inh_count,
            args.con_no_inherit,
            args.con_period,
            args.is_internal,
        )
    });
}

/* ===========================================================================
 * tuple/form helpers for the SearchSysCache form-search projection
 * ========================================================================= */

/// `OIDOID` (`pg_type.h`).
const OIDOID: Oid = 26;
/// `ANYRANGEOID` / `ANYMULTIRANGEOID` (`pg_type.h`).
const ANYRANGEOID: Oid = 3831;
const ANYMULTIRANGEOID: Oid = 4537;

/* ===========================================================================
 * MergeConstraintsIntoExisting (tablecmds.c:17638)
 * ========================================================================= */

/// `((Form_pg_constraint) GETSTRUCT(tup))->connoinherit` — read the
/// `connoinherit` flag off a constraint tuple. Used by
/// `MergeAttributesIntoExisting` (tablecmds) to decide whether a parent's
/// not-null constraint is inherited.
pub fn constraint_connoinherit(mcx: Mcx<'_>, tup: &FormedTuple<'_>) -> PgResult<bool> {
    // Read the whole row to deform connoinherit; the pg_constraint descriptor
    // comes from the relcache. We deform via a fresh pg_constraint open.
    let pg_constraint = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &pg_constraint.rd_att, &tup.data)?;
    let mut values: PgVec<'_, Datum<'_>> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    for (value, _null) in cols.iter() {
        values.push(value.clone());
    }
    let form = form_pg_constraint(&values);
    pg_constraint.close(AccessShareLock)?;
    Ok(form.connoinherit)
}

/// `decompile_conbin(contup, tupdesc)` (tablecmds.c:17436) — reverse-compile a
/// CHECK constraint's stored `conbin` (`pg_node_tree`) into its source string,
/// via `pg_get_expr(conbin, conrelid)`. `elog(ERROR)` on a null `conbin`.
fn decompile_conbin<'mcx>(
    mcx: Mcx<'mcx>,
    htup: &FormedTuple<'mcx>,
    constraintrel: &RelationData<'mcx>,
    conrelid: Oid,
) -> PgResult<String> {
    let cols = heap_deform_tuple(mcx, &htup.tuple, &constraintrel.rd_att, &htup.data)?;
    let idx = Anum_pg_constraint_conbin as usize - 1;
    let is_null = cols.get(idx).map(|(_, n)| *n).unwrap_or(true);
    if is_null {
        return Err(PgError::new(ERROR, "null conbin for constraint".to_string()));
    }
    let (conbin_datum, _) = &cols[idx];
    // pg_get_expr expects the pg_node_tree text; text_to_cstring detoasts.
    let exprstr = varlena_seams::text_to_cstring_v::call(mcx, conbin_datum)?
        .as_str()
        .to_string();
    // DirectFunctionCall2(pg_get_expr, attr, conrelid) — prettyFlags = 0.
    let src = ruleutils_seams::pg_get_expr_worker::call(mcx, &exprstr, conrelid, 0)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(src)
}

/// `constraints_equivalent(a, b, tupleDesc)` (tablecmds.c:17470) — two CHECK
/// constraints are functionally equivalent when their deferral flags agree and
/// they reverse-compile to the same source string. Enforceability is ignored.
fn constraints_equivalent(
    a_condeferrable: bool,
    a_condeferred: bool,
    a_src: &Option<String>,
    b_condeferrable: bool,
    b_condeferred: bool,
    b_src: &Option<String>,
) -> bool {
    if a_condeferrable != b_condeferrable || a_condeferred != b_condeferred {
        return false;
    }
    match (a_src, b_src) {
        (Some(a), Some(b)) => a == b,
        // A null conbin would have errored in decompile_conbin; treat a missing
        // source as non-equivalent defensively.
        _ => false,
    }
}

/// `MergeConstraintsIntoExisting(child_rel, parent_rel)` (tablecmds.c:17638) —
/// match the parent's inheritable CHECK / NOT NULL constraints to the child and
/// bump each matched child constraint's `coninhcount` (and clear `conislocal`
/// for a partitioned parent). Lives here because the `Form_pg_constraint` deform
/// + `pg_constraint` scan/write substrate is owned by this crate.
///
/// NOT NULL constraints are matched by attribute number; CHECK constraints are
/// matched by name and verified equivalent via `constraints_equivalent`
/// (reverse-compile both `conbin`s to source through `pg_get_expr` and compare,
/// plus the deferral flags).
pub fn merge_constraints_into_existing<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &RelationData<'mcx>,
    parent_rel: &RelationData<'mcx>,
) -> PgResult<()> {
    let parent_relid = parent_rel.rd_id;

    let constraintrel = table::table_open(mcx, CONSTRAINT_RELATION_ID, RowExclusiveLock)?;

    // attmap = build_attrmap_by_name(parent_desc, child_desc, true);
    let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
        mcx,
        &parent_rel.rd_att,
        &child_rel.rd_att,
        true,
    )?;

    let parent_partitioned = parent_rel.rd_rel.relkind
        == types_tuple::access::RELKIND_PARTITIONED_TABLE;

    // Collect the parent's inheritable CHECK / NOT NULL constraints first (the C
    // nests two scans on the same relation; we materialize the outer scan to
    // avoid an overlapping systable scan).
    struct ParentCon {
        contype: i8,
        conname: [u8; NAMEDATALEN],
        connoinherit: bool,
        convalidated: bool,
        conenforced: bool,
        condeferrable: bool,
        condeferred: bool,
        attno: AttrNumber, /* for NOT NULL */
        /// For CHECK: the reverse-compiled `conbin` source string (the
        /// `constraints_equivalent` comparison key). `None` for NOT NULL.
        check_src: Option<String>,
    }
    let mut parent_cons: Vec<ParentCon> = Vec::new();
    {
        let key = [oid_key(Anum_pg_constraint_conrelid, parent_relid)?];
        systable_scan_foreach(&constraintrel, ConstraintRelidTypidNameIndexId, &key, |row| {
            let ct = row.form.contype;
            if ct != CONSTRAINT_CHECK && ct != CONSTRAINT_NOTNULL {
                return Ok(true);
            }
            if row.form.connoinherit {
                return Ok(true);
            }
            let attno = if ct == CONSTRAINT_NOTNULL {
                extractNotNullColumn(&row.htup)?
            } else {
                InvalidAttrNumber
            };
            let check_src = if ct == CONSTRAINT_CHECK {
                Some(decompile_conbin(mcx, &row.htup, &constraintrel, parent_relid)?)
            } else {
                None
            };
            parent_cons.push(ParentCon {
                contype: ct,
                conname: row.form.conname,
                connoinherit: row.form.connoinherit,
                convalidated: row.form.convalidated,
                conenforced: row.form.conenforced,
                condeferrable: row.form.condeferrable,
                condeferred: row.form.condeferred,
                attno,
                check_src,
            });
            Ok(true)
        })?;
    }

    for parent_con in parent_cons.iter() {
        let mut found = false;

        // Search for a child constraint matching this one. CHECK constraints are
        // matched by constraint name, NOT NULL ones by attribute number.
        let child_key = [oid_key(Anum_pg_constraint_conrelid, child_rel.rd_id)?];
        let mut matched: Option<(ItemPointerData, FormData_pg_constraint, Option<String>)> = None;
        systable_scan_foreach(&constraintrel, ConstraintRelidTypidNameIndexId, &child_key, |row| {
            if row.form.contype != parent_con.contype {
                return Ok(true);
            }
            if parent_con.contype == CONSTRAINT_CHECK {
                // Matched by constraint name.
                if name_str(&row.form.conname) != name_str(&parent_con.conname) {
                    return Ok(true);
                }
                let src = decompile_conbin(mcx, &row.htup, &constraintrel, child_rel.rd_id)?;
                matched = Some((row.tid, row.form.clone(), Some(src)));
                Ok(false)
            } else {
                // NOT NULL matched by attribute number.
                let child_attno = extractNotNullColumn(&row.htup)?;
                // parent_attno != attmap->attnums[child_attno - 1] ⇒ skip.
                let mapped = attmap
                    .attnums
                    .get((child_attno - 1) as usize)
                    .copied()
                    .unwrap_or(InvalidAttrNumber);
                if parent_con.attno != mapped {
                    return Ok(true);
                }
                matched = Some((row.tid, row.form.clone(), None));
                Ok(false)
            }
        })?;

        if let Some((tid, mut child_form, child_check_src)) = matched {
            // A CHECK constraint matched by name must reverse-compile to the
            // same source as the parent's, else it is a different definition.
            if parent_con.contype == CONSTRAINT_CHECK
                && !constraints_equivalent(
                    parent_con.condeferrable,
                    parent_con.condeferred,
                    &parent_con.check_src,
                    child_form.condeferrable,
                    child_form.condeferred,
                    &child_check_src,
                )
            {
                constraintrel.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "child table \"{}\" has different definition for check constraint \"{}\"",
                        child_rel_name(child_rel),
                        name_str(&parent_con.conname)
                    ))
                    .into_error());
            }

            // If the child constraint is NO INHERIT, cannot merge.
            if child_form.connoinherit {
                constraintrel.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "constraint \"{}\" conflicts with non-inherited constraint on child table \"{}\"",
                        name_str(&child_form.conname),
                        child_rel_name(child_rel)
                    ))
                    .into_error());
            }

            // If the child constraint is "not valid" then cannot merge with a
            // valid parent constraint.
            if parent_con.convalidated && child_form.conenforced && !child_form.convalidated {
                constraintrel.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{}\"",
                        name_str(&child_form.conname),
                        child_rel_name(child_rel)
                    ))
                    .into_error());
            }

            // A NOT ENFORCED child constraint cannot be merged with an ENFORCED
            // parent constraint. The reverse (child ENFORCED) is allowed.
            if parent_con.conenforced && !child_form.conenforced {
                constraintrel.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{}\"",
                        name_str(&child_form.conname),
                        child_rel_name(child_rel)
                    ))
                    .into_error());
            }

            // Bump the child constraint's inheritance count.
            let mut newcount = child_form.coninhcount;
            if pg_add_s16_overflow(child_form.coninhcount, 1, &mut newcount) {
                constraintrel.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg("too many inheritance parents".to_string())
                    .into_error());
            }
            child_form.coninhcount = newcount;

            // For a partitioned parent, the inherited constraint is never local.
            if parent_partitioned {
                child_form.conislocal = false;
            }

            let fields = ConstraintFieldUpdate {
                conname: child_form.conname,
                connamespace: child_form.connamespace,
                conislocal: child_form.conislocal,
                coninhcount: child_form.coninhcount,
                conparentid: child_form.conparentid,
                convalidated: child_form.convalidated,
                connoinherit: child_form.connoinherit,
                conenforced: child_form.conenforced,
                condeferrable: child_form.condeferrable,
                condeferred: child_form.condeferred,
                conindid: child_form.conindid,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(&constraintrel, tid, &fields)?;
            found = true;
        }

        if !found {
            constraintrel.close(RowExclusiveLock)?;
            // NOT NULL gives a specific "must be marked NOT NULL" message; any
            // other (CHECK) constraint falls through to "missing constraint".
            if parent_con.contype == CONSTRAINT_NOTNULL {
                let colname =
                    lsyscache_seams::get_attname::call(mcx, parent_relid, parent_con.attno, false)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                return Err(ereport(ERROR)
                    .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                        colname,
                        child_rel_name(child_rel)
                    ))
                    .into_error());
            }
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "child table is missing constraint \"{}\"",
                    name_str(&parent_con.conname)
                ))
                .into_error());
        }
    }

    constraintrel.close(RowExclusiveLock)?;
    Ok(())
}

/// `RelationGetRelationName(child_rel)` as an owned `String`.
fn child_rel_name(rel: &RelationData<'_>) -> String {
    rel.name().to_string()
}

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
