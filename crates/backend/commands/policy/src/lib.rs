//! `src/backend/commands/policy.c` (PostgreSQL 18.3) — commands for
//! manipulating row-level-security (RLS) policies.
//!
//! Ported 1:1 against the owned node tree, name-for-name. Catalog access
//! mirrors the landed `backend-catalog-pg-attrdef` / `backend-catalog-pg-shdepend`
//! carrier idiom: `table_open`/`close` guard scopes, `ScanKeyInit` + the genam
//! `systable_*` iterator, `heap_deform_tuple` of the columns. The pg_policy
//! INSERT (OID allocation + `heap_form_tuple` + `CatalogTupleInsert`) and the
//! `heap_modify_tuple` + `CatalogTupleUpdate` cross the landed catalog-indexing
//! engine seams `catalog_tuple_insert_pg_policy` / `catalog_tuple_update_pg_policy`
//! as typed [`PgPolicyInsertRow`] / [`PgPolicyUpdateRow`]; the `polqual` /
//! `polwithcheck` `pg_node_tree` images are produced by `nodeToString` via the
//! `node_to_string_with_locations` seam (owner `outfuncs` not yet ported —
//! mirror-PG-and-panic until then).
//!
//! The USING / WITH CHECK quals are transformed over the landed parse-expr
//! engine (`make_parsestate` + `addRangeTableEntryForRelation` +
//! `addNSItemToQuery` + `transformWhereClause` + `assign_expr_collations`).
//! Dependency bookkeeping uses the ported `recordDependencyOn` /
//! `recordDependencyOnExpr` / `deleteDependencyRecordsFor` (pg_depend) and
//! `recordSharedDependencyOn` / `deleteSharedDependencyRecordsFor`
//! (pg_shdepend) directly.
//!
//! `RelationBuildRowSecurity` (the relcache-side per-policy load) lives in the
//! relcache crate (`backend-utils-cache-relcache::derived`): it operates over
//! `&mut RelationData` (relcache's own entry type, which cannot cross a crate
//! seam) and stores the assembled `RowSecurityDesc` in `rd_rsdesc`. Both the
//! `RelationBuildDesc` build path and the corrupt-initfile rebuild path call it
//! directly. It is therefore not a `policy.c` extern here.
//!
//! ─────────────────────────────────────────────────────────────────────────
//! FUNCTION INVENTORY (policy.c, PostgreSQL 18.3) — 11 functions:
//!
//!  | C lines    | kind   | function                          |
//!  |------------|--------|-----------------------------------|
//!  | 63-96      | static | `RangeVarCallbackForPolicy`       |
//!  | 107-129    | static | `parse_policy_command`            |
//!  | 136-183    | static | `policy_role_list_to_array`       |
//!  | 192-322    | extern | `RelationBuildRowSecurity` (in relcache::derived) |
//!  | 331-400    | extern | `RemovePolicyById`                |
//!  | 415-560    | extern | `RemoveRoleFromObjectPolicy`      |
//!  | 568-759    | extern | `CreatePolicy`                    |
//!  | 767-1089   | extern | `AlterPolicy`                     |
//!  | 1095-1195  | extern | `rename_policy`                   |
//!  | 1203-1250  | extern | `get_relation_policy_oid`         |
//!  | 1255-1279  | extern | `relation_has_policies`           |
//! ─────────────────────────────────────────────────────────────────────────

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::{String, ToString};

use mcx::{Mcx, MemoryContext};

use ::types_catalog::catalog::{AUTH_ID_RELATION_ID, RELATION_RELATION_ID};
use ::types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_NORMAL,
};
use ::types_catalog::catalog_shdepend::SHARED_DEPENDENCY_POLICY;
use ::types_catalog::pg_policy::{
    Anum_pg_policy_oid, Anum_pg_policy_polcmd, Anum_pg_policy_polname, Anum_pg_policy_polpermissive,
    Anum_pg_policy_polqual, Anum_pg_policy_polrelid, Anum_pg_policy_polroles,
    Anum_pg_policy_polwithcheck, FormData_pg_policy, PgPolicyInsertRow, PgPolicyUpdateRow,
    PolicyOidIndexId, PolicyPolrelidPolnameIndexId, PolicyRelationId,
};
use ::types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use ::types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use ::nodes::ddlnodes::{AlterPolicyStmt, CreatePolicyStmt, RoleSpec};
use ::nodes::nodes::Node;
use ::nodes::parsenodes::RoleSpecType;
use ::nodes::parsestmt::ParseExprKind;
use rel::{Relation, RelationData};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock,
};
use ::types_tuple::access::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use ::types_tuple::access::RangeVar;
use ::types_tuple::heaptuple::{Datum, FormedTuple};

use ::heaptuple::heap_deform_tuple;
use common_relation as relation;
use ::scankey::ScanKeyInit;
use genam_seams as genam_seams;
use table as table;
use ::catalog_catalog::IsSystemRelation;
use ::catalog_namespace::RangeVarGetRelidExtended;
use ::transam_xact::CommandCounterIncrement;
use pg_depend::{deleteDependencyRecordsFor, recordDependencyOn};
use pg_shdepend::{deleteSharedDependencyRecordsFor, recordSharedDependencyOn};
use ::dependency::recordDependencyOnExpr;
use parser_relation::{addNSItemToQuery, addRangeTableEntryForRelation};
use ::adt_acl::role_membership::get_rolespec_oid;
use ::utils_error::ereport;
use ::types_error::ErrorLocation;
use ::miscinit::GetUserId;

use aclchk_seams as aclchk_seams;
use indexing_seams as indexing_seams;
use objectaccess_seams as objectaccess_seams;
use nodes_core_seams as nodes_seams;
use read_seams as read_seams;
use parser_analyze_seams as analyze_seams;
use ::clause::transformWhereClause;
use ::parse_collate::assign_expr_collations;
use lsyscache_seams as lsyscache_seams;
use guc_seams as guc_seams;
use syscache_seams as syscache_seams;

use types_error::{ERROR, WARNING};

/* ===========================================================================
 * ACL command-char constants (utils/acl.h)
 * ========================================================================= */

/// `ACL_SELECT_CHR` `'r'`. (read)
const ACL_SELECT_CHR: i8 = b'r' as i8;
/// `ACL_INSERT_CHR` `'a'`. (append)
const ACL_INSERT_CHR: i8 = b'a' as i8;
/// `ACL_UPDATE_CHR` `'w'`. (write)
const ACL_UPDATE_CHR: i8 = b'w' as i8;
/// `ACL_DELETE_CHR` `'d'`.
const ACL_DELETE_CHR: i8 = b'd' as i8;

/// `ACL_ID_PUBLIC` — `InvalidOid` (PUBLIC is OID 0 in `polroles`).
const ACL_ID_PUBLIC: Oid = InvalidOid;

/// `OIDOID` — `pg_type` OID of `oid`.
const OIDOID: Oid = 26;

/// `errstart`/`errfinish` source-location helper — policy.c is
/// `src/backend/commands/policy.c`.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/commands/policy.c", lineno, funcname)
}

/* ===========================================================================
 * scan-key builders + the systable scan iterator (mirror pg-attrdef)
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
/// CStringGetDatum(value))`.
fn name_key<'mcx>(mcx: Mcx<'mcx>, attno: AttrNumber, value: &str) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        Datum::ByRef(::mcx::slice_in(mcx, value.as_bytes())?),
    )?;
    Ok(key)
}

/// One scanned `pg_policy` row: the owned full tuple, the deformed scalar
/// `(Form_pg_policy) GETSTRUCT` projection, and the variable-length column
/// Datums needed for re-deriving roles / quals (`polroles`, `polqual`,
/// `polwithcheck`).
struct PolicyScanRow<'mcx> {
    htup: FormedTuple<'mcx>,
    form: FormData_pg_policy,
    polroles: Datum<'mcx>,
    polqual: Option<Datum<'mcx>>,
    polwithcheck: Option<Datum<'mcx>>,
}

/// `systable_beginscan` + `while ((tup = systable_getnext(scan)))` loop +
/// `systable_endscan` (the genam iterator). `body` returning `Ok(false)` stops
/// early (the C `break`). Each row's deformed columns + owned tuple land in a
/// per-iteration scratch context.
fn systable_scan_foreach<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'mcx>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&PolicyScanRow<'mcx>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let smcx = mcx;
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: ::mcx::PgVec<'mcx, Datum<'mcx>> = ::mcx::vec_with_capacity_in(smcx, cols.len())?;
        let mut isnull: ::mcx::PgVec<'mcx, bool> = ::mcx::vec_with_capacity_in(smcx, cols.len())?;
        for (value, null) in cols.iter() {
            values.push(value.clone());
            isnull.push(*null);
        }
        let col = |attno: i16| values[attno as usize - 1].clone();
        let nul = |attno: i16| isnull[attno as usize - 1];
        let form = FormData_pg_policy {
            oid: col(Anum_pg_policy_oid).as_oid(),
            polname: name_from_datum(&col(Anum_pg_policy_polname)),
            polrelid: col(Anum_pg_policy_polrelid).as_oid(),
            polcmd: col(Anum_pg_policy_polcmd).as_i8(),
            polpermissive: col(Anum_pg_policy_polpermissive).as_bool(),
        };
        let polroles = col(Anum_pg_policy_polroles);
        let polqual = if nul(Anum_pg_policy_polqual) {
            None
        } else {
            Some(col(Anum_pg_policy_polqual))
        };
        let polwithcheck = if nul(Anum_pg_policy_polwithcheck) {
            None
        } else {
            Some(col(Anum_pg_policy_polwithcheck))
        };
        let row = PolicyScanRow {
            htup: tup,
            form,
            polroles,
            polqual,
            polwithcheck,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
    }
    scan.end()
}

/// Read a `name` (`NameData`, fixed 64-byte NUL-padded) Datum as a `String`.
fn name_from_datum(d: &Datum<'_>) -> String {
    match d {
        Datum::ByRef(bytes) => {
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            String::from_utf8_lossy(&bytes[..end.min(64)]).into_owned()
        }
        _ => String::new(),
    }
}

/// `DatumGetArrayTypePCopy(roles_datum)` + `ARR_DATA_PTR` + `ARR_DIMS[0]` — the
/// `Oid[]` element values of the `polroles` array (`deconstruct_array_builtin(
/// .., OIDOID, ..)`). polroles is `BKI_FORCE_NOT_NULL`, so always present and
/// never null-bitmap'd.
fn polroles_to_oids(mcx: Mcx<'_>, polroles: &Datum<'_>) -> PgResult<alloc::vec::Vec<Oid>> {
    let bytes = match polroles {
        Datum::ByRef(b) => b.as_slice(),
        _ => {
            return Err(PgError::error(
                "unexpected by-value Datum in pg_policy.polroles",
            ))
        }
    };
    // C: `ArrayType *policy_roles = DatumGetArrayTypeP(datum)` before
    // `deconstruct_array_builtin`. `deconstruct_array_builtin` reads the
    // `ArrayType` header (`ARR_NDIM`/`ARR_ELEMTYPE`/`ARR_DIMS`) at fixed
    // 4-byte-relative offsets, valid only on a plain 4-byte-header varlena.
    // With `SHORT_VARLENA_PACKING` on, a stored `pg_policy.polroles` (`oid[]`)
    // comes back from the syscache 1-byte SHORT-header packed (the whole struct
    // shifted), so `ARR_ELEMTYPE` mis-reads (e.g. 256 instead of 26/OIDOID).
    // Detoast first (un-packs short / decompresses / fetches external), matching
    // `DatumGetArrayTypeP`; a no-op on an already-plain 4-byte array.
    let arr = detoast_seams::detoast_attr::call(mcx, bytes)?;
    let elems = arrayfuncs::construct::deconstruct_array_builtin(
        mcx, arr.as_slice(), OIDOID,
    )?;
    let mut out = alloc::vec::Vec::with_capacity(elems.len());
    for (d, _null) in elems.iter() {
        out.push(d.as_oid());
    }
    Ok(out)
}

/// `TextDatumGetCString(datum)` — the payload bytes of a `text` varlena as a
/// `String` (used for the stored `pg_node_tree` quals). C expands this to
/// `text_to_cstring(DatumGetTextPP(datum))`, i.e. `pg_detoast_datum_packed`
/// (decompress / fetch out-of-line) followed by `VARDATA_ANY` / `VARSIZE_ANY_EXHDR`.
///
/// A stored `pg_node_tree` for a policy whose USING qual contains a subquery
/// runs to a few KB, so it is routinely stored either inline-COMPRESSED or with a
/// 1-byte SHORT header (and out-of-line for the very largest). The previous
/// hand-rolled "assume a flat 4-byte header" path returned the raw compressed /
/// short-header bytes, so `stringToNode` saw the varlena header as its first
/// token (`unrecognized token`) — which on `ALTER POLICY ... TO role,...` (the
/// no-qual leg that re-reads the stored qual to rebuild dependencies) aborted the
/// command and left the new role out of `polroles` / `pg_shdepend`.
fn text_datum_to_string<'mcx>(mcx: Mcx<'mcx>, d: &Datum<'_>) -> PgResult<String> {
    let bytes = match d {
        Datum::ByRef(b) => b.as_slice(),
        _ => return Err(PgError::error("unexpected by-value Datum in pg_node_tree text")),
    };
    // DatumGetTextPP(datum): detoast only compressed / out-of-line values,
    // leaving an inline (short- or 4-byte-header) varlena.
    let packed =
        detoast_seams::pg_detoast_datum_packed::call(mcx, bytes)?;
    let p = packed.as_slice();
    if p.is_empty() {
        return Err(PgError::error("malformed text varlena in pg_policy qual"));
    }
    // VARDATA_ANY / VARSIZE_ANY_EXHDR: byte 0's low bit marks a 1-byte SHORT
    // header (length = byte0 >> 1, payload at offset 1); otherwise a 4-byte
    // header (length word = u32 >> 2, payload at offset 4).
    let payload: &[u8] = if (p[0] & 0x01) != 0 {
        let total = (p[0] >> 1) as usize;
        if total < VARHDRSZ_SHORT || total > p.len() {
            return Err(PgError::error("malformed short text varlena in pg_policy qual"));
        }
        &p[VARHDRSZ_SHORT..total]
    } else {
        if p.len() < VARHDRSZ {
            return Err(PgError::error("malformed text varlena in pg_policy qual"));
        }
        let word = u32::from_ne_bytes([p[0], p[1], p[2], p[3]]);
        let total = ((word >> 2) as usize).min(p.len());
        if total < VARHDRSZ {
            return Err(PgError::error("malformed text varlena in pg_policy qual"));
        }
        &p[VARHDRSZ..total]
    };
    Ok(String::from_utf8_lossy(payload).into_owned())
}

/// `VARHDRSZ` (varatt.h) — the 4-byte varlena header size.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (varatt.h) — the 1-byte short-header varlena size.
const VARHDRSZ_SHORT: usize = 1;

/// `RelationGetRelationName(rel)` (rel.h) — `rd_rel->relname` as a string.
fn rel_name(rel: &Relation<'_>) -> String {
    rel.name().to_string()
}

/* ===========================================================================
 * RangeVarCallbackForPolicy (policy.c:63-96)
 * ========================================================================= */

/// `RangeVarCallbackForPolicy` — callback to `RangeVarGetRelidExtended()`.
/// Checks the relation is a table, the current user owns it, and (unless
/// `allowSystemTableMods`) that it is not a system catalog. policy.c:63-96.
fn RangeVarCallbackForPolicy(rv: &RangeVar, relid: Oid, _oldrelid: Oid) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    // if (!HeapTupleIsValid(tuple)) return;
    if !syscache_seams::reloid_exists::call(relid)? {
        return Ok(());
    }

    // classform = (Form_pg_class) GETSTRUCT(tuple); relkind = classform->relkind;
    let relkind = lsyscache_seams::get_rel_relkind::call(relid)?;
    let relnamespace = lsyscache_seams::get_rel_namespace::call(relid)?;

    let relname: &str = &rv.relname;

    // Must own relation.
    // if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
    //     aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);
    if !aclchk_seams::object_ownercheck::call(RELATION_RELATION_ID, relid, GetUserId())? {
        let objtype = objectaddress_seams::get_relkind_objtype::call(relkind);
        aclchk_seams::aclcheck_error::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            objtype,
            Some(relname.to_string()),
        )?;
    }

    // No system table modifications unless explicitly allowed.
    // if (!allowSystemTableMods && IsSystemClass(relid, classform))
    if !guc_seams::allow_system_table_mods::call()
        && ::catalog_catalog::IsSystemClassByNamespace(relid, relnamespace)
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied: \"{relname}\" is a system catalog"))
            .into_error());
    }

    // Relation type MUST be a table.
    // if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{relname}\" is not a table"))
            .into_error());
    }

    // ReleaseSysCache(tuple); (no-op here)
    Ok(())
}

/* ===========================================================================
 * parse_policy_command (policy.c:107-129)
 * ========================================================================= */

/// `parse_policy_command` — convert a full command name (`all`/`select`/
/// `insert`/`update`/`delete`) to its single-char representation (`'*'` or the
/// corresponding `ACL_*_CHR`). policy.c:107-129.
fn parse_policy_command(cmd_name: Option<&str>) -> PgResult<i8> {
    // if (!cmd_name) elog(ERROR, "unrecognized policy command");
    let name = match cmd_name {
        Some(n) => n,
        None => return Err(PgError::error("unrecognized policy command")),
    };

    let polcmd: i8 = match name {
        "all" => b'*' as i8,
        "select" => ACL_SELECT_CHR,
        "insert" => ACL_INSERT_CHR,
        "update" => ACL_UPDATE_CHR,
        "delete" => ACL_DELETE_CHR,
        _ => return Err(PgError::error("unrecognized policy command")),
    };

    Ok(polcmd)
}

/* ===========================================================================
 * policy_role_list_to_array (policy.c:136-183)
 * ========================================================================= */

/// `policy_role_list_to_array` — convert a list of `RoleSpec`s to a `Vec` of
/// role-OID values. Handles the empty-list→PUBLIC default and the PUBLIC-collapse
/// WARNING. policy.c:136-183.
fn policy_role_list_to_array<'mcx>(
    mcx: Mcx<'mcx>,
    roles: &[::mcx::PgBox<'mcx, Node<'mcx>>],
) -> PgResult<alloc::vec::Vec<Oid>> {
    // Handle no roles being passed in as being for public.
    if roles.is_empty() {
        // *num_roles = 1; role_oids[0] = ACL_ID_PUBLIC;
        return Ok(alloc::vec![ACL_ID_PUBLIC]);
    }

    // *num_roles = list_length(roles);
    let mut role_oids: alloc::vec::Vec<Oid> = alloc::vec::Vec::with_capacity(roles.len());

    // foreach(cell, roles)
    for cell in roles.iter() {
        // RoleSpec *spec = lfirst(cell);
        let spec: &RoleSpec = match cell.as_rolespec() {
            Some(s) => s,
            None => {
                return Err(PgError::error(format!(
                    "policy_role_list_to_array: expected RoleSpec, got node tag {}",
                    cell.node_tag().0
                )))
            }
        };

        // PUBLIC covers all roles, so it only makes sense alone.
        if spec.roletype == RoleSpecType::Public {
            // if (*num_roles != 1) { WARNING; *num_roles = 1; }
            if roles.len() != 1 {
                ereport(WARNING)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("ignoring specified roles other than PUBLIC")
                    .errhint("All roles are members of the PUBLIC role.")
                    .finish(errloc(167, "policy_role_list_to_array"))?;
            }
            // role_oids[0] = ACL_ID_PUBLIC; return role_oids;
            return Ok(alloc::vec![ACL_ID_PUBLIC]);
        } else {
            // role_oids[i++] = ObjectIdGetDatum(get_rolespec_oid(spec, false));
            let spec_pn = to_parsenodes_rolespec(mcx, spec)?;
            let oid = get_rolespec_oid(&spec_pn, false)?;
            role_oids.push(oid);
        }
    }

    Ok(role_oids)
}

/* ===========================================================================
 * RelationBuildRowSecurity (policy.c:192-322)
 *
 * Ported in the relcache crate (`backend-utils-cache-relcache::derived`),
 * where it operates over `&mut RelationData` (relcache's own entry type) and
 * stores the `RowSecurityDesc` in `rd_rsdesc`. Not duplicated here.
 * ========================================================================= */

/* ===========================================================================
 * RemovePolicyById (policy.c:331-400)
 * ========================================================================= */

/// `RemovePolicyById` — remove a policy by its OID, erroring if no such policy
/// exists. policy.c:331-400.
pub fn RemovePolicyById(policy_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("pg_policy");
    let pg_policy_rel = table::table_open(ctx.mcx(), PolicyRelationId, RowExclusiveLock)?;

    // Find the policy to delete.
    let skey = [oid_key(Anum_pg_policy_oid, policy_id)?];

    let mut found: Option<(Oid, ::types_tuple::heaptuple::ItemPointerData)> = None;
    systable_scan_foreach(ctx.mcx(), &pg_policy_rel, PolicyOidIndexId, &skey, |row| {
        found = Some((row.form.polrelid, row.htup.tuple.t_self));
        Ok(false)
    })?;

    // If the policy exists, then remove it, otherwise raise an error.
    let (relid, tid) = match found {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "could not find tuple for policy {policy_id}"
            )))
        }
    };

    // Open and exclusive-lock the relation the policy belongs to.
    let rel_ctx = MemoryContext::new("pg_policy rel");
    let rel = relation::relation_open(rel_ctx.mcx(), relid, AccessExclusiveLock)?;
    let relkind = rel.rd_rel.relkind as u8;
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is not a table", rel_name(&rel)))
            .into_error());
    }

    if !guc_seams::allow_system_table_mods::call() && IsSystemRelation(&rel) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rel_name(&rel)
            ))
            .into_error());
    }

    // CatalogTupleDelete(pg_policy_rel, &tuple->t_self);
    indexing_seams::catalog_tuple_delete::call(&pg_policy_rel, tid)?;

    // CacheInvalidateRelcache(rel);
    inval::cache_invalidate::CacheInvalidateRelcache(&rel)?;

    rel.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;

    Ok(())
}

/* ===========================================================================
 * RemoveRoleFromObjectPolicy (policy.c:415-560)
 * ========================================================================= */

/// `RemoveRoleFromObjectPolicy` — remove a role from a policy's applicable-roles
/// list. Returns `true` if removed; `false` if removal would have emptied
/// `polroles` (the caller should then drop the policy). policy.c:415-560.
pub fn RemoveRoleFromObjectPolicy(roleid: Oid, classid: Oid, policy_id: Oid) -> PgResult<bool> {
    let mut keep_policy = true;

    // Assert(classid == PolicyRelationId);
    debug_assert_eq!(classid, PolicyRelationId);

    let ctx = MemoryContext::new("pg_policy");
    let pg_policy_rel = table::table_open(ctx.mcx(), PolicyRelationId, RowExclusiveLock)?;

    // Find the policy to update.
    let skey = [oid_key(Anum_pg_policy_oid, policy_id)?];

    let mut found: Option<(Oid, FormedTuple<'_>, alloc::vec::Vec<Oid>)> = None;
    systable_scan_foreach(ctx.mcx(), &pg_policy_rel, PolicyOidIndexId, &skey, |row| {
        // relid = GETSTRUCT(tuple)->polrelid;
        // policy_roles = DatumGetArrayTypePCopy(roles_datum);
        // roles = ARR_DATA_PTR; num_roles = ARR_DIMS[0];
        let roles = polroles_to_oids(ctx.mcx(), &row.polroles)?;
        found = Some((row.form.polrelid, row.htup.clone(), roles));
        Ok(false)
    })?;

    // Raise an error if we don't find the policy.
    let (relid, policy_tuple, roles) = match found {
        Some(t) => t,
        None => {
            return Err(PgError::error(format!(
                "could not find tuple for policy {policy_id}"
            )))
        }
    };

    // Rebuild the polroles array, without any mentions of the target role.
    // for (i=0,j=0; ...) if (roles[i] != roleid) role_oids[j++] = roles[i];
    let mut role_oids: alloc::vec::Vec<Oid> = alloc::vec::Vec::with_capacity(roles.len());
    for &r in roles.iter() {
        if r != roleid {
            role_oids.push(r);
        }
    }

    // If any roles remain, update the policy entry.
    if !role_oids.is_empty() {
        // replaces[polroles] = true; values[polroles] = construct_array_builtin(role_oids, ..);
        // new_tuple = heap_modify_tuple(...); CatalogTupleUpdate(...);
        let upd = PgPolicyUpdateRow {
            polroles: Some(role_oids.clone()),
            polqual: None,
            polwithcheck: None,
        };
        indexing_seams::catalog_tuple_update_pg_policy::call(
            ctx.mcx(),
            &pg_policy_rel,
            &policy_tuple,
            &upd,
        )?;

        // Remove all the old shared dependencies (roles).
        deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0)?;

        // Record the new shared dependencies (roles).
        let myself = ObjectAddress {
            classId: PolicyRelationId,
            objectId: policy_id,
            objectSubId: 0,
        };
        for &oid in role_oids.iter() {
            // no need for dependency on the public role
            if oid != ACL_ID_PUBLIC {
                let target = ObjectAddress {
                    classId: AUTH_ID_RELATION_ID,
                    objectId: oid,
                    objectSubId: 0,
                };
                recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY)?;
            }
        }

        // InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);
        objectaccess_seams::invoke_object_post_alter_hook::call(PolicyRelationId, policy_id, 0)?;

        // CommandCounterIncrement();
        CommandCounterIncrement()?;

        // Invalidate relcache entry for rel the policy belongs to. In case of a
        // race where the rel was just dropped, we need do nothing.
        if syscache_seams::reloid_exists::call(relid)? {
            inval_seams::cache_invalidate_relcache::call(relid)?;
        }
    } else {
        // No roles would remain, so drop the policy instead.
        keep_policy = false;
    }

    pg_policy_rel.close(RowExclusiveLock)?;

    Ok(keep_policy)
}

/* ===========================================================================
 * CreatePolicy (policy.c:568-759)
 * ========================================================================= */

/// `CreatePolicy` — execute the CREATE POLICY command. policy.c:568-759.
pub fn CreatePolicy<'mcx>(mcx: Mcx<'mcx>, stmt: &CreatePolicyStmt<'mcx>) -> PgResult<ObjectAddress> {
    // Parse command.
    let polcmd = parse_policy_command(stmt.cmd_name.as_ref().map(|s| s.as_str()))?;

    // If the command is SELECT or DELETE then WITH CHECK should be NULL.
    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt.with_check.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("WITH CHECK cannot be applied to SELECT or DELETE")
            .into_error());
    }

    // If the command is INSERT then WITH CHECK should be the only expression.
    if polcmd == ACL_INSERT_CHR && stmt.qual.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("only WITH CHECK expression allowed for INSERT")
            .into_error());
    }

    // Collect role ids.
    let role_oids = policy_role_list_to_array(mcx, &stmt.roles)?;

    // Parse the supplied clause.
    let mut qual_pstate = analyze_seams::make_parsestate::call(mcx, None)?;
    let mut with_check_pstate = analyze_seams::make_parsestate::call(mcx, None)?;

    // Get id of table.  Also handles permissions checks.
    let rv = expect_range_var(stmt.table.as_deref(), "CreatePolicy")?;
    let mut cb: RangeVarGetRelidCallbackBox = boxed_callback();
    let table_id = RangeVarGetRelidExtended(
        mcx,
        &rv,
        AccessExclusiveLock,
        0,
        Some(&mut *cb),
    )?;

    // Open target_table to build quals. No additional lock is necessary.
    let target_table = relation::relation_open(mcx, table_id, NoLock)?;

    // Add for the regular security quals + with-check quals.
    let nsitem = addRangeTableEntryForRelation(
        mcx,
        &mut qual_pstate,
        &target_table,
        AccessShareLock,
        None,
        false,
        false,
    )?;
    addNSItemToQuery(mcx, &mut qual_pstate, nsitem, false, true, true)?;

    let nsitem = addRangeTableEntryForRelation(
        mcx,
        &mut with_check_pstate,
        &target_table,
        AccessShareLock,
        None,
        false,
        false,
    )?;
    addNSItemToQuery(mcx, &mut with_check_pstate, nsitem, false, true, true)?;

    // qual = transformWhereClause(qual_pstate, stmt->qual, EXPR_KIND_POLICY, "POLICY");
    let qual = transformWhereClause(
        mcx,
        &mut qual_pstate,
        clone_clause(mcx, stmt.qual.as_deref())?,
        ParseExprKind::EXPR_KIND_POLICY,
        "POLICY",
    )?;
    let with_check_qual = transformWhereClause(
        mcx,
        &mut with_check_pstate,
        clone_clause(mcx, stmt.with_check.as_deref())?,
        ParseExprKind::EXPR_KIND_POLICY,
        "POLICY",
    )?;

    // Fix up collation information. The parser-arena `'static` quals are
    // brought into `mcx` (collation assignment mutates in place at `'mcx`, tying
    // pstate+expr to one lifetime; `Expr` is invariant so `clone_in` is required).
    let mut qual: Option<::nodes::primnodes::Expr<'mcx>> = match qual {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };
    if let Some(e) = qual.as_mut() {
        assign_expr_collations(Some(&qual_pstate), e)?;
    }
    let mut with_check_qual: Option<::nodes::primnodes::Expr<'mcx>> = match with_check_qual {
        Some(e) => Some(e.clone_in(mcx)?),
        None => None,
    };
    if let Some(e) = with_check_qual.as_mut() {
        assign_expr_collations(Some(&with_check_pstate), e)?;
    }

    // Open pg_policy catalog.
    let pg_policy_rel = table::table_open(mcx, PolicyRelationId, RowExclusiveLock)?;

    // Complain if the policy name already exists for the table.
    let policy_name = stmt
        .policy_name
        .as_ref()
        .map(|s| s.as_str().to_string())
        .expect("CreatePolicyStmt.policy_name is never NULL");

    let mut exists = false;
    let skey = [
        oid_key(Anum_pg_policy_polrelid, table_id)?,
        name_key(mcx, Anum_pg_policy_polname, &policy_name)?,
    ];
    systable_scan_foreach(mcx, &pg_policy_rel, PolicyPolrelidPolnameIndexId, &skey, |_row| {
        exists = true;
        Ok(false)
    })?;
    if exists {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "policy \"{policy_name}\" for table \"{}\" already exists",
                rel_name(&target_table)
            ))
            .into_error());
    }

    // values[polqual] = CStringGetTextDatum(nodeToString(qual)) ... or isnull.
    let qual_text = node_to_string_opt(mcx, qual.as_ref())?;
    let with_check_text = node_to_string_opt(mcx, with_check_qual.as_ref())?;

    // GetNewOidWithIndex + heap_form_tuple + CatalogTupleInsert.
    let row = PgPolicyInsertRow {
        polname: policy_name,
        polrelid: table_id,
        polcmd,
        polpermissive: stmt.permissive,
        polroles: role_oids.clone(),
        polqual: qual_text,
        polwithcheck: with_check_text,
    };
    let policy_id =
        indexing_seams::catalog_tuple_insert_pg_policy::call(mcx, &pg_policy_rel, &row)?;

    // Record Dependencies.
    let myself = ObjectAddress {
        classId: PolicyRelationId,
        objectId: policy_id,
        objectSubId: 0,
    };
    let target_rel = ObjectAddress {
        classId: RELATION_RELATION_ID,
        objectId: table_id,
        objectSubId: 0,
    };
    recordDependencyOn(mcx, &myself, &target_rel, DEPENDENCY_AUTO)?;

    if let Some(e) = qual.as_ref() {
        let node = wrap_expr(mcx, e)?;
        recordDependencyOnExpr(&myself, &node, &qual_pstate.p_rtable, DEPENDENCY_NORMAL)?;
    }
    if let Some(e) = with_check_qual.as_ref() {
        let node = wrap_expr(mcx, e)?;
        recordDependencyOnExpr(
            &myself,
            &node,
            &with_check_pstate.p_rtable,
            DEPENDENCY_NORMAL,
        )?;
    }

    // Register role dependencies.
    for &oid in role_oids.iter() {
        // no dependency if public
        if oid != ACL_ID_PUBLIC {
            let target = ObjectAddress {
                classId: AUTH_ID_RELATION_ID,
                objectId: oid,
                objectSubId: 0,
            };
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY)?;
        }
    }

    // InvokeObjectPostCreateHook(PolicyRelationId, policy_id, 0);
    objectaccess_seams::invoke_object_post_create_hook::call(PolicyRelationId, policy_id, 0)?;

    // Invalidate Relation Cache.
    inval::cache_invalidate::CacheInvalidateRelcache(&target_table)?;

    // Clean up.
    target_table.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;

    Ok(myself)
}

/* ===========================================================================
 * AlterPolicy (policy.c:767-1089)
 * ========================================================================= */

/// `AlterPolicy` — execute the ALTER POLICY command. policy.c:767-1089.
pub fn AlterPolicy<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterPolicyStmt<'mcx>) -> PgResult<ObjectAddress> {
    let mut role_oids: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
    let mut have_roles = false;

    // Parse role_ids.
    if !stmt.roles.is_empty() {
        role_oids = policy_role_list_to_array(mcx, &stmt.roles)?;
        have_roles = true;
    }

    // Get id of table.  Also handles permissions checks.
    let rv = expect_range_var(stmt.table.as_deref(), "AlterPolicy")?;
    let mut cb = boxed_callback();
    let table_id = RangeVarGetRelidExtended(mcx, &rv, AccessExclusiveLock, 0, Some(&mut *cb))?;

    let target_table = relation::relation_open(mcx, table_id, NoLock)?;

    // Parse the using policy clause.
    let mut qual: Option<::nodes::primnodes::Expr<'mcx>> = None;
    let mut qual_node: Option<Node<'mcx>> = None;
    let mut qual_pstate: Option<::mcx::PgBox<'mcx, ::nodes::parsestmt::ParseState<'mcx>>> = None;
    if stmt.qual.is_some() {
        let mut pstate = analyze_seams::make_parsestate::call(mcx, None)?;
        let nsitem = addRangeTableEntryForRelation(
            mcx,
            &mut pstate,
            &target_table,
            AccessShareLock,
            None,
            false,
            false,
        )?;
        addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
        let mut q = transformWhereClause(
            mcx,
            &mut pstate,
            clone_clause(mcx, stmt.qual.as_deref())?,
            ParseExprKind::EXPR_KIND_POLICY,
            "POLICY",
        )?;
        // Bring the parser-arena `'static` qual into `mcx` for the in-place
        // collation pass (pstate+expr share one invariant `'mcx`).
        let mut q: Option<::nodes::primnodes::Expr<'mcx>> = match q {
            Some(e) => Some(e.clone_in(mcx)?),
            None => None,
        };
        if let Some(e) = q.as_mut() {
            assign_expr_collations(Some(&pstate), e)?;
        }
        qual = q;
        qual_pstate = Some(pstate);
    }

    // Parse the with-check policy clause.
    let mut with_check_qual: Option<::nodes::primnodes::Expr<'mcx>> = None;
    let mut with_check_pstate: Option<::mcx::PgBox<'mcx, ::nodes::parsestmt::ParseState<'mcx>>> =
        None;
    if stmt.with_check.is_some() {
        let mut pstate = analyze_seams::make_parsestate::call(mcx, None)?;
        let nsitem = addRangeTableEntryForRelation(
            mcx,
            &mut pstate,
            &target_table,
            AccessShareLock,
            None,
            false,
            false,
        )?;
        addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
        let q = transformWhereClause(
            mcx,
            &mut pstate,
            clone_clause(mcx, stmt.with_check.as_deref())?,
            ParseExprKind::EXPR_KIND_POLICY,
            "POLICY",
        )?;
        // Bring the parser-arena `'static` qual into `mcx` for the in-place
        // collation pass (pstate+expr share one invariant `'mcx`).
        let mut q: Option<::nodes::primnodes::Expr<'mcx>> = match q {
            Some(e) => Some(e.clone_in(mcx)?),
            None => None,
        };
        if let Some(e) = q.as_mut() {
            assign_expr_collations(Some(&pstate), e)?;
        }
        with_check_qual = q;
        with_check_pstate = Some(pstate);
    }

    // Find policy to update.
    let pg_policy_rel = table::table_open(mcx, PolicyRelationId, RowExclusiveLock)?;

    let policy_name = stmt
        .policy_name
        .as_ref()
        .map(|s| s.as_str().to_string())
        .expect("AlterPolicyStmt.policy_name is never NULL");

    let skey = [
        oid_key(Anum_pg_policy_polrelid, table_id)?,
        name_key(mcx, Anum_pg_policy_polname, &policy_name)?,
    ];

    let mut found: Option<(FormedTuple<'_>, i8, Oid, Datum<'_>, Option<Datum<'_>>, Option<Datum<'_>>)> =
        None;
    systable_scan_foreach(mcx, &pg_policy_rel, PolicyPolrelidPolnameIndexId, &skey, |row| {
        found = Some((
            row.htup.clone(),
            row.form.polcmd,
            row.form.oid,
            row.polroles.clone(),
            row.polqual.clone(),
            row.polwithcheck.clone(),
        ));
        Ok(false)
    })?;

    // Check that the policy is found, raise an error if not.
    let (policy_tuple, polcmd, policy_id, stored_roles_datum, stored_qual_datum, stored_wc_datum) =
        match found {
            Some(t) => t,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!(
                        "policy \"{policy_name}\" for table \"{}\" does not exist",
                        rel_name(&target_table)
                    ))
                    .into_error())
            }
        };

    // If the command is SELECT or DELETE then WITH CHECK should be NULL.
    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt.with_check.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("only USING expression allowed for SELECT, DELETE")
            .into_error());
    }

    // If the command is INSERT then WITH CHECK should be the only expression.
    if polcmd == ACL_INSERT_CHR && stmt.qual.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("only WITH CHECK expression allowed for INSERT")
            .into_error());
    }

    // if (role_ids != NULL) replaces; else pull existing polroles for dependencies.
    let update_roles: Option<alloc::vec::Vec<Oid>>;
    if have_roles {
        update_roles = Some(role_oids.clone());
    } else {
        // nitems = ARR_DIMS(policy_roles)[0]; role_oids[i] = roles[i];
        role_oids = polroles_to_oids(mcx, &stored_roles_datum)?;
        update_roles = None;
    }

    // if (qual != NULL) replace; else pull stored USING expr + range table for deps.
    let replace_qual = qual.is_some();
    if qual.is_none() {
        if let Some(d) = stored_qual_datum.as_ref() {
            // qual = stringToNode(qual_value);
            let qual_value = text_datum_to_string(mcx, d)?;
            let node = read_seams::string_to_node::call(mcx, &qual_value)?;
            // Add this rel to the parsestate's rangetable, for dependencies.
            let mut pstate = analyze_seams::make_parsestate::call(mcx, None)?;
            let _ = addRangeTableEntryForRelation(
                mcx,
                &mut pstate,
                &target_table,
                AccessShareLock,
                None,
                false,
                false,
            )?;
            qual_node = Some((*node).clone_in(mcx)?);
            qual_pstate = Some(pstate);
        }
    } else {
        // Build the Node wrapper for the freshly transformed Expr (for deps).
        if let Some(e) = qual.as_ref() {
            qual_node = Some(wrap_expr_owned(mcx, e)?);
        }
    }

    // if (with_check_qual != NULL) replace; else pull stored WITH CHECK expr.
    let replace_with_check = with_check_qual.is_some();
    let mut with_check_node: Option<Node<'mcx>> = None;
    if with_check_qual.is_none() {
        if let Some(d) = stored_wc_datum.as_ref() {
            let wc_value = text_datum_to_string(mcx, d)?;
            let node = read_seams::string_to_node::call(mcx, &wc_value)?;
            let mut pstate = analyze_seams::make_parsestate::call(mcx, None)?;
            let _ = addRangeTableEntryForRelation(
                mcx,
                &mut pstate,
                &target_table,
                AccessShareLock,
                None,
                false,
                false,
            )?;
            with_check_node = Some((*node).clone_in(mcx)?);
            with_check_pstate = Some(pstate);
        }
    } else if let Some(e) = with_check_qual.as_ref() {
        with_check_node = Some(wrap_expr_owned(mcx, e)?);
    }

    // values[polqual] / values[polwithcheck] written only when the replace flag set.
    let qual_text: Option<Option<String>> = if replace_qual {
        Some(node_to_string_opt(mcx, qual.as_ref())?)
    } else {
        None
    };
    let with_check_text: Option<Option<String>> = if replace_with_check {
        Some(node_to_string_opt(mcx, with_check_qual.as_ref())?)
    } else {
        None
    };

    // heap_modify_tuple + CatalogTupleUpdate.
    let upd = PgPolicyUpdateRow {
        polroles: update_roles,
        polqual: qual_text,
        polwithcheck: with_check_text,
    };
    indexing_seams::catalog_tuple_update_pg_policy::call(
        mcx,
        &pg_policy_rel,
        &policy_tuple,
        &upd,
    )?;

    // Update Dependencies.
    deleteDependencyRecordsFor(PolicyRelationId, policy_id, false)?;

    // Record Dependencies.
    let myself = ObjectAddress {
        classId: PolicyRelationId,
        objectId: policy_id,
        objectSubId: 0,
    };
    let target_rel = ObjectAddress {
        classId: RELATION_RELATION_ID,
        objectId: table_id,
        objectSubId: 0,
    };
    recordDependencyOn(mcx, &myself, &target_rel, DEPENDENCY_AUTO)?;

    // recordDependencyOnExpr(&myself, qual, qual_parse_rtable, DEPENDENCY_NORMAL);
    if let Some(node) = qual_node.as_ref() {
        let rtable = qual_pstate
            .as_ref()
            .map(|p| p.p_rtable.as_slice())
            .unwrap_or(&[]);
        recordDependencyOnExpr(&myself, node, rtable, DEPENDENCY_NORMAL)?;
    }
    if let Some(node) = with_check_node.as_ref() {
        let rtable = with_check_pstate
            .as_ref()
            .map(|p| p.p_rtable.as_slice())
            .unwrap_or(&[]);
        recordDependencyOnExpr(&myself, node, rtable, DEPENDENCY_NORMAL)?;
    }

    // Register role dependencies.
    deleteSharedDependencyRecordsFor(PolicyRelationId, policy_id, 0)?;
    for &oid in role_oids.iter() {
        if oid != ACL_ID_PUBLIC {
            let target = ObjectAddress {
                classId: AUTH_ID_RELATION_ID,
                objectId: oid,
                objectSubId: 0,
            };
            recordSharedDependencyOn(&myself, &target, SHARED_DEPENDENCY_POLICY)?;
        }
    }

    // InvokeObjectPostAlterHook(PolicyRelationId, policy_id, 0);
    objectaccess_seams::invoke_object_post_alter_hook::call(PolicyRelationId, policy_id, 0)?;

    // Invalidate Relation Cache.
    inval::cache_invalidate::CacheInvalidateRelcache(&target_table)?;

    // Clean up.
    target_table.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;

    Ok(myself)
}

/* ===========================================================================
 * rename_policy (policy.c:1095-1195)
 * ========================================================================= */

/// `rename_policy` — change the name of a policy on a relation. policy.c:1095-1195.
///
/// `stmt` is the `parsenodes::RenameStmt` shape the ALTER dispatcher
/// (`commands/alter.c` `ExecRenameStmt`) hands the `OBJECT_POLICY` arm — its
/// `relation` is the already-resolved `RangeVar`, `subname`/`newname` are owned
/// `String`s.
pub fn rename_policy(
    mcx: Mcx<'_>,
    stmt: &parsenodes::RenameStmt,
) -> PgResult<ObjectAddress> {
    // Get id of table.  Also handles permissions checks.
    let rv = stmt
        .relation
        .as_ref()
        .expect("RenameStmt.relation is never NULL for OBJECT_POLICY")
        .clone();
    let mut cb = boxed_callback();
    let table_id = RangeVarGetRelidExtended(mcx, &rv, AccessExclusiveLock, 0, Some(&mut *cb))?;

    let target_table = relation::relation_open(mcx, table_id, NoLock)?;

    let pg_policy_rel = table::table_open(mcx, PolicyRelationId, RowExclusiveLock)?;

    // First pass -- check for conflict.
    let newname = stmt
        .newname
        .clone()
        .expect("RenameStmt.newname is never NULL");

    let mut conflict = false;
    let skey = [
        oid_key(Anum_pg_policy_polrelid, table_id)?,
        name_key(mcx, Anum_pg_policy_polname, &newname)?,
    ];
    systable_scan_foreach(mcx, &pg_policy_rel, PolicyPolrelidPolnameIndexId, &skey, |_row| {
        conflict = true;
        Ok(false)
    })?;
    if conflict {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "policy \"{newname}\" for table \"{}\" already exists",
                rel_name(&target_table)
            ))
            .into_error());
    }

    // Second pass -- find existing policy and update.
    let subname = stmt
        .subname
        .clone()
        .expect("RenameStmt.subname is never NULL for OBJECT_POLICY");

    let skey = [
        oid_key(Anum_pg_policy_polrelid, table_id)?,
        name_key(mcx, Anum_pg_policy_polname, &subname)?,
    ];

    let mut found: Option<(FormedTuple<'_>, Oid)> = None;
    systable_scan_foreach(mcx, &pg_policy_rel, PolicyPolrelidPolnameIndexId, &skey, |row| {
        found = Some((row.htup.clone(), row.form.oid));
        Ok(false)
    })?;

    // Complain if we did not find the policy.
    let (policy_tuple, opoloid) = match found {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "policy \"{subname}\" for table \"{}\" does not exist",
                    rel_name(&target_table)
                ))
                .into_error())
        }
    };

    // policy_tuple = heap_copytuple(policy_tuple);
    // namestrcpy(&GETSTRUCT(policy_tuple)->polname, stmt->newname);
    // CatalogTupleUpdate(pg_policy_rel, &policy_tuple->t_self, policy_tuple);
    indexing_seams::rename_policy_tuple::call(mcx, &pg_policy_rel, &policy_tuple, &newname)?;

    // InvokeObjectPostAlterHook(PolicyRelationId, opoloid, 0);
    objectaccess_seams::invoke_object_post_alter_hook::call(PolicyRelationId, opoloid, 0)?;

    // ObjectAddressSet(address, PolicyRelationId, opoloid);
    let address = ObjectAddress {
        classId: PolicyRelationId,
        objectId: opoloid,
        objectSubId: 0,
    };

    // Invalidate relation's relcache entry.
    inval::cache_invalidate::CacheInvalidateRelcache(&target_table)?;

    // Clean up.
    pg_policy_rel.close(RowExclusiveLock)?;
    target_table.close(NoLock)?;

    Ok(address)
}

/* ===========================================================================
 * get_relation_policy_oid (policy.c:1203-1250)
 * ========================================================================= */

/// `get_relation_policy_oid` — look up a policy by name to find its OID. When
/// `missing_ok` is false, errors if not found; otherwise returns `InvalidOid`.
/// policy.c:1203-1250.
pub fn get_relation_policy_oid(relid: Oid, policy_name: &str, missing_ok: bool) -> PgResult<Oid> {
    let ctx = MemoryContext::new("pg_policy");
    let pg_policy_rel = table::table_open(ctx.mcx(), PolicyRelationId, AccessShareLock)?;

    let skey = [
        oid_key(Anum_pg_policy_polrelid, relid)?,
        name_key(ctx.mcx(), Anum_pg_policy_polname, policy_name)?,
    ];

    let mut found: Option<Oid> = None;
    systable_scan_foreach(ctx.mcx(), &pg_policy_rel, PolicyPolrelidPolnameIndexId, &skey, |row| {
        found = Some(row.form.oid);
        Ok(false)
    })?;

    let policy_oid = match found {
        None => {
            if !missing_ok {
                let relname = lsyscache_seams::get_rel_name::call(ctx.mcx(), relid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_else(|| "(null)".to_string());
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!(
                        "policy \"{policy_name}\" for table \"{relname}\" does not exist"
                    ))
                    .into_error());
            }
            InvalidOid
        }
        Some(oid) => oid,
    };

    pg_policy_rel.close(AccessShareLock)?;

    Ok(policy_oid)
}

/* ===========================================================================
 * relation_has_policies (policy.c:1255-1279)
 * ========================================================================= */

/// `relation_has_policies` — determine whether a relation has any policies.
/// policy.c:1255-1279.
pub fn relation_has_policies(rel: &RelationData<'_>) -> PgResult<bool> {
    let ctx = MemoryContext::new("pg_policy");
    let catalog = table::table_open(ctx.mcx(), PolicyRelationId, AccessShareLock)?;

    let skey = [oid_key(Anum_pg_policy_polrelid, rel.rd_id)?];

    let mut ret = false;
    systable_scan_foreach(ctx.mcx(), &catalog, PolicyPolrelidPolnameIndexId, &skey, |_row| {
        ret = true;
        Ok(false)
    })?;

    catalog.close(AccessShareLock)?;

    Ok(ret)
}

/* ===========================================================================
 * shared helpers
 * ========================================================================= */

type RangeVarGetRelidCallbackBox =
    alloc::boxed::Box<dyn for<'a> FnMut(&'a RangeVar, Oid, Oid) -> PgResult<()>>;

/// Box the [`RangeVarCallbackForPolicy`] hook as the `RangeVarGetRelidCallback`
/// closure expected by `RangeVarGetRelidExtended`.
fn boxed_callback() -> RangeVarGetRelidCallbackBox {
    alloc::boxed::Box::new(|rv: &RangeVar, relid: Oid, oldrelid: Oid| {
        RangeVarCallbackForPolicy(rv, relid, oldrelid)
    })
}

/// Extract the `RangeVar` from a parse-node `table`/`relation` slot, converting
/// the owned-tree `rawnodes::RangeVar` to the resolved-form
/// `::types_tuple::access::RangeVar` that `RangeVarGetRelidExtended` consumes.
fn expect_range_var(node: Option<&Node<'_>>, funcname: &'static str) -> PgResult<RangeVar> {
    match node.and_then(|n| n.as_rangevar()) {
        Some(rv) => Ok(to_access_range_var(rv)),
        None => Err(PgError::error(format!(
            "{funcname}: expected RangeVar in table/relation slot"
        ))),
    }
}

/// Convert an owned-tree `rawnodes::RangeVar` to a resolved
/// `::types_tuple::access::RangeVar` (precedent: lockcmds `to_access_range_var`).
fn to_access_range_var(rv: &::nodes::rawnodes::RangeVar<'_>) -> RangeVar {
    RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// Convert a `ddlnodes::RoleSpec` (owned tree) to the `parsenodes::RoleSpec`
/// `get_rolespec_oid` consumes. Both carry `roletype` + `rolename`.
fn to_parsenodes_rolespec<'mcx>(
    mcx: Mcx<'mcx>,
    spec: &RoleSpec<'mcx>,
) -> PgResult<::nodes::parsenodes::RoleSpec<'mcx>> {
    Ok(::nodes::parsenodes::RoleSpec {
        roletype: spec.roletype,
        rolename: match &spec.rolename {
            Some(s) => Some(s.clone_in(mcx)?),
            None => None,
        },
    })
}

/// `copyObject(clause)` then transform — the grammar's raw clause is cloned into
/// `mcx` so the transform may consume it. Returns the owned clause node.
fn clone_clause<'mcx>(
    mcx: Mcx<'mcx>,
    clause: Option<&Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match clause {
        Some(n) => Ok(Some(n.clone_in(mcx)?)),
        None => Ok(None),
    }
}

/// Wrap a transformed `Expr` as a walkable `&Node` (borrowed, for the
/// dependency walkers).
fn wrap_expr<'mcx>(
    mcx: Mcx<'mcx>,
    e: &::nodes::primnodes::Expr,
) -> PgResult<Node<'mcx>> {
    Ok(Node::mk_expr(mcx, e.clone_in(mcx)?)?)
}

/// As [`wrap_expr`] but returning an owned `Node` value.
fn wrap_expr_owned<'mcx>(
    mcx: Mcx<'mcx>,
    e: &::nodes::primnodes::Expr,
) -> PgResult<Node<'mcx>> {
    Ok(Node::mk_expr(mcx, e.clone_in(mcx)?)?)
}

/// `CStringGetTextDatum(nodeToString(qual))` precursor — render the optional
/// transformed `Expr` to its `pg_node_tree` text (`None` when the qual is NULL).
fn node_to_string_opt<'mcx>(
    mcx: Mcx<'mcx>,
    qual: Option<&::nodes::primnodes::Expr>,
) -> PgResult<Option<String>> {
    match qual {
        Some(e) => {
            let node = Node::mk_expr(mcx, e.clone_in(mcx)?)?;
            let s = nodes_seams::node_to_string_with_locations::call(mcx, &node)?;
            Ok(Some(s.as_str().to_string()))
        }
        None => Ok(None),
    }
}

/* ===========================================================================
 * Inward seam installation
 * ========================================================================= */

pub fn init_seams() {
    use policy_seams as seams;
    use utility_out_seams as rt;

    seams::RemovePolicyById::set(RemovePolicyById);
    seams::remove_role_from_object_policy::set(RemoveRoleFromObjectPolicy);
    seams::get_relation_policy_oid::set(get_relation_policy_oid);
    seams::rename_policy::set(rename_policy);

    // ProcessUtilitySlow dispatch (utility.c): CREATE/ALTER POLICY. The C
    // `castNode(CreatePolicyStmt, parsetree)` is the runtime tag assert.
    rt::create_policy::set(|mcx, parsetree| match parsetree.as_createpolicystmt() {
        Some(stmt) => CreatePolicy(mcx, stmt),
        None => Err(::types_error::PgError::error(
            "create_policy: parse tree is not a CreatePolicyStmt",
        )),
    });
    rt::alter_policy::set(|mcx, parsetree| match parsetree.as_alterpolicystmt() {
        Some(stmt) => AlterPolicy(mcx, stmt),
        None => Err(::types_error::PgError::error(
            "alter_policy: parse tree is not an AlterPolicyStmt",
        )),
    });
}
