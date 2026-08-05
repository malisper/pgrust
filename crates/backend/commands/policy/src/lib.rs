// policy.c: CREATE/ALTER/DROP POLICY + rename_policy + the pg_policy scans.
// RelationBuildRowSecurity's scan half lives in relcache_build::policies
// (rules/trigdesc precedent); the rd_rsdesc analog is relcache::rowsecurity.
#![allow(non_snake_case)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::AUTH_ID_RELATION_ID;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_core::primitive::RegProcedure;
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, NOTICE, WARNING,
};
use types_nodes::parsenodes::{
    AlterPolicyStmt, CreatePolicyStmt, DropStmt, ObjectType, RenameStmt,
};
use types_nodes::{Node, NodeList};
use types_rel::pg_class::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};
use types_rel::{AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

use catalog_dependency::recordDependencyOnExpr;
use pg_shdepend::deleteSharedDependencyRecordsFor;
use parse_clause::transformWhereClause;
use parse_collate::assign_expr_collations;
use parse_relation::{addNSItemToQuery, addRangeTableEntryForRelation};
use parser_small1::{make_parsestate, ParseExprKind};
use pg_depend::{
    recordDependencyOn, DependencyType, ObjectAddress,
};

pub const POLICY_RELATION_ID: Oid = 3256;
const POLICY_OID_INDEX_ID: Oid = 3257;
const POLICY_POLRELID_POLNAME_INDEX_ID: Oid = 3258;
const Anum_pg_policy_oid: i32 = 1;
const Anum_pg_policy_polname: i32 = 2;
const Anum_pg_policy_polrelid: i32 = 3;
const Anum_pg_policy_polcmd: i32 = 4;
const Anum_pg_policy_polpermissive: i32 = 5;
const Anum_pg_policy_polroles: i32 = 6;
const Anum_pg_policy_polqual: i32 = 7;
const Anum_pg_policy_polwithcheck: i32 = 8;
const Natts_pg_policy: usize = 8;

const ACL_SELECT_CHR: u8 = b'r';
const ACL_INSERT_CHR: u8 = b'a';
const ACL_UPDATE_CHR: u8 = b'w';
const ACL_DELETE_CHR: u8 = b'd';
const ACL_ID_PUBLIC: Oid = 0;
const OIDOID: Oid = 26;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "identifier truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a pg_policy row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn text_attr<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    attno: i32,
) -> PgResult<Option<&'mcx str>> {
    let (d, isnull) = getattr(td, tup, attno);
    if isnull {
        return Ok(None);
    }
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes; length is
    // taken from its own header before slicing.
    let raw = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            detoast::varsize_any(core::slice::from_raw_parts(p, 2))
        } else if b0 & 0x01 != 0 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let image = detoast::detoast_attr(mcx, raw)?;
    let bytes = mcx::slice_borrow_in(mcx, &image[datum::varlena::VARHDRSZ..])?;
    Ok(Some(core::str::from_utf8(bytes).expect("pg_policy qual text is UTF-8")))
}

fn polroles_attr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>) -> PgResult<Vec<Oid>> {
    let (d, isnull) = getattr(td, tup, Anum_pg_policy_polroles);
    if isnull {
        return Err(Box::new(PgError::error("unexpected null value in pg_policy.polroles")));
    }
    let cx = mcx::MemoryContext::new("polroles");
    let mcx = cx.mcx();
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum addresses in-tuple bytes.
    let raw = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            detoast::varsize_any(core::slice::from_raw_parts(p, 2))
        } else if b0 & 0x01 != 0 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let image = detoast::detoast_attr(mcx, raw)?;
    let a = &image[datum::varlena::VARHDRSZ..];
    let word = |off: usize| u32::from_ne_bytes([a[off], a[off + 1], a[off + 2], a[off + 3]]);
    if word(0) != 1 || word(4) != 0 || word(8) != OIDOID {
        return Err(Box::new(PgError::error("unexpected pg_policy.polroles array shape")));
    }
    let nelems = word(12) as usize;
    Ok((0..nelems).map(|i| word(20 + 4 * i)).collect())
}

fn oid_array_datum<'mcx>(mcx: Mcx<'mcx>, oids: &[Oid]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, oids.len())?;
    v.extend(oids.iter().map(|&o| Datum::from_oid(o)));
    datum::array_build::construct_array_image(mcx, &v, OIDOID, 4, true, b'i')
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(mcx, s.as_bytes())?.into_image().leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

fn get_relkind_objtype(relkind: u8) -> ObjectType {
    match relkind {
        RELKIND_SEQUENCE => ObjectType::OBJECT_SEQUENCE,
        RELKIND_VIEW => ObjectType::OBJECT_VIEW,
        RELKIND_MATVIEW => ObjectType::OBJECT_MATVIEW,
        RELKIND_FOREIGN_TABLE => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    }
}

fn not_a_table(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{relname}\" is not a table"))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

fn system_catalog(relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("permission denied: \"{relname}\" is a system catalog"))
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
    )
}

fn policy_not_found(polname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "policy \"{polname}\" for table \"{relname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

fn policy_exists(polname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "policy \"{polname}\" for table \"{relname}\" already exists"
        ))
        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
    )
}

fn RangeVarCallbackForPolicy(rv: &rel_vocab::RangeVar<'_>, relid: Oid) -> PgResult<()> {
    let Some(shape) = syscache_seams::lookup_pg_class_ls_shape::call(relid)? else {
        return Ok(());
    };
    let relkind = shape.relkind as u8;

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            get_relkind_objtype(relkind),
            rv.relname,
        )?;
    }

    if !init_small::globals::allowSystemTableMods()
        && (catalog::IsCatalogRelationOid(relid) || catalog::IsToastNamespace(shape.relnamespace))
    {
        return Err(system_catalog(rv.relname));
    }

    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(not_a_table(rv.relname));
    }

    Ok(())
}

fn parse_policy_command(cmd_name: Option<&str>) -> PgResult<u8> {
    Ok(match cmd_name {
        Some("all") => b'*',
        Some("select") => ACL_SELECT_CHR,
        Some("insert") => ACL_INSERT_CHR,
        Some("update") => ACL_UPDATE_CHR,
        Some("delete") => ACL_DELETE_CHR,
        _ => return Err(Box::new(PgError::error("unrecognized policy command"))),
    })
}

fn policy_role_list_to_array(roles: &NodeList<'_>) -> PgResult<Vec<Oid>> {
    if roles.is_nil() {
        return Ok(vec![ACL_ID_PUBLIC]);
    }
    let mut role_oids: Vec<Oid> = Vec::with_capacity(roles.len());
    for cell in roles.iter() {
        let spec = cell.as_role_spec().expect("policy TO list cell is a RoleSpec");
        if spec.roletype == types_nodes::parsenodes::RoleSpecType::ROLESPEC_PUBLIC {
            if roles.len() != 1 {
                elog::ereport(WARNING)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("ignoring specified roles other than PUBLIC")
                    .errhint("All roles are members of the PUBLIC role.")
                    .finish(types_error::ErrorLocation::new(
                        "policy.c",
                        0,
                        "policy_role_list_to_array",
                    ))?;
            }
            return Ok(vec![ACL_ID_PUBLIC]);
        }
        role_oids.push(aclchk::get_rolespec_oid(spec, false)?);
    }
    Ok(role_oids)
}

fn to_rel_vocab_rv<'mcx>(prv: &types_nodes::primnodes::RangeVar<'mcx>) -> rel_vocab::RangeVar<'mcx> {
    rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    }
}

fn lock_table_for_policy<'mcx>(
    prv: &types_nodes::primnodes::RangeVar<'mcx>,
) -> PgResult<Oid> {
    let rv = to_rel_vocab_rv(prv);
    let mut callback = |rv: &rel_vocab::RangeVar<'_>, relid: Oid, _old: Oid| {
        RangeVarCallbackForPolicy(rv, relid)
    };
    catalog_namespace::RangeVarGetRelidExtended(&rv, AccessExclusiveLock, 0, Some(&mut callback))
}

struct PolicyQual<'mcx> {
    expr: Node<'mcx>,
    rtable: NodeList<'mcx>,
}

fn transform_policy_qual<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    raw: Node<'mcx>,
) -> PgResult<PolicyQual<'mcx>> {
    let mut pstate = make_parsestate(mcx, None);
    let nsitem = addRangeTableEntryForRelation(mcx, &mut pstate, rel, AccessShareLock, None, false, false)?;
    addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;
    let qual = transformWhereClause(mcx, &mut pstate, Some(raw), ParseExprKind::EXPR_KIND_POLICY, "POLICY")?
        .expect("policy qual transform yields an expression");
    assign_expr_collations(mcx, &pstate, qual)?;
    Ok(PolicyQual { expr: qual, rtable: pstate.p_rtable })
}

// Range table carrying only the policy's relation, for dependency extraction
// on the ALTER POLICY legs that re-read a stored qual.
fn stored_qual_rtable<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<NodeList<'mcx>> {
    let mut pstate = make_parsestate(mcx, None);
    let _ = addRangeTableEntryForRelation(mcx, &mut pstate, rel, AccessShareLock, None, false, false)?;
    Ok(pstate.p_rtable)
}

pub fn CreatePolicy<'mcx>(mcx: Mcx<'mcx>, stmt: &CreatePolicyStmt<'mcx>) -> PgResult<ObjectAddress> {
    let polcmd = parse_policy_command(stmt.cmd_name)?;

    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt.with_check.is_some() {
        return Err(Box::new(
            PgError::error("WITH CHECK cannot be applied to SELECT or DELETE")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if polcmd == ACL_INSERT_CHR && stmt.qual.is_some() {
        return Err(Box::new(
            PgError::error("only WITH CHECK expression allowed for INSERT")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }

    let role_oids = policy_role_list_to_array(&stmt.roles)?;

    let table_id = lock_table_for_policy(stmt.table.expect("CreatePolicyStmt.table"))?;
    let target_table = table::table_open(mcx, table_id, NoLock)?;

    let qual = match stmt.qual {
        Some(raw) => Some(transform_policy_qual(mcx, &target_table, raw)?),
        None => None,
    };
    let with_check = match stmt.with_check {
        Some(raw) => Some(transform_policy_qual(mcx, &target_table, raw)?),
        None => None,
    };

    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, RowExclusiveLock)?;

    let policy_name = stmt.policy_name.expect("CreatePolicyStmt.policy_name");
    if policy_scan_exists(mcx, &pg_policy_rel, table_id, policy_name)? {
        return Err(policy_exists(policy_name, target_table.name()));
    }

    let policy_id = catalog::GetNewOidWithIndex(
        mcx,
        &pg_policy_rel,
        POLICY_OID_INDEX_ID,
        Anum_pg_policy_oid as AttrNumber,
    )?;

    let mut values = [Datum::null(); Natts_pg_policy];
    let mut isnull = [true; Natts_pg_policy];
    let mut set = |anum: i32, v: Datum| {
        values[(anum - 1) as usize] = v;
        isnull[(anum - 1) as usize] = false;
    };
    let pname = name_arg(mcx, policy_name)?;
    set(Anum_pg_policy_oid, Datum::from_oid(policy_id));
    set(Anum_pg_policy_polname, Datum::from_usize(pname.as_ptr() as usize));
    set(Anum_pg_policy_polrelid, Datum::from_oid(table_id));
    set(Anum_pg_policy_polcmd, Datum::from_i8(polcmd as i8));
    set(Anum_pg_policy_polpermissive, Datum::from_bool(stmt.permissive));
    let roles_img = oid_array_datum(mcx, &role_oids)?;
    set(Anum_pg_policy_polroles, Datum::from_usize(roles_img.as_ptr() as usize));
    let qual_text = match &qual {
        Some(q) => Some(outfuncs::nodeToString(mcx, q.expr)?),
        None => None,
    };
    if let Some(t) = &qual_text {
        set(Anum_pg_policy_polqual, text_datum(mcx, t.as_str())?);
    }
    let wc_text = match &with_check {
        Some(q) => Some(outfuncs::nodeToString(mcx, q.expr)?),
        None => None,
    };
    if let Some(t) = &wc_text {
        set(Anum_pg_policy_polwithcheck, text_datum(mcx, t.as_str())?);
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, pg_policy_rel.descr(), &values, &isnull)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_policy_rel, &mut tuple)?;

    let myself = ObjectAddress::set(POLICY_RELATION_ID, policy_id);
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(RELATION_RELATION_ID, table_id),
        DependencyType::Auto,
    )?;
    if let Some(q) = &qual {
        recordDependencyOnExpr(mcx, &myself, q.expr, &q.rtable, DependencyType::Normal)?;
    }
    if let Some(q) = &with_check {
        recordDependencyOnExpr(mcx, &myself, q.expr, &q.rtable, DependencyType::Normal)?;
    }
    record_role_dependencies(mcx, &myself, &role_oids)?;

    inval::invalidate::CacheInvalidateRelcache(&target_table)?;

    target_table.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;
    Ok(myself)
}

fn record_role_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    myself: &ObjectAddress,
    role_oids: &[Oid],
) -> PgResult<()> {
    for &oid in role_oids {
        if oid != ACL_ID_PUBLIC {
            pg_shdepend::recordSharedDependencyOn(
                mcx,
                myself.classId,
                myself.objectId,
                AUTH_ID_RELATION_ID,
                oid,
                pg_shdepend::SharedDependencyType::Policy,
            )?;
        }
    }
    Ok(())
}

fn policy_scan_exists<'mcx>(
    mcx: Mcx<'mcx>,
    pg_policy_rel: &Relation<'mcx>,
    table_id: Oid,
    polname: &str,
) -> PgResult<bool> {
    let pname = name_arg(mcx, polname)?;
    let keys = [
        eq_key(Anum_pg_policy_polrelid as AttrNumber, F_OIDEQ, Datum::from_oid(table_id)),
        eq_key(
            Anum_pg_policy_polname as AttrNumber,
            F_NAMEEQ,
            Datum::from_usize(pname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        pg_policy_rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    Ok(found)
}

pub fn AlterPolicy<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterPolicyStmt<'mcx>) -> PgResult<ObjectAddress> {
    let mut role_oids: Vec<Oid> = Vec::new();
    let replace_roles = !stmt.roles.is_nil();
    if replace_roles {
        role_oids = policy_role_list_to_array(&stmt.roles)?;
    }

    let table_id = lock_table_for_policy(stmt.table.expect("AlterPolicyStmt.table"))?;
    let target_table = table::table_open(mcx, table_id, NoLock)?;

    let new_qual = match stmt.qual {
        Some(raw) => Some(transform_policy_qual(mcx, &target_table, raw)?),
        None => None,
    };
    let new_with_check = match stmt.with_check {
        Some(raw) => Some(transform_policy_qual(mcx, &target_table, raw)?),
        None => None,
    };

    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, RowExclusiveLock)?;
    let policy_name = stmt.policy_name.expect("AlterPolicyStmt.policy_name");

    let pname = name_arg(mcx, policy_name)?;
    let keys = [
        eq_key(Anum_pg_policy_polrelid as AttrNumber, F_OIDEQ, Datum::from_oid(table_id)),
        eq_key(
            Anum_pg_policy_polname as AttrNumber,
            F_NAMEEQ,
            Datum::from_usize(pname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_policy_rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let Some(policy_tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(policy_not_found(policy_name, target_table.name()));
    };
    let td = pg_policy_rel.descr();

    let (polcmd_d, _) = getattr(td, policy_tuple, Anum_pg_policy_polcmd);
    let polcmd = polcmd_d.as_u8();

    if (polcmd == ACL_SELECT_CHR || polcmd == ACL_DELETE_CHR) && stmt.with_check.is_some() {
        return Err(Box::new(
            PgError::error("only USING expression allowed for SELECT, DELETE")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if polcmd == ACL_INSERT_CHR && stmt.qual.is_some() {
        return Err(Box::new(
            PgError::error("only WITH CHECK expression allowed for INSERT")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }

    let (poid_d, _) = getattr(td, policy_tuple, Anum_pg_policy_oid);
    let policy_id = poid_d.as_oid();

    if !replace_roles {
        role_oids = polroles_attr(td, policy_tuple)?;
    }

    // Dependency sources: the freshly transformed exprs, or the stored ones
    // re-read from the catalog (C rebuilds the range table for both).
    let stored_qual: Option<PolicyQual<'mcx>> = if new_qual.is_some() {
        None
    } else {
        match text_attr(mcx, td, policy_tuple, Anum_pg_policy_polqual)? {
            Some(stored) => Some(PolicyQual {
                expr: readfuncs::stringToNode(mcx, stored)?,
                rtable: stored_qual_rtable(mcx, &target_table)?,
            }),
            None => None,
        }
    };
    let stored_with_check: Option<PolicyQual<'mcx>> = if new_with_check.is_some() {
        None
    } else {
        match text_attr(mcx, td, policy_tuple, Anum_pg_policy_polwithcheck)? {
            Some(stored) => Some(PolicyQual {
                expr: readfuncs::stringToNode(mcx, stored)?,
                rtable: stored_qual_rtable(mcx, &target_table)?,
            }),
            None => None,
        }
    };

    let natts = td.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    let roles_img;
    if replace_roles {
        roles_img = oid_array_datum(mcx, &role_oids)?;
        repl_values[(Anum_pg_policy_polroles - 1) as usize] =
            Datum::from_usize(roles_img.as_ptr() as usize);
        repl[(Anum_pg_policy_polroles - 1) as usize] = true;
    }
    if let Some(q) = &new_qual {
        let t = outfuncs::nodeToString(mcx, q.expr)?;
        repl_values[(Anum_pg_policy_polqual - 1) as usize] = text_datum(mcx, t.as_str())?;
        repl[(Anum_pg_policy_polqual - 1) as usize] = true;
    }
    if let Some(q) = &new_with_check {
        let t = outfuncs::nodeToString(mcx, q.expr)?;
        repl_values[(Anum_pg_policy_polwithcheck - 1) as usize] = text_datum(mcx, t.as_str())?;
        repl[(Anum_pg_policy_polwithcheck - 1) as usize] = true;
    }

    let mut new_tuple =
        heaptuple::heap_modify_tuple(mcx, policy_tuple, td, &repl_values, &repl_isnull, &repl)?;
    let otid = policy_tuple.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_policy_rel, &otid, &mut new_tuple)?;

    let _ = catalog_dependency::deleteDependencyRecordsFor(mcx, POLICY_RELATION_ID, policy_id, false)?;

    let myself = ObjectAddress::set(POLICY_RELATION_ID, policy_id);
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(RELATION_RELATION_ID, table_id),
        DependencyType::Auto,
    )?;
    if let Some(q) = new_qual.as_ref().or(stored_qual.as_ref()) {
        recordDependencyOnExpr(mcx, &myself, q.expr, &q.rtable, DependencyType::Normal)?;
    }
    if let Some(q) = new_with_check.as_ref().or(stored_with_check.as_ref()) {
        recordDependencyOnExpr(mcx, &myself, q.expr, &q.rtable, DependencyType::Normal)?;
    }

    deleteSharedDependencyRecordsFor(mcx, POLICY_RELATION_ID, policy_id, 0)?;
    record_role_dependencies(mcx, &myself, &role_oids)?;

    inval::invalidate::CacheInvalidateRelcache(&target_table)?;

    target_table.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;
    Ok(myself)
}

pub fn rename_policy<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<ObjectAddress> {
    let table_id = lock_table_for_policy(stmt.relation.expect("RenameStmt.relation"))?;
    let target_table = table::table_open(mcx, table_id, NoLock)?;
    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, RowExclusiveLock)?;

    let newname = stmt.newname.expect("RenameStmt.newname");
    let subname = stmt.subname.expect("RenameStmt.subname");

    if policy_scan_exists(mcx, &pg_policy_rel, table_id, newname)? {
        return Err(policy_exists(newname, target_table.name()));
    }

    let pname = name_arg(mcx, subname)?;
    let keys = [
        eq_key(Anum_pg_policy_polrelid as AttrNumber, F_OIDEQ, Datum::from_oid(table_id)),
        eq_key(
            Anum_pg_policy_polname as AttrNumber,
            F_NAMEEQ,
            Datum::from_usize(pname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_policy_rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let Some(policy_tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(policy_not_found(subname, target_table.name()));
    };
    let td = pg_policy_rel.descr();
    let (poid_d, _) = getattr(td, policy_tuple, Anum_pg_policy_oid);
    let opoloid = poid_d.as_oid();

    let natts = td.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    let nname = name_arg(mcx, newname)?;
    repl_values[(Anum_pg_policy_polname - 1) as usize] =
        Datum::from_usize(nname.as_ptr() as usize);
    repl[(Anum_pg_policy_polname - 1) as usize] = true;

    let mut new_tuple =
        heaptuple::heap_modify_tuple(mcx, policy_tuple, td, &repl_values, &repl_isnull, &repl)?;
    let otid = policy_tuple.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_policy_rel, &otid, &mut new_tuple)?;

    inval::invalidate::CacheInvalidateRelcache(&target_table)?;

    pg_policy_rel.close(RowExclusiveLock)?;
    target_table.close(NoLock)?;
    Ok(ObjectAddress::set(POLICY_RELATION_ID, opoloid))
}

pub fn RemovePolicyById<'mcx>(mcx: Mcx<'mcx>, policy_id: Oid) -> PgResult<()> {
    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, RowExclusiveLock)?;

    let keys = [eq_key(Anum_pg_policy_oid as AttrNumber, F_OIDEQ, Datum::from_oid(policy_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_policy_rel, POLICY_OID_INDEX_ID, true, None, &keys)?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "could not find tuple for policy {policy_id}"
        ))));
    };
    let td = pg_policy_rel.descr();
    let (relid_d, _) = getattr(td, tuple, Anum_pg_policy_polrelid);
    let relid = relid_d.as_oid();
    let tid = tuple.t_self;
    genam::systable_endscan(mcx, scan)?;

    let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;
    let relkind = rel.rd_rel.relkind as u8;
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(not_a_table(rel.name()));
    }
    if !init_small::globals::allowSystemTableMods() && catalog::IsSystemRelation(&rel) {
        return Err(system_catalog(rel.name()));
    }

    catalog_indexing::CatalogTupleDelete(&pg_policy_rel, &tid)?;

    inval::invalidate::CacheInvalidateRelcache(&rel)?;

    rel.close(NoLock)?;
    pg_policy_rel.close(RowExclusiveLock)?;
    Ok(())
}

pub fn RemoveRoleFromObjectPolicy<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    classid: Oid,
    policy_id: Oid,
) -> PgResult<bool> {
    debug_assert_eq!(classid, POLICY_RELATION_ID);
    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, RowExclusiveLock)?;

    let keys = [eq_key(Anum_pg_policy_oid as AttrNumber, F_OIDEQ, Datum::from_oid(policy_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_policy_rel, POLICY_OID_INDEX_ID, true, None, &keys)?;
    let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "could not find tuple for policy {policy_id}"
        ))));
    };
    let td = pg_policy_rel.descr();
    let (relid_d, _) = getattr(td, tuple, Anum_pg_policy_polrelid);
    let relid = relid_d.as_oid();

    let roles = polroles_attr(td, tuple)?;
    let remaining: Vec<Oid> = roles.iter().copied().filter(|&r| r != roleid).collect();

    let keep_policy = !remaining.is_empty();
    if keep_policy {
        let natts = td.natts as usize;
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        let roles_img = oid_array_datum(mcx, &remaining)?;
        repl_values[(Anum_pg_policy_polroles - 1) as usize] =
            Datum::from_usize(roles_img.as_ptr() as usize);
        repl[(Anum_pg_policy_polroles - 1) as usize] = true;
        let mut new_tuple =
            heaptuple::heap_modify_tuple(mcx, tuple, td, &repl_values, &repl_isnull, &repl)?;
        let otid = tuple.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &pg_policy_rel, &otid, &mut new_tuple)?;

        deleteSharedDependencyRecordsFor(mcx, POLICY_RELATION_ID, policy_id, 0)?;
        let myself = ObjectAddress::set(POLICY_RELATION_ID, policy_id);
        record_role_dependencies(mcx, &myself, &remaining)?;

        xact::CommandCounterIncrement()?;

        if syscache_seams::lookup_pg_class_by_relid::call(relid)?.is_some() {
            inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
        }
    } else {
        genam::systable_endscan(mcx, scan)?;
    }

    pg_policy_rel.close(RowExclusiveLock)?;
    Ok(keep_policy)
}

pub fn get_relation_policy_oid<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    policy_name: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    let pg_policy_rel = table::table_open(mcx, POLICY_RELATION_ID, AccessShareLock)?;
    let pname = name_arg(mcx, policy_name)?;
    let keys = [
        eq_key(Anum_pg_policy_polrelid as AttrNumber, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(
            Anum_pg_policy_polname as AttrNumber,
            F_NAMEEQ,
            Datum::from_usize(pname.as_ptr() as usize),
        ),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_policy_rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let policy_oid = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tuple) => {
            let (d, _) = getattr(pg_policy_rel.descr(), tuple, Anum_pg_policy_oid);
            d.as_oid()
        }
        None => {
            if !missing_ok {
                let relname = lsyscache::relation::get_rel_name(mcx, relid)?
                    .map(|n| n.as_str().to_string())
                    .unwrap_or_default();
                return Err(policy_not_found(policy_name, &relname));
            }
            InvalidOid
        }
    };
    genam::systable_endscan(mcx, scan)?;
    pg_policy_rel.close(AccessShareLock)?;
    Ok(policy_oid)
}

pub fn relation_has_policies<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<bool> {
    let catalog_rel = table::table_open(mcx, POLICY_RELATION_ID, AccessShareLock)?;
    let keys =
        [eq_key(Anum_pg_policy_polrelid as AttrNumber, F_OIDEQ, Datum::from_oid(rel.rd_id))];
    let mut scan = genam::systable_beginscan(
        mcx,
        &catalog_rel,
        POLICY_POLRELID_POLNAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let ret = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    catalog_rel.close(AccessShareLock)?;
    Ok(ret)
}

fn name_list_to_string(names: &NodeList<'_>, upto: usize) -> String {
    let mut out = String::new();
    for (i, n) in names.iter().enumerate() {
        if i >= upto {
            break;
        }
        if i > 0 {
            out.push('.');
        }
        out.push_str(n.as_string().expect("name list component").sval);
    }
    out
}

// RemoveObjects (dropcmds.c), OBJECT_POLICY arm only: address lookup per
// get_object_address_relobject + relation-owner check, then
// performMultipleDeletions over doDeletion's policy arm.
pub fn RemovePolicyObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    debug_assert_eq!(stmt.removeType, ObjectType::OBJECT_POLICY);
    let mut objects = catalog_dependency::ObjectAddresses::new();

    for cell in stmt.objects.iter() {
        let names = cell.as_list().expect("DROP POLICY object is a name list");
        let nnames = names.len();
        if nnames < 2 {
            return Err(Box::new(
                PgError::error("must specify relation and object name")
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
        let polname = names.nth(nnames - 1).as_string().expect("policy name").sval;

        let mut rel_parts: Vec<&str> = Vec::with_capacity(nnames - 1);
        for (i, n) in names.iter().enumerate() {
            if i < nnames - 1 {
                rel_parts.push(n.as_string().expect("name list component").sval);
            }
        }
        let mut rv = rel_vocab::RangeVar {
            catalogname: None,
            schemaname: None,
            relname: "",
            inh: true,
            relpersistence: types_core::RELPERSISTENCE_PERMANENT,
            location: -1,
        };
        match rel_parts.as_slice() {
            [r] => rv.relname = r,
            [s, r] => {
                rv.schemaname = Some(s);
                rv.relname = r;
            }
            [c, s, r] => {
                rv.catalogname = Some(c);
                rv.schemaname = Some(s);
                rv.relname = r;
            }
            _ => {
                return Err(Box::new(
                    PgError::error("improper relation name (too many dotted names)")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        }

        let relid = catalog_namespace::RangeVarGetRelidExtended(
            &rv,
            AccessShareLock,
            if stmt.missing_ok { catalog_namespace::RVR_MISSING_OK } else { 0 },
            None,
        )?;
        if relid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "relation \"{}\" does not exist, skipping",
                    name_list_to_string(&names, nnames - 1)
                ),
                None,
            )?;
            continue;
        }

        let policy_oid = get_relation_policy_oid(mcx, relid, polname, stmt.missing_ok)?;
        if policy_oid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "policy \"{polname}\" for relation \"{}\" does not exist, skipping",
                    name_list_to_string(&names, nnames - 1)
                ),
                None,
            )?;
            continue;
        }

        // check_object_ownership (objectaddress.c): must own the relation.
        if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, miscinit::GetUserId())? {
            let relname = lsyscache::relation::get_rel_name(mcx, relid)?
                .map(|n| n.as_str().to_string())
                .unwrap_or_default();
            aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, ObjectType::OBJECT_POLICY, &relname)?;
        }

        objects.add_exact_object_address(ObjectAddress::set(POLICY_RELATION_ID, policy_oid));
    }

    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)
}

pub fn init_seams() {
    policy_seams::remove_policy_by_id::set(RemovePolicyById);
    policy_seams::remove_role_from_object_policy::set(RemoveRoleFromObjectPolicy);
}
