// AlterTable three-phase machinery (ATController), ADD/DROP COLUMN slice.
// LOUD: every other subtype, inheritance children, table rewrites (volatile
// defaults), verify scans, partitions. Ownership checks ride the aclchk lane
// (superuser fast path only), as in DefineRelation.
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, RELATION_RELATION_ID, TYPE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_TOO_MANY_COLUMNS, ERRCODE_UNDEFINED_COLUMN,
    ERROR, NOTICE,
};
use types_nodes::parsenodes::{AlterTableCmd, AlterTableStmt, AlterTableType};
use types_nodes::rawnodes::ColumnDef;
use types_nodes::{Node, NodeList};
use types_rel::{AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, ShareRowExclusiveLock, LOCKMODE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::MaxHeapAttributeNumber;

const AT_NUM_PASSES: usize = 12;
const AT_PASS_DROP: usize = 0;
const AT_PASS_ADD_COL: usize = 2;
const AT_PASS_ADD_CONSTR: usize = 6;
const AT_REWRITE_DEFAULT_VAL: i32 = 1 << 1;

const AttributeRelidNumIndexId: Oid = 2659;
const InheritsRelationId: Oid = 2611;
const InheritsParentIndexId: Oid = 2187;
const Anum_pg_inherits_inhparent: usize = 2;
const Anum_pg_class_relnatts: usize = 19;
const CollationRelationId: Oid = 3456;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: tablecmds ALTER {what}")
}

fn oid_key(attno: usize, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub fn AlterTableGetLockLevel(cmds: &NodeList<'_>) -> LOCKMODE {
    for cnode in cmds.iter() {
        let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
        match cmd.subtype {
            AlterTableType::AT_AddColumn | AlterTableType::AT_DropColumn => {
                return AccessExclusiveLock;
            }
            AlterTableType::AT_AddConstraint => {}
            other => unported(&format!("AlterTableGetLockLevel {other:?}")),
        }
    }
    // C computes the max across subcommands; AT_AddConstraint alone is
    // ShareRowExclusiveLock, any column change escalates above.
    ShareRowExclusiveLock
}

pub fn AlterTableLookupRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableStmt<'_>,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let prv = stmt.relation.expect("AlterTableStmt.relation");
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let mut callback = |rv: &rel_vocab::RangeVar<'_>, relOid: Oid, _old: Oid| {
        RangeVarCallbackForAlterRelation(mcx, rv, relOid)
    };
    let flags = if stmt.missing_ok { catalog_namespace::RVR_MISSING_OK } else { 0 };
    catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, flags, Some(&mut callback))
}

// RangeVarCallbackForAlterRelation slice: relkind gate + superuser ownership
// fast path (per-role object_ownercheck rides the aclchk lane).
fn RangeVarCallbackForAlterRelation<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &rel_vocab::RangeVar<'_>,
    relOid: Oid,
) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
    }
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        unported("RangeVarCallbackForAlterRelation: object_ownercheck for non-superusers");
    }
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_key(1, relOid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        genam::systable_endscan(mcx, scan)?;
        pg_class.close(types_rel::AccessShareLock)?;
        return Ok(());
    };
    let desc = pg_class.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
    let relnamespace = unsafe { types_tuple::heap_getattr(tup, 3, desc, &mut isnull) }.as_oid();
    // SAFETY: as above.
    let relkind =
        unsafe { types_tuple::heap_getattr(tup, 18, desc, &mut isnull) }.as_i8() as u8;
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;

    if relkind != RELKIND_RELATION {
        unported("RangeVarCallbackForAlterRelation: non-plain-table relkind");
    }
    let is_system =
        catalog::IsCatalogRelationOid(relOid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{}\" is a system catalog", rel.relname),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

struct AlteredTableInfo<'mcx> {
    relid: Oid,
    subcmds: [NodeList<'mcx>; AT_NUM_PASSES],
    rewrite: i32,
    has_newvals: bool,
    verify_new_notnull: bool,
}

pub fn AlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    lockmode: LOCKMODE,
    stmt: &AlterTableStmt<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relid, NoLock)?;
    // CheckAlterTableIsSafe: other-session temp tables are unreachable
    // (temp relations unported).
    catalog_heap::CheckTableNotInUse(&rel, "ALTER TABLE")?;
    let recurse = stmt.relation.expect("AlterTableStmt.relation").inh;
    ATController(mcx, rel, &stmt.cmds, recurse, lockmode, query_string)
}

fn ATController<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    cmds: &NodeList<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    let mut tab = AlteredTableInfo {
        relid: rel.rd_id,
        subcmds: core::array::from_fn(|_| NodeList::nil()),
        rewrite: 0,
        has_newvals: false,
        verify_new_notnull: false,
    };

    for cnode in cmds.iter() {
        ATPrepCmd(mcx, &mut tab, &rel, cnode, recurse)?;
    }
    rel.close(NoLock)?;

    ATRewriteCatalogs(mcx, &mut tab, lockmode, query_string)?;
    ATRewriteTables(&tab);
    Ok(())
}

// ATPrepCmd: the statement arena is single-use, so the subcommand is
// scribbled on in place instead of C's copyObject.
fn ATPrepCmd<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    recurse: bool,
) -> PgResult<()> {
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    // ATSimplePermissions relkind gate; ownership was checked at lookup.
    if rel.rd_rel.relkind != RELKIND_RELATION {
        unported("ATSimplePermissions: non-plain-table relkind");
    }
    let pass = match cmd.subtype {
        AlterTableType::AT_AddColumn => {
            // ATPrepAddColumn: typed-table/composite arms unreachable.
            if recurse {
                // SAFETY: parse tree is statement-owned; no derived refs live.
                unsafe {
                    cnode.with_mut::<AlterTableCmd, _>(|c| c.recurse = true).expect("AlterTableCmd");
                }
            }
            AT_PASS_ADD_COL
        }
        AlterTableType::AT_DropColumn => {
            if recurse {
                // SAFETY: as above.
                unsafe {
                    cnode.with_mut::<AlterTableCmd, _>(|c| c.recurse = true).expect("AlterTableCmd");
                }
            }
            AT_PASS_DROP
        }
        AlterTableType::AT_AddConstraint => {
            if recurse {
                // SAFETY: as above.
                unsafe {
                    cnode.with_mut::<AlterTableCmd, _>(|c| c.recurse = true).expect("AlterTableCmd");
                }
            }
            AT_PASS_ADD_CONSTR
        }
        other => unported(&format!("ATPrepCmd {other:?}")),
    };
    tab.subcmds[pass].lappend(mcx, cnode)?;
    Ok(())
}

fn ATRewriteCatalogs<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    _lockmode: LOCKMODE,
    query_string: &str,
) -> PgResult<()> {
    for pass in 0..AT_NUM_PASSES {
        if tab.subcmds[pass].is_nil() {
            continue;
        }
        let mut nodes: mcx::PgVec<'_, Node<'mcx>> = mcx::PgVec::new_in(mcx);
        for c in tab.subcmds[pass].iter() {
            nodes.push(c);
        }
        for &cnode in nodes.iter() {
            let rel = table::table_open(mcx, tab.relid, NoLock)?;
            let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
            match cmd.subtype {
                AlterTableType::AT_AddColumn => {
                    ATExecAddColumn(mcx, tab, &rel, cnode, query_string)?;
                }
                AlterTableType::AT_DropColumn => {
                    ATExecDropColumn(mcx, &rel, cmd)?;
                }
                AlterTableType::AT_AddConstraint => {
                    let cons = cmd
                        .def
                        .expect("AT_AddConstraint def")
                        .as_variant::<types_nodes::rawnodes::Constraint>()
                        .expect("Constraint");
                    crate::fk::ATExecAddConstraint(mcx, &rel, cons)?;
                }
                other => unported(&format!("ATExecCmd {other:?}")),
            }
            rel.close(NoLock)?;
        }
    }
    // AlterTableCreateToastTable: a no-op when a toast table already exists
    // or none is needed.
    catalog_toasting::NewRelationCreateToastTable(mcx, tab.relid)
}

fn ATRewriteTables(tab: &AlteredTableInfo<'_>) {
    // find_composite_type_dependencies: composite-type columns are unported,
    // so no dependent rowtype uses can exist.
    if tab.rewrite > 0 {
        unported("ATRewriteTable rewrite (volatile-default ADD COLUMN)");
    }
    if tab.verify_new_notnull {
        unported("ATRewriteTable verify scan (NOT NULL over existing rows)");
    }
    let _ = tab.has_newvals;
}

fn ATExecAddColumn<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    cnode: Node<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    let myrelid = rel.rd_id;
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let if_not_exists = cmd.missing_ok;
    let defnode = cmd.def.expect("AT_AddColumn ColumnDef");
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
    debug_assert!(col_def.inhcount == 0);
    let colname = col_def.colname.expect("ColumnDef.colname");
    let relname = rel.name().to_string();

    if !check_for_column_name_collision(mcx, myrelid, &relname, colname, if_not_exists)? {
        return Ok(());
    }

    parse_utilcmd::transformAlterTableCmd(mcx, &relname, cnode)?;
    let col_def = defnode.as_variant::<ColumnDef>().expect("ColumnDef");

    let pgclass = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_key(1, myrelid);
    let mut scan =
        genam::systable_beginscan(mcx, &pgclass, catalog::ClassOidIndexId, true, None, &[key])?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {myrelid}"));
    let cdesc = pgclass.descr();
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_class column under pg_class's descriptor.
    let relnatts = unsafe {
        types_tuple::heap_getattr(reltup, Anum_pg_class_relnatts as i32, cdesc, &mut isnull)
    }
    .as_i16();
    let newattnum = relnatts as i32 + 1;
    if newattnum > MaxHeapAttributeNumber {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("tables can have at most {MaxHeapAttributeNumber} columns"),
            )
            .with_sqlstate(ERRCODE_TOO_MANY_COLUMNS),
        ));
    }

    let elts = NodeList::make1(mcx, defnode)?;
    let mut tupdesc = crate::BuildDescForRelation(mcx, &elts)?;
    tupdesc.attr_mut(0).attnum = newattnum as AttrNumber;
    // CheckAttributeType surrogate; the name half re-proves what the
    // collision check established.
    catalog_heap::CheckAttributeNamesTypes(&tupdesc, RELKIND_RELATION)?;
    let attribute = tupdesc.attrs[0];

    let attrdesc = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &attrdesc)?;
    catalog_heap::insert_pg_attribute_tuple(mcx, &attrdesc, &attribute, myrelid, &mut indstate)?;
    catalog_indexing::CatalogCloseIndexes(indstate)?;
    attrdesc.close(RowExclusiveLock)?;

    let natts = cdesc.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_class_relnatts - 1] = Datum::from_i16(newattnum as i16);
    repl[Anum_pg_class_relnatts - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, reltup, cdesc, &repl_values, &repl_isnull, &repl)?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pgclass, &otid, &mut newtup)?;
    pgclass.close(RowExclusiveLock)?;

    xact::CommandCounterIncrement()?;

    if let Some(raw_default) = col_def.raw_default {
        // AddRelationNewConstraints over the one RawColumnDefault; the rel
        // must be re-opened to see the new attribute (C rebuilds in place).
        let rel2 = table::table_open(mcx, myrelid, NoLock)?;
        crate::constraints::add_relation_new_constraints(
            mcx,
            &rel2,
            &[(newattnum as AttrNumber, raw_default)],
            &NodeList::nil(),
            query_string,
        )?;
        rel2.close(NoLock)?;
        xact::CommandCounterIncrement()?;
    }

    // Phase-3 fill / attmissingval fast path. Domain and generated columns
    // are unreachable (loud at parse), so defval exists iff atthasdef.
    let mut has_missing = false;
    let rel3 = table::table_open(mcx, myrelid, NoLock)?;
    if rel3.rd_att.attr(newattnum as usize - 1).atthasdef {
        let defval = rewrite_handler::build_column_default(mcx, &rel3, newattnum as usize)?;
        let defval = clauses::eval_const_expressions(mcx, defval)?;
        tab.has_newvals = true;
        if !clauses::contain_volatile_functions(defval)? {
            let mut state = execexpr::exec_init_expr(mcx, Some(defval), execexpr::ParamBind::NONE)?
                .expect("non-nil default expression");
            let mut slots =
                execexpr::EvalSlots { scan: None, inner: None, outer: None };
            let r = execexpr::exec_eval_expr(&mut state, &mut slots)?;
            if !r.isnull {
                catalog_heap::StoreAttrMissingVal(mcx, &rel3, newattnum as AttrNumber, r.value)?;
                xact::CommandCounterIncrement()?;
                has_missing = true;
            }
        } else {
            tab.rewrite |= AT_REWRITE_DEFAULT_VAL;
        }
    }
    if !has_missing {
        tab.verify_new_notnull |= col_def.is_not_null;
    }

    let myself =
        pg_depend::ObjectAddress::sub_set(RELATION_RELATION_ID, myrelid, newattnum);
    let referenced = pg_depend::ObjectAddress::set(TYPE_RELATION_ID, attribute.atttypid);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, pg_depend::DependencyType::Normal)?;
    if attribute.attcollation != InvalidOid && attribute.attcollation != DEFAULT_COLLATION_OID {
        let referenced = pg_depend::ObjectAddress::set(CollationRelationId, attribute.attcollation);
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &referenced,
            pg_depend::DependencyType::Normal,
        )?;
    }
    rel3.close(NoLock)?;

    if find_inheritance_children_exist(mcx, myrelid)? {
        unported("ATExecAddColumn inheritance recursion");
    }
    Ok(())
}

// check_for_column_name_collision: deliberately not attisdropped-aware.
fn check_for_column_name_collision<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relname: &str,
    colname: &str,
    if_not_exists: bool,
) -> PgResult<bool> {
    let Some((attnum, _)) = attname_lookup(mcx, relid, colname, true)? else {
        return Ok(true);
    };
    if attnum <= 0 {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column name \"{colname}\" conflicts with a system column name"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
        ));
    }
    if if_not_exists {
        elog_seams::ereport::call(
            PgError::new(
                NOTICE,
                format!("column \"{colname}\" of relation \"{relname}\" already exists, skipping"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
        )?;
        return Ok(false);
    }
    Err(Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" of relation \"{relname}\" already exists"),
        )
        .with_sqlstate(ERRCODE_DUPLICATE_COLUMN),
    ))
}

// SearchSysCache(ATTNAME) surrogate: pg_attribute scan filtered by name.
// include_dropped mirrors SearchSysCache2 (collision check) vs
// SearchSysCacheAttName (skips dropped).
fn attname_lookup<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    colname: &str,
    include_dropped: bool,
) -> PgResult<Option<(i16, i16)>> {
    let attrel = table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
    let key = oid_key(1, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &attrel, AttributeRelidNumIndexId, true, None, &[key])?;
    let desc = attrel.descr();
    let mut found: Option<(i16, i16)> = None;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_attribute columns under its descriptor.
        let name = unsafe { types_tuple::heap_getattr(tup, 2, desc, &mut isnull) };
        let name = unsafe {
            core::slice::from_raw_parts(name.as_usize() as *const u8, 64)
        };
        let len = name.iter().position(|&b| b == 0).unwrap_or(64);
        if &name[..len] != colname.as_bytes() {
            continue;
        }
        let dropped =
            unsafe { types_tuple::heap_getattr(tup, 17, desc, &mut isnull) }.as_bool();
        if dropped && !include_dropped {
            continue;
        }
        let attnum = unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_i16();
        let inhcount =
            unsafe { types_tuple::heap_getattr(tup, 19, desc, &mut isnull) }.as_i16();
        found = Some((attnum, inhcount));
        break;
    }
    genam::systable_endscan(mcx, scan)?;
    attrel.close(types_rel::AccessShareLock)?;
    Ok(found)
}

fn ATExecDropColumn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
) -> PgResult<()> {
    let col_name = cmd.name.expect("AT_DropColumn name");
    let relname = rel.name().to_string();

    let Some((attnum, attinhcount)) = attname_lookup(mcx, rel.rd_id, col_name, false)? else {
        if !cmd.missing_ok {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "column \"{col_name}\" of relation \"{relname}\" does not exist"
                    ),
                )
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!("column \"{col_name}\" of relation \"{relname}\" does not exist, skipping"),
            None,
        )?;
        return Ok(());
    };

    if attnum <= 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot drop system column \"{col_name}\""))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if attinhcount > 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot drop inherited column \"{col_name}\""))
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    // has_partition_attrs: false for plain relations (partitioning unported).

    if find_inheritance_children_exist(mcx, rel.rd_id)? {
        unported("ATExecDropColumn inheritance recursion");
    }

    let mut addrs = catalog_dependency::ObjectAddresses::new();
    addrs.add_exact_object_address(pg_depend::ObjectAddress::sub_set(
        RELATION_RELATION_ID,
        rel.rd_id,
        attnum as i32,
    ));
    catalog_dependency::performMultipleDeletions(
        mcx,
        &addrs,
        cmd.behavior,
        0,
    )
}

fn find_inheritance_children_exist<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, InheritsRelationId, types_rel::AccessShareLock)?;
    let key = oid_key(Anum_pg_inherits_inhparent, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsParentIndexId, true, None, &[key])?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(found)
}
