//! view.c CREATE VIEW lane, including CREATE OR REPLACE.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::catalog::{RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{AlterTableCmd, AlterTableType, DefElem, DefElemAction, Query};
use types_nodes::primnodes::RangeVar;
use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName, ViewCheckOption, ViewStmt};
use types_nodes::{Node, RawStmt};
use types_portal::QueryEnvHandle;
use types_rel::RELKIND_VIEW;

// DefineView (view.c).
pub fn DefineView<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ViewStmt<'mcx>,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<Oid> {
    let rawstmt = RawStmt {
        stmt: stmt.query,
        stmt_location,
        stmt_len,
    };
    let mut viewParse =
        parser_analyze::parse_analyze_fixedparams(mcx, &rawstmt, query_string, &[], QueryEnvHandle::NULL)?;

    if viewParse.utilityStmt.is_some() {
        return Err(feature_not_supported("views must not contain SELECT INTO"));
    }
    if viewParse.commandType != CmdType::CMD_SELECT {
        return Err(Box::new(PgError::error("unexpected parse analysis result")));
    }
    if viewParse.hasModifyingCTE {
        return Err(feature_not_supported(
            "views must not contain data-modifying statements in WITH",
        ));
    }
    let mut options = stmt.options.clone_in(mcx)?;
    match stmt.withCheckOption {
        ViewCheckOption::LOCAL_CHECK_OPTION => {
            options.lappend(mcx, check_option_defelem(mcx, "local")?)?;
        }
        ViewCheckOption::CASCADED_CHECK_OPTION => {
            options.lappend(mcx, check_option_defelem(mcx, "cascaded")?)?;
        }
        ViewCheckOption::NO_CHECK_OPTION => {}
    }

    let mut check_option = false;
    for dnode in options.iter() {
        let defel = dnode.as_def_elem().expect("DefElem");
        if defel.defname == Some("check_option") {
            check_option = true;
        }
    }
    if check_option {
        if let Some(view_updatable_error) =
            rewrite_handler::view_query_is_auto_updatable(&viewParse, true)
        {
            return Err(Box::new(
                PgError::error(
                    "WITH CHECK OPTION is supported only on automatically updatable views",
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_hint(view_updatable_error),
            ));
        }
    }

    if !stmt.aliases.is_nil() {
        let mut alias_iter = stmt.aliases.iter();
        let mut next_alias = alias_iter.next();
        for item in viewParse.targetList.iter() {
            if next_alias.is_none() {
                break;
            }
            let te = item.as_target_entry().expect("targetList entry");
            if te.resjunk {
                continue;
            }
            let alias = next_alias.expect("alias").as_string().expect("alias is a String").sval;
            // SAFETY: tree is statement-owned; no derived refs live.
            unsafe {
                item.with_mut::<types_nodes::primnodes::TargetEntry, _>(|t| t.resname = Some(alias))
                    .expect("TargetEntry");
            }
            next_alias = alias_iter.next();
        }
        if next_alias.is_some() {
            return Err(Box::new(
                PgError::error("CREATE VIEW specifies more column names than columns")
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
    }

    let mut view = stmt.view.expect("ViewStmt.view");
    if view.relpersistence == RELPERSISTENCE_UNLOGGED {
        return Err(Box::new(
            PgError::error("views cannot be unlogged because they do not have storage")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if view.relpersistence == RELPERSISTENCE_PERMANENT
        && parse_relation::isQueryUsingTempRelation(mcx, &viewParse)?
    {
        view = mcx::alloc_leak_in(
            mcx,
            RangeVar {
                catalogname: view.catalogname,
                schemaname: view.schemaname,
                relname: view.relname,
                inh: view.inh,
                relpersistence: RELPERSISTENCE_TEMP,
                alias: view.alias,
                location: view.location,
            },
        )?;
        elog::ereport(types_error::NOTICE)
            .errmsg(format!(
                "view \"{}\" will be a temporary view",
                view.relname.unwrap_or("")
            ))
            .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "DefineView"))?;
    }

    DefineVirtualRelation(mcx, stmt, view, options, &mut viewParse, query_string)
}

fn check_option_defelem<'mcx>(mcx: Mcx<'mcx>, value: &'mcx str) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some("check_option"),
            arg: Some(Node::mk_string(mcx, value)?),
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        },
    )
}

// DefineVirtualRelation (view.c), create lane; OR REPLACE is loud.
fn DefineVirtualRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ViewStmt<'mcx>,
    view: &'mcx RangeVar<'mcx>,
    options: NodeList<'mcx>,
    viewParse: &mut Query<'mcx>,
    query_string: &str,
) -> PgResult<Oid> {
    let mut attrList = NodeList::nil();
    for item in viewParse.targetList.iter() {
        let te = item.as_target_entry().expect("targetList entry");
        if te.resjunk {
            continue;
        }
        let type_oid = parse_expr::expr_type(te.expr);
        let coll_oid = parse_expr::expr_collation(te.expr);
        let mut tn = Node::build::<TypeName>(mcx)?;
        tn.typeOid = type_oid;
        tn.typemod = parse_expr::expr_typmod(te.expr);
        tn.location = -1;
        let mut def = Node::build::<ColumnDef>(mcx)?;
        def.colname = te.resname;
        def.typeName = Some(tn.seal());
        def.inhcount = 0;
        def.is_local = true;
        def.collOid = coll_oid;
        def.location = -1;
        if lsyscache::type_is_collatable(type_oid)? {
            if coll_oid == InvalidOid {
                return Err(Box::new(
                    PgError::error(format!(
                        "could not determine which collation to use for view column \"{}\"",
                        te.resname.unwrap_or("")
                    ))
                    .with_sqlstate(types_error::ERRCODE_INDETERMINATE_COLLATION)
                    .with_hint("Use the COLLATE clause to set the collation explicitly."),
                ));
            }
        } else {
            debug_assert!(coll_oid == InvalidOid);
        }
        attrList.lappend(mcx, def.seal())?;
    }

    // Look up, check permissions on, and lock the creation namespace; also
    // check for a preexisting view with the same name (view.c:96). The helper
    // owns the OR REPLACE ownercheck + AccessExclusiveLock on the existing
    // relation, exactly as C's lockmode argument does.
    let creation_rv = rel_vocab::RangeVar {
        catalogname: view.catalogname,
        schemaname: view.schemaname,
        relname: view.relname.expect("RangeVar.relname"),
        inh: view.inh,
        relpersistence: view.relpersistence,
        location: view.location,
    };
    let lockmode =
        if stmt.replace { types_rel::AccessExclusiveLock } else { types_rel::NoLock };
    let (namespace_id, view_oid, _relpersistence) =
        catalog_namespace::RangeVarGetAndCheckCreationNamespace(
            mcx,
            &creation_rv,
            lockmode,
            true,
        )?;

    if stmt.replace && view_oid != InvalidOid {
        return ReplaceViewQuery(mcx, view_oid, attrList, options, viewParse);
    }

    let mut createStmt = Node::build::<CreateStmt>(mcx)?;
    createStmt.relation = Some(view);
    createStmt.tableElts = attrList;
    createStmt.inhRelations = NodeList::nil();
    createStmt.constraints = NodeList::nil();
    createStmt.options = options;
    createStmt.oncommit = OnCommitAction::ONCOMMIT_NOOP;
    createStmt.tablespacename = None;
    createStmt.if_not_exists = false;

    let view_oid = tablecmds::DefineRelation(mcx, &createStmt, RELKIND_VIEW, InvalidOid, query_string)?;
    xact::CommandCounterIncrement()?;
    StoreViewQuery(mcx, view_oid, viewParse, stmt.replace)?;
    Ok(view_oid)
}

// DefineVirtualRelation (view.c), OR REPLACE arm over an existing view.
fn ReplaceViewQuery<'mcx>(
    mcx: Mcx<'mcx>,
    view_oid: Oid,
    attrList: NodeList<'mcx>,
    options: NodeList<'mcx>,
    viewParse: &mut Query<'mcx>,
) -> PgResult<Oid> {
    let rel = relation_seams::relation_open::call(mcx, view_oid, types_rel::NoLock)?;

    if rel.rd_rel.relkind != RELKIND_VIEW {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a view", rel.name()))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    catalog_heap::CheckTableNotInUse(&rel, "CREATE OR REPLACE VIEW")?;

    let descriptor = tablecmds::BuildDescForRelation(mcx, &attrList)?;
    checkViewColumns(mcx, &descriptor, rel.descr())?;

    let old_natts = rel.rd_att.natts;
    if attrList.len() as i32 > old_natts {
        let mut atcmds = NodeList::nil();
        for (i, c) in attrList.iter().enumerate() {
            if (i as i32) < old_natts {
                continue;
            }
            let mut atcmd = Node::build::<AlterTableCmd>(mcx)?;
            atcmd.subtype = AlterTableType::AT_AddColumnToView;
            atcmd.def = Some(c);
            atcmds.lappend(mcx, atcmd.seal())?;
        }
        tablecmds::AlterTableInternal(mcx, view_oid, &atcmds, true)?;
        xact::CommandCounterIncrement()?;
    }

    StoreViewQuery(mcx, view_oid, viewParse, true)?;
    xact::CommandCounterIncrement()?;

    let mut atcmd = Node::build::<AlterTableCmd>(mcx)?;
    atcmd.subtype = AlterTableType::AT_ReplaceRelOptions;
    atcmd.def = if options.is_nil() {
        None
    } else {
        Some(Node::mk_list(mcx, options)?)
    };
    let atcmds = NodeList::make1(mcx, atcmd.seal())?;
    tablecmds::AlterTableInternal(mcx, view_oid, &atcmds, true)?;

    let address = pg_depend::ObjectAddress::set(types_core::RELATION_RELATION_ID, view_oid);
    pg_depend::recordDependencyOnCurrentExtension(mcx, &address, true)?;

    rel.close(types_rel::NoLock)?;
    Ok(view_oid)
}

// checkViewColumns (view.c): the old column list must be an initial prefix of
// the new one, with names/types/collations unchanged.
fn checkViewColumns(
    mcx: Mcx<'_>,
    newdesc: &types_tuple::TupleDescData<'_>,
    olddesc: &types_tuple::TupleDescData<'_>,
) -> PgResult<()> {
    let invalid = |msg: String| -> Box<PgError> {
        Box::new(
            PgError::error(msg).with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
        )
    };
    if newdesc.natts < olddesc.natts {
        return Err(invalid("cannot drop columns from view".to_string()));
    }
    for i in 0..olddesc.natts as usize {
        let newattr = newdesc.attr(i);
        let oldattr = olddesc.attr(i);

        if newattr.attisdropped != oldattr.attisdropped {
            return Err(invalid("cannot drop columns from view".to_string()));
        }

        let newname = core::str::from_utf8(newattr.attname.name_str())
            .expect("attribute name is UTF-8");
        let oldname = core::str::from_utf8(oldattr.attname.name_str())
            .expect("attribute name is UTF-8");
        if newname != oldname {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot change name of view column \"{oldname}\" to \"{newname}\""
                ))
                .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION)
                .with_hint(
                    "Use ALTER VIEW ... RENAME COLUMN ... to change name of view column instead.",
                ),
            ));
        }

        if newattr.atttypid != oldattr.atttypid || newattr.atttypmod != oldattr.atttypmod {
            return Err(invalid(format!(
                "cannot change data type of view column \"{oldname}\" from {} to {}",
                format_type::format_type_with_typemod(oldattr.atttypid, oldattr.atttypmod)?,
                format_type::format_type_with_typemod(newattr.atttypid, newattr.atttypmod)?,
            )));
        }

        if newattr.attcollation != oldattr.attcollation {
            let collname = |oid: Oid| -> PgResult<String> {
                Ok(lsyscache::get_collation_name(mcx, oid)?
                    .map(|n| n.to_string())
                    .unwrap_or_default())
            };
            return Err(invalid(format!(
                "cannot change collation of view column \"{oldname}\" from \"{}\" to \"{}\"",
                collname(oldattr.attcollation)?,
                collname(newattr.attcollation)?,
            )));
        }
    }
    Ok(())
}

// StoreViewQuery -> DefineViewRules (view.c): the ON SELECT _RETURN rule.
pub fn StoreViewQuery<'mcx>(
    mcx: Mcx<'mcx>,
    viewOid: Oid,
    viewParse: &mut Query<'mcx>,
    replace: bool,
) -> PgResult<()> {
    let query_node = Node::mk(mcx, core::mem::take(viewParse))?;
    let action = NodeList::make1(mcx, query_node)?;
    rewrite_define::DefineQueryRewrite(
        mcx,
        rewrite_define::ViewSelectRuleName,
        viewOid,
        None,
        CmdType::CMD_SELECT,
        true,
        replace,
        action,
    )?;
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}
