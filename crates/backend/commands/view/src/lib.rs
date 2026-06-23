//! `backend/commands/view.c` — use rewrite rules to construct views
//! (`CREATE [OR REPLACE] VIEW`).
//!
//! Every function — the exported [`DefineView`] / [`StoreViewQuery`] plus the
//! file-static `DefineVirtualRelation`, `checkViewColumns`, and
//! `DefineViewRules` — is implemented here with the same branch order,
//! permission ordering, error codes / messages / SQLSTATE, lock levels,
//! dependency recording, and invalidation (`CommandCounterIncrement`) as
//! PostgreSQL 18.3.
//!
//! Node trees are owned (`'mcx`): `ViewStmt`, `RangeVar`, `Query`,
//! `TargetEntry`, `ColumnDef`, and `DefElem` are owned values, so the
//! `ColumnDef`-list construction, the indeterminate-collation double-check, the
//! `checkViewColumns` comparison loop, the `check_option` reloption append +
//! defname scan, the alias-assignment loop, and the implicit-temp `copyObject`
//! are direct value operations.
//!
//! Pure node walkers / catalog reads cross to ported siblings directly, exactly
//! as the C calls them: `exprType` / `exprTypmod` / `exprCollation`
//! ([`::nodes_core::nodefuncs`]), `makeColumnDef` / `makeDefElem`
//! ([`::nodes_core::makefuncs`]), `type_is_collatable` /
//! `get_collation_name` ([`lsyscache`]),
//! `format_type_with_typemod` ([`adt_format_type`]),
//! `RangeVarGetAndCheckCreationNamespace` ([`catalog_namespace`]),
//! `recordDependencyOnCurrentExtension` ([`pg_depend`]),
//! `parse_analyze_fixedparams` ([`parser_analyze`]),
//! `DefineQueryRewrite` ([`rewriteDefine`]),
//! `isQueryUsingTempRelation` ([`parser_relation`]),
//! `CheckTableNotInUse` ([`tablecmds_seams`]),
//! `relation_open` / `relation_close` ([`common_relation`]), and
//! `CommandCounterIncrement` ([`transam_xact`]).
//!
//! The genuine cross-subsystem externals whose owners are not yet ported cross
//! the [`view_seams`] outward seams: `DefineRelation`,
//! `BuildDescForRelation`, and the `AlterTableInternal` legs (`tablecmds.c`),
//! and `view_query_is_auto_updatable` (`rewriteHandler.c`). The gettext `_()`
//! wrapper is the accepted project-wide i18n deferral.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::utils_error::ereport;
use ::mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgString, PgVec};

use ::common_relation::relation_open;
use ::transam_xact::CommandCounterIncrement;
use ::catalog_namespace::RangeVarGetAndCheckCreationNamespace;
use ::pg_depend::recordDependencyOnCurrentExtension;
use ::nodes_core::makefuncs::make_column_def;
use ::nodes_core::nodefuncs::{expr_collation, expr_type, expr_typmod};
use ::parser_analyze::parse_analyze_fixedparams;
use ::parser_relation::isQueryUsingTempRelation;
use ::rewriteDefine::DefineQueryRewrite;
use ::adt_format_type::format_type_with_typemod;
use ::lsyscache::type_::type_is_collatable;
use ::lsyscache::collation_constraint_language_cast::get_collation_name;

use ::tablecmds::AlterTableInternal;
use tablecmds_seams as tablecmds_seam;
use view_seams as seam;

use ::types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use ::types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
    NOTICE,
};
use ::nodes::copy_query::Query;
use ::nodes::ddlnodes::{
    AlterTableCmd, AlterTableType, CreateStmt, DefElem, DefElemAction, ViewStmt,
    CASCADED_CHECK_OPTION, LOCAL_CHECK_OPTION,
};
use ::nodes::nodes::{CmdType, Node, NodePtr};
use ::nodes::parsenodes::DROP_RESTRICT;
use ::nodes::primnodes::OnCommitAction;
use ::nodes::parsestmt::RawStmt;
use ::nodes::rawnodes::{ColumnDef, RangeVar};
use ::nodes::value::StringNode;
use ::rel::Relation;
use ::types_storage::lock::{AccessExclusiveLock, NoLock, LOCKMODE};
use ::types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_VIEW, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use ::types_tuple::heaptuple::{FormData_pg_attribute, TupleDescData};

/// `RelationRelationId` — `pg_class` OID.
const RelationRelationId: Oid = ::types_core::catalog::RELATION_RELATION_ID;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/commands/view.c", 0, funcname)
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// Bridge the owned grammar `RangeVar` node into the lifetime-free
/// `access::RangeVar` the namespace machinery consumes. (The two carriers exist
/// in distinct type layers; the relevant fields are identical.)
fn to_access_range_var(rv: &RangeVar<'_>) -> AccessRangeVar {
    AccessRangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/*---------------------------------------------------------------------
 * DefineVirtualRelation   (view.c lines 44-258)
 *
 * Create a view relation and use the rules system to store the query
 * for the view.
 *
 * EventTriggerAlterTableStart must have been called already.
 *---------------------------------------------------------------------
 */
fn DefineVirtualRelation<'mcx>(
    mcx: Mcx<'mcx>,
    mut relation: RangeVar<'mcx>,
    replace: bool,
    options: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>,
    view_parse: Query<'mcx>,
) -> PgResult<ObjectAddress> {
    /*
     * create a list of ColumnDef nodes based on the names and types of the
     * (non-junk) targetlist items from the view's SELECT list.
     */
    let mut attr_list: PgVec<ColumnDef> = vec_with_capacity_in(mcx, 0)?;
    for tle in view_parse.targetList.iter() {
        if !tle.resjunk {
            let expr = tle.expr.as_deref();
            let type_oid = expr_type(expr)?;
            let typmod = expr_typmod(expr)?;
            let coll = expr_collation(expr)?;
            let resname = tle.resname.as_ref().map(|s| s.as_str()).unwrap_or("");
            let def: ColumnDef = make_column_def(mcx, resname, type_oid, typmod, coll)?;

            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.
             */
            if type_is_collatable(expr_type(expr)?)? {
                if !OidIsValid(def.collOid) {
                    return ereport(ERROR)
                        .errcode(ERRCODE_INDETERMINATE_COLLATION)
                        .errmsg(format!(
                            "could not determine which collation to use for view column \"{}\"",
                            def.colname.as_ref().map(|s| s.as_str()).unwrap_or("")
                        ))
                        .errhint("Use the COLLATE clause to set the collation explicitly.")
                        .finish(here("DefineVirtualRelation"))
                        .map(|()| InvalidObjectAddress);
                }
            } else {
                debug_assert!(!OidIsValid(def.collOid));
            }

            attr_list.push(def);
        }
    }

    /*
     * Look up, check permissions on, and lock the creation namespace; also
     * check for a preexisting view with the same name.  This will also set
     * relation->relpersistence to RELPERSISTENCE_TEMP if the selected
     * namespace is temporary.
     */
    let lockmode: LOCKMODE = if replace { AccessExclusiveLock } else { NoLock };
    let mut access_rv = to_access_range_var(&relation);
    let mut view_oid: Oid = InvalidOid;
    RangeVarGetAndCheckCreationNamespace(mcx, &mut access_rv, lockmode, Some(&mut view_oid))?;
    /* propagate the (possibly temp-promoted) persistence back to the node */
    relation.relpersistence = access_rv.relpersistence as i8;

    if OidIsValid(view_oid) && replace {
        /* Relation is already locked, but we must build a relcache entry. */
        let rel: Relation = relation_open(mcx, view_oid, NoLock)?;

        /* Make sure it *is* a view. */
        if rel.rd_rel.relkind != RELKIND_VIEW {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{}\" is not a view", rel.name()))
                .finish(here("DefineVirtualRelation"))
                .map(|()| InvalidObjectAddress);
        }

        /* Also check it's not in use already */
        tablecmds_seam::check_table_not_in_use::call(&rel, "CREATE OR REPLACE VIEW")?;

        /*
         * Due to the namespace visibility rules for temporary objects, we
         * should only end up replacing a temporary view with another temporary
         * view, and similarly for permanent views.
         */
        debug_assert_eq!(
            relation.relpersistence as u8,
            rel.rd_rel.relpersistence
        );

        /*
         * Create a tuple descriptor to compare against the existing view, and
         * verify that the old column list is an initial prefix of the new
         * column list.
         */
        let descriptor: TupleDescData =
            ::tablecmds::build_desc_for_relation(mcx, &attr_list)?;
        checkViewColumns(mcx, &descriptor, &rel.rd_att)?;

        /*
         * If new attributes have been added, we must add pg_attribute entries
         * for them.  It is convenient (although overkill) to use the ALTER
         * TABLE ADD COLUMN infrastructure for this.
         *
         * Note that we must do this before updating the query for the view,
         * since the rules system requires that the correct view columns be in
         * place when defining the new rules.
         *
         * Also note that ALTER TABLE doesn't run parse transformation on
         * AT_AddColumnToView commands.  The ColumnDef we supply must be ready to
         * execute as-is.
         */
        let old_natts = rel.rd_att.natts;
        if attr_list.len() as i32 > old_natts {
            let mut atcmds: PgVec<NodePtr> = vec_with_capacity_in(mcx, 0)?;
            for def in attr_list.iter().skip(old_natts as usize) {
                let atcmd = AlterTableCmd {
                    subtype: AlterTableType::AT_AddColumnToView,
                    name: None,
                    num: 0,
                    newowner: None,
                    def: Some(alloc_in(mcx, Node::mk_column_def(mcx, def.clone_in(mcx)?)?)?),
                    behavior: DROP_RESTRICT,
                    missing_ok: false,
                    recurse: false,
                };
                atcmds.push(alloc_in(mcx, Node::mk_alter_table_cmd(mcx, atcmd)?)?);
            }

            /* EventTriggerAlterTableStart called by ProcessUtilitySlow */
            AlterTableInternal(mcx, view_oid, &atcmds, true)?;

            /* Make the new view columns visible */
            CommandCounterIncrement()?;
        }

        /*
         * Update the query for the view.
         *
         * Note that we must do this before updating the view options, because
         * the new options may not be compatible with the old view query.
         */
        StoreViewQuery(mcx, view_oid, view_parse, replace)?;

        /* Make the new view query visible */
        CommandCounterIncrement()?;

        /*
         * Update the view's options.  The new options list replaces the
         * existing options list, even if it's empty.
         *
         *   atcmd->subtype = AT_ReplaceRelOptions;
         *   atcmd->def = (Node *) options;
         *   atcmds = list_make1(atcmd);
         *   AlterTableInternal(viewOid, atcmds, true);
         */
        let replace_cmd = AlterTableCmd {
            subtype: AlterTableType::AT_ReplaceRelOptions,
            name: None,
            num: 0,
            newowner: None,
            def: Some(alloc_in(mcx, Node::mk_list(mcx, options)?)?),
            behavior: DROP_RESTRICT,
            missing_ok: false,
            recurse: false,
        };
        let mut atcmds: PgVec<NodePtr> = vec_with_capacity_in(mcx, 1)?;
        atcmds.push(alloc_in(mcx, Node::mk_alter_table_cmd(mcx, replace_cmd)?)?);
        AlterTableInternal(mcx, view_oid, &atcmds, true)?;

        /*
         * There is very little to do here to update the view's dependencies.
         * What remains is only to check that view replacement is allowed when
         * we're creating an extension.
         */
        let address = object_address_set(RelationRelationId, view_oid);
        recordDependencyOnCurrentExtension(mcx, &address, true)?;

        /* Seems okay, so return the OID of the pre-existing view. */
        rel.close(NoLock)?; /* keep the lock! */

        Ok(address)
    } else {
        /*
         * Set the parameters for keys/inheritance etc. All of these are
         * uninteresting for views.  Create the relation (this will error out if
         * there's an existing view, so we don't need more code to complain if
         * "replace" is false).
         *
         *   createStmt->relation = relation;
         *   createStmt->tableElts = attrList;
         *   createStmt->options = options;
         *   createStmt->oncommit = ONCOMMIT_NOOP;
         *   ...
         *   address = DefineRelation(createStmt, RELKIND_VIEW, InvalidOid, NULL, NULL);
         */
        let mut table_elts: PgVec<NodePtr> = vec_with_capacity_in(mcx, attr_list.len())?;
        for def in attr_list.into_iter() {
            table_elts.push(alloc_in(mcx, Node::mk_column_def(mcx, def)?)?);
        }
        let create_stmt = CreateStmt {
            relation: Some(alloc_in(mcx, Node::mk_range_var(mcx, relation)?)?),
            tableElts: table_elts,
            inhRelations: vec_with_capacity_in(mcx, 0)?,
            partbound: None,
            partspec: None,
            ofTypename: None,
            constraints: vec_with_capacity_in(mcx, 0)?,
            nnconstraints: vec_with_capacity_in(mcx, 0)?,
            options,
            oncommit: OnCommitAction::ONCOMMIT_NOOP,
            tablespacename: None,
            accessMethod: None,
            if_not_exists: false,
        };
        let address = tablecmds_seam::define_relation::call(
            mcx,
            create_stmt,
            RELKIND_VIEW,
            InvalidOid,
            None,
        )?;
        debug_assert!(address.objectId != InvalidOid);

        /* Make the new view relation visible */
        CommandCounterIncrement()?;

        /* Store the query for the view */
        StoreViewQuery(mcx, address.objectId, view_parse, replace)?;

        Ok(address)
    }
}

/*
 * checkViewColumns   (view.c lines 266-329)
 *
 * Verify that the columns associated with proposed new view definition match
 * the columns of the old view.  This is similar to equalRowTypes(), with code
 * added to generate specific complaints.  Also, we allow the new view to have
 * more columns than the old.
 */
fn checkViewColumns<'mcx>(
    mcx: Mcx<'mcx>,
    newdesc: &TupleDescData<'mcx>,
    olddesc: &TupleDescData<'mcx>,
) -> PgResult<()> {
    let old_natts = olddesc.natts;

    if newdesc.natts < old_natts {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot drop columns from view")
            .finish(here("checkViewColumns"));
    }

    for i in 0..old_natts as usize {
        let newattr: &FormData_pg_attribute = newdesc.attr(i);
        let oldattr: &FormData_pg_attribute = olddesc.attr(i);

        /* XXX msg not right, but we don't support DROP COL on view anyway */
        if newattr.attisdropped != oldattr.attisdropped {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("cannot drop columns from view")
                .finish(here("checkViewColumns"));
        }

        let new_attname = attname_str(newattr);
        let old_attname = attname_str(oldattr);
        if new_attname != old_attname {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "cannot change name of view column \"{old_attname}\" to \"{new_attname}\""
                ))
                .errhint(
                    "Use ALTER VIEW ... RENAME COLUMN ... to change name of view column instead.",
                )
                .finish(here("checkViewColumns"));
        }

        /*
         * We cannot allow type, typmod, or collation to change, since these
         * properties may be embedded in Vars of other views/rules referencing
         * this one.  Other column attributes can be ignored.
         */
        if newattr.atttypid != oldattr.atttypid || newattr.atttypmod != oldattr.atttypmod {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "cannot change data type of view column \"{}\" from {} to {}",
                    old_attname,
                    format_type_with_typemod(mcx, oldattr.atttypid, oldattr.atttypmod)?.as_str(),
                    format_type_with_typemod(mcx, newattr.atttypid, newattr.atttypmod)?.as_str()
                ))
                .finish(here("checkViewColumns"));
        }

        /*
         * At this point, attcollations should be both valid or both invalid, so
         * applying get_collation_name unconditionally should be fine.
         */
        if newattr.attcollation != oldattr.attcollation {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "cannot change collation of view column \"{}\" from \"{}\" to \"{}\"",
                    old_attname,
                    collation_name_or_null(mcx, oldattr.attcollation)?,
                    collation_name_or_null(mcx, newattr.attcollation)?
                ))
                .finish(here("checkViewColumns"));
        }
    }

    /*
     * We ignore the constraint fields.  The new view desc can't have any
     * constraints, and the only ones that could be on the old view are
     * defaults, which we are happy to leave in place.
     */
    Ok(())
}

/*
 * DefineViewRules   (view.c lines 331-349)
 */
fn DefineViewRules<'mcx>(
    mcx: Mcx<'mcx>,
    view_oid: Oid,
    view_parse: Query<'mcx>,
    replace: bool,
) -> PgResult<()> {
    /*
     * Set up the ON SELECT rule.  Since the query has already been through
     * parse analysis, we use DefineQueryRewrite() directly.
     *
     *   DefineQueryRewrite(pstrdup(ViewSelectRuleName), viewOid, NULL,
     *                      CMD_SELECT, true, replace, list_make1(viewParse));
     */
    let action = [view_parse];
    DefineQueryRewrite(
        mcx,
        ViewSelectRuleName,
        view_oid,
        None,
        CmdType::CMD_SELECT,
        true,
        replace,
        &action,
    )?;

    /*
     * Someday: automatic ON INSERT, etc
     */
    Ok(())
}

/// `ViewSelectRuleName` (rewriteDefine.h) — the fixed name of the ON SELECT
/// rule that implements a view.
const ViewSelectRuleName: &str = "_RETURN";

/*
 * DefineView   (view.c lines 355-505)
 *		Execute a CREATE VIEW command.
 */
pub fn DefineView<'mcx>(
    mcx: Mcx<'mcx>,
    mut stmt: ViewStmt<'mcx>,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<ObjectAddress> {
    /*
     * Run parse analysis to convert the raw parse tree to a Query.  Note this
     * also acquires sufficient locks on the source table(s).
     *
     *   rawstmt = makeNode(RawStmt);
     *   rawstmt->stmt = stmt->query;
     *   rawstmt->stmt_location = stmt_location;
     *   rawstmt->stmt_len = stmt_len;
     *   viewParse = parse_analyze_fixedparams(rawstmt, queryString, NULL, 0, NULL);
     */
    let query_node = stmt.query.take().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("CREATE VIEW has no query")
            .into_error()
    })?;
    let raw_stmt = RawStmt {
        stmt: query_node,
        stmt_location,
        stmt_len,
    };
    let mut view_parse = parse_analyze_fixedparams(mcx, &raw_stmt, query_string, &[])?;

    /*
     * The grammar should ensure that the result is a single SELECT Query.
     * However, it doesn't forbid SELECT INTO, so we have to check for that.
     */
    if let Some(util) = view_parse.utilityStmt.as_deref() {
        if util.is_createtableasstmt() {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("views must not contain SELECT INTO")
                .finish(here("DefineView"))
                .map(|()| InvalidObjectAddress);
        }
    }
    if view_parse.commandType != CmdType::CMD_SELECT {
        return ereport(ERROR)
            .errmsg_internal("unexpected parse analysis result")
            .finish(here("DefineView"))
            .map(|()| InvalidObjectAddress);
    }

    /*
     * Check for unsupported cases.  These tests are redundant with ones in
     * DefineQueryRewrite(), but that function will complain about a bogus ON
     * SELECT rule, and we'd rather the message complain about a view.
     */
    if view_parse.hasModifyingCTE {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("views must not contain data-modifying statements in WITH")
            .finish(here("DefineView"))
            .map(|()| InvalidObjectAddress);
    }

    /*
     * If the user specified the WITH CHECK OPTION, add it to the list of
     * reloptions.
     */
    if stmt.withCheckOption == LOCAL_CHECK_OPTION {
        let defelem = make_check_option_defelem(mcx, "local")?;
        stmt.options.push(defelem);
    } else if stmt.withCheckOption == CASCADED_CHECK_OPTION {
        let defelem = make_check_option_defelem(mcx, "cascaded")?;
        stmt.options.push(defelem);
    }

    /*
     * Check that the view is auto-updatable if WITH CHECK OPTION was specified.
     */
    let mut check_option = false;
    for opt in stmt.options.iter() {
        if let Some(defel) = opt.as_defelem() {
            if defel.defname.as_ref().map(|s| s.as_str()) == Some("check_option") {
                check_option = true;
            }
        }
    }

    /*
     * If the check option is specified, look to see if the view is actually
     * auto-updatable or not.
     */
    if check_option {
        let view_updatable_error =
            seam::view_query_is_auto_updatable::call(mcx, &view_parse)?;

        if let Some(view_updatable_error) = view_updatable_error {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("WITH CHECK OPTION is supported only on automatically updatable views")
                .errhint(gettext(view_updatable_error.as_str()))
                .finish(here("DefineView"))
                .map(|()| InvalidObjectAddress);
        }
    }

    /*
     * If a list of column names was given, run through and insert these into
     * the actual query tree. - thomas 2000-03-08
     */
    if !stmt.aliases.is_empty() {
        let mut alias_iter = stmt.aliases.iter();
        let mut alist_item = alias_iter.next();

        for te in view_parse.targetList.iter_mut() {
            /* junk columns don't get aliases */
            if te.resjunk {
                continue;
            }
            let Some(alias) = alist_item else {
                break;
            };
            /* te->resname = pstrdup(strVal(lfirst(alist_item))); */
            let alias_str = match alias.as_string() {
                Some(s) => s.sval.as_str(),
                None => {
                    return Err(ereport(ERROR)
                        .errmsg_internal("CREATE VIEW alias is not a String")
                        .into_error())
                }
            };
            te.resname = Some(PgString::from_str_in(alias_str, mcx)?);
            alist_item = alias_iter.next();
            if alist_item.is_none() {
                break; /* done assigning aliases */
            }
        }

        if alist_item.is_some() {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("CREATE VIEW specifies more column names than columns")
                .finish(here("DefineView"))
                .map(|()| InvalidObjectAddress);
        }
    }

    /* Unlogged views are not sensible. */
    if rangevar_relpersistence(stmt.view.as_deref()) == RELPERSISTENCE_UNLOGGED {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("views cannot be unlogged because they do not have storage")
            .finish(here("DefineView"))
            .map(|()| InvalidObjectAddress);
    }

    /*
     * If the user didn't explicitly ask for a temporary view, check whether we
     * need one implicitly.  We allow TEMP to be inserted automatically as long
     * as the CREATE command is consistent with that --- no explicit schema
     * name.
     */
    let mut view: RangeVar = copy_view_rangevar(mcx, stmt.view.as_deref())?;
    if view.relpersistence as u8 == RELPERSISTENCE_PERMANENT
        && isQueryUsingTempRelation(mcx, &view_parse)?
    {
        view.relpersistence = RELPERSISTENCE_TEMP as i8;
        ereport(NOTICE)
            .errmsg(format!(
                "view \"{}\" will be a temporary view",
                view.relname.as_ref().map(|s| s.as_str()).unwrap_or("")
            ))
            .finish(here("DefineView"))?;
    }

    /*
     * Create the view relation
     *
     * NOTE: if it already exists and replace is false, the xact will be
     * aborted.
     */
    let address = DefineVirtualRelation(mcx, view, stmt.replace, stmt.options, view_parse)?;

    Ok(address)
}

/*
 * StoreViewQuery   (view.c lines 510-517)
 *
 * Use the rules system to store the query for the view.
 */
pub fn StoreViewQuery<'mcx>(
    mcx: Mcx<'mcx>,
    view_oid: Oid,
    view_parse: Query<'mcx>,
    replace: bool,
) -> PgResult<()> {
    /*
     * Now create the rules associated with the view.
     */
    DefineViewRules(mcx, view_oid, view_parse, replace)
}

/* -------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------- */

/// `makeDefElem("check_option", (Node *) makeString(value), -1)` wrapped as a
/// `Node::DefElem` for the owned `stmt.options` list.
fn make_check_option_defelem<'mcx>(
    mcx: Mcx<'mcx>,
    value: &str,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    let string_arg = Node::mk_string(
        mcx,
        StringNode {
            sval: PgString::from_str_in(value, mcx)?,
        },
    )?;
    let defelem = DefElem {
        defnamespace: None,
        defname: Some(PgString::from_str_in("check_option", mcx)?),
        arg: Some(alloc_in(mcx, string_arg)?),
        defaction: DefElemAction::DEFELEM_UNSPEC,
        location: -1,
    };
    alloc_in(mcx, Node::mk_def_elem(mcx, defelem)?)
}

/// `view->relpersistence` read for the owned `RangeVar` node, tolerating a NULL
/// `stmt->view` defensively (the grammar always supplies one).
fn rangevar_relpersistence(view: Option<&Node>) -> u8 {
    match view.and_then(|v| v.as_rangevar()) {
        Some(rv) => rv.relpersistence as u8,
        None => RELPERSISTENCE_PERMANENT,
    }
}

/// `copyObject(stmt->view)` — deep-copy the `RangeVar` out of the
/// `Node::RangeVar` variant.
fn copy_view_rangevar<'mcx>(
    mcx: Mcx<'mcx>,
    view: Option<&Node<'mcx>>,
) -> PgResult<RangeVar<'mcx>> {
    match view.and_then(|v| v.as_rangevar()) {
        Some(rv) => rv.clone_in(mcx),
        None => Err(ereport(ERROR)
            .errmsg_internal("CREATE VIEW target is not a RangeVar")
            .into_error()),
    }
}

/// `NameStr(attr->attname)` rendered as a `&str`.
fn attname_str(attr: &FormData_pg_attribute) -> String {
    String::from_utf8_lossy(attr.attname.name_str()).into_owned()
}

/// `get_collation_name(collid)` for the `checkViewColumns` error text.  The C
/// returns a possibly-NULL `char *` fed to a `%s`; mirror printf's "(null)"
/// rendering for the NULL case.
fn collation_name_or_null<'mcx>(mcx: Mcx<'mcx>, collid: Oid) -> PgResult<String> {
    Ok(get_collation_name(mcx, collid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "(null)".to_string()))
}

/// `_()` (gettext) — the project-wide i18n deferral: returns the message
/// unchanged.  Used for `errhint("%s", _(view_updatable_error))`.
fn gettext(msg: &str) -> String {
    msg.to_string()
}

/// `tcop/utility.c` dispatch adapter for `CREATE VIEW`: marshal the
/// `Node::ViewStmt` the `ProcessUtilitySlow` switch hands across the command
/// boundary into the owned `ViewStmt` `DefineView` consumes.
fn define_view_from_node<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
    query_string: &str,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<ObjectAddress> {
    let Some(view_stmt) = stmt.as_viewstmt() else {
        return Err(ereport(ERROR)
            .errmsg_internal("DefineView dispatched on a non-ViewStmt node")
            .into_error());
    };
    let owned = view_stmt.clone_in(mcx)?;
    DefineView(mcx, owned, query_string, stmt_location, stmt_len)
}

/// Install the inward `define_view` seam (`backend-commands-view-seams`) and the
/// tcop/utility dispatch seam (`backend-tcop-utility-out-seams::define_view`)
/// this crate owns.
pub fn init_seams() {
    seam::define_view::set(DefineView);
    seam::store_view_query::set(StoreViewQuery);
    utility_out_seams::define_view::set(define_view_from_node);
}
