//! `transformColumnDefinition` (`parse_utilcmd.c`) — transform a single
//! `ColumnDef`, detecting SERIAL pseudo-types and distributing the column's
//! constraints into the [`CreateStmtContext`] accumulators.
//!
//! The constraint-scanning core (the `[NOT] NULL` / DEFAULT / IDENTITY /
//! GENERATED / PRIMARY / UNIQUE / CHECK / FOREIGN bucketing and the mutually-
//! exclusive-clause checks) is fully node-independent and ported 1:1. The
//! catalog-bound leaves — column-type / COLLATE validation
//! ([`crate::coltype::transformColumnType`], grounded in-crate) and the SERIAL /
//! IDENTITY sequence generation ([`generateSerialExtraStmts`], still seamed:
//! its ALTER leg reads a live relcache `Relation` the context model omits).

use mcx::{Mcx, PgString, PgVec};

use backend_utils_error::ereport;
use types_core::Oid;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR};

use types_nodes::ddlnodes::{
    AlterTableCmd, AlterTableStmt, Constraint, ConstrType, AT_AlterColumnGenericOptions,
    CONSTR_ATTR_DEFERRABLE, CONSTR_ATTR_DEFERRED, CONSTR_ATTR_ENFORCED, CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_NOT_DEFERRABLE, CONSTR_ATTR_NOT_ENFORCED, CONSTR_CHECK, CONSTR_DEFAULT,
    CONSTR_EXCLUSION, CONSTR_FOREIGN, CONSTR_GENERATED, CONSTR_IDENTITY, CONSTR_NOTNULL,
    CONSTR_NULL, CONSTR_PRIMARY, CONSTR_UNIQUE,
};
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::{DROP_RESTRICT, OBJECT_FOREIGN_TABLE};
use types_nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL;
use types_nodes::rawnodes::{A_Const, FuncCall, TypeCast, TypeName};

/// `INT2OID` / `INT4OID` / `INT8OID` (`catalog/pg_type_d.h`) — fixed catalog
/// OIDs for the SERIAL pseudo-type rewrites.
const INT8OID: Oid = 20;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;

use backend_parser_parse_utilcmd_outward_seams as sx;

use crate::core::{make_string, CreateStmtContext, NodePtr};
use crate::errpos::parser_errposition;
use crate::fk_check_attrs::transformConstraintAttrs;

use alloc::string::ToString;

use backend_parser_parse_type::{typenameType, typeTypeCollation, typeTypeId, LookupCollation};
use types_core::OidIsValid;
use types_error::ERRCODE_DATATYPE_MISMATCH;

/// `transformColumnDefinition` — transform one column definition.
pub fn transformColumnDefinition<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    column: NodePtr<'mcx>,
) -> PgResult<()> {
    let mcx = cxt.mcx;

    // C: cxt->columns = lappend(cxt->columns, column); the C code appends the
    // pointer at entry and mutates it in place. Since nothing reads cxt.columns
    // until all columns are processed, we keep the (owned) `column` local and
    // push the finished node at the end (equivalent).
    let column_node = mcx::PgBox::into_inner(column);
    let column_tag = column_node.node_tag();
    let mut column = match column_node.into_columndef() {
        Some(c) => c,
        None => unreachable!("transformColumnDefinition: not a ColumnDef node: {}", column_tag),
    };

    let mut need_notnull = false;
    let mut disallow_noinherit_notnull = false;
    // Index of the tracked not-null constraint in `cxt.nnconstraints`, if any
    // (C's `notnull_constraint` pointer; we walk by index into the live vector).
    let mut notnull_idx: Option<usize> = None;

    // Check for SERIAL pseudo-types.
    let mut is_serial = false;
    if let Some(tn) = column.typeName.as_deref_mut() {
        if tn.names.len() == 1 && !tn.pct_type {
            let typname = strval_of(tn.names[0].as_ref()).unwrap_or("");
            if typname == "smallserial" || typname == "serial2" {
                is_serial = true;
                tn.names = PgVec::new_in(mcx);
                tn.typeOid = INT2OID;
            } else if typname == "serial" || typname == "serial4" {
                is_serial = true;
                tn.names = PgVec::new_in(mcx);
                tn.typeOid = INT4OID;
            } else if typname == "bigserial" || typname == "serial8" {
                is_serial = true;
                tn.names = PgVec::new_in(mcx);
                tn.typeOid = INT8OID;
            }

            // We have to reject "serial[]" explicitly.
            if is_serial && !tn.arrayBounds.is_empty() {
                let loc = tn.location;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("array of serial is not implemented")
                    .errposition(parser_errposition(&cxt.pstate, loc))
                    .into_error());
            }
        }
    }

    // Do necessary work on the column type declaration (catalog-bound: verify
    // the type + any COLLATE via the seam).
    if column.typeName.is_some() {
        let column_node = mcx::alloc_in(mcx, Node::mk_column_def(mcx, column.clone_in(mcx)?))?;
        crate::coltype::transformColumnType(mcx, &cxt.pstate, column_node.as_ref())?;
    }

    // Special actions for SERIAL pseudo-types.
    if is_serial {
        let seq_type_id = column.typeName.as_deref().map_or(0 as Oid, |tn| tn.typeOid);
        let relation = clone_relation(cxt)?;

        let column_node = mcx::alloc_in(mcx, Node::mk_column_def(mcx, column.clone_in(mcx)?))?;
        let (column_out, snamespace, sname, before_stmts, after_stmts) =
            sx::generateSerialExtraStmts::call(
                mcx,
                column_node,
                relation,
                cxt.rel_oid,
                cxt.isalter,
                cxt.stmtType,
                seq_type_id,
                PgVec::new_in(mcx),
                false,
                false,
            )?;
        // generateSerialExtraStmts sets column->identitySequence.
        if let Some(c) = column_out.as_columndef() {
            column.identitySequence = match &c.identitySequence {
                Some(r) => Some(mcx::alloc_in(mcx, r.clone_in(mcx)?)?),
                None => None,
            };
        }
        cxt.blist.extend(before_stmts);
        cxt.blist.extend(after_stmts);

        // Create an expression tree representing the function call
        // nextval('sequencename')::regclass, and build the CONSTR_DEFAULT for it.
        let snamespace_str = snamespace.as_ref().map(PgString::as_str);
        let sname_str = sname.as_ref().map_or("", PgString::as_str);
        let qstring = backend_utils_adt_ruleutils::quote_qualified_identifier(
            mcx,
            snamespace_str,
            sname_str,
        )?;

        let snamenode = A_Const {
            // snamenode->val.node.type = T_String; snamenode->val.sval.sval = qstring;
            val: Some(mcx::alloc_in(
                mcx,
                Node::mk_string(mcx, types_nodes::value::StringNode { sval: qstring }),
            )?),
            isnull: false,
            location: -1,
        };
        let castnode = TypeCast {
            arg: Some(mcx::alloc_in(mcx, Node::mk_a_const(mcx, snamenode))?),
            typeName: Some(mcx::alloc_in(mcx, system_type_name(mcx, "regclass")?)?),
            location: -1,
        };
        let mut funccall_args: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        funccall_args.push(mcx::alloc_in(mcx, Node::mk_type_cast(mcx, castnode))?);
        let funccallnode = FuncCall {
            funcname: system_func_name(mcx, "nextval")?,
            args: funccall_args,
            agg_order: PgVec::new_in(mcx),
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star: false,
            agg_distinct: false,
            func_variadic: false,
            funcformat: COERCE_EXPLICIT_CALL,
            location: -1,
        };
        let constraint = Constraint {
            contype: CONSTR_DEFAULT,
            location: -1,
            raw_expr: Some(mcx::alloc_in(mcx, Node::mk_func_call(mcx, funccallnode))?),
            cooked_expr: None,
            ..default_constraint(mcx)
        };
        column
            .constraints
            .push(mcx::alloc_in(mcx, Node::mk_constraint(mcx, constraint))?);

        // have a not-null constraint added later
        need_notnull = true;
        disallow_noinherit_notnull = true;
    }

    // Process column constraint attributes (DEFERRABLE / ENFORCED markers).
    transformConstraintAttrs(cxt, column.constraints.as_mut_slice())?;

    // First, scan to see whether an added not-null must be prevented from being
    // NO INHERIT (PRIMARY KEY / IDENTITY).
    if !disallow_noinherit_notnull {
        for c in column.constraints.iter() {
            if let Some(con) = c.as_constraint() {
                match con.contype {
                    CONSTR_IDENTITY | CONSTR_PRIMARY => {
                        disallow_noinherit_notnull = true;
                    }
                    _ => {}
                }
            }
        }
    }

    // Now scan again to do full processing.
    let mut saw_nullable = false;
    let mut saw_default = false;
    let mut saw_identity = false;
    let mut saw_generated = false;

    // We consume `column.constraints` by value, so we can move the matching ones
    // into the `cxt` accumulators (C appends the same pointer).
    let constraints = core::mem::replace(&mut column.constraints, PgVec::new_in(mcx));
    let colname = clone_colname(mcx, &column)?;

    for constraint in constraints {
        let (contype, location, is_no_inherit, conname) = match constraint.node_tag() {
            ntag::T_Constraint => {
                let c = constraint.expect_constraint();
                (
                    c.contype,
                    c.location,
                    c.is_no_inherit,
                    opt_clone_str(mcx, &c.conname)?,
                )
            }
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "unrecognized node type: {}",
                        constraint.node_tag()
                    ))
                    .into_error());
            }
        };

        match contype {
            CONSTR_NULL => {
                if (saw_nullable && column.is_not_null) || need_notnull {
                    return Err(conflicting_null(cxt, &colname, location));
                }
                column.is_not_null = false;
                saw_nullable = true;
            }

            CONSTR_NOTNULL => {
                if cxt.ispartitioned && is_no_inherit {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("not-null constraints on partitioned tables cannot be NO INHERIT")
                        .into_error());
                }

                if saw_nullable && !column.is_not_null {
                    return Err(conflicting_null(cxt, &colname, location));
                }

                if disallow_noinherit_notnull && is_no_inherit {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "conflicting NO INHERIT declarations for not-null constraints on column \"{colname}\""
                        ))
                        .into_error());
                }

                if !column.is_not_null {
                    column.is_not_null = true;
                    saw_nullable = true;
                    need_notnull = false;

                    // constraint->keys = list_make1(makeString(column->colname));
                    let mut constraint = constraint;
                    if let Some(c) = constraint.as_constraint_mut() {
                        let mut keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                        keys.push(make_string(mcx, &colname)?);
                        c.keys = keys;
                    }
                    cxt.nnconstraints.push(constraint);
                    notnull_idx = Some(cxt.nnconstraints.len() - 1);
                } else if let Some(idx) = notnull_idx {
                    let (existing_conname, existing_no_inherit) =
                        match cxt.nnconstraints[idx].as_constraint() {
                            Some(c) => (
                                opt_clone_str(mcx, &c.conname)?,
                                c.is_no_inherit,
                            ),
                            None => (None, false),
                        };

                    if conname.is_some()
                        && existing_conname.is_some()
                        && existing_conname.as_deref() != conname.as_deref()
                    {
                        return Err(ereport(ERROR)
                            .errmsg_internal(alloc::format!(
                                "conflicting not-null constraint names \"{}\" and \"{}\"",
                                existing_conname.unwrap_or_default(),
                                conname.unwrap_or_default()
                            ))
                            .into_error());
                    }

                    if existing_no_inherit != is_no_inherit {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_SYNTAX_ERROR)
                            .errmsg(alloc::format!(
                                "conflicting NO INHERIT declarations for not-null constraints on column \"{colname}\""
                            ))
                            .into_error());
                    }

                    if existing_conname.is_none() && conname.is_some() {
                        if let Some(c) = cxt.nnconstraints[idx].as_constraint_mut() {
                            c.conname = match &conname {
                                Some(s) => Some(PgString::from_str_in(s, mcx)?),
                                None => None,
                            };
                        }
                    }
                }
            }

            CONSTR_DEFAULT => {
                if saw_default {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "multiple default values specified for column \"{colname}\" of table \"{}\"",
                            cxt.relname()
                        ))
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                // column->raw_default = constraint->raw_expr;
                column.raw_default = match mcx::PgBox::into_inner(constraint).into_constraint() {
                    Some(c) => c.raw_expr,
                    None => None,
                };
                saw_default = true;
            }

            CONSTR_IDENTITY => {
                if cxt.ofType {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("identity columns are not supported on typed tables")
                        .into_error());
                }
                if cxt.partbound.is_some() {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("identity columns are not supported on partitions")
                        .into_error());
                }

                // ctype = typenameType(...); typeOid = ctype->oid (catalog seam).
                let column_node = mcx::alloc_in(mcx, Node::mk_column_def(mcx, column.clone_in(mcx)?))?;
                let type_oid =
                    crate::coltype::transformColumnType(mcx, &cxt.pstate, column_node.as_ref())?;

                if saw_identity {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "multiple identity specifications for column \"{colname}\" of table \"{}\"",
                            cxt.relname()
                        ))
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }

                let (options, generated_when) = match mcx::PgBox::into_inner(constraint).into_constraint() {
                    Some(c) => (c.options, c.generated_when),
                    None => (PgVec::new_in(mcx), 0),
                };

                let relation = clone_relation(cxt)?;
                let column_node = mcx::alloc_in(mcx, Node::mk_column_def(mcx, column.clone_in(mcx)?))?;
                let (_column_out, _snamespace, _sname, before_stmts, after_stmts) =
                    sx::generateSerialExtraStmts::call(
                        mcx,
                        column_node,
                        relation,
                        cxt.rel_oid,
                        cxt.isalter,
                        cxt.stmtType,
                        type_oid,
                        options,
                        true,
                        false,
                    )?;
                cxt.blist.extend(before_stmts);
                cxt.blist.extend(after_stmts);

                column.identity = generated_when;
                saw_identity = true;

                // Identity columns are always NOT NULL.
                if !saw_nullable {
                    need_notnull = true;
                } else if !column.is_not_null {
                    return Err(conflicting_null(cxt, &colname, location));
                }
            }

            CONSTR_GENERATED => {
                if cxt.ofType {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("generated columns are not supported on typed tables")
                        .into_error());
                }
                if saw_generated {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(alloc::format!(
                            "multiple generation clauses specified for column \"{colname}\" of table \"{}\"",
                            cxt.relname()
                        ))
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                let (generated_kind, raw_expr) = match mcx::PgBox::into_inner(constraint).into_constraint() {
                    Some(c) => (c.generated_kind, c.raw_expr),
                    None => (0, None),
                };
                column.generated = generated_kind;
                column.raw_default = raw_expr;
                saw_generated = true;
            }

            CONSTR_CHECK => {
                cxt.ckconstraints.push(constraint);
            }

            CONSTR_PRIMARY => {
                if saw_nullable && !column.is_not_null {
                    return Err(conflicting_null(cxt, &colname, location));
                }
                need_notnull = true;

                if cxt.isforeign {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("primary key constraints are not supported on foreign tables")
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                // FALL THRU to CONSTR_UNIQUE handling.
                if cxt.isforeign {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("unique constraints are not supported on foreign tables")
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                let mut constraint = constraint;
                if let Some(c) = constraint.as_constraint_mut() {
                    if c.keys.is_empty() {
                        let mut keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                        keys.push(make_string(mcx, &colname)?);
                        c.keys = keys;
                    }
                }
                cxt.ixconstraints.push(constraint);
            }

            CONSTR_UNIQUE => {
                if cxt.isforeign {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("unique constraints are not supported on foreign tables")
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                let mut constraint = constraint;
                if let Some(c) = constraint.as_constraint_mut() {
                    if c.keys.is_empty() {
                        let mut keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                        keys.push(make_string(mcx, &colname)?);
                        c.keys = keys;
                    }
                }
                cxt.ixconstraints.push(constraint);
            }

            CONSTR_EXCLUSION => {
                // grammar does not allow EXCLUDE as a column constraint
                return Err(ereport(ERROR)
                    .errmsg_internal("column exclusion constraints are not supported")
                    .into_error());
            }

            CONSTR_FOREIGN => {
                if cxt.isforeign {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("foreign key constraints are not supported on foreign tables")
                        .errposition(parser_errposition(&cxt.pstate, location))
                        .into_error());
                }
                // Fill in the FK's attribute name and queue it.
                let mut constraint = constraint;
                if let Some(c) = constraint.as_constraint_mut() {
                    let mut fk_attrs: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                    fk_attrs.push(make_string(mcx, &colname)?);
                    c.fk_attrs = fk_attrs;
                }
                cxt.fkconstraints.push(constraint);
            }

            CONSTR_ATTR_DEFERRABLE
            | CONSTR_ATTR_NOT_DEFERRABLE
            | CONSTR_ATTR_DEFERRED
            | CONSTR_ATTR_IMMEDIATE
            | CONSTR_ATTR_ENFORCED
            | CONSTR_ATTR_NOT_ENFORCED => {
                // transformConstraintAttrs took care of these.
            }
        }

        if saw_default && saw_identity {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(alloc::format!(
                    "both default and identity specified for column \"{colname}\" of table \"{}\"",
                    cxt.relname()
                ))
                .errposition(parser_errposition(&cxt.pstate, location))
                .into_error());
        }
        if saw_default && saw_generated {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(alloc::format!(
                    "both default and generation expression specified for column \"{colname}\" of table \"{}\"",
                    cxt.relname()
                ))
                .errposition(parser_errposition(&cxt.pstate, location))
                .into_error());
        }
        if saw_identity && saw_generated {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(alloc::format!(
                    "both identity and generation expression specified for column \"{colname}\" of table \"{}\"",
                    cxt.relname()
                ))
                .errposition(parser_errposition(&cxt.pstate, location))
                .into_error());
        }
    }

    // If we need a not-null constraint and one was not explicitly specified.
    if need_notnull && !(saw_nullable && column.is_not_null) {
        column.is_not_null = true;
        let nn = make_not_null_constraint(mcx, &colname)?;
        cxt.nnconstraints.push(mcx::alloc_in(mcx, Node::mk_constraint(mcx, nn))?);
    }

    // If needed, generate ALTER FOREIGN TABLE ... per-column FDW options.
    if !column.fdwoptions.is_empty() {
        // C: cmd->def = (Node *) column->fdwoptions; the DefElem list is carried
        // as the subcommand's `def`. We move the fdwoptions out and rewrap them
        // as a single `List`-style node is not modelled, so the def is left
        // None here (tablecmds reads the per-column FDW options elsewhere in this
        // owned model). Mirror the rest of the statement faithfully.
        let altercmd = AlterTableCmd {
            subtype: AT_AlterColumnGenericOptions,
            name: match &column.colname {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            num: 0,
            newowner: None,
            def: None,
            behavior: DROP_RESTRICT,
            missing_ok: false,
            recurse: false,
        };
        let relation = clone_relation_opt(cxt)?;
        let mut cmds: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        cmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, altercmd))?);
        let stmt = AlterTableStmt {
            relation,
            cmds,
            objtype: OBJECT_FOREIGN_TABLE,
            missing_ok: false,
        };
        cxt.alist
            .push(mcx::alloc_in(mcx, Node::mk_alter_table_stmt(mcx, stmt))?);
    }

    // Finally, the (mutated) column itself goes into cxt.columns.
    cxt.columns
        .push(mcx::alloc_in(mcx, Node::mk_column_def(mcx, column))?);

    Ok(())
}

/// `strVal(node)` for a name-list element (`String` value node).
fn strval_of<'a>(n: &'a Node<'_>) -> Option<&'a str> {
    match n.node_tag() {
        ntag::T_String => Some(n.expect_string().sval.as_str()),
        _ => None,
    }
}

fn clone_colname<'mcx>(
    _mcx: Mcx<'mcx>,
    column: &types_nodes::rawnodes::ColumnDef<'mcx>,
) -> PgResult<alloc::string::String> {
    Ok(column
        .colname
        .as_ref()
        .map_or_else(alloc::string::String::new, |s| s.as_str().into()))
}

fn opt_clone_str(
    _mcx: Mcx<'_>,
    s: &Option<PgString<'_>>,
) -> PgResult<Option<alloc::string::String>> {
    Ok(s.as_ref().map(|s| s.as_str().into()))
}

fn clone_relation<'mcx>(cxt: &CreateStmtContext<'mcx>) -> PgResult<NodePtr<'mcx>> {
    let mcx = cxt.mcx;
    match cxt.relation.as_deref() {
        Some(n) => mcx::alloc_in(mcx, n.clone_in(mcx)?),
        None => Err(types_error::PgError::error(
            "transformColumnDefinition: SERIAL/IDENTITY column requires cxt.relation",
        )),
    }
}

fn clone_relation_opt<'mcx>(cxt: &CreateStmtContext<'mcx>) -> PgResult<Option<NodePtr<'mcx>>> {
    let mcx = cxt.mcx;
    match cxt.relation.as_deref() {
        Some(n) => Ok(Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

fn conflicting_null(
    cxt: &CreateStmtContext<'_>,
    colname: &str,
    location: i32,
) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(alloc::format!(
            "conflicting NULL/NOT NULL declarations for column \"{colname}\" of table \"{}\"",
            cxt.relname()
        ))
        .errposition(parser_errposition(&cxt.pstate, location))
        .into_error()
}

/// `SystemTypeName(name)` (`parser/parse_type.h`-ish helper from gram.y) — build
/// a `TypeName` for a pg_catalog type by name.
fn system_type_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<TypeName<'mcx>> {
    let mut names: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    names.push(make_string(mcx, "pg_catalog")?);
    names.push(make_string(mcx, name)?);
    Ok(TypeName {
        names,
        typeOid: 0,
        setof: false,
        pct_type: false,
        typmods: PgVec::new_in(mcx),
        typemod: -1,
        arrayBounds: PgVec::new_in(mcx),
        location: -1,
    })
}

/// `SystemFuncName(name)` — build a `pg_catalog`-qualified function name list.
fn system_func_name<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut names: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    names.push(make_string(mcx, "pg_catalog")?);
    names.push(make_string(mcx, name)?);
    Ok(names)
}

/// `makeNotNullConstraint(makeString(colname))` (`nodes/makefuncs.c`) — build a
/// fresh CONSTR_NOTNULL constraint over a single column key.
pub(crate) fn make_not_null_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
) -> PgResult<Constraint<'mcx>> {
    let mut keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    keys.push(make_string(mcx, colname)?);
    Ok(Constraint {
        contype: CONSTR_NOTNULL,
        is_enforced: true,
        initially_valid: true,
        keys,
        location: -1,
        ..default_constraint(mcx)
    })
}

/// A zeroed `Constraint` skeleton (the `makeNode(Constraint)` palloc0 baseline).
fn default_constraint<'mcx>(mcx: Mcx<'mcx>) -> Constraint<'mcx> {
    Constraint {
        contype: ConstrType::CONSTR_NULL,
        conname: None,
        deferrable: false,
        initdeferred: false,
        is_enforced: false,
        skip_validation: false,
        initially_valid: false,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: PgVec::new_in(mcx),
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
        old_pktable_oid: 0,
        location: -1,
    }
}
