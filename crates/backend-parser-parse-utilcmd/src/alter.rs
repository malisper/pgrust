//! `transformAlterTableStmt` (`parse_utilcmd.c:3524`) — parse analysis for
//! ALTER TABLE.
//!
//! Opens the target relation by OID (the caller holds the lock; we open with
//! `NoLock`), sets up a [`CreateStmtContext`] over the relation's range-table
//! entry, and re-uses the CREATE TABLE element transforms for the subcommand
//! types that need them (`transformColumnDefinition` for ADD COLUMN,
//! `transformTableConstraint` for ADD CONSTRAINT, the USING-clause
//! `transformExpr` for ALTER COLUMN TYPE, and the identity-column ALTER SEQUENCE
//! generation for ADD/SET IDENTITY). Constraints accumulated in the context are
//! then postprocessed (`transformIndexConstraints` / `transformFKConstraints` /
//! `transformCheckConstraints`) and pushed back into the command list as
//! follow-on `AT_AddIndex` / `AT_AddIndexConstraint` / `AT_AddConstraint`
//! subcommands, exactly as CREATE TABLE does.

#![allow(non_snake_case)]

use alloc::string::ToString;

use mcx::{Mcx, PgBox, PgString, PgVec};

use backend_utils_error::ereport;
use types_core::{Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_UNDEFINED_COLUMN, ERROR};
use types_storage::lock::{AccessShareLock, NoLock};

use types_nodes::ddlnodes::{AlterSeqStmt, AlterTableCmd, AlterTableType::*, ConstrType, DEFELEM_UNSPEC};
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsestmt::ParseExprKind::EXPR_KIND_ALTER_COL_TRANSFORM;
use types_nodes::ddlnodes::DefElem;
use types_nodes::rawnodes::{RangeVar, TypeName};

use backend_access_common_relation::relation_open;
use backend_access_table_table::table_close;
use backend_catalog_pg_depend::getIdentitySequence;
use backend_parser_parse_expr::transformExpr;
use backend_parser_parse_type::typenameTypeId;
use backend_parser_relation::{addNSItemToQuery, addRangeTableEntryForRelation};
use backend_parser_small1::{free_parsestate, make_parsestate};
use backend_utils_cache_lsyscache::attribute::{get_attnum, get_atttype};
use backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name;
use backend_utils_cache_lsyscache::relation::{get_rel_name, get_rel_namespace};

use types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE};

use crate::column::transformColumnDefinition;
use crate::constraint::{transformCheckConstraints, transformTableConstraint};
use crate::core::{CreateStmtContext, NodePtr};
use crate::fk_check_attrs::transformFKConstraints;
use crate::index_constraint::transformIndexConstraints;
use crate::partition::transformPartitionCmd;
use crate::serial::generateSerialExtraStmts;

const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
const INVALID_ATTR_NUMBER: i16 = 0;

/// `makeRangeVar(schemaname, relname, location)` (`nodes/makefuncs.c`).
fn make_range_var<'mcx>(
    mcx: Mcx<'mcx>,
    schemaname: Option<&str>,
    relname: &str,
    location: i32,
) -> PgResult<RangeVar<'mcx>> {
    Ok(RangeVar {
        catalogname: None,
        schemaname: match schemaname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        relname: Some(PgString::from_str_in(relname, mcx)?),
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        alias: None,
        location,
    })
}

/// `makeTypeNameFromOid(typeOid, typmod)` (`nodes/makefuncs.c`) as a `Node`.
fn make_type_name_node<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid, typmod: i32) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_type_name(
            mcx,
            TypeName {
                names: PgVec::new_in(mcx),
                typeOid: type_oid,
                setof: false,
                pct_type: false,
                typmods: PgVec::new_in(mcx),
                typemod: typmod,
                arrayBounds: PgVec::new_in(mcx),
                location: -1,
            },
        )?,
    )
}

/// `makeDefElem(name, arg, location)` (`nodes/makefuncs.c`) as a `Node`.
fn make_def_elem_node<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    arg: Option<NodePtr<'mcx>>,
    location: i32,
) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_def_elem(
            mcx,
            DefElem {
                defnamespace: None,
                defname: Some(PgString::from_str_in(name, mcx)?),
                arg,
                defaction: DEFELEM_UNSPEC,
                location,
            },
        )?,
    )
}

/// `makeNode(ColumnDef)` with only `colname` set (everything else zero), as
/// the AT_AddIdentity case constructs.
fn make_bare_column_def<'mcx>(
    mcx: Mcx<'mcx>,
    colname: &str,
) -> PgResult<types_nodes::rawnodes::ColumnDef<'mcx>> {
    Ok(types_nodes::rawnodes::ColumnDef {
        colname: Some(PgString::from_str_in(colname, mcx)?),
        typeName: None,
        compression: None,
        inhcount: 0,
        is_local: false,
        is_not_null: false,
        is_from_type: false,
        storage: 0,
        storage_name: None,
        raw_default: None,
        cooked_default: None,
        identity: 0,
        identitySequence: None,
        generated: 0,
        collClause: None,
        collOid: 0,
        constraints: PgVec::new_in(mcx),
        fdwoptions: PgVec::new_in(mcx),
        location: -1,
    })
}

/// Build a fresh `AT_AddConstraint`/`AT_AddIndex`/`AT_AddIndexConstraint`
/// subcommand carrying `def`.
fn make_alter_cmd<'mcx>(
    mcx: Mcx<'mcx>,
    subtype: types_nodes::ddlnodes::AlterTableType,
    def: NodePtr<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_alter_table_cmd(
            mcx,
            AlterTableCmd {
                subtype,
                name: None,
                num: 0,
                newowner: None,
                def: Some(def),
                behavior: types_nodes::parsenodes::DROP_RESTRICT,
                missing_ok: false,
                recurse: false,
            },
        )?,
    )
}

/// `transformAlterTableStmt(relid, stmt, queryString, &beforeStmts,
/// &afterStmts)` (parse_utilcmd.c:3524). Returns `(stmt, beforeStmts,
/// afterStmts)`.
pub fn transformAlterTableStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<(NodePtr<'mcx>, PgVec<'mcx, NodePtr<'mcx>>, PgVec<'mcx, NodePtr<'mcx>>)> {
    let stmt_node = PgBox::into_inner(stmt);
    let mut stmt = match stmt_node.into_altertablestmt() {
        Some(s) => s,
        None => unreachable!("transformAlterTableStmt: not an AlterTableStmt node"),
    };

    let mut newcmds: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let mut skip_validation = true;

    // Caller is responsible for locking the relation.
    let rel = relation_open(mcx, relid, NoLock)?;
    let is_partition = rel.rd_rel.relispartition;
    let relkind = rel.rd_rel.relkind;

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);
    let nsitem =
        addRangeTableEntryForRelation(mcx, &mut pstate, &rel, AccessShareLock, None, false, true)?;
    addNSItemToQuery(mcx, &mut pstate, nsitem, false, true, true)?;

    // Set up CreateStmtContext.
    let relation_clone = match stmt.relation.as_deref() {
        Some(n) => Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
        None => None,
    };
    let (stmt_type, isforeign): (&'static str, bool) = if relkind == RELKIND_FOREIGN_TABLE {
        ("ALTER FOREIGN TABLE", true)
    } else {
        ("ALTER TABLE", false)
    };
    let mut cxt = CreateStmtContext {
        mcx,
        pstate,
        stmtType: stmt_type,
        relation: relation_clone,
        rel_oid: relid,
        inhRelations: PgVec::new_in(mcx),
        isforeign,
        isalter: true,
        columns: PgVec::new_in(mcx),
        ckconstraints: PgVec::new_in(mcx),
        nnconstraints: PgVec::new_in(mcx),
        fkconstraints: PgVec::new_in(mcx),
        ixconstraints: PgVec::new_in(mcx),
        likeclauses: PgVec::new_in(mcx),
        blist: PgVec::new_in(mcx),
        alist: PgVec::new_in(mcx),
        pkey: None,
        ispartitioned: relkind == RELKIND_PARTITIONED_TABLE,
        partbound: None,
        ofType: false,
    };

    // Transform ALTER subcommands that need it (most don't).
    let cmds = core::mem::replace(&mut stmt.cmds, PgVec::new_in(mcx));
    for cmd_node in cmds.into_iter() {
        let mut cmd = match PgBox::into_inner(cmd_node).into_altertablecmd() {
            Some(c) => c,
            None => unreachable!("AlterTableStmt.cmds element is an AlterTableCmd"),
        };
        let subtype = cmd.subtype;

        match subtype {
            AT_AddColumn => {
                // ColumnDef *def = castNode(ColumnDef, cmd->def);
                let def_node = cmd.def.take().expect("AT_AddColumn: cmd.def is NULL");
                let def = match PgBox::into_inner(def_node).into_columndef() {
                    Some(d) => d,
                    None => unreachable!("AT_AddColumn: cmd.def is not a ColumnDef"),
                };

                // transformColumnDefinition(&cxt, def): processes the column's
                // inline constraints (incl. moving a DEFAULT into def->raw_default)
                // and appends the transformed column to cxt.columns.
                let def_ptr = mcx::alloc_in(mcx, Node::mk_column_def(mcx, def)?)?;
                transformColumnDefinition(&mut cxt, def_ptr)?;

                // Pull the transformed ColumnDef back out of cxt.columns.
                let mut transformed = match PgBox::into_inner(
                    cxt.columns
                        .pop()
                        .expect("transformColumnDefinition appended a column"),
                )
                .into_columndef()
                {
                    Some(d) => d,
                    None => unreachable!("cxt.columns tail is a ColumnDef"),
                };

                // If the column has a non-null default, we can't skip validation
                // of foreign keys. (Read AFTER the transform, which sets it.)
                if transformed.raw_default.is_some() {
                    skip_validation = false;
                }

                // All constraints are processed in other ways; remove the
                // original list before the column is reattached to the cmd.
                transformed.constraints = PgVec::new_in(mcx);
                cmd.def = Some(mcx::alloc_in(mcx, Node::mk_column_def(mcx, transformed)?)?);
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }

            AT_AddConstraint => {
                // The original AddConstraint cmd node doesn't go to newcmds.
                let def = cmd.def.as_deref().expect("AT_AddConstraint: cmd.def is NULL");
                if def.node_tag() == ntag::T_Constraint {
                    let contype = def.expect_constraint().contype;
                    let def_owned = mcx::alloc_in(mcx, def.clone_in(mcx)?)?;
                    transformTableConstraint(&mut cxt, def_owned)?;
                    if contype == ConstrType::CONSTR_FOREIGN {
                        skip_validation = false;
                    }
                } else {
                    return Err(ereport(ERROR)
                        .errmsg(alloc::format!(
                            "unrecognized node type: {}",
                            def.node_tag().0
                        ))
                        .into_error());
                }
            }

            AT_AlterColumnType => {
                let def_node = cmd.def.take().expect("AT_AlterColumnType: cmd.def is NULL");
                let mut def = match PgBox::into_inner(def_node).into_columndef() {
                    Some(d) => d,
                    None => unreachable!("AT_AlterColumnType: cmd.def is not a ColumnDef"),
                };

                // For ALTER COLUMN TYPE, transform the USING clause if specified.
                if let Some(raw) = def.raw_default.take() {
                    let raw_inner = PgBox::into_inner(raw);
                    let cooked = transformExpr(
                        &mut cxt.pstate,
                        Some(raw_inner),
                        EXPR_KIND_ALTER_COL_TRANSFORM,
                    )?;
                    def.cooked_default = match cooked {
                        // Bring the parser-arena `'static` result into `mcx` (Node
                        // wrap is `'mcx`; `Expr` is invariant so clone_in is required).
                        Some(e) => Some(mcx::alloc_in(mcx, Node::mk_expr(mcx, e.clone_in(mcx)?)?)?),
                        None => None,
                    };
                }

                // For identity column, create ALTER SEQUENCE to change the data
                // type of the sequence. (Skip partitions.)
                if !is_partition {
                    let colname = cmd
                        .name
                        .as_ref()
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    let attnum = get_attnum(relid, &colname)?;
                    if attnum == INVALID_ATTR_NUMBER {
                        return undefined_column_error(mcx, &colname, relid);
                    }
                    if attnum > 0 && rel.rd_att.attr((attnum - 1) as usize).attidentity != 0 {
                        let seq_relid = getIdentitySequence(mcx, &rel, attnum, false)?;
                        let type_name = def
                            .typeName
                            .as_deref()
                            .expect("AT_AlterColumnType: ColumnDef.typeName is NULL");
                        let type_name_pn = crate::coltype::raw_typename_to_parse(type_name)?;
                        let type_oid = typenameTypeId(mcx, Some(&cxt.pstate), &type_name_pn)?;
                        let as_arg = make_type_name_node(mcx, type_oid, -1)?;
                        let as_def = make_def_elem_node(mcx, "as", Some(as_arg), -1)?;
                        let mut options: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                        options.push(as_def);
                        let seq_rangevar = build_seq_rangevar(mcx, seq_relid)?;
                        let altseqstmt = AlterSeqStmt {
                            sequence: Some(seq_rangevar),
                            options,
                            for_identity: true,
                            missing_ok: false,
                        };
                        cxt.blist
                            .push(mcx::alloc_in(mcx, Node::mk_alter_seq_stmt(mcx, altseqstmt)?)?);
                    }
                }

                cmd.def = Some(mcx::alloc_in(mcx, Node::mk_column_def(mcx, def)?)?);
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }

            AT_AddIdentity => {
                // Constraint *def = castNode(Constraint, cmd->def);
                let def = cmd.def.as_deref().expect("AT_AddIdentity: cmd.def is NULL");
                let con = def.expect_constraint();
                let generated_when = con.generated_when;
                let con_options = {
                    let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                    for o in con.options.iter() {
                        v.push(mcx::alloc_in(mcx, o.clone_in(mcx)?)?);
                    }
                    v
                };
                let colname = cmd
                    .name
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();

                // newdef = makeNode(ColumnDef); newdef->colname = cmd->name;
                // newdef->identity = def->generated_when;
                // newdef = makeNode(ColumnDef); only colname + identity are set.
                let mut newdef = make_bare_column_def(mcx, &colname)?;
                newdef.identity = generated_when;

                let attnum = get_attnum(relid, &colname)?;
                if attnum == INVALID_ATTR_NUMBER {
                    return undefined_column_error(mcx, &colname, relid);
                }

                generateSerialExtraStmts(
                    &mut cxt,
                    &mut newdef,
                    get_atttype(relid, attnum)?,
                    con_options,
                    true,
                    true,
                )?;

                cmd.def = Some(mcx::alloc_in(mcx, Node::mk_column_def(mcx, newdef)?)?);
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }

            AT_SetIdentity => {
                // Split options into ALTER SEQUENCE opts and ALTER TABLE opts.
                let mut newseqopts: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                let mut newdef: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
                let def_list_node = cmd.def.take().expect("AT_SetIdentity: cmd.def is NULL");
                let def_list = PgBox::into_inner(def_list_node);
                for el in def_list.expect_list().iter() {
                    let defname = el
                        .expect_defelem()
                        .defname
                        .as_ref()
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    let owned = mcx::alloc_in(mcx, el.clone_in(mcx)?)?;
                    if defname == "generated" {
                        newdef.push(owned);
                    } else {
                        newseqopts.push(owned);
                    }
                }

                let colname = cmd
                    .name
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let attnum = get_attnum(relid, &colname)?;
                if attnum == INVALID_ATTR_NUMBER {
                    return undefined_column_error(mcx, &colname, relid);
                }

                let seq_relid = getIdentitySequence(mcx, &rel, attnum, true)?;
                if OidIsValid(seq_relid) {
                    let seq_rangevar = build_seq_rangevar(mcx, seq_relid)?;
                    let seqstmt = AlterSeqStmt {
                        sequence: Some(seq_rangevar),
                        options: newseqopts,
                        for_identity: true,
                        missing_ok: false,
                    };
                    cxt.blist
                        .push(mcx::alloc_in(mcx, Node::mk_alter_seq_stmt(mcx, seqstmt)?)?);
                }

                cmd.def = Some(mcx::alloc_in(mcx, Node::mk_list(mcx, newdef)?)?);
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }

            AT_AttachPartition | AT_DetachPartition => {
                let def = cmd.def.as_deref().expect("partition cmd: cmd.def is NULL");
                transformPartitionCmd(&mut cxt, def)?;
                // assign transformed value of the partition bound
                let bound = cxt.partbound.take();
                let mut partcmd = match PgBox::into_inner(cmd.def.take().unwrap()).into_partitioncmd()
                {
                    Some(p) => p,
                    None => unreachable!("partition cmd: cmd.def is not a PartitionCmd"),
                };
                partcmd.bound = bound;
                cmd.def = Some(mcx::alloc_in(mcx, Node::mk_partition_cmd(mcx, partcmd)?)?);
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }

            _ => {
                // Subcommand types that don't require transformation: emit
                // unchanged.
                newcmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?);
            }
        }
    }

    // Transfer anything we already have in cxt.alist into save_alist, to keep it
    // separate from the output of transformIndexConstraints.
    let save_alist = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));

    // Postprocess constraints.
    transformIndexConstraints(&mut cxt)?;
    transformFKConstraints(&mut cxt, skip_validation, true)?;
    transformCheckConstraints(&mut cxt, false);

    // Push any index-creation commands into the ALTER (cxt.alist holds only
    // IndexStmts generated from primary key constraints).
    let alist_now = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));
    for istmt in alist_now.into_iter() {
        if istmt.node_tag() == ntag::T_IndexStmt {
            let index_oid = istmt.expect_indexstmt().indexOid;
            // The IndexStmt attached must already have been through
            // transformIndexStmt; transformIndexConstraints produced it.
            let idxstmt = crate::index_stats::transformIndexStmt(mcx, relid, istmt, query_string)?;
            let subtype = if OidIsValid(index_oid) {
                AT_AddIndexConstraint
            } else {
                AT_AddIndex
            };
            newcmds.push(make_alter_cmd(mcx, subtype, idxstmt)?);
        } else {
            return Err(ereport(ERROR)
                .errmsg(alloc::format!(
                    "unexpected stmt type {}",
                    istmt.node_tag().0
                ))
                .into_error());
        }
    }

    // Append any CHECK, NOT NULL or FK constraints to the commands list.
    let ck = core::mem::replace(&mut cxt.ckconstraints, PgVec::new_in(mcx));
    for def in ck.into_iter() {
        newcmds.push(make_alter_cmd(mcx, AT_AddConstraint, def)?);
    }
    let nn = core::mem::replace(&mut cxt.nnconstraints, PgVec::new_in(mcx));
    for def in nn.into_iter() {
        newcmds.push(make_alter_cmd(mcx, AT_AddConstraint, def)?);
    }
    let fk = core::mem::replace(&mut cxt.fkconstraints, PgVec::new_in(mcx));
    for def in fk.into_iter() {
        newcmds.push(make_alter_cmd(mcx, AT_AddConstraint, def)?);
    }

    free_parsestate(core::mem::replace(
        &mut cxt.pstate,
        make_parsestate(mcx, None)?,
    ))?;

    // Close rel (owned carrier releases the relcache reference; lock kept).
    table_close(rel, NoLock)?;

    // Output results.
    stmt.cmds = newcmds;
    let before = core::mem::replace(&mut cxt.blist, PgVec::new_in(mcx));
    // *afterStmts = list_concat(cxt.alist, save_alist); cxt.alist is empty here.
    let mut after = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));
    for n in save_alist.into_iter() {
        after.push(n);
    }

    let stmt_ptr = mcx::alloc_in(mcx, Node::mk_alter_table_stmt(mcx, stmt)?)?;
    Ok((stmt_ptr, before, after))
}

/// `makeRangeVar(get_namespace_name(get_rel_namespace(seq_relid)),
/// get_rel_name(seq_relid), -1)` as a boxed `Node`.
fn build_seq_rangevar<'mcx>(mcx: Mcx<'mcx>, seq_relid: Oid) -> PgResult<NodePtr<'mcx>> {
    let nspid = get_rel_namespace(seq_relid)?;
    let nspname = get_namespace_name(mcx, nspid)?;
    let relname = get_rel_name(mcx, seq_relid)?;
    let rv = make_range_var(
        mcx,
        nspname.as_deref(),
        relname.as_deref().unwrap_or(""),
        -1,
    )?;
    mcx::alloc_in(mcx, Node::mk_range_var(mcx, rv)?)
}

/// `ereport(ERROR, column "%s" of relation "%s" does not exist)`.
fn undefined_column_error<'mcx, T>(
    mcx: Mcx<'mcx>,
    colname: &str,
    relid: Oid,
) -> PgResult<T> {
    let relname = get_rel_name(mcx, relid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_COLUMN)
        .errmsg(alloc::format!(
            "column \"{colname}\" of relation \"{relname}\" does not exist"
        ))
        .into_error())
}
