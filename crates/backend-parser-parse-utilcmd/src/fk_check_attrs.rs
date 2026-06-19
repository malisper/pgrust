//! `transformFKConstraints` + `transformConstraintAttrs` (`parse_utilcmd.c`).
//!
//! Both node-independent: `transformFKConstraints` marks FK `Constraint`s and
//! (for CREATE / ADD COLUMN) gins up an `ALTER TABLE ... ADD CONSTRAINT`
//! statement; `transformConstraintAttrs` validates the trailing DEFERRABLE /
//! INITIALLY / ENFORCED attribute markers against the preceding constraint.
//! Ported 1:1, same branch order and error text/SQLSTATE as the C source.

use backend_utils_error::ereport;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};

use types_core::Oid;
use types_nodes::ddlnodes::{
    AlterTableCmd, AlterTableStmt, ConstrType, AT_AddConstraint, CONSTR_ATTR_DEFERRABLE,
    CONSTR_ATTR_DEFERRED, CONSTR_ATTR_ENFORCED, CONSTR_ATTR_IMMEDIATE, CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_NOT_ENFORCED, CONSTR_CHECK, CONSTR_EXCLUSION, CONSTR_FOREIGN, CONSTR_PRIMARY,
    CONSTR_UNIQUE,
};
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::{DROP_RESTRICT, OBJECT_TABLE};

use crate::core::{CreateStmtContext, NodePtr};
use crate::errpos::parser_errposition;

const INVALID_OID: Oid = 0;

/// `transformFKConstraints` — mark the collected FK `Constraint`s and, unless
/// this came from ADD CONSTRAINT, append an `ALTER TABLE ... ADD CONSTRAINT`
/// for each one to `cxt.alist` (to run after index creation).
pub fn transformFKConstraints<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    skip_validation: bool,
    is_add_constraint: bool,
) -> PgResult<()> {
    if cxt.fkconstraints.is_empty() {
        return Ok(());
    }

    if skip_validation {
        for c in cxt.fkconstraints.iter_mut() {
            if let Some(constraint) = c.as_constraint_mut() {
                constraint.skip_validation = true;
                constraint.initially_valid = constraint.is_enforced;
            }
        }
    }

    if !is_add_constraint {
        let mcx = cxt.mcx;

        // alterstmt->relation = cxt->relation; the owned context owns the
        // relation node, so clone it for the new statement.
        let relation = match cxt.relation.as_deref() {
            Some(n) => Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
            None => None,
        };

        let mut cmds: mcx::PgVec<'mcx, NodePtr<'mcx>> = mcx::PgVec::new_in(mcx);
        for constraint in cxt.fkconstraints.iter() {
            let altercmd = AlterTableCmd {
                subtype: AT_AddConstraint,
                name: None,
                num: 0,
                newowner: None,
                def: Some(mcx::alloc_in(mcx, constraint.clone_in(mcx)?)?),
                behavior: DROP_RESTRICT,
                missing_ok: false,
                recurse: false,
            };
            cmds.push(mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, altercmd))?);
        }

        let alterstmt = AlterTableStmt {
            relation,
            cmds,
            objtype: OBJECT_TABLE,
            missing_ok: false,
        };
        cxt.alist
            .push(mcx::alloc_in(mcx, Node::mk_alter_table_stmt(mcx, alterstmt))?);
    }
    Ok(())
}

/// `SUPPORTS_ATTRS(node)` — the C macro: a constraint that may carry the
/// trailing DEFERRABLE / INITIALLY attributes.
fn supports_attrs(con: Option<ConstrType>) -> bool {
    matches!(
        con,
        Some(CONSTR_PRIMARY) | Some(CONSTR_UNIQUE) | Some(CONSTR_EXCLUSION) | Some(CONSTR_FOREIGN)
    )
}

/// `transformConstraintAttrs` — process the trailing CONSTR_ATTR_* markers,
/// applying them to the preceding "primary" constraint in `constraintList` and
/// rejecting misplaced / duplicate / inconsistent ones.
///
/// In the owned tree the markers mutate the preceding `Constraint` node in
/// `constraint_list`; we walk by index so the mutation lands on the live
/// vector element (the C code mutates `lastprimarycon` in place).
pub fn transformConstraintAttrs<'mcx>(
    cxt: &CreateStmtContext<'mcx>,
    constraint_list: &mut [NodePtr<'mcx>],
) -> PgResult<()> {
    // Index of the last "primary" (attribute-bearing) constraint seen, or None.
    let mut lastprimary: Option<usize> = None;
    let mut saw_deferrability = false;
    let mut saw_initially = false;
    let mut saw_enforced = false;

    for i in 0..constraint_list.len() {
        let (contype, location) = match constraint_list[i].node_tag() {
            ntag::T_Constraint => {
                let c = constraint_list[i].expect_constraint();
                (c.contype, c.location)
            }
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "unrecognized node type: {}",
                        constraint_list[i].node_tag()
                    ))
                    .into_error());
            }
        };

        // The contype of the current lastprimary (for SUPPORTS_ATTRS / ENFORCED).
        let primary_contype = lastprimary.map(|j| match constraint_list[j].as_constraint() {
            Some(c) => c.contype,
            None => unreachable!(),
        });

        match contype {
            CONSTR_ATTR_DEFERRABLE => {
                if !supports_attrs(primary_contype) {
                    return Err(syntax_err(cxt, "misplaced DEFERRABLE clause", location));
                }
                if saw_deferrability {
                    return Err(syntax_err(
                        cxt,
                        "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed",
                        location,
                    ));
                }
                saw_deferrability = true;
                set_deferrable(&mut constraint_list[require_primary(lastprimary)?], true);
            }

            CONSTR_ATTR_NOT_DEFERRABLE => {
                if !supports_attrs(primary_contype) {
                    return Err(syntax_err(cxt, "misplaced NOT DEFERRABLE clause", location));
                }
                if saw_deferrability {
                    return Err(syntax_err(
                        cxt,
                        "multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed",
                        location,
                    ));
                }
                saw_deferrability = true;
                let lp = require_primary(lastprimary)?;
                set_deferrable(&mut constraint_list[lp], false);
                if saw_initially && get_initdeferred(&constraint_list[lp]) {
                    return Err(syntax_err(
                        cxt,
                        "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
                        location,
                    ));
                }
            }

            CONSTR_ATTR_DEFERRED => {
                if !supports_attrs(primary_contype) {
                    return Err(syntax_err(cxt, "misplaced INITIALLY DEFERRED clause", location));
                }
                if saw_initially {
                    return Err(syntax_err(
                        cxt,
                        "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed",
                        location,
                    ));
                }
                saw_initially = true;
                let lp = require_primary(lastprimary)?;
                set_initdeferred(&mut constraint_list[lp], true);

                // If only INITIALLY DEFERRED appears, assume DEFERRABLE
                if !saw_deferrability {
                    set_deferrable(&mut constraint_list[lp], true);
                } else if !get_deferrable(&constraint_list[lp]) {
                    return Err(syntax_err(
                        cxt,
                        "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
                        location,
                    ));
                }
            }

            CONSTR_ATTR_IMMEDIATE => {
                if !supports_attrs(primary_contype) {
                    return Err(syntax_err(cxt, "misplaced INITIALLY IMMEDIATE clause", location));
                }
                if saw_initially {
                    return Err(syntax_err(
                        cxt,
                        "multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed",
                        location,
                    ));
                }
                saw_initially = true;
                set_initdeferred(&mut constraint_list[require_primary(lastprimary)?], false);
            }

            CONSTR_ATTR_ENFORCED => {
                if !matches!(primary_contype, Some(CONSTR_CHECK) | Some(CONSTR_FOREIGN)) {
                    return Err(syntax_err(cxt, "misplaced ENFORCED clause", location));
                }
                if saw_enforced {
                    return Err(syntax_err(
                        cxt,
                        "multiple ENFORCED/NOT ENFORCED clauses not allowed",
                        location,
                    ));
                }
                saw_enforced = true;
                if let Some(c) = constraint_list[require_primary(lastprimary)?].as_constraint_mut() {
                    c.is_enforced = true;
                }
            }

            CONSTR_ATTR_NOT_ENFORCED => {
                if !matches!(primary_contype, Some(CONSTR_CHECK) | Some(CONSTR_FOREIGN)) {
                    return Err(syntax_err(cxt, "misplaced NOT ENFORCED clause", location));
                }
                if saw_enforced {
                    return Err(syntax_err(
                        cxt,
                        "multiple ENFORCED/NOT ENFORCED clauses not allowed",
                        location,
                    ));
                }
                saw_enforced = true;
                if let Some(c) = constraint_list[require_primary(lastprimary)?].as_constraint_mut() {
                    c.is_enforced = false;
                    // A NOT ENFORCED constraint must be marked as invalid.
                    c.skip_validation = true;
                    c.initially_valid = false;
                }
            }

            _ => {
                // Otherwise it's not an attribute: this is a new primary node.
                lastprimary = Some(i);
                saw_deferrability = false;
                saw_initially = false;
                saw_enforced = false;
            }
        }
    }
    Ok(())
}

fn require_primary(lastprimary: Option<usize>) -> PgResult<usize> {
    lastprimary.ok_or_else(|| {
        PgError::error("transformConstraintAttrs: constraint attribute without a preceding constraint")
    })
}

fn syntax_err(cxt: &CreateStmtContext<'_>, msg: &str, location: i32) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(msg)
        .errposition(parser_errposition(&cxt.pstate, location))
        .into_error()
}

fn set_deferrable(n: &mut NodePtr<'_>, v: bool) {
    if let Some(c) = n.as_constraint_mut() {
        c.deferrable = v;
    }
}
fn get_deferrable(n: &NodePtr<'_>) -> bool {
    n.as_ref().as_constraint().is_some_and(|c| c.deferrable)
}
fn set_initdeferred(n: &mut NodePtr<'_>, v: bool) {
    if let Some(c) = n.as_constraint_mut() {
        c.initdeferred = v;
    }
}
fn get_initdeferred(n: &NodePtr<'_>) -> bool {
    n.as_ref().as_constraint().is_some_and(|c| c.initdeferred)
}

// Suppress unused-const warning if INVALID_OID is not referenced; kept for
// parity with the C `InvalidOid` sentinel used by callers.
#[allow(dead_code)]
const _: Oid = INVALID_OID;
