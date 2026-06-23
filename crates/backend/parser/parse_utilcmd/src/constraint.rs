//! `transformTableConstraint` + `transformCheckConstraints` (`parse_utilcmd.c`).
//!
//! Both are fully node-independent: they bucket / mark `Constraint` nodes in
//! the [`CreateStmtContext`] accumulators, with no expression walking. Ported
//! 1:1, same branch order and error text/SQLSTATE as the C source.

use alloc::format;

use ::utils_error::ereport;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};

use ::nodes::ddlnodes::{
    CONSTR_ATTR_DEFERRABLE, CONSTR_ATTR_DEFERRED, CONSTR_ATTR_ENFORCED, CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_NOT_DEFERRABLE, CONSTR_ATTR_NOT_ENFORCED, CONSTR_CHECK, CONSTR_DEFAULT,
    CONSTR_EXCLUSION, CONSTR_FOREIGN, CONSTR_GENERATED, CONSTR_IDENTITY, CONSTR_NOTNULL,
    CONSTR_NULL, CONSTR_PRIMARY, CONSTR_UNIQUE,
};
use ::nodes::nodes::ntag;

use crate::core::{CreateStmtContext, NodePtr};
use crate::errpos::parser_errposition;

/// `transformTableConstraint` — transform a `Constraint` node within CREATE
/// TABLE or ALTER TABLE: drop it into the matching `cxt` accumulator.
pub fn transformTableConstraint<'mcx>(
    cxt: &mut CreateStmtContext<'mcx>,
    constraint: NodePtr<'mcx>,
) -> PgResult<()> {
    let (contype, location, is_no_inherit) = match constraint.node_tag() {
        ntag::T_Constraint => {
            let c = constraint.expect_constraint();
            (c.contype, c.location, c.is_no_inherit)
        }
        _ => unreachable!("transformTableConstraint: not a Constraint node: {}", constraint.node_tag()),
    };

    match contype {
        CONSTR_PRIMARY => {
            if cxt.isforeign {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("primary key constraints are not supported on foreign tables")
                    .errposition(parser_errposition(&cxt.pstate, location))
                    .into_error());
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
            cxt.ixconstraints.push(constraint);
        }

        CONSTR_EXCLUSION => {
            if cxt.isforeign {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("exclusion constraints are not supported on foreign tables")
                    .errposition(parser_errposition(&cxt.pstate, location))
                    .into_error());
            }
            cxt.ixconstraints.push(constraint);
        }

        CONSTR_CHECK => {
            cxt.ckconstraints.push(constraint);
        }

        CONSTR_NOTNULL => {
            if cxt.ispartitioned && is_no_inherit {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("not-null constraints on partitioned tables cannot be NO INHERIT")
                    .into_error());
            }
            cxt.nnconstraints.push(constraint);
        }

        CONSTR_FOREIGN => {
            if cxt.isforeign {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("foreign key constraints are not supported on foreign tables")
                    .errposition(parser_errposition(&cxt.pstate, location))
                    .into_error());
            }
            cxt.fkconstraints.push(constraint);
        }

        CONSTR_NULL
        | CONSTR_DEFAULT
        | CONSTR_ATTR_DEFERRABLE
        | CONSTR_ATTR_NOT_DEFERRABLE
        | CONSTR_ATTR_DEFERRED
        | CONSTR_ATTR_IMMEDIATE
        | CONSTR_ATTR_ENFORCED
        | CONSTR_ATTR_NOT_ENFORCED => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "invalid context for constraint type {}",
                    contype as i32
                ))
                .into_error());
        }

        // CONSTR_IDENTITY / CONSTR_GENERATED are not table-level constraints;
        // they reach `transformTableConstraint` only through a corrupt parse
        // tree, so the C `default:` "unrecognized constraint type" arm applies.
        CONSTR_IDENTITY | CONSTR_GENERATED => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized constraint type: {}", contype as i32))
                .into_error());
        }
    }
    Ok(())
}

/// `transformCheckConstraints` — when creating a new (non-foreign) table, mark
/// the collected CHECK constraints as skip-validation / valid, since the table
/// is new and therefore empty.  Node-independent.
pub fn transformCheckConstraints(cxt: &mut CreateStmtContext<'_>, skip_validation: bool) {
    if cxt.ckconstraints.is_empty() {
        return;
    }

    if skip_validation {
        for c in cxt.ckconstraints.iter_mut() {
            if let Some(constraint) = c.as_constraint_mut() {
                constraint.skip_validation = true;
                constraint.initially_valid = constraint.is_enforced;
            }
        }
    }
}
