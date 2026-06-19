//! `transformCreateSchemaStmtElements` + `setSchemaName` (`parse_utilcmd.c`).
//!
//! Node-independent: splits the CREATE SCHEMA element list into per-kind
//! buckets (so there are no forward references), setting / checking the schema
//! name on each element's target `RangeVar`. Ported 1:1, same ordering and
//! error text/SQLSTATE as the C source.

use mcx::{Mcx, PgString, PgVec};

use backend_utils_error::ereport;
use types_error::{PgResult, ERRCODE_INVALID_SCHEMA_DEFINITION, ERROR};

use types_nodes::nodes::{ntag, Node};

use crate::core::{CreateSchemaStmtContext, NodePtr};

/// `transformCreateSchemaStmtElements` — analyze the elements of a CREATE SCHEMA
/// statement, returning a list ordered so there are no forward references
/// (sequences, tables, views, indexes, triggers, grants).
pub fn transformCreateSchemaStmtElements<'mcx>(
    mcx: Mcx<'mcx>,
    schema_elts: &[Node<'_>],
    schema_name: &str,
) -> PgResult<PgVec<'mcx, Node<'mcx>>> {
    let mut cxt = CreateSchemaStmtContext {
        schemaname: Some(PgString::from_str_in(schema_name, mcx)?),
        sequences: PgVec::new_in(mcx),
        tables: PgVec::new_in(mcx),
        views: PgVec::new_in(mcx),
        indexes: PgVec::new_in(mcx),
        triggers: PgVec::new_in(mcx),
        grants: PgVec::new_in(mcx),
    };

    // Run through each schema element, separating statements by type.
    for element in schema_elts {
        let mut element = mcx::alloc_in(mcx, element.clone_in(mcx)?)?;
        match element.node_tag() {
            ntag::T_CreateSeqStmt => {
                let elp = element.as_createseqstmt_mut().unwrap();
                set_schema_on_rangevar(mcx, cxt.schemaname.as_ref(), elp.sequence.as_deref_mut())?;
                cxt.sequences.push(element);
            }
            ntag::T_CreateStmt => {
                let elp = element.as_createstmt_mut().unwrap();
                set_schema_on_rangevar(mcx, cxt.schemaname.as_ref(), elp.relation.as_deref_mut())?;
                // XXX todo: deal with constraints
                cxt.tables.push(element);
            }
            ntag::T_ViewStmt => {
                let elp = element.as_viewstmt_mut().unwrap();
                set_schema_on_rangevar(mcx, cxt.schemaname.as_ref(), elp.view.as_deref_mut())?;
                // XXX todo: deal with references between views
                cxt.views.push(element);
            }
            ntag::T_IndexStmt => {
                let elp = element.as_indexstmt_mut().unwrap();
                set_schema_on_rangevar(mcx, cxt.schemaname.as_ref(), elp.relation.as_deref_mut())?;
                cxt.indexes.push(element);
            }
            ntag::T_CreateTrigStmt => {
                let elp = element.as_createtrigstmt_mut().unwrap();
                set_schema_on_rangevar(mcx, cxt.schemaname.as_ref(), elp.relation.as_deref_mut())?;
                cxt.triggers.push(element);
            }
            ntag::T_GrantStmt => {
                cxt.grants.push(element);
            }
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "unrecognized node type: {}",
                        element.node_tag()
                    ))
                    .into_error());
            }
        }
    }

    let mut result: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    extend_unbox(&mut result, cxt.sequences);
    extend_unbox(&mut result, cxt.tables);
    extend_unbox(&mut result, cxt.views);
    extend_unbox(&mut result, cxt.indexes);
    extend_unbox(&mut result, cxt.triggers);
    extend_unbox(&mut result, cxt.grants);
    Ok(result)
}

/// Move the boxed nodes out into the unboxed result list (C's `list_concat`
/// onto a `List *` of `Node *`; the inward seam contract returns
/// `PgVec<Node>`).
fn extend_unbox<'mcx>(dst: &mut PgVec<'mcx, Node<'mcx>>, src: PgVec<'mcx, NodePtr<'mcx>>) {
    for boxed in src {
        dst.push(mcx::PgBox::into_inner(boxed));
    }
}

/// Helper: apply [`setSchemaName`] to an element's target `RangeVar` (carried as
/// a `Node::RangeVar` behind the element's pointer field).
fn set_schema_on_rangevar<'mcx>(
    mcx: Mcx<'mcx>,
    context_schema: Option<&PgString<'mcx>>,
    rv: Option<&mut Node<'mcx>>,
) -> PgResult<()> {
    match rv.and_then(|n| n.as_rangevar_mut()) {
        Some(rangevar) => {
            setSchemaName(mcx, context_schema, &mut rangevar.schemaname)
        }
        None => Ok(()),
    }
}

/// `setSchemaName` — set or check a schema name in an element of a CREATE SCHEMA
/// command.  If the element has no schema name, fill it in from the context;
/// otherwise the two must match.
pub fn setSchemaName<'mcx>(
    mcx: Mcx<'mcx>,
    context_schema: Option<&PgString<'mcx>>,
    stmt_schema_name: &mut Option<PgString<'mcx>>,
) -> PgResult<()> {
    if stmt_schema_name.is_none() {
        // *stmt_schema_name = unconstify(char *, context_schema);
        *stmt_schema_name = match context_schema {
            Some(s) => Some(s.clone_in(mcx)?),
            None => None,
        };
    } else {
        let stmt = stmt_schema_name.as_ref().map_or("", PgString::as_str);
        let ctx = context_schema.map_or("", PgString::as_str);
        if ctx != stmt {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_SCHEMA_DEFINITION)
                .errmsg(alloc::format!(
                    "CREATE specifies a schema ({stmt}) different from the one being created ({ctx})"
                ))
                .into_error());
        }
    }
    Ok(())
}
