//! makefuncs.c residue. make_whole_row_var returns the unsealed Var so
//! callers set location/varreturningtype before Node::mk (C mutates post-make).
use mcx::Mcx;
use types_core::{Index, InvalidOid, OidIsValid};
use types_error::{ErrorLocation, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::primnodes::{Var, VarReturningType};

use crate::node_funcs::{expr_collation, expr_type};

fn make_var<'mcx>(varno: Index, varattno: i16, vartype: types_core::Oid, varcollid: types_core::Oid, varlevelsup: Index) -> Var<'mcx> {
    Var {
        varno: varno as i32,
        varattno,
        vartype,
        vartypmod: -1,
        varcollid,
        varnullingrels: types_nodes::Bitmapset::empty(),
        varlevelsup,
        varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
        varnosyn: varno,
        varattnosyn: varattno,
        location: -1,
    }
}

pub fn make_whole_row_var<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    varno: Index,
    varlevelsup: Index,
    allow_scalar: bool,
) -> PgResult<Var<'mcx>> {
    match rte.rtekind {
        RTEKind::RTE_RELATION => {
            let toid = lsyscache::get_rel_type_id(rte.relid)?;
            if !OidIsValid(toid) {
                return Err(composite_type_err(mcx, rte.relid));
            }
            Ok(make_var(varno, 0, toid, InvalidOid, varlevelsup))
        }
        RTEKind::RTE_SUBQUERY => {
            let toid = if OidIsValid(rte.relid) {
                let toid = lsyscache::get_rel_type_id(rte.relid)?;
                if !OidIsValid(toid) {
                    return Err(composite_type_err(mcx, rte.relid));
                }
                toid
            } else if !rte.functions.is_nil() {
                debug_assert!(!allow_scalar);
                let fexpr = rte
                    .functions
                    .nth(0)
                    .as_range_tbl_function()
                    .expect("RangeTblFunction")
                    .funcexpr
                    .expect("funcexpr");
                let toid = expr_type(fexpr);
                if lsyscache::type_is_rowtype(toid)? {
                    toid
                } else {
                    types_core::catalog::RECORDOID
                }
            } else {
                types_core::catalog::RECORDOID
            };
            Ok(make_var(varno, 0, toid, InvalidOid, varlevelsup))
        }
        RTEKind::RTE_FUNCTION => {
            if rte.funcordinality || rte.functions.len() != 1 {
                return Ok(make_var(
                    varno,
                    0,
                    types_core::catalog::RECORDOID,
                    InvalidOid,
                    varlevelsup,
                ));
            }
            let fexpr = rte
                .functions
                .nth(0)
                .as_range_tbl_function()
                .expect("RangeTblFunction")
                .funcexpr
                .expect("funcexpr");
            let toid = expr_type(fexpr);
            if lsyscache::type_is_rowtype(toid)? {
                Ok(make_var(varno, 0, toid, InvalidOid, varlevelsup))
            } else if allow_scalar {
                Ok(make_var(varno, 1, toid, expr_collation(fexpr), varlevelsup))
            } else {
                Ok(make_var(varno, 0, types_core::catalog::RECORDOID, InvalidOid, varlevelsup))
            }
        }
        _ => Ok(make_var(varno, 0, types_core::catalog::RECORDOID, InvalidOid, varlevelsup)),
    }
}

#[cold]
fn composite_type_err(
    mcx: Mcx<'_>,
    relid: types_core::Oid,
) -> Box<types_error::PgError> {
    let name = lsyscache::get_rel_name(mcx, relid).ok().flatten();
    let name = name.as_ref().map(|s| s.as_str()).unwrap_or("");
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("relation \"{name}\" does not have a composite type"))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "makeWholeRowVar")),
    )
}
