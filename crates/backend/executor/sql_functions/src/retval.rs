// check_sql_fn_retval / check_sql_stmt_retval (executor/functions.c), scalar
// leg only: composite/RECORD results and the upper-projection Query injection
// are loud; insertDroppedCols has no caller here.
use coerce::CoercionContext;
use elog::ereport;
use types_nodes::primnodes::CoercionForm;
use mcx::{Mcx, PgVec};
use types_core::catalog::{RECORDOID, VOIDOID};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_FUNCTION_DEFINITION, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::TargetEntry;
use types_nodes::{Node, NodeList};

use lsyscache::typ::{
    TYPTYPE_BASE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE, TYPTYPE_RANGE,
};

#[cold]
pub(crate) fn retval_mismatch_final_stmt(rettype: Oid) -> Box<PgError> {
    let tn = format_type::format_type_be(rettype).unwrap_or_else(|_| "???".into());
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg(format!("return type mismatch in function declared to return {tn}"))
        .errdetail(
            "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING.",
        )
        .into_error()
        .into()
}

#[cold]
fn retval_mismatch(rettype: Oid, detail: String) -> Box<PgError> {
    let tn = format_type::format_type_be(rettype).unwrap_or_else(|_| "???".into());
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
        .errmsg(format!("return type mismatch in function declared to return {tn}"))
        .errdetail(detail)
        .into_error()
        .into()
}

pub fn check_sql_stmt_retval<'mcx>(
    mcx: Mcx<'mcx>,
    query_list: &mut PgVec<'mcx, Query<'mcx>>,
    rettype: Oid,
) -> PgResult<()> {
    if rettype == VOIDOID {
        return Ok(());
    }
    let Some(idx) = query_list.iter().rposition(|q| q.canSetTag) else {
        return Err(retval_mismatch_final_stmt(rettype));
    };
    check_query_retval(mcx, &query_list[idx], rettype)
}

pub(crate) fn check_query_retval<'mcx>(
    mcx: Mcx<'mcx>,
    q: &Query<'mcx>,
    rettype: Oid,
) -> PgResult<()> {
    if rettype == VOIDOID {
        return Ok(());
    }
    let (tlist, tlist_is_modifiable): (&NodeList<'mcx>, bool) = match q.commandType {
        CmdType::CMD_SELECT => (&q.targetList, q.setOperations.is_none()),
        CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
            if !q.returningList.is_nil() =>
        {
            (&q.returningList, true)
        }
        _ => return Err(retval_mismatch_final_stmt(rettype)),
    };

    let fn_typtype = lsyscache::typ::get_typtype(rettype)?;
    if !matches!(
        fn_typtype,
        TYPTYPE_BASE | TYPTYPE_DOMAIN | TYPTYPE_ENUM | TYPTYPE_RANGE | TYPTYPE_MULTIRANGE
    ) {
        if fn_typtype == lsyscache::typ::TYPTYPE_COMPOSITE || rettype == RECORDOID {
            panic!("check_sql_fn_retval: composite/RECORD SQL function results unported");
        }
        let tn = format_type::format_type_be(rettype)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!("return type {tn} is not supported for SQL functions"))
            .into_error()
            .into());
    }

    let nonjunk = tlist
        .iter()
        .filter(|n| n.as_target_entry().is_some_and(|t| !t.resjunk))
        .count();
    if nonjunk != 1 {
        return Err(retval_mismatch(
            rettype,
            "Final statement must return exactly one column.".into(),
        ));
    }
    let tle_node: Node<'mcx> = tlist.iter().next().expect("nonempty tlist");
    let tle = tle_node.as_target_entry().expect("first tlist entry is a TargetEntry");
    assert!(!tle.resjunk, "non-junk TLEs must come first");
    let expr = tle.expr;
    let exprtype = parse_expr::expr_type(expr);
    if exprtype == rettype {
        return Ok(());
    }
    if !tlist_is_modifiable || tle.ressortgroupref != 0 {
        panic!(
            "check_sql_fn_retval: upper-projection coercion (setop or sort/group result column) \
             unported"
        );
    }
    let pstate = parser_small1::make_parsestate(mcx, None);
    let cast = coerce::coerce_to_target_type(
        mcx,
        &pstate,
        expr,
        exprtype,
        rettype,
        -1,
        CoercionContext::COERCION_ASSIGNMENT,
        CoercionForm::COERCE_IMPLICIT_CAST,
        -1,
    )?;
    let Some(cast) = cast else {
        let actual = format_type::format_type_be(exprtype)?;
        return Err(retval_mismatch(rettype, format!("Actual return type is {actual}.")));
    };
    parse_collate::assign_expr_collations(mcx, &pstate, cast)?;
    // SAFETY: sole mutation of this parser-owned tree; no derived refs live.
    unsafe {
        tle_node.with_mut::<TargetEntry, _>(|t| t.expr = cast);
    }
    Ok(())
}
