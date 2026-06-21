//! The scan/modify target-relation helpers of `commands/explain.c`:
//! `ExplainScanTarget`, `ExplainModifyTarget`, `ExplainTargetRel`,
//! `ExplainIndexScanDetails`, and `explain_get_index_name`.
//!
//! These resolve relation / function / CTE names through the catalog
//! (`get_rel_name`, `get_func_name`, `get_rel_namespace`,
//! `get_namespace_name_or_temp`) and quote them through ruleutils
//! `quote_identifier`. None of them deparse expressions, so they are part of the
//! structural EXPLAIN slice (this is why `EXPLAIN SELECT * FROM pg_class`
//! prints "Seq Scan on pg_class").

extern crate alloc;

use alloc::format;

use mcx::{Mcx, PgString};
use types_core::primitive::Index;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_explain::{ExplainFormat, ExplainState};
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_scan::sdir::{ScanDirection, BackwardScanDirection, ForwardScanDirection};

use backend_commands_explain_format as fmt;
use backend_utils_adt_ruleutils as ruleutils;
use backend_utils_cache_lsyscache as lsyscache;

/// `explain_get_index_name(indexId)` (explain.c:4022). The
/// `explain_get_index_name_hook` is unported (no extension hook); the default
/// behavior looks the index name up in the catalogs. `elog(ERROR, ...)` on a
/// cache miss is an `Err`.
pub fn explain_get_index_name<'mcx>(mcx: Mcx<'mcx>, index_id: Oid) -> PgResult<PgString<'mcx>> {
    match lsyscache::relation::get_rel_name(mcx, index_id)? {
        Some(name) => Ok(name),
        None => Err(PgError::error(format!(
            "cache lookup failed for index {index_id}"
        ))),
    }
}

/// `ExplainIndexScanDetails(indexid, indexorderdir, es)` (explain.c:4324).
pub fn ExplainIndexScanDetails<'mcx>(
    es: &mut ExplainState<'mcx>,
    index_id: Oid,
    index_order_dir: ScanDirection,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let indexname = explain_get_index_name(mcx, index_id)?;

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        // ScanDirectionIsBackward(indexorderdir)
        if index_order_dir == BackwardScanDirection {
            es.str.try_push_str(" Backward")?;
        }
        let quoted = ruleutils::quote_identifier(mcx, indexname.as_str())?;
        es.str.try_push_str(" using ")?;
        es.str.try_push_str(quoted.as_str())?;
    } else {
        let scandir = if index_order_dir == BackwardScanDirection {
            "Backward"
        } else if index_order_dir == ForwardScanDirection {
            "Forward"
        } else {
            "???"
        };
        fmt::ExplainPropertyText("Scan Direction", scandir, es)?;
        fmt::ExplainPropertyText("Index Name", indexname.as_str(), es)?;
    }
    Ok(())
}

/// `ExplainScanTarget(plan, es)` (explain.c:4360) — show the target of a Scan
/// node: `ExplainTargetRel((Plan *) plan, plan->scanrelid, es)`.
pub fn ExplainScanTarget<'mcx>(
    es: &mut ExplainState<'mcx>,
    plan_node: &Node<'_>,
    scanrelid: Index,
) -> PgResult<()> {
    ExplainTargetRel(es, plan_node, scanrelid)
}

/// `ExplainModifyTarget(plan, es)` (explain.c:4373) — show the nominal target of
/// a ModifyTable node: `ExplainTargetRel((Plan *) plan, plan->nominalRelation,
/// es)`.
pub fn ExplainModifyTarget<'mcx>(
    es: &mut ExplainState<'mcx>,
    plan_node: &Node<'_>,
    nominal_relation: Index,
) -> PgResult<()> {
    ExplainTargetRel(es, plan_node, nominal_relation)
}

/// `rt_fetch(rti, rtable)` — 1-based access into the range table.
fn rt_fetch<'a, 'mcx>(rtable: &'a [RangeTblEntry<'mcx>], rti: Index) -> &'a RangeTblEntry<'mcx> {
    &rtable[(rti - 1) as usize]
}

/// `ExplainTargetRel(plan, rti, es)` (explain.c:4382) — show the target relation
/// of a scan or modify node.
pub fn ExplainTargetRel<'mcx>(
    es: &mut ExplainState<'mcx>,
    plan_node: &Node<'_>,
    rti: Index,
) -> PgResult<()> {
    let mcx = es.str.allocator();

    // objectname / namespace / objecttag computed below.
    let mut objectname: Option<PgString<'mcx>> = None;
    let mut namespace: Option<PgString<'mcx>> = None;
    let mut objecttag: Option<&'static str> = None;

    // rte = rt_fetch(rti, es->rtable);
    // refname = (char *) list_nth(es->rtable_names, rti - 1);
    // if (refname == NULL) refname = rte->eref->aliasname;
    //
    // We must clone the data we need out of `es.rtable`/`es.rtable_names` before
    // mutating `es.str`, since both borrow `es`.
    let rtable = es
        .rtable
        .as_ref()
        .expect("ExplainTargetRel: es->rtable not set");
    let rte = rt_fetch(rtable, rti);
    let rtekind = rte.rtekind;
    let relid = rte.relid;

    // refname override from rtable_names (may be shorter than rtable, or hold
    // None entries); fall back to the eref aliasname.
    let refname_override: Option<PgString<'mcx>> = es
        .rtable_names
        .get((rti - 1) as usize)
        .and_then(|o| o.as_ref())
        .map(|s| PgString::from_str_in(s.as_str(), mcx))
        .transpose()?;
    let refname: PgString<'mcx> = match refname_override {
        Some(r) => r,
        None => {
            let aliasname = rte
                .eref
                .as_ref()
                .and_then(|e| e.aliasname.as_ref())
                .map(|s| s.as_str())
                .unwrap_or("");
            PgString::from_str_in(aliasname, mcx)?
        }
    };

    // CTE / named-tuplestore names are read off the RTE directly (no catalog).
    let ctename: Option<PgString<'mcx>> = rte
        .ctename
        .as_ref()
        .map(|s| PgString::from_str_in(s.as_str(), mcx))
        .transpose()?;
    let enrname: Option<PgString<'mcx>> = rte
        .enrname
        .as_ref()
        .map(|s| PgString::from_str_in(s.as_str(), mcx))
        .transpose()?;
    let self_reference = rte.self_reference;
    let verbose = es.verbose;

    // The borrow of `es.rtable` ends here; subsequent catalog calls and string
    // appends can take `&mut es` / `mcx`.

    match plan_node.node_tag() {
        ntag::T_SeqScan
        | ntag::T_SampleScan
        | ntag::T_IndexScan
        | ntag::T_IndexOnlyScan
        | ntag::T_TidScan
        | ntag::T_TidRangeScan
        | ntag::T_ForeignScan
        | ntag::T_CustomScan
        | ntag::T_ModifyTable => {
            // Assert(rte->rtekind == RTE_RELATION);  (C also covers
            // BitmapHeapScan, which the Node enum does not yet model.)
            objectname = lsyscache::relation::get_rel_name(mcx, relid)?;
            if verbose {
                let nspid = lsyscache::relation::get_rel_namespace(relid)?;
                namespace =
                    lsyscache::namespace_range_index_pubsub::get_namespace_name_or_temp(mcx, nspid)?;
            }
            objecttag = Some("Relation Name");
        }
        ntag::T_FunctionScan => {
            // Assert(rte->rtekind == RTE_FUNCTION);
            //
            // If the expression is still a function call of a single function,
            // we can get the real name of the function. Otherwise, punt. (Even
            // if it was a single function call originally, the optimizer could
            // have simplified it away.)  Mirrors explain.c ExplainTargetRel's
            // T_FunctionScan case: read `fscan->functions`, and if it holds a
            // single RangeTblFunction whose funcexpr is a FuncExpr, resolve
            // its funcid through get_func_name / get_func_namespace.
            let fscan = plan_node.expect_functionscan();
            if let Some(functions) = fscan.functions.as_ref() {
                if functions.len() == 1 {
                    let rtfunc = &functions[0];
                    if let Some(funcexpr) = rtfunc.funcexpr.as_ref() {
                        if let Some(fe) = funcexpr.as_funcexpr() {
                            let funcid = fe.funcid;
                            objectname = lsyscache::function::get_func_name(mcx, funcid)?;
                            if verbose {
                                let nspid = lsyscache::function::get_func_namespace(funcid)?;
                                namespace = lsyscache::namespace_range_index_pubsub::get_namespace_name_or_temp(
                                    mcx, nspid,
                                )?;
                            }
                        }
                    }
                }
            }
            objecttag = Some("Function Name");
        }
        ntag::T_TableFuncScan => {
            // Assert(rte->rtekind == RTE_TABLEFUNC);
            // objectname = rte->tablefunc->functype == TFT_XMLTABLE ?
            //   "xmltable" : "json_table". The TableFunc node is carried by the
            //   TableFuncScan plan.
            let tfs = plan_node.expect_tablefuncscan();
            objectname = Some(mcx::PgString::from_str_in(
                if tfs.tablefunc.functype == types_nodes::primnodes::TFT_XMLTABLE {
                    "xmltable"
                } else {
                    "json_table"
                },
                mcx,
            )?);
            objecttag = Some("Table Function Name");
        }
        ntag::T_ValuesScan => {
            // Assert(rte->rtekind == RTE_VALUES);  (no objectname)
        }
        ntag::T_CteScan => {
            // Assert(rte->rtekind == RTE_CTE); Assert(!rte->self_reference);
            objectname = ctename;
            objecttag = Some("CTE Name");
        }
        ntag::T_NamedTuplestoreScan => {
            // Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
            objectname = enrname;
            objecttag = Some("Tuplestore Name");
        }
        ntag::T_WorkTableScan => {
            // Assert(rte->rtekind == RTE_CTE); Assert(rte->self_reference);
            let _ = self_reference;
            objectname = ctename;
            objecttag = Some("CTE Name");
        }
        _ => {}
    }
    let _ = (rtekind, self_reference);

    if es.format == ExplainFormat::EXPLAIN_FORMAT_TEXT {
        es.str.try_push_str(" on")?;
        match (&namespace, &objectname) {
            (Some(ns), Some(obj)) => {
                let qns = ruleutils::quote_identifier(mcx, ns.as_str())?;
                let qobj = ruleutils::quote_identifier(mcx, obj.as_str())?;
                es.str.try_push(' ')?;
                es.str.try_push_str(qns.as_str())?;
                es.str.try_push('.')?;
                es.str.try_push_str(qobj.as_str())?;
            }
            (None, Some(obj)) => {
                let qobj = ruleutils::quote_identifier(mcx, obj.as_str())?;
                es.str.try_push(' ')?;
                es.str.try_push_str(qobj.as_str())?;
            }
            _ => {}
        }
        // if (objectname == NULL || strcmp(refname, objectname) != 0)
        let differs = match &objectname {
            Some(obj) => obj.as_str() != refname.as_str(),
            None => true,
        };
        if differs {
            let qref = ruleutils::quote_identifier(mcx, refname.as_str())?;
            es.str.try_push(' ')?;
            es.str.try_push_str(qref.as_str())?;
        }
    } else {
        if let (Some(tag), Some(obj)) = (objecttag, &objectname) {
            fmt::ExplainPropertyText(tag, obj.as_str(), es)?;
        }
        if let Some(ns) = &namespace {
            fmt::ExplainPropertyText("Schema", ns.as_str(), es)?;
        }
        fmt::ExplainPropertyText("Alias", refname.as_str(), es)?;
    }
    let _ = RTEKind::RTE_RELATION;
    Ok(())
}
