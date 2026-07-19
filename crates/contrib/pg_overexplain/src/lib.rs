//! `contrib/pg_overexplain` — EXPLAIN (DEBUG) and EXPLAIN (RANGE_TABLE):
//! extra planner internals via the EXPLAIN extension-option registry and the
//! per-node/per-plan hooks. LOAD-only module (no SQL extension).

#![allow(non_snake_case)]

use std::cell::Cell;

use explain::{
    defGetBoolean, ExplainCloseGroup, ExplainIndentText, ExplainOpenGroup,
    ExplainPropertyBool, ExplainPropertyFloat, ExplainPropertyInteger, ExplainPropertyText,
    ExplainPropertyUInteger, ExplainState, EXPLAIN_FORMAT_TEXT,
};
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, OidList};
use types_nodes::parsenodes::{DefElem, RTEKind, RangeTblEntry};
use types_nodes::plannodes::PlannedStmt;
use types_nodes::primnodes::Alias;
use types_nodes::{CmdType, JoinType, Node, NodeTag};

const LIBRARY: &str = "pg_overexplain";
const DEFAULT_LOCKMETHOD: u16 = 1;

#[derive(Default)]
struct OverexplainOptions {
    debug: Cell<bool>,
    range_table: Cell<bool>,
}

thread_local! {
    static ES_EXTENSION_ID: Cell<usize> = const { Cell::new(usize::MAX) };
    static PREV_PER_NODE_HOOK: Cell<Option<explain::ExplainPerNodeHook>> =
        const { Cell::new(None) };
    static PREV_PER_PLAN_HOOK: Cell<Option<explain::ExplainPerPlanHook>> =
        const { Cell::new(None) };
}

fn pg_init() -> PgResult<()> {
    ES_EXTENSION_ID.set(explain::GetExplainExtensionId("pg_overexplain"));
    explain::RegisterExtensionExplainOption("debug", overexplain_debug_handler);
    explain::RegisterExtensionExplainOption("range_table", overexplain_range_table_handler);
    PREV_PER_NODE_HOOK.set(explain::explain_per_node_hook());
    explain::set_explain_per_node_hook(Some(overexplain_per_node_hook));
    PREV_PER_PLAN_HOOK.set(explain::explain_per_plan_hook());
    explain::set_explain_per_plan_hook(Some(overexplain_per_plan_hook));
    Ok(())
}

fn options_of<'mcx>(es: &ExplainState<'mcx>) -> Option<&'mcx OverexplainOptions> {
    explain::GetExplainExtensionState(es, ES_EXTENSION_ID.get()).map(|s| {
        s.downcast_ref::<OverexplainOptions>()
            .expect("pg_overexplain extension state")
    })
}

fn ensure_options<'mcx>(
    es: &mut ExplainState<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<&'mcx OverexplainOptions> {
    if let Some(o) = options_of(es) {
        return Ok(o);
    }
    let opts: &'mcx OverexplainOptions =
        mcx::leak_in(mcx::alloc_in(mcx, OverexplainOptions::default())?);
    explain::SetExplainExtensionState(es, ES_EXTENSION_ID.get(), opts);
    Ok(opts)
}

fn overexplain_debug_handler<'mcx>(
    es: &mut ExplainState<'mcx>,
    opt: &DefElem<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let options = ensure_options(es, mcx)?;
    options.debug.set(defGetBoolean(opt)?);
    Ok(())
}

fn overexplain_range_table_handler<'mcx>(
    es: &mut ExplainState<'mcx>,
    opt: &DefElem<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let options = ensure_options(es, mcx)?;
    options.range_table.set(defGetBoolean(opt)?);
    Ok(())
}

fn overexplain_per_node_hook<'mcx>(
    node: Node<'mcx>,
    relationship: Option<&str>,
    plan_name: Option<&str>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if let Some(prev) = PREV_PER_NODE_HOOK.get() {
        prev(node, relationship, plan_name, es)?;
    }
    let Some(options) = options_of(es) else {
        return Ok(());
    };

    if options.debug.get() {
        let plan = node.as_plan().expect("ExplainNode reached a plan node");
        // Raw disabled_nodes (normal EXPLAIN shows the child-relative delta).
        ExplainPropertyInteger("Disabled Nodes", None, plan.disabled_nodes as i64, es);
        ExplainPropertyBool("Parallel Safe", plan.parallel_safe, es);
        ExplainPropertyInteger("Plan Node ID", None, plan.plan_node_id as i64, es);
        if es.format != EXPLAIN_FORMAT_TEXT || !plan.extParam.is_empty() {
            overexplain_bitmapset("extParam", &plan.extParam, es);
        }
        if es.format != EXPLAIN_FORMAT_TEXT || !plan.allParam.is_empty() {
            overexplain_bitmapset("allParam", &plan.allParam, es);
        }
    }

    if options.range_table.get() {
        match node.node_tag() {
            NodeTag::T_SeqScan
            | NodeTag::T_SampleScan
            | NodeTag::T_IndexScan
            | NodeTag::T_IndexOnlyScan
            | NodeTag::T_BitmapHeapScan
            | NodeTag::T_TidScan
            | NodeTag::T_TidRangeScan
            | NodeTag::T_SubqueryScan
            | NodeTag::T_FunctionScan
            | NodeTag::T_TableFuncScan
            | NodeTag::T_ValuesScan
            | NodeTag::T_CteScan
            | NodeTag::T_NamedTuplestoreScan
            | NodeTag::T_WorkTableScan => {
                ExplainPropertyInteger("Scan RTI", None, scanrelid_of(node) as i64, es);
            }
            NodeTag::T_ForeignScan => {
                overexplain_bitmapset(
                    "Scan RTIs",
                    &node.as_foreign_scan().expect("ForeignScan").fs_base_relids,
                    es,
                );
            }
            // T_CustomScan: no custom-scan providers exist in this build.
            NodeTag::T_ModifyTable => {
                let mt = node.as_modify_table().expect("ModifyTable");
                ExplainPropertyInteger("Nominal RTI", None, mt.nominalRelation as i64, es);
                ExplainPropertyInteger("Exclude Relation RTI", None, mt.exclRelRTI as i64, es);
            }
            NodeTag::T_Append => {
                overexplain_bitmapset("Append RTIs", &node.as_append().expect("Append").apprelids, es);
            }
            NodeTag::T_MergeAppend => {
                overexplain_bitmapset(
                    "Append RTIs",
                    &node.as_merge_append().expect("MergeAppend").apprelids,
                    es,
                );
            }
            _ => {}
        }
    }
    Ok(())
}

fn scanrelid_of(node: Node<'_>) -> u32 {
    match node.node_tag() {
        NodeTag::T_SeqScan => node.as_seq_scan().unwrap().scan.scanrelid,
        NodeTag::T_SampleScan => node.as_sample_scan().unwrap().scan.scanrelid,
        NodeTag::T_IndexScan => node.as_index_scan().unwrap().scan.scanrelid,
        NodeTag::T_IndexOnlyScan => node.as_index_only_scan().unwrap().scan.scanrelid,
        NodeTag::T_BitmapHeapScan => node.as_bitmap_heap_scan().unwrap().scan.scanrelid,
        NodeTag::T_TidScan => node.as_tid_scan().unwrap().scan.scanrelid,
        NodeTag::T_TidRangeScan => node.as_tid_range_scan().unwrap().scan.scanrelid,
        NodeTag::T_SubqueryScan => node.as_subquery_scan().unwrap().scan.scanrelid,
        NodeTag::T_FunctionScan => node.as_function_scan().unwrap().scan.scanrelid,
        NodeTag::T_TableFuncScan => node.as_table_func_scan().unwrap().scan.scanrelid,
        NodeTag::T_ValuesScan => node.as_values_scan().unwrap().scan.scanrelid,
        NodeTag::T_CteScan => node.as_cte_scan().unwrap().scan.scanrelid,
        NodeTag::T_NamedTuplestoreScan => node.as_named_tuplestore_scan().unwrap().scan.scanrelid,
        NodeTag::T_WorkTableScan => node.as_work_table_scan().unwrap().scan.scanrelid,
        other => unreachable!("scanrelid_of: {other:?} is not a Scan node"),
    }
}

fn overexplain_per_plan_hook<'mcx>(
    plannedstmt: &'mcx PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
    query_string: &str,
) -> PgResult<()> {
    if let Some(prev) = PREV_PER_PLAN_HOOK.get() {
        prev(plannedstmt, es, query_string)?;
    }
    let Some(options) = options_of(es) else {
        return Ok(());
    };

    if options.debug.get() {
        overexplain_debug(plannedstmt, es)?;
    }
    if options.range_table.get() {
        overexplain_range_table(plannedstmt, es)?;
    }
    Ok(())
}

fn overexplain_debug<'mcx>(
    plannedstmt: &'mcx PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    // Its own group even in text mode.
    ExplainOpenGroup("PlannedStmt", Some("PlannedStmt"), true, es);
    if es.format == EXPLAIN_FORMAT_TEXT {
        ExplainIndentText(es);
        es.str.append_str("PlannedStmt:\n")?;
        es.indent += 1;
    }

    let command_type = match plannedstmt.commandType {
        CmdType::CMD_UNKNOWN => "unknown",
        CmdType::CMD_SELECT => "select",
        CmdType::CMD_UPDATE => "update",
        CmdType::CMD_INSERT => "insert",
        CmdType::CMD_DELETE => "delete",
        CmdType::CMD_MERGE => "merge",
        CmdType::CMD_UTILITY => "utility",
        CmdType::CMD_NOTHING => "nothing",
    };
    ExplainPropertyText("Command Type", command_type, es);

    let mut flags = String::new();
    for (set, name) in [
        (plannedstmt.hasReturning, "hasReturning"),
        (plannedstmt.hasModifyingCTE, "hasModifyingCTE"),
        (plannedstmt.canSetTag, "canSetTag"),
        (plannedstmt.transientPlan, "transientPlan"),
        (plannedstmt.dependsOnRole, "dependsOnRole"),
        (plannedstmt.parallelModeNeeded, "parallelModeNeeded"),
    ] {
        if set {
            flags.push_str(", ");
            flags.push_str(name);
        }
    }
    if flags.is_empty() {
        flags.push_str(", none");
    }
    ExplainPropertyText("Flags", &flags[2..], es);

    overexplain_bitmapset("Subplans Needing Rewind", &plannedstmt.rewindPlanIDs, es);
    overexplain_oidlist("Relation OIDs", &plannedstmt.relationOids, es);
    overexplain_oidlist("Executor Parameter Types", &plannedstmt.paramExecTypes, es);

    if plannedstmt.stmt_location == -1 {
        ExplainPropertyText("Parse Location", "Unknown", es);
    } else if plannedstmt.stmt_len == 0 {
        ExplainPropertyText(
            "Parse Location",
            &format!("{} to end", plannedstmt.stmt_location),
            es,
        );
    } else {
        ExplainPropertyText(
            "Parse Location",
            &format!(
                "{} for {} bytes",
                plannedstmt.stmt_location, plannedstmt.stmt_len
            ),
            es,
        );
    }

    if es.format == EXPLAIN_FORMAT_TEXT {
        es.indent -= 1;
    }
    ExplainCloseGroup("PlannedStmt", Some("PlannedStmt"), true, es);
    Ok(())
}

fn overexplain_range_table<'mcx>(
    plannedstmt: &'mcx PlannedStmt<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    // SAFETY: the arming context outlives this EXPLAIN emission.
    let mcx = es.str.mcx();

    ExplainOpenGroup("Range Table", Some("Range Table"), false, es);

    for (i, rte_node) in plannedstmt.rtable.iter().enumerate() {
        let rti = (i + 1) as u32;
        let rte: &RangeTblEntry<'mcx> =
            rte_node.as_range_tbl_entry().expect("rtable holds RangeTblEntry");

        let kind = match rte.rtekind {
            RTEKind::RTE_RELATION => "relation",
            RTEKind::RTE_SUBQUERY => "subquery",
            RTEKind::RTE_JOIN => "join",
            RTEKind::RTE_FUNCTION => "function",
            RTEKind::RTE_TABLEFUNC => "tablefunc",
            RTEKind::RTE_VALUES => "values",
            RTEKind::RTE_CTE => "cte",
            RTEKind::RTE_NAMEDTUPLESTORE => "namedtuplestore",
            RTEKind::RTE_RESULT => "result",
            RTEKind::RTE_GROUP => "group",
        };

        ExplainOpenGroup("Range Table Entry", None, true, es);

        if es.format == EXPLAIN_FORMAT_TEXT {
            ExplainIndentText(es);
            es.str.append_str(&format!(
                "RTI {rti} ({kind}{}{}):\n",
                if rte.inh { ", inherited" } else { "" },
                if rte.inFromCl { ", in-from-clause" } else { "" },
            ))?;
            es.indent += 1;
        } else {
            ExplainPropertyUInteger("RTI", None, rti as u64, es);
            ExplainPropertyText("Kind", kind, es);
            ExplainPropertyBool("Inherited", rte.inh, es);
            ExplainPropertyBool("In From Clause", rte.inFromCl, es);
        }

        if let Some(alias) = rte.alias {
            overexplain_alias("Alias", alias, es)?;
        }
        overexplain_alias("Eref", rte.eref.expect("eref is required"), es)?;

        if rte.relid != 0 {
            let relname = lsyscache::get_rel_name(mcx, rte.relid)?
                .expect("relation in plan rtable exists");
            let relname = format_type::quote_identifier(&relname).into_owned();
            let qualname = if es.verbose {
                let nspoid = lsyscache::get_rel_namespace(rte.relid)?;
                let nspname = lsyscache::get_namespace_name_or_temp(mcx, nspoid)?
                    .expect("namespace exists");
                format!("{}.{relname}", format_type::quote_identifier(&nspname))
            } else {
                relname
            };
            ExplainPropertyText("Relation", &qualname, es);
        }

        let relkind = match rte.relkind {
            b'r' => Some("relation".to_string()),
            b'i' => Some("index".to_string()),
            b'S' => Some("sequence".to_string()),
            b't' => Some("toastvalue".to_string()),
            b'v' => Some("view".to_string()),
            b'm' => Some("matview".to_string()),
            b'c' => Some("composite_type".to_string()),
            b'f' => Some("foreign_table".to_string()),
            b'p' => Some("partitioned_table".to_string()),
            b'I' => Some("partitioned_index".to_string()),
            0 => None,
            other => Some((other as char).to_string()),
        };
        if let Some(rk) = relkind {
            ExplainPropertyText("Relation Kind", &rk, es);
        }

        if rte.rellockmode != 0 {
            ExplainPropertyText(
                "Relation Lock Mode",
                lock::GetLockmodeName(DEFAULT_LOCKMETHOD, rte.rellockmode),
                es,
            );
        }

        if rte.perminfoindex != 0 {
            ExplainPropertyInteger("Permission Info Index", None, rte.perminfoindex as i64, es);
        }

        if es.format != EXPLAIN_FORMAT_TEXT || rte.security_barrier {
            ExplainPropertyBool("Security Barrier", rte.security_barrier, es);
        }

        if rte.rtekind == RTEKind::RTE_JOIN {
            let jointype = match rte.jointype {
                JoinType::JOIN_INNER => "Inner",
                JoinType::JOIN_LEFT => "Left",
                JoinType::JOIN_FULL => "Full",
                JoinType::JOIN_RIGHT => "Right",
                JoinType::JOIN_SEMI => "Semi",
                JoinType::JOIN_ANTI => "Anti",
                JoinType::JOIN_RIGHT_SEMI => "Right Semi",
                JoinType::JOIN_RIGHT_ANTI => "Right Anti",
                _ => "???",
            };
            ExplainPropertyText("Join Type", jointype, es);
            if es.format != EXPLAIN_FORMAT_TEXT || rte.joinmergedcols != 0 {
                ExplainPropertyInteger("JOIN USING Columns", None, rte.joinmergedcols as i64, es);
            }
        }

        if rte.rtekind == RTEKind::RTE_FUNCTION {
            ExplainPropertyBool("WITH ORDINALITY", rte.funcordinality, es);
        }

        if rte.rtekind == RTEKind::RTE_CTE {
            ExplainPropertyText("CTE Name", rte.ctename.expect("cte name"), es);
            ExplainPropertyUInteger("CTE Levels Up", None, rte.ctelevelsup as u64, es);
            ExplainPropertyBool("CTE Self-Reference", rte.self_reference, es);
        }

        if rte.rtekind == RTEKind::RTE_NAMEDTUPLESTORE {
            ExplainPropertyText("ENR Name", rte.enrname.expect("enr name"), es);
            ExplainPropertyFloat("ENR Tuples", None, rte.enrtuples, 0, es);
        }

        if es.format != EXPLAIN_FORMAT_TEXT || rte.lateral {
            ExplainPropertyBool("Lateral", rte.lateral, es);
        }

        if es.format == EXPLAIN_FORMAT_TEXT {
            es.indent -= 1;
        }
        ExplainCloseGroup("Range Table Entry", None, true, es);
    }

    if es.format != EXPLAIN_FORMAT_TEXT || !plannedstmt.unprunableRelids.is_empty() {
        overexplain_bitmapset("Unprunable RTIs", &plannedstmt.unprunableRelids, es);
    }
    if es.format != EXPLAIN_FORMAT_TEXT || !plannedstmt.resultRelations.is_nil() {
        overexplain_intlist("Result RTIs", &plannedstmt.resultRelations, es);
    }

    ExplainCloseGroup("Range Table", Some("Range Table"), false, es);
    Ok(())
}

fn overexplain_alias(
    qlabel: &str,
    alias: &Alias<'_>,
    es: &mut ExplainState<'_>,
) -> PgResult<()> {
    let name = alias.aliasname.unwrap_or("");
    let mut buf = format!("{} (", format_type::quote_identifier(name));
    let mut first = true;
    for cn in alias.colnames.iter() {
        let s = cn.as_string().expect("colnames are String nodes").sval;
        if !first {
            buf.push_str(", ");
        }
        buf.push_str(&format_type::quote_identifier(s));
        first = false;
    }
    buf.push(')');
    ExplainPropertyText(qlabel, &buf, es);
    Ok(())
}

fn overexplain_bitmapset(qlabel: &str, bms: &Bitmapset<'_>, es: &mut ExplainState<'_>) {
    if bms.is_empty() {
        ExplainPropertyText(qlabel, "none", es);
        return;
    }
    let mut buf = String::new();
    let mut x = -1;
    loop {
        x = bms.next_member(x);
        if x < 0 {
            break;
        }
        buf.push_str(&format!(" {x}"));
    }
    ExplainPropertyText(qlabel, &buf[1..], es);
}

fn overexplain_intlist(qlabel: &str, list: &IntList<'_>, es: &mut ExplainState<'_>) {
    if list.is_nil() {
        ExplainPropertyText(qlabel, "none", es);
        return;
    }
    let mut buf = String::new();
    for i in list.iter() {
        buf.push_str(&format!(" {i}"));
    }
    ExplainPropertyText(qlabel, &buf[1..], es);
}

fn overexplain_oidlist(qlabel: &str, list: &OidList<'_>, es: &mut ExplainState<'_>) {
    if list.is_nil() {
        ExplainPropertyText(qlabel, "none", es);
        return;
    }
    let mut buf = String::new();
    for o in list.iter() {
        buf.push_str(&format!(" {o}"));
    }
    ExplainPropertyText(qlabel, &buf[1..], es);
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup: |_| None,
        pg_init: Some(pg_init),
    });
}
