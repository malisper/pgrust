// execCurrent.c. The portal's live executor state is reached through the
// QueryDesc registry handle stored on the portal (never the handle of the
// statement currently running, so the with_qd re-entry assert cannot trip).

use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_CURSOR_STATE, ERRCODE_UNDEFINED_CURSOR};
use ::types_portal::PortalStrategy;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerIsValid};

use crate::procnode::PlanStateNode;
use crate::querydesc;

pub(crate) fn exec_current_of_seam(
    cursor_name: Option<&str>,
    cursor_param: i32,
    table_oid: Oid,
    table_name: &str,
) -> PgResult<Option<ItemPointerData>> {
    let Some(cursor_name) = cursor_name else {
        // fetch_cursor_param_value: REFCURSOR params only arrive via the
        // plpgsql columnref hooks, which are structurally absent.
        panic!("execCurrentOf (execCurrent.c): cursor_param {cursor_param}; plpgsql lane");
    };
    exec_current_of(cursor_name, table_oid, table_name)
}

pub fn exec_current_of(
    cursor_name: &str,
    table_oid: Oid,
    table_name: &str,
) -> PgResult<Option<ItemPointerData>> {
    let Some(portal) = ::portalmem::GetPortalByName(Some(cursor_name)) else {
        return Err(Box::new(
            PgError::error(format!("cursor \"{cursor_name}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_CURSOR),
        ));
    };

    // Non-SELECT queries and held cursors both lack a live queryDesc.
    let (strategy, query_desc, at_start, at_end) = {
        let p = portal.borrow();
        (p.strategy, p.queryDesc, p.atStart, p.atEnd)
    };
    if strategy != PortalStrategy::PORTAL_ONE_SELECT {
        return Err(invalid_cursor_state(format!(
            "cursor \"{cursor_name}\" is not a SELECT query"
        )));
    }
    if query_desc.is_null() {
        return Err(invalid_cursor_state(format!(
            "cursor \"{cursor_name}\" is held from a previous transaction"
        )));
    }

    querydesc::with_qd(query_desc, |qd| -> PgResult<Option<ItemPointerData>> {
        let Some(exec) = qd.exec.as_mut() else {
            return Err(invalid_cursor_state(format!(
                "cursor \"{cursor_name}\" is held from a previous transaction"
            )));
        };
        exec.with_mut(|d| {
        let estate = &d.estate;
        let planstate = &d.planstate;

        // Two strategies: FOR UPDATE/SHARE digs the ctid out of the rowmark;
        // otherwise we search the plan tree for the scan node.
        let has_rowmarks = estate.es_rowmarks.iter().any(|m| m.is_some());
        if has_rowmarks {
            let mut erm: Option<&::executils::ExecRowMark> = None;
            for thiserm in estate.es_rowmarks.iter().flatten() {
                if !thiserm.markType.requires_row_share_lock() {
                    continue;
                }
                if thiserm.relid == table_oid {
                    if erm.is_some() {
                        return Err(invalid_cursor_state(format!(
                            "cursor \"{cursor_name}\" has multiple FOR UPDATE/SHARE references to table \"{table_name}\""
                        )));
                    }
                    erm = Some(thiserm);
                }
            }
            let Some(erm) = erm else {
                return Err(invalid_cursor_state(format!(
                    "cursor \"{cursor_name}\" does not have a FOR UPDATE/SHARE reference to table \"{table_name}\""
                )));
            };

            // Per the SQL spec the cursor must be on a row.
            if at_start || at_end {
                return Err(not_positioned(cursor_name));
            }

            if ItemPointerIsValid(&erm.curCtid) {
                Ok(Some(erm.curCtid))
            } else {
                // Another inheritance child produced the current row.
                Ok(None)
            }
        } else {
            let scanstate = planstate
                .as_ref()
                .and_then(|root| search_plan_tree(root, table_oid));
            let Some(scanstate) = scanstate else {
                return Err(not_simply_updatable(cursor_name, table_name));
            };

            if at_start || at_end {
                return Err(not_positioned(cursor_name));
            }

            // IndexOnlyScan slots can be virtual (no ctid); the TID comes
            // from the scan descriptor instead.
            if let Found::IndexOnly(ios) = scanstate {
                let slot = estate.slot(ios.ss.ss_ScanTupleSlot);
                if slot.base().is_empty() {
                    return Ok(None);
                }
                let tid = ios
                    .ioss_ScanDesc
                    .as_deref()
                    .map(|sd| sd.xs_heaptid)
                    .unwrap_or_else(ItemPointerData::invalid);
                if !ItemPointerIsValid(&tid) {
                    return Err(not_simply_updatable(cursor_name, table_name));
                }
                return Ok(Some(tid));
            }

            let ss = match scanstate {
                Found::Scan(ss) => ss,
                Found::IndexOnly(_) => unreachable!(),
            };
            let slot = estate.slot(ss.ss_ScanTupleSlot);
            if slot.base().is_empty() {
                // Inactive scan (inheritance case): do nothing on this table.
                return Ok(None);
            }
            // C digs SelfItemPointerAttributeNumber out of the physical scan
            // tuple; the slot carries it as tts_tid. Invalid = the C lisnull
            // arm (no physical tuple).
            let tid = slot.base().tts_tid;
            if !ItemPointerIsValid(&tid) {
                return Err(not_simply_updatable(cursor_name, table_name));
            }
            debug_assert_eq!(slot.base().tts_tableOid, table_oid);
            Ok(Some(tid))
        }
        })
    })
}

enum Found<'a, 'mcx> {
    Scan(&'a ::execscan::ScanState<'mcx>),
    IndexOnly(&'a ::nodeindexonlyscan::IndexOnlyScanState<'mcx>),
}

// search_plan_tree (execCurrent.c): find THE scan node on table_oid that
// produced the tree's current output row; None on no or multiple matches.
// C's pending_rescan (chgParam) leg is structurally absent: this executor
// runs rescans eagerly instead of deferring them via chgParam.
fn search_plan_tree<'a, 'mcx>(
    node: &'a PlanStateNode<'mcx>,
    table_oid: Oid,
) -> Option<Found<'a, 'mcx>> {
    fn scanning<'a, 'mcx>(
        ss: &'a ::execscan::ScanState<'mcx>,
        table_oid: Oid,
    ) -> Option<&'a ::execscan::ScanState<'mcx>> {
        match ss.ss_currentRelation.as_ref() {
            Some(rel) if rel.rd_id == table_oid => Some(ss),
            _ => None,
        }
    }

    match node {
        PlanStateNode::Instrumented(w) => search_plan_tree(&w.inner, table_oid),
        PlanStateNode::SeqScan(s) => scanning(&s.ss, table_oid).map(Found::Scan),
        PlanStateNode::SampleScan(s) => scanning(&s.ss, table_oid).map(Found::Scan),
        PlanStateNode::IndexScan(s) => scanning(&s.ss, table_oid).map(Found::Scan),
        PlanStateNode::TidScan(s) => scanning(&s.ss, table_oid).map(Found::Scan),
        PlanStateNode::TidRangeScan(s) => scanning(&s.ss, table_oid).map(Found::Scan),
        PlanStateNode::BitmapHeapScan(s) => scanning(&s.scan.ss, table_oid).map(Found::Scan),
        PlanStateNode::IndexOnlyScan(s) => {
            scanning(&s.ss, table_oid).map(|_| Found::IndexOnly(s))
        }
        // Append: only the input that produced the current output row can be
        // positioned on a tuple; reject multiple matches (UNION ALL).
        PlanStateNode::Append(a) => {
            let mut result = None;
            for child in a.substates.iter() {
                let Some(elem) = search_plan_tree(child, table_oid) else {
                    continue;
                };
                if result.is_some() {
                    return None;
                }
                result = Some(elem);
            }
            result
        }
        // Result and Limit always return their input's current row.
        PlanStateNode::Result(rs) => {
            rs.outer.as_deref().and_then(|o| search_plan_tree(o, table_oid))
        }
        PlanStateNode::Limit(l) => search_plan_tree(&l.outer, table_oid),
        PlanStateNode::SubqueryScan(s) => search_plan_tree(&s.subplan, table_oid),
        _ => None,
    }
}

#[cold]
#[inline(never)]
fn invalid_cursor_state(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_CURSOR_STATE))
}

#[cold]
#[inline(never)]
fn not_positioned(cursor_name: &str) -> Box<PgError> {
    invalid_cursor_state(format!("cursor \"{cursor_name}\" is not positioned on a row"))
}

#[cold]
#[inline(never)]
fn not_simply_updatable(cursor_name: &str, table_name: &str) -> Box<PgError> {
    invalid_cursor_state(format!(
        "cursor \"{cursor_name}\" is not a simply updatable scan of table \"{table_name}\""
    ))
}
