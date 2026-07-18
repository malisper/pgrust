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
    // WS-CA wave-10: store-armed portals also carry the §4.2 sidecar fields.
    let (strategy, query_desc, at_start, at_end, store_armed, tid_store, portal_pos) = {
        let p = portal.borrow();
        (
            p.strategy,
            p.queryDesc,
            p.atStart,
            p.atEnd,
            p.cursorStoreArmed,
            p.cursorTidStore,
            p.portalPos,
        )
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

            // WS-CA wave-10 (contract §4.2): a store-armed portal's scan sits
            // at the fill high-water mark, not at the cursor position — the
            // CURRENT row's identity comes from the tid sidecar at row
            // portalPos-1, never from the scan state. The error arms above
            // (search + positioned) already byte-matched C.
            if store_armed {
                // search succeeded ⇒ the eligibility probe was true ⇒ the
                // sidecar exists (fill creates both together).
                debug_assert!(!tid_store.is_null());
                if tid_store.is_null() {
                    return Err(not_simply_updatable(cursor_name, table_name));
                }
                debug_assert!(portal_pos >= 1);
                let row = (portal_pos - 1) as i64;
                let Some((stored_oid, packed)) = ::tuplestore::hold::tidstore_get(tid_store, row)?
                else {
                    // Sidecar shortfall cannot happen (row count == store row
                    // count by construction); fail like C's lisnull arm.
                    return Err(not_simply_updatable(cursor_name, table_name));
                };
                if stored_oid != table_oid {
                    // Another inheritance child produced the current row.
                    return Ok(None);
                }
                let tid = unpack_tid(packed);
                if !ItemPointerIsValid(&tid) {
                    return Err(not_simply_updatable(cursor_name, table_name));
                }
                return Ok(Some(tid));
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

// --- WS-CA wave-10 (cursors inc-2, contract §4) --------------------------------
//
// Store-armed portals: the plan below a SCROLL cursor sits at the fill
// high-water mark, so the scan state must NEVER be read for the CURRENT row
// (the §4 wrong-row hazard). The row identity is captured per row at fill
// emit (the same scan-state data C reads) into the portal's tid sidecar; the
// resolution above reads sidecar row `portalPos - 1` instead.

/// §4.2 tid packing: block<<16 | offset (both fields of ItemPointerData;
/// posid 0 == invalid survives the round trip).
fn pack_tid(t: &ItemPointerData) -> u64 {
    let block = ::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(t) as u64;
    (block << 16) | (t.ip_posid as u64)
}

fn unpack_tid(v: u64) -> ItemPointerData {
    ItemPointerData::new((v >> 16) as u32, (v & 0xffff) as u16)
}

/// The wildcard twin of `search_plan_tree`: same spine recursion, matches ANY
/// scan that could ever answer a per-table search. Probe FALSE ⇒ the
/// per-table walk can never succeed ⇒ execCurrentOf reaches only C's error
/// arms for this plan, and no capture runs.
fn has_capturable_scan(node: &PlanStateNode<'_>) -> bool {
    match node {
        PlanStateNode::Instrumented(w) => has_capturable_scan(&w.inner),
        PlanStateNode::SeqScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::SampleScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::IndexScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::TidScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::TidRangeScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::BitmapHeapScan(s) => s.scan.ss.ss_currentRelation.is_some(),
        PlanStateNode::IndexOnlyScan(s) => s.ss.ss_currentRelation.is_some(),
        PlanStateNode::Append(a) => a.substates.iter().any(has_capturable_scan),
        PlanStateNode::Result(rs) => rs.outer.as_deref().is_some_and(has_capturable_scan),
        PlanStateNode::Limit(l) => has_capturable_scan(&l.outer),
        PlanStateNode::SubqueryScan(s) => has_capturable_scan(&s.subplan),
        _ => false,
    }
}

/// Find the scan positioned on the plan's current output row and read its
/// identity — the §4.2 capture source. Among Append children exactly the
/// producing child holds a non-empty scan slot (exhausted children cleared,
/// unstarted children empty), which is what C's per-target-table
/// `search_plan_tree` answer resolves to.
fn capture_positioned<'mcx>(
    node: &PlanStateNode<'mcx>,
    estate: &::executils::EStateData<'mcx>,
) -> Option<(Oid, ItemPointerData)> {
    fn from_scan<'mcx>(
        ss: &::execscan::ScanState<'mcx>,
        estate: &::executils::EStateData<'mcx>,
    ) -> Option<(Oid, ItemPointerData)> {
        let rel = ss.ss_currentRelation.as_ref()?;
        let slot = estate.slot(ss.ss_ScanTupleSlot);
        if slot.base().is_empty() {
            return None;
        }
        // Invalid tts_tid is CAPTURED as invalid: resolution mirrors C's
        // lisnull arm (not-simply-updatable error) at FETCH position parity.
        Some((rel.rd_id, slot.base().tts_tid))
    }
    match node {
        PlanStateNode::Instrumented(w) => capture_positioned(&w.inner, estate),
        PlanStateNode::SeqScan(s) => from_scan(&s.ss, estate),
        PlanStateNode::SampleScan(s) => from_scan(&s.ss, estate),
        PlanStateNode::IndexScan(s) => from_scan(&s.ss, estate),
        PlanStateNode::TidScan(s) => from_scan(&s.ss, estate),
        PlanStateNode::TidRangeScan(s) => from_scan(&s.ss, estate),
        PlanStateNode::BitmapHeapScan(s) => from_scan(&s.scan.ss, estate),
        PlanStateNode::IndexOnlyScan(s) => {
            let rel = s.ss.ss_currentRelation.as_ref()?;
            let slot = estate.slot(s.ss.ss_ScanTupleSlot);
            if slot.base().is_empty() {
                return None;
            }
            let tid = s
                .ioss_ScanDesc
                .as_deref()
                .map(|sd| sd.xs_heaptid)
                .unwrap_or_else(ItemPointerData::invalid);
            Some((rel.rd_id, tid))
        }
        PlanStateNode::Append(a) => {
            a.substates.iter().find_map(|c| capture_positioned(c, estate))
        }
        PlanStateNode::Result(rs) => {
            rs.outer.as_deref().and_then(|o| capture_positioned(o, estate))
        }
        PlanStateNode::Limit(l) => capture_positioned(&l.outer, estate),
        PlanStateNode::SubqueryScan(s) => capture_positioned(&s.subplan, estate),
        _ => None,
    }
}

/// Seam impl (execmain_seams::cursor_capture_probe, escalation EX-CA-1).
pub(crate) fn cursor_capture_probe_seam(query_desc: ::types_portal::QueryDescHandle) -> bool {
    querydesc::with_qd(query_desc, |qd| {
        let Some(exec) = qd.exec.as_mut() else {
            return false;
        };
        exec.with_mut(|d| d.planstate.as_ref().is_some_and(|root| has_capturable_scan(root)))
    })
}

/// Seam impl (execmain_seams::cursor_capture_current, escalation EX-CA-1).
pub(crate) fn cursor_capture_current_seam(
    query_desc: ::types_portal::QueryDescHandle,
) -> PgResult<Option<(Oid, u64)>> {
    querydesc::with_qd(query_desc, |qd| {
        let Some(exec) = qd.exec.as_mut() else {
            return Ok(None);
        };
        exec.with_mut(|d| {
            let hit = d
                .planstate
                .as_ref()
                .and_then(|root| capture_positioned(root, &d.estate));
            Ok(hit.map(|(oid, tid)| (oid, pack_tid(&tid))))
        })
    })
}
// --- end WS-CA wave-10 -----------------------------------------------------------

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
