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
                // sidecar exists (fill creates both together). SEAM-WIRING:
                // ONE exception — the AM-narrowed probe answers FALSE for
                // pgrcolumnar scans (scan-only AM, no tids) although C's
                // search walk still finds them; those portals legitimately
                // carry no sidecar and answer `not_simply_updatable`, the
                // same surface the legacy invalid-`tts_tid` read produces.
                debug_assert!(
                    !tid_store.is_null()
                        || match &scanstate {
                            Found::Scan(ss) => ss.ss_currentRelation.as_ref().is_some_and(
                                |rel| ::reloptions::relam_is_pgrcolumnar(rel.rd_rel.relam),
                            ),
                            Found::IndexOnly(_) => false,
                        },
                    "store-armed portal without a tid sidecar on a tid-capable scan \
                     (§4.1 probe / search_plan_tree disagreement)"
                );
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
/// pub(crate): SE-R41's in-run capture surfaces (the capture-armed batch
/// sink and the capture row loop) pack the same identity.
pub(crate) fn pack_tid(t: &ItemPointerData) -> u64 {
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
///
/// Debug-wired into `cursor_capture_current_seam` below: capture runs only
/// for portals the §4.1 plan-shape probe judged eligible, so this planstate
/// walk must return true there. That is one direction of the probe/planstate
/// spine-agreement invariant; the other direction (probe FALSE + per-table
/// search success) is guarded by the `tid_store.is_null()` debug_assert in
/// `exec_current_of`'s store-armed arm.
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

/// SEAM-WIRING (SE10-GATES item 1): is a scan of this range-table entry
/// TID-capturable? pgrcolumnar (cbstore) is a scan-only AM — its slots are
/// VIRTUAL (no `tts_tid`), TID scans/updates are unsupported — so §4.2
/// capture over it is structurally useless: the un-narrowed probe forced
/// exactly the lane batch-fill breadth (standalone SeqScan over
/// pgrcolumnar) onto the row chain for nothing (the CB F1/F4 seam gap).
/// Both the legacy scan-state read (virtual slot ⇒ invalid `tts_tid`) and
/// the narrowed store-armed arm (no sidecar) answer `not_simply_updatable`
/// for these scans — byte-identical error surface, pinned by the cb
/// corpus. Lookup failure answers CAPTURABLE (conservative: keeps the
/// row-chain fill + capture, never loses a CURRENT-OF answer).
fn scan_rte_tid_capturable(
    scanrelid: ::types_core::Index,
    rtable: &::types_nodes::NodeList<'_>,
) -> bool {
    debug_assert!(scanrelid >= 1);
    let Some(rte) = rtable
        .iter()
        .nth((scanrelid.saturating_sub(1)) as usize)
        .and_then(|n| n.as_range_tbl_entry())
    else {
        return true;
    };
    // Seamless unit-fixture worlds (scanfix; no syscache installed) answer
    // CAPTURABLE — the pre-narrowing behavior the band-94001 pins pin.
    // Production processes always install syscache.
    if !::syscache_seams::lookup_pg_class_ls_shape::is_installed() {
        return true;
    }
    match ::lsyscache::get_rel_relam(rte.relid) {
        Ok(relam) => !::reloptions::relam_is_pgrcolumnar(relam),
        Err(_) => true,
    }
}

/// The PLAN-tree twin of the wildcard walk: the §4.1 shape test as run at
/// PortalStart, BEFORE ExecutorStart fixes the eflags. Same spine as
/// `search_plan_tree` (and as `has_capturable_scan` above, which the capture
/// seam debug_asserts against this probe's answer at fill time — the
/// SEAM-WIRING AM narrowing only ever turns probe answers TRUE→FALSE, so
/// the probe-TRUE ⇒ walk-TRUE direction is preserved). Only the T_SeqScan
/// arm carries the narrowing: pgrcolumnar relations cannot appear under the
/// other scan tags (the AM supports no indexes and refuses TID/sample/
/// bitmap scans).
fn plan_has_capturable_scan(
    node: Option<::types_nodes::node_tree::Node<'_>>,
    rtable: &::types_nodes::NodeList<'_>,
) -> bool {
    use ::types_nodes::NodeTag;
    let Some(node) = node else {
        return false;
    };
    let plan = node.as_plan().expect("plan-tree node has a Plan prefix");
    match node.node_tag() {
        NodeTag::T_SeqScan => {
            let ss = node
                .as_variant::<::types_nodes::plannodes::SeqScan<'_>>()
                .expect("T_SeqScan");
            scan_rte_tid_capturable(ss.scan.scanrelid, rtable)
        }
        NodeTag::T_SampleScan
        | NodeTag::T_IndexScan
        | NodeTag::T_IndexOnlyScan
        | NodeTag::T_TidScan
        | NodeTag::T_TidRangeScan
        | NodeTag::T_BitmapHeapScan => true,
        NodeTag::T_Append => {
            let a = node.as_append().expect("T_Append");
            a.appendplans.iter().any(|p| plan_has_capturable_scan(Some(p), rtable))
        }
        NodeTag::T_Result | NodeTag::T_Limit => {
            plan_has_capturable_scan(plan.lefttree, rtable)
        }
        NodeTag::T_SubqueryScan => plan_has_capturable_scan(
            node.as_subquery_scan().expect("T_SubqueryScan").subplan,
            rtable,
        ),
        _ => false,
    }
}

/// Seam impl (execmain_seams::cursor_plan_current_of_eligible, escalation
/// EX-CA-1): the §4.1 eligibility answer for PortalStart's arming decision.
pub(crate) fn cursor_plan_current_of_eligible_seam(
    pstmt: &::types_nodes::plannodes::PlannedStmt<'_>,
) -> bool {
    plan_has_capturable_scan(pstmt.planTree, &pstmt.rtable)
}

/// Seam impl (execmain_seams::cursor_plan_capture_batch_fill; SE-R41,
/// notes/se-r41-retire.md §3.1): is this ELIGIBLE plan the batch store-fill
/// shape — a bare T_SeqScan top (the exact `PlanStateNode::SeqScan` gate of
/// `cursor_store_batch_fill`; no Result/Limit/Append/SubqueryScan wrapper)
/// over a tid-capable AM? TRUE moves the §4.2 capture inside the run (per
/// accepted row, at the emit surface — settle-safe by ordering), which is
/// what lets the portal drop the D-CA-2 fence eflags and lets the lane
/// batch fill own the heap fill cell. The AM re-check is defensive: the
/// caller only probes when the eligibility answer (already AM-narrowed on
/// the T_SeqScan arm) was TRUE.
pub(crate) fn cursor_plan_capture_batch_fill_seam(
    pstmt: &::types_nodes::plannodes::PlannedStmt<'_>,
) -> bool {
    use ::types_nodes::NodeTag;
    let Some(node) = pstmt.planTree else {
        return false;
    };
    if node.node_tag() != NodeTag::T_SeqScan {
        return false;
    }
    let ss = node
        .as_variant::<::types_nodes::plannodes::SeqScan<'_>>()
        .expect("T_SeqScan");
    scan_rte_tid_capturable(ss.scan.scanrelid, &pstmt.rtable)
}

/// SE-R41 (notes/se-r41-retire.md §3.7): the capture row loop's per-row
/// body — the run-seam fallback for capture-armed budgeted runs the batch
/// fill refused (Instrumented wrapper, page-batch refusal, lane family
/// OFF, …). Same identity walk as the seam-driven row-chain capture
/// (`cursor_capture_current_seam`), applied INSIDE the run right after the
/// receiver accepted the row — which is exactly what keeps it settle-safe
/// and sidecar/store-aligned regardless of which engine drove.
pub(crate) fn capture_current_into_sidecar<'mcx>(
    planstate: &PlanStateNode<'mcx>,
    estate: &::executils::EStateData<'mcx>,
    sidecar: ::types_portal::TuplestoreHandle,
) -> PgResult<()> {
    let hit = capture_positioned(planstate, estate);
    let (oid, packed) = hit.map(|(o, t)| (o, pack_tid(&t))).unwrap_or((0, 0));
    ::tuplestore::hold::tidstore_put(sidecar, oid, packed)
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
            // This seam only runs for portals the §4.1 plan-shape probe
            // judged eligible; assert the live-planstate spine agrees
            // (probe TRUE ⇒ wildcard walk TRUE). The opposite disagreement
            // direction is the tid_store.is_null() debug_assert in
            // exec_current_of's store-armed arm.
            debug_assert!(
                d.planstate.as_ref().is_some_and(|root| has_capturable_scan(root)),
                "cursor_capture_current: §4.1 plan-shape probe disagrees with the planstate spine"
            );
            let hit = d
                .planstate
                .as_ref()
                .and_then(|root| capture_positioned(root, &d.estate));
            Ok(hit.map(|(oid, tid)| (oid, pack_tid(&tid))))
        })
    })
}
// --- end WS-CA wave-10 -----------------------------------------------------------

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_cursor_state(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_CURSOR_STATE))
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_positioned(cursor_name: &str) -> Box<PgError> {
    invalid_cursor_state(format!("cursor \"{cursor_name}\" is not positioned on a row"))
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_simply_updatable(cursor_name: &str, table_name: &str) -> Box<PgError> {
    invalid_cursor_state(format!(
        "cursor \"{cursor_name}\" is not a simply updatable scan of table \"{table_name}\""
    ))
}
