// tablesync.c: the initial-copy state machine. Subset per round-5 inc E:
// plain (non-partitioned) tables, no row filters, no published generated
// columns, no binary copy_format — each refuses loudly. The state machine and
// the apply<->sync worker handshake (SYNCWAIT -> CATCHUP -> SYNCDONE -> READY)
// are ported 1:1; C's relmutex-guarded shared fields live in the launcher
// pool behind its Mutex, and the last_start_times HTAB is the launcher ctx
// HashMap.
#![allow(non_snake_case)]

use std::cell::Cell;

use elog::ereport;
use mcx::Mcx;
use types_core::{InvalidOid, InvalidRepOriginId, InvalidXLogRecPtr, Oid, XLogRecPtr};
use types_error::{PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, LOG};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use launcher::{SUBREL_STATE_CATCHUP, SUBREL_STATE_SYNCWAIT};
use pg_subscription::{
    GetSubscriptionRelState, GetSubscriptionRelations, UpdateSubscriptionRelState,
    SUBREL_STATE_DATASYNC, SUBREL_STATE_FINISHEDCOPY, SUBREL_STATE_INIT, SUBREL_STATE_READY,
    SUBREL_STATE_SYNCDONE, SUBREL_STATE_UNKNOWN,
};
use walreceiver::client::{CopyData, ExecStatus, PgConn};

use crate::{loc, my_sub};

thread_local! {
    pub(crate) static AM_TABLESYNC_WORKER: Cell<bool> = const { Cell::new(false) };
    // FetchTableStates cache (C file-statics), invalidated by the
    // SUBSCRIPTIONRELMAP syscache callback.
    static TABLE_STATES_VALID: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn invalidate_table_states_cb(_arg: datum::Datum, _cacheid: i32, _hash: u32) {
    TABLE_STATES_VALID.set(false);
}

// ReplicationSlotNameForTablesync (tablesync.c:1302).
pub fn ReplicationSlotNameForTablesync(suboid: Oid, relid: Oid) -> String {
    format!("pg_{}_sync_{}_{}", suboid, relid, transam_xlog::control_file::GetSystemIdentifier())
}

fn wait_latch_10ms() -> PgResult<()> {
    let rc = latch::WaitLatch(
        init_small::globals::MyLatch(),
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        10,
        0,
    )?;
    if rc & WL_LATCH_SET != 0 {
        if let Some(l) = init_small::globals::MyLatch() {
            latch::ResetLatch(l);
        }
        postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

// wait_for_relation_state_change (tablesync.c:229): apply side waits for the
// catalog state to reach `expected_state` (or the sync worker to vanish).
fn wait_for_relation_state_change(mcx: Mcx<'_>, relid: Oid, expected_state: u8) -> PgResult<()> {
    let subid = my_sub(|s| s.oid);
    loop {
        postgres_seams::check_for_interrupts::call()?;
        inval::local::InvalidateSystemCaches()?;

        xact::StartTransactionCommand()?;
        let (state, _lsn) = GetSubscriptionRelState(mcx, subid, relid)?;
        xact::CommitTransactionCommand()?;

        if state == SUBREL_STATE_UNKNOWN || state == expected_state {
            return Ok(());
        }
        // Bail if the worker has disappeared (it owns the transition).
        if launcher::logicalrep_worker_find(subid, relid, false).is_none() {
            return Ok(());
        }
        wait_latch_10ms()?;
    }
}

// wait_for_worker_state_change (tablesync.c:190ish): tablesync side waits for
// the apply worker to promote our shared state to `expected_state`.
fn wait_for_worker_state_change(expected_state: u8) -> PgResult<()> {
    loop {
        postgres_seams::check_for_interrupts::call()?;
        let (state, _lsn) = launcher::my_worker_relstate();
        if state == expected_state {
            return Ok(());
        }
        // Wake the apply leader in case it's waiting on us (C signals the
        // apply worker each iteration).
        launcher::logicalrep_worker_wakeup(my_sub(|s| s.oid), InvalidOid);
        wait_latch_10ms()?;
    }
}

// finish_sync_worker (tablesync.c:143).
fn finish_sync_worker() -> PgResult<()> {
    if xact::IsTransactionState() {
        xact::CommitTransactionCommand()?;
    }
    let (name, _) = my_sub(|s| (s.name.clone(), ()));
    let relid = launcher::worker_snapshot(launcher::my_worker_slot().expect("attached"))
        .map(|w| w.relid)
        .unwrap_or(InvalidOid);
    let _ = elog::elog(
        LOG,
        format!(
            "logical replication table synchronization worker for subscription \"{name}\", relation OID {relid} has finished"
        ),
    );
    // Wake the leader so it notices SYNCDONE promptly.
    launcher::logicalrep_worker_wakeup(my_sub(|s| s.oid), InvalidOid);
    crate::request_apply_worker_exit();
    Ok(())
}

// process_syncing_tables (tablesync.c:695).
pub(crate) fn process_syncing_tables(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    current_lsn: XLogRecPtr,
) -> PgResult<()> {
    if AM_TABLESYNC_WORKER.with(Cell::get) {
        process_syncing_tables_for_sync(mcx, conn, current_lsn)
    } else {
        process_syncing_tables_for_apply(mcx, current_lsn)
    }
}

// process_syncing_tables_for_sync (tablesync.c:300).
fn process_syncing_tables_for_sync(
    mcx: Mcx<'static>,
    conn: &mut PgConn,
    current_lsn: XLogRecPtr,
) -> PgResult<()> {
    let (state, lsn) = launcher::my_worker_relstate();
    if !(state == SUBREL_STATE_CATCHUP && current_lsn >= lsn) {
        return Ok(());
    }

    let subid = my_sub(|s| s.oid);
    let relid = launcher::worker_snapshot(launcher::my_worker_slot().expect("attached"))
        .expect("worker slot")
        .relid;

    launcher::my_worker_set_relstate(SUBREL_STATE_SYNCDONE, current_lsn);

    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
    }
    UpdateSubscriptionRelState(mcx, subid, relid, SUBREL_STATE_SYNCDONE, current_lsn, false)?;

    // End streaming so we can use the connection for the slot drop
    // (walrcv_endstreaming): CopyDone, then drain results.
    let _ = conn.put_copy_end();
    while let Ok(Some(_)) = conn.get_result() {}

    // Drop the tablesync slot on the publisher.
    let slotname = ReplicationSlotNameForTablesync(subid, relid);
    let res = conn.exec(&format!("DROP_REPLICATION_SLOT \"{}\" WAIT", slotname.replace('"', "\"\"")))?;
    if res.status == ExecStatus::Error {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not drop replication slot \"{slotname}\" on publisher: {}",
                res.err
            ))
            .finish(loc("process_syncing_tables_for_sync"))?;
    }

    xact::CommitTransactionCommand()?;

    // Cleanup the tablesync origin tracking; session first, then drop.
    xact::StartTransactionCommand()?;
    let originname = format!("pg_{subid}_{relid}");
    let _ = origin::replorigin_session_reset();
    origin::set_replorigin_session_origin(InvalidRepOriginId);
    origin::replorigin_drop_by_name(mcx, &originname, true, false)?;
    xact::CommitTransactionCommand()?;

    finish_sync_worker()
}

// process_syncing_tables_for_apply (tablesync.c:459).
fn process_syncing_tables_for_apply(mcx: Mcx<'static>, current_lsn: XLogRecPtr) -> PgResult<()> {
    debug_assert!(!xact::IsTransactionState());
    let subid = my_sub(|s| s.oid);

    // FetchTableStates: reread not-READY states when invalidated.
    let mut started_tx = false;
    let not_ready: Vec<(Oid, u8, XLogRecPtr)> = {
        if !xact::IsTransactionState() {
            xact::StartTransactionCommand()?;
            started_tx = true;
        }
        let rstates = GetSubscriptionRelations(mcx, subid, true)?;
        TABLE_STATES_VALID.set(true);
        rstates.iter().map(|r| (r.relid, r.state, r.lsn)).collect()
    };

    for (relid, mut state, mut lsn) in not_ready {
        if state == SUBREL_STATE_SYNCDONE {
            if current_lsn >= lsn {
                state = SUBREL_STATE_READY;
                lsn = current_lsn;
                // C (tablesync.c:502): hold the subscription object lock and
                // pg_subscription_rel open RowExclusive across origin drop +
                // state update; UpdateSubscriptionRelState(already_locked)
                // asserts that lock is already held.
                lmgr::LockSharedObject(
                    pg_subscription::SubscriptionRelationId,
                    subid,
                    0,
                    types_rel::AccessShareLock,
                )?;
                let relrel = table::table_open(
                    mcx,
                    pg_subscription::SubscriptionRelRelationId,
                    types_rel::RowExclusiveLock,
                )?;
                let originname = format!("pg_{subid}_{relid}");
                origin::replorigin_drop_by_name(mcx, &originname, true, false)?;
                UpdateSubscriptionRelState(mcx, subid, relid, state, lsn, true)?;
                relrel.close(types_rel::NoLock)?;
            }
            continue;
        }

        match launcher::sync_worker_read_and_maybe_catchup(subid, relid, current_lsn) {
            Some((SUBREL_STATE_SYNCWAIT, _)) => {
                // Told the worker to catch up; wait for SYNCDONE.
                if started_tx {
                    xact::CommitTransactionCommand()?;
                    started_tx = false;
                }
                xact::StartTransactionCommand()?;
                started_tx = true;
                wait_for_relation_state_change(mcx, relid, SUBREL_STATE_SYNCDONE)?;
            }
            Some(_) => {}
            None => {
                // No sync worker: launch one, bounded + throttled.
                let nsync = launcher::logicalrep_sync_worker_count(subid);
                if nsync < launcher::max_sync_workers_per_subscription() as usize {
                    let now = timestamp_seams::get_current_timestamp::call();
                    let interval = guc_tables::vars::wal_retrieve_retry_interval.read();
                    if launcher::tablesync_start_time_check_and_set(relid, now, interval) {
                        let w = launcher::worker_snapshot(
                            launcher::my_worker_slot().expect("attached"),
                        )
                        .expect("worker slot");
                        let name = my_sub(|s| s.name.clone());
                        let _ = launcher::logicalrep_worker_launch(
                            launcher::LogicalRepWorkerType::TableSync,
                            w.dbid,
                            subid,
                            &name,
                            w.userid,
                            relid,
                        )?;
                    }
                }
            }
        }
    }

    if started_tx {
        xact::CommitTransactionCommand()?;
    }
    Ok(())
}

// AllTablesyncsReady (tablesync.c): the subscription has relations and every
// one of them is READY.
pub(crate) fn all_tablesyncs_ready(mcx: Mcx<'_>) -> PgResult<bool> {
    let subid = my_sub(|s| s.oid);
    let mut started_tx = false;
    if !xact::IsTransactionState() {
        xact::StartTransactionCommand()?;
        started_tx = true;
    }
    let not_ready = GetSubscriptionRelations(mcx, subid, true)?.len();
    let has_subrels = if not_ready > 0 {
        true
    } else {
        !GetSubscriptionRelations(mcx, subid, false)?.is_empty()
    };
    if started_tx {
        xact::CommitTransactionCommand()?;
    }
    Ok(has_subrels && not_ready == 0)
}

// make_copy_attnamelist (tablesync.c:726): remote attnames as the COPY FROM
// column list (name-matched locally).
fn make_copy_attnamelist<'mcx>(
    mcx: Mcx<'mcx>,
    attnames: &[std::string::String],
) -> PgResult<types_nodes::NodeList<'mcx>> {
    let mut list = types_nodes::NodeList::nil();
    for name in attnames {
        let sval = {
            let v = mcx::slice_in(mcx, name.as_bytes())?;
            core::str::from_utf8(v.leak()).expect("copied str stays UTF-8")
        };
        let node = types_nodes::Node::mk(mcx, types_nodes::String { sval })?;
        list.lappend(mcx, node)?;
    }
    Ok(list)
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// fetch_remote_table_info (tablesync.c:825), plain-table subset: refuse row
// filters and published generated columns loudly.
fn fetch_remote_table_info(
    conn: &mut PgConn,
    nspname: &str,
    relname: &str,
) -> PgResult<logicalproto::LogicalRepRelation> {
    fn text(r: &[Option<Vec<u8>>], i: usize) -> String {
        r.get(i).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    }
    let lit = |s: &str| format!("'{}'", s.replace('\'', "''"));

    // Relation info.
    let cmd = format!(
        "SELECT c.oid, c.relreplident, c.relkind FROM pg_catalog.pg_class c INNER JOIN \
         pg_catalog.pg_namespace n ON (c.relnamespace = n.oid) WHERE n.nspname = {} AND \
         c.relname = {}",
        lit(nspname),
        lit(relname)
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk || res.rows.len() != 1 {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "table \"{nspname}.{relname}\" not found on publisher: {}",
                res.err
            ))
            .finish(loc("fetch_remote_table_info"))?;
    }
    let remoteid: Oid = text(&res.rows[0], 0).parse().unwrap_or(InvalidOid);
    let replident = text(&res.rows[0], 1).bytes().next().unwrap_or(b'd');
    let relkind = text(&res.rows[0], 2).bytes().next().unwrap_or(b'r');

    // Row filters: any non-null qual for this relation in the subscribed
    // publications is out of the ported subset.
    let pubnames = my_sub(|s| s.publications.clone());
    let publist = pubnames.iter().map(|p| lit(p)).collect::<Vec<_>>().join(", ");
    let cmd = format!(
        "SELECT DISTINCT pg_get_expr(gpt.qual, gpt.relid) FROM pg_publication p, LATERAL \
         pg_get_publication_tables(p.pubname) gpt WHERE gpt.relid = {remoteid} AND p.pubname IN ({publist})"
    );
    let res = conn.exec(&cmd)?;
    if res.status == ExecStatus::TuplesOk
        && res.rows.iter().any(|r| r.first().map(|c| c.is_some()).unwrap_or(false))
    {
        panic!("unported: row-filter publication in tablesync (round-5 subset)");
    }

    // Columns (attgenerated = '' excludes generated; gencol publication is
    // therefore refused implicitly — C's gencol arm is phase-2 here).
    let cmd = format!(
        "SELECT a.attnum, a.attname, a.atttypid, a.attnum = ANY(i.indkey) FROM \
         pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_index i ON (i.indexrelid = \
         pg_get_replica_identity_index({remoteid})) WHERE a.attnum > 0::pg_catalog.int2 AND NOT \
         a.attisdropped AND a.attgenerated = '' AND a.attrelid = {remoteid} ORDER BY a.attnum"
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not fetch table info for table \"{nspname}.{relname}\": {}",
                res.err
            ))
            .finish(loc("fetch_remote_table_info"))?;
    }

    let mut attnames = Vec::new();
    let mut atttyps = Vec::new();
    let mut attkeys = Vec::new();
    for row in &res.rows {
        attnames.push(text(row, 1));
        atttyps.push(text(row, 2).parse().unwrap_or(InvalidOid));
        attkeys.push(text(row, 3) == "t");
    }

    Ok(logicalproto::LogicalRepRelation {
        remoteid,
        nspname: nspname.to_string(),
        relname: relname.to_string(),
        natts: attnames.len(),
        attnames,
        atttyps,
        replident,
        relkind,
        attkeys,
    })
}

// copy_table (tablesync.c:1143), plain-table arm.
fn copy_table(mcx: Mcx<'static>, conn: &mut PgConn, nspname: &str, relname: &str) -> PgResult<()> {
    let lrel = fetch_remote_table_info(conn, nspname, relname)?;

    if lrel.relkind != b'r' {
        // Sequences/views/partitioned publisher rels: COPY (SELECT ...) arm.
        panic!("unported: tablesync of non-plain publisher relation (relkind '{}')", lrel.relkind as char);
    }

    logicalrelation::logicalrep_relmap_update(&lrel);
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, lrel.remoteid, types_rel::NoLock, subid)?;
    let _ = &entry;

    // COPY nsp.rel (cols...) TO STDOUT on the publisher.
    let mut cmd = format!("COPY {}.{}", quote_ident(nspname), quote_ident(relname));
    if lrel.natts > 0 {
        cmd.push_str(" (");
        cmd.push_str(&lrel.attnames.iter().map(|a| quote_ident(a)).collect::<Vec<_>>().join(", "));
        cmd.push(')');
    }
    cmd.push_str(" TO STDOUT");

    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::CopyOut {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("could not start initial contents copy for table \"{nspname}.{relname}\": {}", res.err))
            .finish(loc("copy_table"))?;
    }

    // Local COPY FROM fed by the publisher's COPY OUT stream. C's
    // copy_read_data: block for at least one byte, hand over what's buffered.
    let attnamelist = make_copy_attnamelist(mcx, &lrel.attnames)?;
    let options = types_nodes::NodeList::nil();
    let conn_cell = std::cell::RefCell::new(conn);
    let pending: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::new());
    let cb: Box<dyn FnMut(&mut [u8], usize) -> PgResult<usize> + '_> =
        Box::new(|buf: &mut [u8], _minread: usize| -> PgResult<usize> {
        let mut pending = pending.borrow_mut();
        loop {
            if !pending.is_empty() {
                let n = pending.len().min(buf.len());
                buf[..n].copy_from_slice(&pending[..n]);
                pending.drain(..n);
                return Ok(n);
            }
            let mut conn = conn_cell.borrow_mut();
            match conn.get_copy_data() {
                Ok(CopyData::Msg(m)) => {
                    *pending = m;
                }
                Ok(CopyData::End) => return Ok(0),
                Ok(CopyData::Block) => {
                    postgres_seams::check_for_interrupts::call()?;
                    conn.wait_readable()?;
                    if !conn.consume_input() {
                        elog::elog(
                            ERROR,
                            format!("could not read COPY data: {}", conn.error_message()),
                        )?;
                        unreachable!();
                    }
                }
                Err(e) => {
                    elog::elog(ERROR, format!("could not read COPY data: {e}"))?;
                    unreachable!();
                }
            }
        }
    });

    let mut cstate = copy_cmd::BeginCopyFromCallback(mcx, &rel, &attnamelist, &options, cb)?;
    copy_cmd::CopyFrom(mcx, &mut cstate, &rel)?;
    copy_cmd::EndCopyFrom(cstate)?;

    // Drain the publisher's CommandComplete tail.
    {
        let mut conn = conn_cell.borrow_mut();
        while let Ok(Some(r)) = conn.get_result() {
            if r.status == ExecStatus::Error {
                ereport(ERROR)
                    .errcode(ERRCODE_CONNECTION_FAILURE)
                    .errmsg(format!("table copy failed: {}", r.err))
                    .finish(loc("copy_table"))?;
            }
        }
    }

    logicalrelation::logicalrep_rel_close(rel, types_rel::NoLock)?;
    Ok(())
}

// walrcv_create_slot's USE_SNAPSHOT arm: returns the consistent point.
fn create_slot_use_snapshot(
    conn: &mut PgConn,
    slotname: &str,
    failover: bool,
) -> PgResult<XLogRecPtr> {
    let mut opts: Vec<&str> = vec!["SNAPSHOT 'use'"];
    if failover {
        opts.push("FAILOVER");
    }
    let cmd = format!(
        "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput ({})",
        slotname.replace('"', "\"\""),
        opts.join(", ")
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk || res.rows.is_empty() {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("could not create replication slot \"{slotname}\": {}", res.err))
            .finish(loc("create_slot_use_snapshot"))?;
    }
    // Row: slot_name, consistent_point, snapshot_name, output_plugin.
    let lsn_text = res.rows[0]
        .get(1)
        .and_then(|c| c.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let (hi, lo) = lsn_text.split_once('/').unwrap_or(("0", "0"));
    let lsn = (u64::from_str_radix(hi, 16).unwrap_or(0) << 32)
        | u64::from_str_radix(lo, 16).unwrap_or(0);
    Ok(lsn)
}

// LogicalRepSyncTableStart (tablesync.c:1318). Returns (conn, slotname,
// origin_startpos) ready for the catchup stream.
pub(crate) fn LogicalRepSyncTableStart(
    mcx: Mcx<'static>,
    relid: Oid,
) -> PgResult<(PgConn, String, XLogRecPtr)> {
    let subid = my_sub(|s| s.oid);

    xact::StartTransactionCommand()?;
    let (relstate, relstate_lsn) = GetSubscriptionRelState(mcx, subid, relid)?;
    xact::CommitTransactionCommand()?;

    launcher::my_worker_set_relstate(relstate, relstate_lsn);

    if matches!(relstate, SUBREL_STATE_SYNCDONE | SUBREL_STATE_READY | SUBREL_STATE_UNKNOWN) {
        finish_sync_worker()?;
        // The caller sees the exit flag and unwinds.
        return Err(Box::new(types_error::PgError::error(
            "tablesync already done".to_string(),
        )));
    }

    let slotname = ReplicationSlotNameForTablesync(subid, relid);
    let must_use_password = my_sub(|s| s.passwordrequired && !s.ownersuperuser);
    let (conninfo, name) = my_sub(|s| (s.conninfo.clone(), s.name.clone()));

    let mut conn =
        match walreceiver::client::connect_extended(&conninfo, true, true, must_use_password, &slotname)? {
            Ok(c) => c,
            Err(e) => {
                ereport(ERROR)
                    .errcode(ERRCODE_CONNECTION_FAILURE)
                    .errmsg(format!(
                        "table synchronization worker for subscription \"{name}\" could not connect to the publisher: {e}"
                    ))
                    .finish(loc("LogicalRepSyncTableStart"))?;
                unreachable!();
            }
        };

    debug_assert!(matches!(
        relstate,
        SUBREL_STATE_INIT | SUBREL_STATE_DATASYNC | SUBREL_STATE_FINISHEDCOPY
    ));

    let originname = format!("pg_{subid}_{relid}");

    if relstate == SUBREL_STATE_FINISHEDCOPY {
        // Copy already done in a previous attempt: reuse the origin position.
        xact::StartTransactionCommand()?;
        let originid = origin::replorigin_by_name(&originname, false)?;
        origin::replorigin_session_setup(originid, 0)?;
        origin::set_replorigin_session_origin(originid);
        let origin_startpos = origin::replorigin_session_get_progress(false)?;
        xact::CommitTransactionCommand()?;

        launcher::my_worker_set_relstate(SUBREL_STATE_SYNCWAIT, origin_startpos);
        wait_for_worker_state_change(SUBREL_STATE_CATCHUP)?;
        return Ok((conn, slotname, origin_startpos));
    }

    if relstate == SUBREL_STATE_DATASYNC {
        // Previous attempt crashed mid-copy: drop its slot, missing_ok.
        let res = conn
            .exec(&format!("DROP_REPLICATION_SLOT \"{}\" WAIT", slotname.replace('"', "\"\"")))?;
        let _ = res; // missing slot is fine
    }

    launcher::my_worker_set_relstate(SUBREL_STATE_DATASYNC, InvalidXLogRecPtr);

    xact::StartTransactionCommand()?;
    UpdateSubscriptionRelState(mcx, subid, relid, SUBREL_STATE_DATASYNC, InvalidXLogRecPtr, false)?;
    let mut originid = origin::replorigin_by_name(&originname, true)?;
    if originid == InvalidRepOriginId {
        originid = origin::replorigin_create(mcx, &originname)?;
    }
    xact::CommitTransactionCommand()?;

    // The copy runs in a REPEATABLE READ transaction pinned to the slot's
    // initial snapshot on BOTH sides.
    xact::StartTransactionCommand()?;
    let rel = table::table_open(mcx, relid, types_rel::RowExclusiveLock)?;
    let (nspname, relname) = {
        let nsp = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        let name = rel.name().to_string();
        (nsp, name)
    };

    let res = conn.exec("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")?;
    if res.status == ExecStatus::Error {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("table copy could not start transaction on publisher: {}", res.err))
            .finish(loc("LogicalRepSyncTableStart"))?;
    }

    let failover = false; // C passes MySubscription->failover; failover slots unported here
    let origin_startpos = create_slot_use_snapshot(&mut conn, &slotname, failover)?;

    origin::replorigin_advance(originid, origin_startpos, InvalidXLogRecPtr, true, true)?;
    origin::replorigin_session_setup(originid, 0)?;
    origin::set_replorigin_session_origin(originid);

    // SwitchToUntrustedUser (run_as_owner=false) is not ported — recorded
    // divergence shared with the apply worker; RLS-enabled targets refuse.
    if rel.rd_rel.relrowsecurity {
        ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot replicate into relation with row-level security enabled: \"{relname}\""
            ))
            .finish(loc("LogicalRepSyncTableStart"))?;
    }

    snapmgr::PushActiveSnapshot(&snapmgr::GetTransactionSnapshot()?)?;
    copy_table(mcx, &mut conn, &nspname, &relname)?;
    snapmgr::PopActiveSnapshot()?;

    let res = conn.exec("COMMIT")?;
    if res.status == ExecStatus::Error {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("table copy could not finish transaction on publisher: {}", res.err))
            .finish(loc("LogicalRepSyncTableStart"))?;
    }

    rel.close(types_rel::NoLock)?;
    xact::CommandCounterIncrement()?;

    UpdateSubscriptionRelState(
        mcx,
        subid,
        relid,
        SUBREL_STATE_FINISHEDCOPY,
        launcher::my_worker_relstate().1,
        false,
    )?;
    xact::CommitTransactionCommand()?;

    // Copy done: hand off to the leader (SYNCWAIT -> wait for CATCHUP).
    launcher::my_worker_set_relstate(SUBREL_STATE_SYNCWAIT, origin_startpos);
    wait_for_worker_state_change(SUBREL_STATE_CATCHUP)?;

    Ok((conn, slotname, origin_startpos))
}

// run_tablesync_worker (tablesync.c:1721): copy phase, then catch up on the
// tablesync slot until the leader's target LSN, via the shared apply loop.
pub(crate) fn run_tablesync_worker(mcx: Mcx<'static>, relid: Oid) -> PgResult<()> {
    AM_TABLESYNC_WORKER.with(|c| c.set(true));

    let (mut conn, slotname, origin_startpos) = match LogicalRepSyncTableStart(mcx, relid) {
        Ok(v) => v,
        Err(e) => {
            if crate::apply_worker_exit_requested() {
                return Ok(()); // already-done states exit cleanly
            }
            return Err(e);
        }
    };

    // START_REPLICATION on the tablesync slot from the copy end position.
    crate::start_logical_streaming_on(&mut conn, &slotname, origin_startpos)?;
    crate::apply_loop(&mut conn, origin_startpos)
}

#[cfg(test)]
mod tests {
    // ReplicationSlotNameForTablesync embeds the system identifier; verify the
    // C format "pg_%u_sync_%u_" UINT64 (tablesync.c:1302) structurally.
    #[test]
    fn tablesync_slot_name_format() {
        let name = format!("pg_{}_sync_{}_{}", 16385u32, 16401u32, 7234567890123456789u64);
        assert!(name.starts_with("pg_16385_sync_16401_"));
        let parts: Vec<&str> = name.split('_').collect();
        assert_eq!(parts.len(), 5);
        assert!(parts[4].parse::<u64>().is_ok());
    }
}
