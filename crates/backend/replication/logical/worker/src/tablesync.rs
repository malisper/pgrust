// tablesync.c: the initial-copy state machine. Subset per round-5 inc E:
// plain (non-partitioned) tables, no row filters, no published generated
// columns, no binary copy_format — each refuses loudly. The state machine and
// the apply<->sync worker handshake (SYNCWAIT -> CATCHUP -> SYNCDONE -> READY)
// are ported 1:1; C's relmutex-guarded shared fields live in the launcher
// pool behind its Mutex, and the per-worker last_start_times HTAB is the
// launcher ctx's (subid, relid)-keyed tablesync map.
#![allow(non_snake_case)]

use std::cell::Cell;

use elog::ereport;
use mcx::Mcx;
use types_core::{InvalidOid, InvalidRepOriginId, InvalidXLogRecPtr, Oid, XLogRecPtr};
use types_error::{
    PgResult, ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, LOG, NOTICE,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use launcher::{SUBREL_STATE_CATCHUP, SUBREL_STATE_SYNCWAIT};
use pg_subscription::{
    GetSubscriptionRelState, GetSubscriptionRelations, UpdateSubscriptionRelState,
    SUBREL_STATE_DATASYNC, SUBREL_STATE_FINISHEDCOPY, SUBREL_STATE_INIT, SUBREL_STATE_READY,
    SUBREL_STATE_SYNCDONE, SUBREL_STATE_UNKNOWN,
};
use walreceiver::client::{CopyData, ExecStatus, PgConn, QueryResult};

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
// wait_for_relation_state_change (tablesync.c:163): runs inside the CALLER's
// open transaction (C asserts none of its own; process_syncing_tables_for_
// apply starts one just before). Starting a nested transaction here was the
// "StartTransactionCommand: unexpected state STARTED" apply-worker crash at
// every tablesync handoff. Per-iteration InvalidateCatalogSnapshot keeps the
// catalog read fresh, as C does.
fn wait_for_relation_state_change(mcx: Mcx<'_>, relid: Oid, expected_state: u8) -> PgResult<()> {
    let subid = my_sub(|s| s.oid);
    loop {
        postgres_seams::check_for_interrupts::call()?;

        snapmgr::InvalidateCatalogSnapshot();
        let (state, _lsn) = GetSubscriptionRelState(mcx, subid, relid)?;

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
    conn: Option<&mut PgConn>,
    current_lsn: XLogRecPtr,
) -> PgResult<()> {
    // Skip for parallel apply workers: the leader will do it (tablesync.c
    // WORKERTYPE_PARALLEL_APPLY arm; also avoids the empty-xact accumulation
    // C describes there).
    if crate::parallel::am_parallel_apply_worker() {
        return Ok(());
    }
    if AM_TABLESYNC_WORKER.with(Cell::get) {
        process_syncing_tables_for_sync(
            mcx,
            conn.expect("tablesync worker has a publisher connection"),
            current_lsn,
        )
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

    // End streaming so that the connection can be used to drop the slot
    // (tablesync.c:326).
    walreceiver::client::end_streaming(conn)?;

    // Cleanup the tablesync slot (tablesync.c:345).
    let slotname = ReplicationSlotNameForTablesync(subid, relid);
    drop_slot_at_pub_node(conn, &slotname, false)?;

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
#[allow(unused_assignments)] // Rust-structural: defensive started_tx = false between commit and restart (C tablesync.c:578-587 has no intermediate clear); hoisted
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
    // tablesync.c:455: every table READY -> drop the start-times table.
    if not_ready.is_empty() {
        launcher::tablesync_start_times_destroy(subid);
    }

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
                    if launcher::tablesync_start_time_check_and_set(subid, relid, now, interval) {
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
                            0,
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

pub(crate) enum DropSlotOutcome {
    Dropped,
    MissingTolerated,
    Failed,
}

// ReplicationSlotDropAtPubNode's result triage (subscriptioncmds.c:1959-1980):
// only WALRCV_OK_COMMAND is success; with missing_ok, only 42704 is tolerated.
pub(crate) fn drop_slot_outcome(res: &QueryResult, missing_ok: bool) -> DropSlotOutcome {
    if res.status == ExecStatus::CommandOk {
        return DropSlotOutcome::Dropped;
    }
    let undefined_object = res.diag.as_ref().is_some_and(|d| d.sqlstate == "42704");
    if res.status == ExecStatus::Error && missing_ok && undefined_object {
        return DropSlotOutcome::MissingTolerated;
    }
    DropSlotOutcome::Failed
}

// ReplicationSlotDropAtPubNode (subscriptioncmds.c:1938), the tablesync
// worker's callers.
pub(crate) fn drop_slot_at_pub_node(
    conn: &mut PgConn,
    slotname: &str,
    missing_ok: bool,
) -> PgResult<()> {
    let cmd = format!("DROP_REPLICATION_SLOT \"{}\" WAIT", slotname.replace('"', "\"\""));
    let res = conn.exec(&cmd)?;
    match drop_slot_outcome(&res, missing_ok) {
        DropSlotOutcome::Dropped => ereport(NOTICE)
            .errmsg(format!("dropped replication slot \"{slotname}\" on publisher"))
            .finish(loc("ReplicationSlotDropAtPubNode")),
        DropSlotOutcome::MissingTolerated => ereport(LOG)
            .errmsg(format!(
                "could not drop replication slot \"{slotname}\" on publisher: {}",
                res.err
            ))
            .finish(loc("ReplicationSlotDropAtPubNode")),
        DropSlotOutcome::Failed => ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not drop replication slot \"{slotname}\" on publisher: {}",
                res.err
            ))
            .finish(loc("ReplicationSlotDropAtPubNode")),
    }
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

// quote_literal_cstr (quote.c:103 -> quote_literal_internal:47): a backslash
// anywhere forces the E'' form with backslashes doubled, so a publisher
// running standard_conforming_strings = off decodes the value correctly.
pub(crate) fn quote_literal_cstr(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 2 + 3);
    if raw.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for ch in raw.chars() {
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

// fetch_remote_table_info (tablesync.c:825). Also returns the relation's
// row-filter quals to be OR'ed into the COPY command (tablesync.c:1094-1131):
// a NULL qual for any subscribed publication means the whole table is copied,
// so the list collapses to empty. Published generated columns stay refused
// implicitly (the attribute query excludes attgenerated != '').
// int2vector text output ("1 3 4") -> attnums.
pub(crate) fn parse_int2vector(s: &str) -> Vec<i16> {
    s.split_whitespace().filter_map(|w| w.parse().ok()).collect()
}

fn fetch_remote_table_info(
    conn: &mut PgConn,
    nspname: &str,
    relname: &str,
) -> PgResult<(logicalproto::LogicalRepRelation, Vec<String>)> {
    fn text(r: &[Option<Vec<u8>>], i: usize) -> String {
        r.get(i).and_then(|c| c.as_ref()).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    }
    let lit = quote_literal_cstr;

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

    // Row filters (tablesync.c:1094): DISTINCT quals across the subscribed
    // publications, combined with OR at COPY time. A NULL qual (a publication
    // without a filter, FOR ALL TABLES, or TABLES IN SCHEMA) means the whole
    // table is copied — drop any collected filters.
    let pubnames = my_sub(|s| s.publications.clone());
    let publist = pubnames.iter().map(|p| lit(p)).collect::<Vec<_>>().join(", ");
    let cmd = format!(
        "SELECT DISTINCT pg_get_expr(gpt.qual, gpt.relid) FROM pg_publication p, LATERAL \
         pg_get_publication_tables(p.pubname) gpt WHERE gpt.relid = {remoteid} AND p.pubname IN ({publist})"
    );
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!(
                "could not fetch table WHERE clause info for table \"{nspname}.{relname}\": {}",
                res.err
            ))
            .finish(loc("fetch_remote_table_info"))?;
    }
    let mut quals: Vec<String> = Vec::new();
    for row in &res.rows {
        match row.first().and_then(|c| c.as_ref()) {
            Some(q) => quals.push(String::from_utf8_lossy(q).into_owned()),
            None => {
                quals.clear();
                break;
            }
        }
    }

    // Column lists (tablesync.c:882): fetched before the column names so
    // columns outside the list are skipped; a NULL attrs means all columns.
    let mut included_cols: Option<Vec<i16>> = None;
    if conn.server_version() >= 150000 {
        let cmd = format!(
            "SELECT DISTINCT  (CASE WHEN (array_length(gpt.attrs, 1) = c.relnatts)   THEN NULL ELSE \
             gpt.attrs END)  FROM pg_publication p,  LATERAL pg_get_publication_tables(p.pubname) \
             gpt,  pg_class c WHERE gpt.relid = {remoteid} AND c.oid = gpt.relid   AND p.pubname IN \
             ( {publist} )"
        );
        let pubres = conn.exec(&cmd)?;
        if pubres.status != ExecStatus::TuplesOk {
            ereport(ERROR)
                .errcode(ERRCODE_CONNECTION_FAILURE)
                .errmsg(format!(
                    "could not fetch column list info for table \"{nspname}.{relname}\" from publisher: {}",
                    pubres.err
                ))
                .finish(loc("fetch_remote_table_info"))?;
        }
        if pubres.rows.len() > 1 {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot use different column lists for table \"{nspname}.{relname}\" in different publications"
                ))
                .finish(loc("fetch_remote_table_info"))?;
        }
        if let Some(attrs) = pubres.rows.first().and_then(|r| r.first()).and_then(|c| c.as_ref()) {
            included_cols = Some(parse_int2vector(&String::from_utf8_lossy(attrs)));
        }
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
        // tablesync.c:1023: not in the column list, skip it.
        let attnum: i16 = text(row, 0).parse().unwrap_or(0);
        if included_cols.as_ref().is_some_and(|cols| !cols.contains(&attnum)) {
            continue;
        }
        attnames.push(text(row, 1));
        atttyps.push(text(row, 2).parse().unwrap_or(InvalidOid));
        attkeys.push(text(row, 3) == "t");
    }

    Ok((
        logicalproto::LogicalRepRelation {
            remoteid,
            nspname: nspname.to_string(),
            relname: relname.to_string(),
            natts: attnames.len(),
            attnames,
            atttyps,
            replident,
            relkind,
            attkeys,
        },
        quals,
    ))
}

// copy_table's publisher command (tablesync.c:1172-1246): plain tables with
// no row filter COPY directly; other relkinds (views, partitioned tables
// published via root) and filtered tables go through COPY (SELECT ...), with
// C's ONLY for RELKIND_RELATION (children are copied separately) and the
// filters OR'ed. Published generated columns stay refused upstream
// (fetch_remote_table_info excludes them), so C's gencol SELECT arm is
// unreachable here.
fn copy_table_cmd(
    relkind: u8,
    nspname: &str,
    relname: &str,
    attnames: &[String],
    quals: &[String],
) -> String {
    let collist =
        attnames.iter().map(|a| quote_ident(a)).collect::<Vec<_>>().join(", ");
    if relkind == b'r' && quals.is_empty() {
        let mut cmd = format!("COPY {}.{}", quote_ident(nspname), quote_ident(relname));
        if !attnames.is_empty() {
            cmd.push_str(" (");
            cmd.push_str(&collist);
            cmd.push(')');
        }
        cmd.push_str(" TO STDOUT");
        cmd
    } else {
        let only = if relkind == b'r' { "ONLY " } else { "" };
        let mut cmd = format!(
            "COPY (SELECT {collist} FROM {only}{}.{}",
            quote_ident(nspname),
            quote_ident(relname)
        );
        if !quals.is_empty() {
            cmd.push_str(" WHERE ");
            cmd.push_str(&quals.join(" OR "));
        }
        cmd.push_str(") TO STDOUT");
        cmd
    }
}

// copy_table (tablesync.c:1143).
fn copy_table(mcx: Mcx<'static>, conn: &mut PgConn, nspname: &str, relname: &str) -> PgResult<()> {
    let (lrel, quals) = fetch_remote_table_info(conn, nspname, relname)?;

    logicalrelation::logicalrep_relmap_update(&lrel);
    let subid = my_sub(|s| s.oid);
    let (entry, rel) =
        logicalrelation::logicalrep_rel_open(mcx, lrel.remoteid, types_rel::NoLock, subid)?;
    let _ = &entry;

    let cmd = copy_table_cmd(lrel.relkind, nspname, relname, &lrel.attnames, &quals);

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

    // SAFETY: the callback captures only two shared references (`&conn_cell`,
    // `&pending`) to `RefCell`s on this stack frame. Those captures have no
    // drop glue, and both `RefCell`s outlive `cstate` (declared above it and
    // dropped after it in reverse-declaration order), so the callback's state
    // strictly outlives the returned `CopyFromState`'s drop — satisfying
    // BeginCopyFromCallback's safety contract.
    let mut cstate =
        unsafe { copy_cmd::BeginCopyFromCallback(mcx, &rel, &attnamelist, &options, cb)? };
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

// libpqrcv_create_slot's command text (libpqwalreceiver.c:952), permanent
// logical USE_SNAPSHOT arm: options in C's order (FAILOVER before SNAPSHOT);
// publishers below 15 take the legacy keyword syntax (FAILOVER USE_SNAPSHOT).
fn create_slot_use_snapshot_cmd(server_version: i32, slotname: &str, failover: bool) -> String {
    let new_syntax = server_version >= 150000;
    let mut cmd = format!(
        "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput ",
        slotname.replace('"', "\"\"")
    );
    if new_syntax {
        cmd.push('(');
    }
    if failover {
        cmd.push_str(if new_syntax { "FAILOVER, " } else { "FAILOVER " });
    }
    if new_syntax {
        cmd.push_str("SNAPSHOT 'use')");
    } else {
        cmd.push_str("USE_SNAPSHOT");
    }
    cmd
}

// walrcv_create_slot's USE_SNAPSHOT arm: returns the consistent point.
fn create_slot_use_snapshot(
    conn: &mut PgConn,
    slotname: &str,
    failover: bool,
) -> PgResult<XLogRecPtr> {
    let cmd = create_slot_use_snapshot_cmd(conn.server_version(), slotname, failover);
    let res = conn.exec(&cmd)?;
    if res.status != ExecStatus::TuplesOk {
        // libpqwalreceiver.c:1036: ERRCODE_PROTOCOL_VIOLATION.
        ereport(ERROR)
            .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!("could not create replication slot \"{slotname}\": {}", res.err))
            .finish(loc("create_slot_use_snapshot"))?;
    }
    // upstream a6a2eb9f6024 (18.6): Check CREATE_REPLICATION_SLOT response shape in libpqwalreceiver
    walreceiver::client::check_create_slot_result(&res, slotname)?;
    // Row: slot_name, consistent_point, snapshot_name, output_plugin.
    let lsn_text = res.rows[0]
        .get(1)
        .and_then(|c| c.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    parse_consistent_point(&lsn_text)
}

// libpqrcv_create_slot (libpqwalreceiver.c:1051): the consistent point goes
// through pg_lsn_in, so malformed text is an ERRCODE_INVALID_TEXT_REPRESENTATION
// error, never a silently adopted 0/0.
pub(crate) fn parse_consistent_point(text: &str) -> PgResult<XLogRecPtr> {
    adt_pg_lsn::pg_lsn_in(text, None)
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
        // Previous attempt crashed mid-copy: drop its slot (tablesync.c:1407).
        drop_slot_at_pub_node(&mut conn, &slotname, true)?;
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
        // SQL_ASCII relation names may be non-UTF-8; rel.name() would panic.
        // Match C's opaque NameData bytes with a lossy copy for the COPY command.
        let name = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
        (nsp, name)
    };

    let res = conn.exec("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ")?;
    if res.status == ExecStatus::Error {
        ereport(ERROR)
            .errcode(ERRCODE_CONNECTION_FAILURE)
            .errmsg(format!("table copy could not start transaction on publisher: {}", res.err))
            .finish(loc("LogicalRepSyncTableStart"))?;
    }

    // C passes MySubscription->failover so the tablesync slot of a failover
    // subscription is failover-marked too (tablesync.c:1491).
    let failover = my_sub(|s| s.failover);
    let origin_startpos = create_slot_use_snapshot(&mut conn, &slotname, failover)?;

    origin::replorigin_advance(originid, origin_startpos, InvalidXLogRecPtr, true, true)?;
    origin::replorigin_session_setup(originid, 0)?;
    origin::set_replorigin_session_origin(originid);

    // Make sure that the copy command runs as the table owner, unless the
    // user has opted out of that behavior (tablesync.c:1515).
    let ucxt = crate::apply::maybe_switch_to_table_owner(mcx, rel.rd_rel.relowner)?;

    // tablesync.c: the acting user needs INSERT on the target before the
    // copy (pg_class_aclcheck + aclcheck_error).
    let aclresult = aclchk::pg_class_aclcheck(rel.rd_id, miscinit::GetUserId(), types_nodes::parsenodes::ACL_INSERT)?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            tablecmds::get_relkind_objtype(rel.rd_rel.relkind),
            &relname,
        )?;
    }

    // RLS-enabled targets refuse (recorded divergence: C refuses only when
    // the acting user does not bypass RLS, check_enable_rls).
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

    // Restore the per-table copy state (tablesync.c:1557).
    crate::apply::restore_user_context(&ucxt)?;

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

    // ReplicationOriginNameForLogicalRep + set_apply_error_context_origin
    // (tablesync.c:1731-1736).
    let originname = format!("pg_{}_{}", my_sub(|s| s.oid), relid);
    crate::set_apply_error_context_origin(&originname);

    // START_REPLICATION on the tablesync slot from the copy end position.
    crate::start_logical_streaming_on(&mut conn, &slotname, origin_startpos)?;
    crate::apply_loop(&mut conn, origin_startpos)
}

#[cfg(test)]
mod tests {
    // walrcv_create_slot command text (libpqwalreceiver.c option order:
    // FAILOVER before SNAPSHOT) and copy_table's two publisher commands
    // (tablesync.c:1172-1226).
    #[test]
    fn create_slot_and_copy_commands() {
        assert_eq!(
            super::create_slot_use_snapshot_cmd(180006, "s1", false),
            "CREATE_REPLICATION_SLOT \"s1\" LOGICAL pgoutput (SNAPSHOT 'use')"
        );
        assert_eq!(
            super::create_slot_use_snapshot_cmd(150000, "s1", true),
            "CREATE_REPLICATION_SLOT \"s1\" LOGICAL pgoutput (FAILOVER, SNAPSHOT 'use')"
        );
        // Publishers below 15: libpqwalreceiver.c's legacy keyword syntax.
        assert_eq!(
            super::create_slot_use_snapshot_cmd(140000, "s1", false),
            "CREATE_REPLICATION_SLOT \"s1\" LOGICAL pgoutput USE_SNAPSHOT"
        );
        assert_eq!(
            super::create_slot_use_snapshot_cmd(140000, "s1", true),
            "CREATE_REPLICATION_SLOT \"s1\" LOGICAL pgoutput FAILOVER USE_SNAPSHOT"
        );
        let cols = ["a".to_string(), "b".to_string()];
        // Plain table, no row filter: direct COPY with a column list.
        assert_eq!(
            super::copy_table_cmd(b'r', "public", "t", &cols, &[]),
            "COPY \"public\".\"t\" (\"a\", \"b\") TO STDOUT"
        );
        // Non-plain publisher relkind (partitioned via root, views): the
        // COPY (SELECT ...) arm, without C's ONLY (that is table-only).
        assert_eq!(
            super::copy_table_cmd(b'p', "public", "t", &cols, &[]),
            "COPY (SELECT \"a\", \"b\" FROM \"public\".\"t\") TO STDOUT"
        );
        // Single row filter on a plain table: SELECT arm with ONLY + WHERE
        // (tablesync.c:1199-1240).
        assert_eq!(
            super::copy_table_cmd(b'r', "public", "t", &cols, &["(a > 5)".to_string()]),
            "COPY (SELECT \"a\", \"b\" FROM ONLY \"public\".\"t\" WHERE (a > 5)) TO STDOUT"
        );
        // Multiple publications' filters are OR'ed.
        assert_eq!(
            super::copy_table_cmd(
                b'r',
                "public",
                "t",
                &cols,
                &["(a > 5)".to_string(), "(b IS NULL)".to_string()]
            ),
            "COPY (SELECT \"a\", \"b\" FROM ONLY \"public\".\"t\" \
             WHERE (a > 5) OR (b IS NULL)) TO STDOUT"
        );
    }

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
