use super::*;
use std::sync::{Arc, Mutex, Once};
use types_storage::latch::LatchHandle;
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        use init_small::globals as g;
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        lmgr_proc_seams::proc_latch::set(|p| &lmgr_proc::GetPGProcByNumber(p).procLatch);
        g::SetIsUnderPostmaster(false);
        g::SetMaxConnections(4);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(4 + 3 + 2 + 2 + NUM_SPECIAL_WORKER_PROCS);
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        waiteventset::init_seams();
        latch::init_seams();
    });
}

fn become_backend(procno: types_core::ProcNumber, pid: i32) {
    use init_small::globals as g;
    g::SetMyProcNumber(procno);
    g::SetMyProcPid(pid);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let h = LatchHandle::proc(procno);
    lmgr_proc::GetPGProcByNumber(procno)
        .procLatch
        .owner_pid
        .store(0, std::sync::atomic::Ordering::SeqCst);
    latch::OwnLatch(h).unwrap();
    g::SetMyLatch(Some(h));
    latch::InitializeLatchWaitSet().unwrap();
}

fn queue_pair() -> (shm_mq::ShmMqHandle, shm_mq::ShmMqHandle) {
    let mq = shm_mq::shm_mq_create(1024);
    mq.set_receiver(0);
    mq.set_sender(0);
    let rx = shm_mq::shm_mq_attach(Arc::clone(&mq));
    let tx = shm_mq::shm_mq_attach(mq);
    (tx, rx)
}

fn teardown_redirect() {
    pq_stop_redirect_to_shm_mq();
    pqcomm::set_pq_comm_methods(&pqcomm::PQ_COMM_SOCKET_METHODS);
    pq_set_parallel_leader(0, types_core::INVALID_PROC_NUMBER);
}

#[test]
fn redirect_frames_protocol_bytes() {
    let _s = serial();
    setup();
    become_backend(0, 7300);
    let (tx, mut rx) = queue_pair();

    pq_redirect_to_shm_mq(tx);
    assert_eq!(elog::config::where_to_send_output(), types_dest::CommandDest::Remote);
    assert_eq!(init_small::globals::FrontendProtocol(), (3 << 16) | 2);

    // One queue message = msgtype byte + body, no length word.
    assert_eq!(pqcomm::pq_putmessage(b'E', b"SERROR\0\0").unwrap(), 0);
    assert_eq!(pqcomm::pq_putmessage(b'N', b"notice-body").unwrap(), 0);
    assert_eq!(pqcomm::pq_putmessage(b'A', b"").unwrap(), 0);

    match rx.receive(true).unwrap() {
        shm_mq::ShmMqRecv::Success(data) => assert_eq!(data, b"ESERROR\0\0"),
        other => panic!("expected Success, got {other:?}"),
    }
    match rx.receive(true).unwrap() {
        shm_mq::ShmMqRecv::Success(data) => assert_eq!(data, b"Nnotice-body"),
        other => panic!("expected Success, got {other:?}"),
    }
    match rx.receive(true).unwrap() {
        shm_mq::ShmMqRecv::Success(data) => assert_eq!(data, b"A"),
        other => panic!("expected Success, got {other:?}"),
    }

    assert_eq!(pqcomm::pq_flush().unwrap(), 0);
    assert_eq!(pqcomm::pq_flush_if_writable().unwrap(), 0);
    assert!(!pqcomm::pq_is_send_pending());
    pqcomm::pq_comm_reset();

    teardown_redirect();
    assert_eq!(elog::config::where_to_send_output(), types_dest::CommandDest::None);
    drop(rx);
}

#[test]
fn missing_queue_swallows_message() {
    let _s = serial();
    setup();
    become_backend(0, 7301);
    let (tx, rx) = queue_pair();

    pq_redirect_to_shm_mq(tx);
    pq_stop_redirect_to_shm_mq();
    assert_eq!(pqcomm::pq_putmessage(b'N', b"late debug").unwrap(), 0);

    teardown_redirect();
    drop(rx);
}

#[test]
fn detached_receiver_returns_eof() {
    let _s = serial();
    setup();
    become_backend(0, 7302);
    let (tx, rx) = queue_pair();

    pq_redirect_to_shm_mq(tx);
    drop(rx);
    assert_eq!(pqcomm::pq_putmessage(b'E', b"who hears me").unwrap(), EOF);

    teardown_redirect();
}

#[test]
fn putmessage_noblock_unsupported() {
    let _s = serial();
    setup();
    become_backend(0, 7303);
    let (tx, rx) = queue_pair();

    pq_redirect_to_shm_mq(tx);
    let err = pqcomm::pq_putmessage_noblock(b'N', b"x").unwrap_err();
    assert_eq!(err.message, "not currently supported");

    teardown_redirect();
    drop(rx);
}

fn errnotice_msg<'mcx>(ctx: &'mcx mcx::MemoryContext, fields: &[(u8, &[u8])]) -> StringInfo<'mcx> {
    let mut bytes = Vec::new();
    for (code, value) in fields {
        bytes.push(*code);
        bytes.extend_from_slice(value);
        bytes.push(0);
    }
    bytes.push(0);
    StringInfo::from_vec(mcx::slice_in(ctx.mcx(), &bytes).unwrap()).unwrap()
}

#[test]
fn parse_errornotice_all_fields() {
    let ctx = mcx::MemoryContext::new("pqmq-test");
    let mut msg = errnotice_msg(
        &ctx,
        &[
            (b'S', b"ERROR"),
            (b'V', b"ERROR"),
            (b'C', b"42P01"),
            (b'M', b"relation \"nope\" does not exist"),
            (b'D', b"the detail"),
            (b'H', b"the hint"),
            (b'P', b"12"),
            (b'p', b"7"),
            (b'q', b"select 1"),
            (b'W', b"the context"),
            (b's', b"myschema"),
            (b't', b"mytable"),
            (b'c', b"mycol"),
            (b'd', b"mytype"),
            (b'n', b"myconstraint"),
            (b'F', b"parse_relation.c"),
            (b'L', b"1384"),
            (b'R', b"parserOpenTable"),
        ],
    );
    let edata = pq_parse_errornotice(&mut msg).unwrap();
    assert_eq!(edata.level, ERROR);
    assert_eq!(edata.sqlstate, make_sqlstate(*b"42P01"));
    assert_eq!(edata.message, "relation \"nope\" does not exist");
    assert_eq!(edata.detail.as_deref(), Some("the detail"));
    assert_eq!(edata.hint.as_deref(), Some("the hint"));
    assert_eq!(edata.cursor_position, Some(12));
    assert_eq!(edata.internal_position, Some(7));
    assert_eq!(edata.internal_query.as_deref(), Some("select 1"));
    assert_eq!(edata.context.as_deref(), Some("the context"));
    assert_eq!(edata.schema_name.as_deref(), Some("myschema"));
    assert_eq!(edata.table_name.as_deref(), Some("mytable"));
    assert_eq!(edata.column_name.as_deref(), Some("mycol"));
    assert_eq!(edata.datatype_name.as_deref(), Some("mytype"));
    assert_eq!(edata.constraint_name.as_deref(), Some("myconstraint"));
    let loc = edata.location.expect("F/L/R fields present");
    assert_eq!(loc.filename.as_deref(), Some("parse_relation.c"));
    assert_eq!(loc.lineno, 1384);
    assert_eq!(loc.funcname.as_deref(), Some("parserOpenTable"));
}

#[test]
fn parse_errornotice_severities() {
    let ctx = mcx::MemoryContext::new("pqmq-test");
    for (sev, level) in [
        (&b"DEBUG"[..], DEBUG1),
        (b"LOG", LOG),
        (b"INFO", INFO),
        (b"NOTICE", NOTICE),
        (b"WARNING", WARNING),
        (b"ERROR", ERROR),
        (b"FATAL", FATAL),
        (b"PANIC", PANIC),
    ] {
        let mut msg = errnotice_msg(&ctx, &[(b'V', sev), (b'M', b"m")]);
        assert_eq!(pq_parse_errornotice(&mut msg).unwrap().level, level);
    }
    // The localized 'S' field is ignored; without 'V' the default holds.
    let mut msg = errnotice_msg(&ctx, &[(b'S', b"FEHLER"), (b'M', b"m")]);
    assert_eq!(pq_parse_errornotice(&mut msg).unwrap().level, ERROR);
}

#[test]
fn parse_errornotice_rejects_garbage() {
    let ctx = mcx::MemoryContext::new("pqmq-test");

    let mut msg = errnotice_msg(&ctx, &[(b'V', b"SHOUTING")]);
    let err = pq_parse_errornotice(&mut msg).unwrap_err();
    assert_eq!(err.message, "unrecognized error severity: \"SHOUTING\"");

    let mut msg = errnotice_msg(&ctx, &[(b'C', b"4200")]);
    let err = pq_parse_errornotice(&mut msg).unwrap_err();
    assert_eq!(err.message, "invalid SQLSTATE: \"4200\"");

    let mut msg = errnotice_msg(&ctx, &[(b'Z', b"whatever")]);
    let err = pq_parse_errornotice(&mut msg).unwrap_err();
    assert_eq!(err.message, "unrecognized error field code: 90");
}
