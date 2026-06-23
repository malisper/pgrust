//! Unit tests for the `mcxtfuncs.c` port.
//!
//! The runtime seams are process-global function-pointer slots, so all tests run
//! under one serialization mutex and install deterministic mocks backed by a
//! synthetic context tree addressed by integer handles.

extern crate std;

use super::*;
use alloc::vec;
use alloc::vec::Vec;
use seam::{
    McxtRow, McxtSignalTarget, MemoryContextCounters, MemoryContextNode, MemoryContextRef,
    MemoryContextType,
};
use std::sync::{Mutex, Once};
use ::types_core::ProcNumber;

#[derive(Clone)]
struct FakeContext {
    parent: Option<MemoryContextRef>,
    firstchild: Option<MemoryContextRef>,
    nextchild: Option<MemoryContextRef>,
    name: Option<Vec<u8>>,
    ident: Option<Vec<u8>>,
    context_type: MemoryContextType,
    counters: MemoryContextCounters,
}

struct TestState {
    nodes: Vec<(MemoryContextRef, FakeContext)>,
    top: MemoryContextRef,
    rows: Vec<McxtRow>,
    target: Option<McxtSignalTarget>,
    signals: Vec<(i32, ProcSignalReason, ProcNumber)>,
    signal_result: i32,
}

static TEST: Mutex<TestState> = Mutex::new(TestState {
    nodes: Vec::new(),
    top: 0,
    rows: Vec::new(),
    target: None,
    signals: Vec::new(),
    signal_result: 0,
});
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn get(ptr: MemoryContextRef) -> FakeContext {
    TEST.lock()
        .unwrap()
        .nodes
        .iter()
        .find(|&&(p, _)| p == ptr)
        .map(|(_, n)| n.clone())
        .unwrap()
}

fn mock_top() -> PgResult<MemoryContextRef> {
    Ok(TEST.lock().unwrap().top)
}

fn mock_context_node(context: MemoryContextRef) -> PgResult<MemoryContextNode> {
    let c = get(context);
    Ok(MemoryContextNode {
        parent: c.parent,
        firstchild: c.firstchild,
        nextchild: c.nextchild,
        name: c.name,
        ident: c.ident,
        context_type: c.context_type,
    })
}

fn mock_context_stats(context: MemoryContextRef) -> PgResult<MemoryContextCounters> {
    Ok(get(context).counters)
}

fn mock_putvalues(row: McxtRow) -> PgResult<()> {
    TEST.lock().unwrap().rows.push(row);
    Ok(())
}

fn mock_pid_get_proc(_pid: i32) -> PgResult<Option<McxtSignalTarget>> {
    Ok(TEST.lock().unwrap().target)
}

fn mock_send_signal(pid: i32, reason: ProcSignalReason, proc_number: ProcNumber) -> i32 {
    let mut t = TEST.lock().unwrap();
    t.signals.push((pid, reason, proc_number));
    t.signal_result
}

static INSTALL: Once = Once::new();

fn install() {
    // Seam slots are process-global `OnceLock`s: install exactly once for the
    // whole test binary, then `reset()` per test to clear the synthetic state.
    INSTALL.call_once(|| {
        seam::top_memory_context::set(mock_top);
        seam::context_node::set(mock_context_node);
        seam::context_stats::set(mock_context_stats);
        seam::tuplestore_putvalues::set(mock_putvalues);
        seam::pid_get_proc::set(mock_pid_get_proc);
        send_proc_signal::set(mock_send_signal);
        // SQL_ASCII identity clip for `oversize_ident_is_clipped`: clip to `limit`
        // bytes (every ASCII byte is its own character).
        pg_mbcliplen::set(|_mbstr, _len, limit| limit);
    });
}

fn reset() {
    let mut t = TEST.lock().unwrap();
    t.nodes = Vec::new();
    t.top = 0;
    t.rows = Vec::new();
    t.target = None;
    t.signals = Vec::new();
    t.signal_result = 0;
}

fn ctx(
    ptr: MemoryContextRef,
    parent: Option<MemoryContextRef>,
    firstchild: Option<MemoryContextRef>,
    nextchild: Option<MemoryContextRef>,
    name: &str,
    context_type: MemoryContextType,
) -> (MemoryContextRef, FakeContext) {
    (
        ptr,
        FakeContext {
            parent,
            firstchild,
            nextchild,
            name: Some(name.as_bytes().to_vec()),
            ident: None,
            context_type,
            counters: MemoryContextCounters {
                nblocks: 1,
                freechunks: 2,
                totalspace: 100,
                freespace: 40,
            },
        },
    )
}

#[test]
fn breadth_first_walk_assigns_ids_and_builds_path() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();

    // Tree (handles 1..=4):
    //   1 (Top)
    //    +-- 2  (firstchild)
    //    |    +-- 4
    //    +-- 3  (2->nextchild)
    {
        let mut t = TEST.lock().unwrap();
        t.nodes = vec![
            ctx(1, None, Some(2), None, "TopMemoryContext", MemoryContextType::AllocSet),
            ctx(2, Some(1), Some(4), Some(3), "child2", MemoryContextType::Generation),
            ctx(3, Some(1), None, None, "child3", MemoryContextType::Slab),
            ctx(4, Some(2), None, None, "grandchild4", MemoryContextType::Bump),
        ];
        t.top = 1;
    }

    pg_get_backend_memory_contexts_core().unwrap();

    let t = TEST.lock().unwrap();
    // Breadth-first emission order: 1, 2, 3, 4
    assert_eq!(t.rows.len(), 4);

    // level column per row: Top=1, child2=2, child3=2, gc4=3
    assert_eq!(t.rows[0].level, 1);
    assert_eq!(t.rows[1].level, 2);
    assert_eq!(t.rows[2].level, 2);
    assert_eq!(t.rows[3].level, 3);

    // Path arrays constructed in emission order, one per row.
    assert_eq!(t.rows[0].path, vec![1]); // Top
    assert_eq!(t.rows[1].path, vec![1, 2]); // child2
    assert_eq!(t.rows[2].path, vec![1, 3]); // child3
    assert_eq!(t.rows[3].path, vec![1, 2, 4]); // grandchild4

    // totalspace = 100, totalspace - freespace = 60 for every node.
    for row in t.rows.iter() {
        assert_eq!(row.total_bytes, 100);
        assert_eq!(row.used_bytes, 60);
        assert_eq!(row.free_bytes, 40);
        assert_eq!(row.n_blocks, 1);
        assert_eq!(row.free_chunks, 2);
    }

    // name/type text columns carry the expected bytes.
    assert_eq!(t.rows[0].name.as_deref(), Some(b"TopMemoryContext".as_slice()));
    assert_eq!(t.rows[0].context_type, b"AllocSet");
    assert_eq!(t.rows[1].context_type, b"Generation");
    assert_eq!(t.rows[2].context_type, b"Slab");
    assert_eq!(t.rows[3].context_type, b"Bump");
    // ident column is NULL for every node (none set).
    for row in t.rows.iter() {
        assert!(row.ident.is_none());
    }
}

#[test]
fn unknown_context_type_renders_question_marks() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();
    {
        let mut t = TEST.lock().unwrap();
        t.nodes = vec![ctx(1, None, None, None, "ctx", MemoryContextType::Unknown)];
        t.top = 1;
    }
    pg_get_backend_memory_contexts_core().unwrap();
    let t = TEST.lock().unwrap();
    assert_eq!(t.rows[0].context_type, b"???");
}

#[test]
fn dynahash_context_uses_ident_as_name() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();

    {
        let mut t = TEST.lock().unwrap();
        t.nodes = vec![(
            1,
            FakeContext {
                parent: None,
                firstchild: None,
                nextchild: None,
                name: Some(b"dynahash".to_vec()),
                ident: Some(b"my hash table".to_vec()),
                context_type: MemoryContextType::AllocSet,
                counters: MemoryContextCounters::default(),
            },
        )];
        t.top = 1;
    }

    pg_get_backend_memory_contexts_core().unwrap();

    let t = TEST.lock().unwrap();
    assert_eq!(t.rows.len(), 1);
    // name column not null (relabeled ident), ident column null.
    assert_eq!(t.rows[0].name.as_deref(), Some(b"my hash table".as_slice()));
    assert!(t.rows[0].ident.is_none());
}

#[test]
fn ident_high_bytes_are_preserved_not_lossy() {
    // A server-encoding identifier with high bytes that is *not* valid UTF-8 must
    // be emitted byte-for-byte, matching C's CStringGetTextDatum on the raw char*.
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();

    let raw_ident: Vec<u8> = vec![b'a', 0xFF, 0xFE, b'z'];
    {
        let mut t = TEST.lock().unwrap();
        t.nodes = vec![(
            1,
            FakeContext {
                parent: None,
                firstchild: None,
                nextchild: None,
                name: Some(b"AllocSetContext".to_vec()),
                ident: Some(raw_ident.clone()),
                context_type: MemoryContextType::AllocSet,
                counters: MemoryContextCounters::default(),
            },
        )];
        t.top = 1;
    }

    pg_get_backend_memory_contexts_core().unwrap();

    let t = TEST.lock().unwrap();
    assert_eq!(t.rows.len(), 1);
    assert_eq!(t.rows[0].ident.as_deref(), Some(raw_ident.as_slice()));
}

#[test]
fn oversize_ident_is_clipped() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();

    // A 2000-byte ASCII identifier must be clipped to
    // MEMORY_CONTEXT_IDENT_DISPLAY_SIZE - 1 == 1023.
    let long_ident: Vec<u8> = vec![b'x'; 2000];
    {
        let mut t = TEST.lock().unwrap();
        t.nodes = vec![(
            1,
            FakeContext {
                parent: None,
                firstchild: None,
                nextchild: None,
                name: Some(b"ctx".to_vec()),
                ident: Some(long_ident),
                context_type: MemoryContextType::AllocSet,
                counters: MemoryContextCounters::default(),
            },
        )];
        t.top = 1;
    }

    pg_get_backend_memory_contexts_core().unwrap();

    let t = TEST.lock().unwrap();
    assert_eq!(t.rows[0].ident.as_ref().unwrap().len(), 1023);
}

#[test]
fn log_contexts_warns_when_pid_not_found() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();

    let result = pg_log_backend_memory_contexts(4242).unwrap();
    assert!(!result);
    assert!(TEST.lock().unwrap().signals.is_empty());
}

#[test]
fn log_contexts_sends_signal_on_success() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();
    TEST.lock().unwrap().target = Some(McxtSignalTarget { proc_number: 7 });

    let result = pg_log_backend_memory_contexts(123).unwrap();
    assert!(result);

    let t = TEST.lock().unwrap();
    assert_eq!(
        t.signals.as_slice(),
        [(123, ProcSignalReason::PROCSIG_LOG_MEMORY_CONTEXT, 7)]
    );
}

#[test]
fn log_contexts_warns_when_signal_fails() {
    let _g = TEST_LOCK.lock().unwrap();
    install();
    reset();
    {
        let mut t = TEST.lock().unwrap();
        t.target = Some(McxtSignalTarget { proc_number: 7 });
        // SendProcSignal returning < 0 => WARNING, return false.
        t.signal_result = -1;
    }

    let result = pg_log_backend_memory_contexts(123).unwrap();
    assert!(!result);
}
