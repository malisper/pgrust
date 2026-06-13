//! Tests for nodeNestloop pure (non-seam) logic: the `TupIsNull` predicate
//! over the EState slot arena and the filtered-tuple instrumentation counters.

use mcx::MemoryContext;
use types_core::instrument::Instrumentation;
use types_nodes::executor::{TupleTableSlot, TTS_FLAG_EMPTY};
use types_nodes::{EStateData, NestLoopStateData, TupleSlotKind};

use super::{instr_count_filtered1, instr_count_filtered2, tup_is_null};

#[test]
fn tup_is_null_none_is_null() {
    // C: `slot == NULL` => TupIsNull true. `None` slot id is the C NULL.
    let ctx = MemoryContext::new("nestloop-test");
    let estate = EStateData::new_in(ctx.mcx());
    assert!(tup_is_null(None, &estate));
}

#[test]
fn tup_is_null_empty_slot_is_null() {
    // C: TTS_EMPTY(slot) => TupIsNull true.
    let ctx = MemoryContext::new("nestloop-test");
    let mut estate = EStateData::new_in(ctx.mcx());
    let empty = TupleTableSlot {
        tts_flags: TTS_FLAG_EMPTY,
        tts_ops: TupleSlotKind::Virtual,
        tts_tid: Default::default(),
        tts_tableOid: 0,
    };
    let id = estate.make_slot(empty).unwrap();
    assert!(tup_is_null(Some(id), &estate));
}

#[test]
fn tup_is_null_nonempty_slot_is_not_null() {
    let ctx = MemoryContext::new("nestloop-test");
    let mut estate = EStateData::new_in(ctx.mcx());
    let full = TupleTableSlot {
        tts_flags: 0,
        tts_ops: TupleSlotKind::Virtual,
        tts_tid: Default::default(),
        tts_tableOid: 0,
    };
    let id = estate.make_slot(full).unwrap();
    assert!(!tup_is_null(Some(id), &estate));
}

#[test]
fn instr_counters_bump_only_with_instrumentation() {
    let ctx = MemoryContext::new("nestloop-test");
    let mut node = NestLoopStateData::default();

    // No instrument: the counters are a no-op (C: `if (node->instrument)`).
    instr_count_filtered1(&mut node);
    instr_count_filtered2(&mut node);
    assert!(node.js.ps.instrument.is_none());

    // With instrument: each call adds 1 to the respective counter.
    node.js.ps.instrument = Some(mcx::alloc_in(ctx.mcx(), Instrumentation::default()).unwrap());
    instr_count_filtered1(&mut node);
    instr_count_filtered1(&mut node);
    instr_count_filtered2(&mut node);
    let instr = node.js.ps.instrument.as_deref().unwrap();
    assert_eq!(instr.nfiltered1, 2.0);
    assert_eq!(instr.nfiltered2, 1.0);
}
