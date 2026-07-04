// The Rust marshal for C's `Tuplestorestate *` held by PortalData<'static>:
// generation-checked handles over a thread-local registry (stmt_list shape).
// A stale handle is a loud panic; ops never re-enter the registry.
use core::cell::{Cell, RefCell};

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_portal::TuplestoreHandle;
use ::types_slot::SlotData;

use crate::Tuplestore;

struct Entry {
    generation: u32,
    store: Tuplestore,
}

thread_local! {
    static ENTRIES: RefCell<Vec<Option<Entry>>> = const { RefCell::new(Vec::new()) };
    static FREE: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static GENERATION: Cell<u32> = const { Cell::new(0) };
}

fn encode(idx: u32, generation: u32) -> TuplestoreHandle {
    TuplestoreHandle((u64::from(generation) << 32) | u64::from(idx + 1))
}

fn decode(h: TuplestoreHandle) -> (u32, u32) {
    ((h.0 as u32) - 1, (h.0 >> 32) as u32)
}

pub fn register(store: Tuplestore) -> TuplestoreHandle {
    let generation = GENERATION.with(|g| {
        let v = g.get().wrapping_add(1);
        g.set(v);
        v
    });
    let entry = Entry { generation, store };
    let idx = match FREE.with(|f| f.borrow_mut().pop()) {
        Some(i) => {
            ENTRIES.with(|e| e.borrow_mut()[i as usize] = Some(entry));
            i
        }
        None => ENTRIES.with(|e| {
            let mut e = e.borrow_mut();
            e.push(Some(entry));
            (e.len() - 1) as u32
        }),
    };
    encode(idx, generation)
}

pub fn with_store<R>(h: TuplestoreHandle, f: impl FnOnce(&mut Tuplestore) -> R) -> R {
    assert!(!h.is_null(), "tuplestore: NULL handle dereferenced");
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize).and_then(Option::as_mut) {
            Some(entry) if entry.generation == generation => f(&mut entry.store),
            _ => panic!("tuplestore: stale TuplestoreHandle {h:?} (ended)"),
        }
    })
}

pub fn end(h: TuplestoreHandle) {
    if h.is_null() {
        return;
    }
    let (idx, generation) = decode(h);
    let entry = ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize) {
            Some(slot) if slot.as_ref().map(|en| en.generation) == Some(generation) => {
                FREE.with(|f| f.borrow_mut().push(idx));
                slot.take()
            }
            _ => None,
        }
    });
    if let Some(e) = entry {
        e.store.end();
    }
}

/// Unregister and hand back the store (C: transferring the
/// `Tuplestorestate *` itself, e.g. into `ReturnSetInfo.setResult`).
pub fn take(h: TuplestoreHandle) -> Option<crate::Tuplestore> {
    if h.is_null() {
        return None;
    }
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize) {
            Some(slot) if slot.as_ref().map(|en| en.generation) == Some(generation) => {
                FREE.with(|f| f.borrow_mut().push(idx));
                slot.take().map(|en| en.store)
            }
            _ => None,
        }
    })
}

// The slot's own allocator IS C's tts_mcxt; ambient there, carried here.
#[inline]
fn slot_mcx<'mcx>(slot: &SlotData<'mcx>) -> Mcx<'mcx> {
    *slot.base().tts_values.allocator()
}

pub fn puttupleslot(h: TuplestoreHandle, slot: &mut SlotData<'_>) -> PgResult<()> {
    let mcx = slot_mcx(slot);
    with_store(h, |store| store.puttupleslot(slot, mcx))
}

pub fn putvalues(
    h: TuplestoreHandle,
    tdesc: &::types_tuple::TupleDescData<'_>,
    values: &[::datum::Datum],
    isnull: &[bool],
) -> PgResult<()> {
    with_store(h, |store| store.putvalues(tdesc, values, isnull))
}

fn begin_heap_hold(random_access: bool) -> PgResult<TuplestoreHandle> {
    let work_mem = init_small::globals::work_mem();
    Ok(register(Tuplestore::begin_heap(random_access, true, work_mem)))
}

fn gettupleslot_hold(
    h: TuplestoreHandle,
    forward: bool,
    copy: bool,
    slot: &mut SlotData<'_>,
) -> PgResult<bool> {
    let mcx = slot_mcx(slot);
    with_store(h, |store| store.gettupleslot(forward, copy, slot, mcx))
}

fn rescan_hold(h: TuplestoreHandle) {
    with_store(h, |store| store.rescan())
}

fn skiptuples_hold(h: TuplestoreHandle, ntuples: i64, forward: bool) -> bool {
    with_store(h, |store| store.skiptuples(ntuples, forward))
}

pub(crate) fn install_seams() {
    tuplestore_hold_seams::tuplestore_begin_heap_hold::set(begin_heap_hold);
    tuplestore_hold_seams::tuplestore_end::set(end);
    tuplestore_hold_seams::tuplestore_gettupleslot::set(gettupleslot_hold);
    tuplestore_hold_seams::tuplestore_rescan::set(rescan_hold);
    tuplestore_hold_seams::tuplestore_skiptuples::set(skiptuples_hold);
}
