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
    // try_with: reachable from guard Drops inside TLS destructors at thread
    // exit; a destroyed registry must leak, never abort the process.
    let entry = ENTRIES.try_with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize) {
            Some(slot) if slot.as_ref().map(|en| en.generation) == Some(generation) => {
                let _ = FREE.try_with(|f| f.borrow_mut().push(idx));
                slot.take()
            }
            _ => None,
        }
    });
    if let Ok(Some(e)) = entry {
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

pub fn put_heap_tuple(h: TuplestoreHandle, htup: &::types_tuple::HeapTupleData<'_>) -> PgResult<()> {
    with_store(h, |store| store.put_heap_tuple(htup))
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

fn rescan_hold(h: TuplestoreHandle) -> PgResult<()> {
    with_store(h, |store| store.rescan())
}

fn skiptuples_hold(h: TuplestoreHandle, ntuples: i64, forward: bool) -> PgResult<bool> {
    with_store(h, |store| store.skiptuples(ntuples, forward))
}

// --- WS-CA wave-10 (cursors inc-2): the portal cursor-store seams + the §4.2
// row-identity sidecar. The sidecar rows are 2x int8 (tableoid,
// block<<16|offset) formed/deformed with a locally-built minimal tupdesc —
// internal layout, never client-visible; ~16 B payload + minimal-tuple
// framing per FETCHED row of a CURRENT-OF-eligible plan (worklog D-CA-1
// records the delta vs the contract's ~10 B/row trailing-column estimate).

fn begin_heap_cursor(random_access: bool, inter_xact: bool) -> PgResult<TuplestoreHandle> {
    let work_mem = init_small::globals::work_mem();
    Ok(register(Tuplestore::begin_heap(random_access, inter_xact, work_mem)))
}

fn tuple_count_hold(h: TuplestoreHandle) -> i64 {
    with_store(h, |store| store.tuple_count())
}

/// The sidecar row schema: 2x not-null int8, double-aligned, plain storage.
/// Built per call in a throwaway bump arena (the desc is only read during
/// form/deform; per-row build cost is a named perf lever, not contract
/// scope — eligible fills are already per-row executor drives).
fn with_tid_desc<R>(
    f: impl for<'m> FnOnce(::mcx::Mcx<'m>, std::rc::Rc<::types_tuple::TupleDescData<'m>>) -> PgResult<R>,
) -> PgResult<R> {
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, TYPALIGN_DOUBLE, TYPSTORAGE_PLAIN,
    };
    let ctx = ::mcx::MemoryContext::new_bump("cursor tidstore desc");
    let mcx = ctx.mcx();
    let mut compact_attrs = ::mcx::PgVec::new_in(mcx);
    let mut attrs = ::mcx::PgVec::new_in(mcx);
    for attnum in 1..=2i16 {
        let att = FormData_pg_attribute {
            atttypid: 20, // INT8OID
            attlen: 8,
            attnum,
            atttypmod: -1,
            attbyval: true,
            attalign: TYPALIGN_DOUBLE,
            attstorage: TYPSTORAGE_PLAIN,
            attnotnull: true,
            attislocal: true,
            ..FormData_pg_attribute::default()
        };
        compact_attrs.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    let desc = std::rc::Rc::new(::types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: 2249, // RECORDOID
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs,
        attrs,
    });
    f(mcx, desc)
}

/// §4.2 sidecar append. Plain pub fn AND the seam impl: the seam serves
/// pquery's row-chain capture loop; execmain's in-run capture surfaces
/// (SE-R41 batch sink + capture row loop) link tuplestore directly — the
/// `tidstore_get` precedent below.
pub fn tidstore_put(h: TuplestoreHandle, tableoid: u32, tid_packed: u64) -> PgResult<()> {
    with_tid_desc(|_mcx, desc| {
        let values = [
            ::datum::Datum::from_i64(tableoid as i64),
            ::datum::Datum::from_i64(tid_packed as i64),
        ];
        with_store(h, |store| store.putvalues(&desc, &values, &[false, false]))
    })
}

/// §4.2 resolution read (execCurrentOf on a store-armed portal): row
/// `row_index` (0-based) of the sidecar, as (tableoid, packed tid). Seeks
/// the sidecar's own ptr0 — the sidecar has no other reader. Plain pub fn
/// (not a seam): the caller is execmain, which links tuplestore directly.
pub fn tidstore_get(h: TuplestoreHandle, row_index: i64) -> PgResult<Option<(u32, u64)>> {
    with_tid_desc(|mcx, desc| {
        let taken = with_store(h, |store| -> PgResult<bool> {
            store.rescan()?;
            if row_index > 0 && !store.skiptuples(row_index, true)? {
                return Ok(false);
            }
            Ok(true)
        })?;
        if !taken {
            return Ok(None);
        }
        // The desc lives exactly as long as the slot (same arena, same scope).
        let mut slot = exectuples::make_tuple_table_slot(
            mcx,
            ::types_slot::TupleSlotKind::MinimalTuple,
            Some(desc),
        );
        let got = with_store(h, |store| store.gettupleslot(true, true, &mut slot, mcx))?;
        if !got {
            return Ok(None);
        }
        let mut isnull = false;
        let tableoid = exectuples::slot_getattr(&mut slot, 1, &mut isnull).as_i64() as u32;
        debug_assert!(!isnull);
        let tid = exectuples::slot_getattr(&mut slot, 2, &mut isnull).as_i64() as u64;
        debug_assert!(!isnull);
        drop(slot);
        Ok(Some((tableoid, tid)))
    })
}

pub(crate) fn install_seams() {
    tuplestore_hold_seams::tuplestore_begin_heap_hold::set(begin_heap_hold);
    tuplestore_hold_seams::tuplestore_end::set(end);
    tuplestore_hold_seams::tuplestore_gettupleslot::set(gettupleslot_hold);
    tuplestore_hold_seams::tuplestore_rescan::set(rescan_hold);
    tuplestore_hold_seams::tuplestore_skiptuples::set(skiptuples_hold);
    // WS-CA wave-10 (cursors inc-2):
    tuplestore_hold_seams::tuplestore_begin_heap_cursor::set(begin_heap_cursor);
    tuplestore_hold_seams::tuplestore_tuple_count::set(tuple_count_hold);
    tuplestore_hold_seams::tuplestore_tidstore_put::set(tidstore_put);
}
