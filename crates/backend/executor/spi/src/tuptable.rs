use core::cell::Cell;

use elog::ereport;
use mcx::{McxOwned, MemoryContext, PgVec};
use types_core::SubTransactionId;
use types_error::{PgResult, WARNING};
use types_slot::SlotData;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::{loc, set_spi_tuptable, with_current, SPI_STACK};

pub struct TuptabData<'mcx> {
    pub tupdesc: TupleDescData<'mcx>,
    pub vals: PgVec<'mcx, HeapTupleData<'mcx>>,
}

mcx::bind!(pub TuptabTy => TuptabData<'mcx>);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TuptabHandle(pub u64);

pub(crate) struct TuptabEntry {
    pub id: u64,
    pub subid: SubTransactionId,
    pub table: McxOwned<TuptabTy>,
}

thread_local! {
    static NEXT_TUPTAB_ID: Cell<u64> = const { Cell::new(1) };
}

pub(crate) fn spi_dest_startup(_operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
    let connected = with_current(|conn| conn.tuptable.is_some());
    match connected {
        None => panic!("spi_dest_startup called while not connected to SPI"),
        Some(true) => panic!("improper call to spi_dest_startup"),
        Some(false) => {}
    }

    let ctx = MemoryContext::new("SPI TupTable");
    let table = McxOwned::<TuptabTy>::try_new(ctx, |mcx| {
        let tupdesc = tupdesc::CreateTupleDescCopy(mcx, typeinfo)?;
        let vals = mcx::vec_with_capacity_in(mcx, 128)?;
        Ok(TuptabData { tupdesc, vals })
    })?;
    let id = NEXT_TUPTAB_ID.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    });
    let subid = xact::GetCurrentSubTransactionId();
    with_current(|conn| {
        conn.tuptables.push(TuptabEntry { id, subid, table });
        conn.tuptable = Some(id);
    });
    Ok(())
}

pub(crate) fn spi_printtup(slot: &mut SlotData<'_>) -> PgResult<bool> {
    let slot_mcx = *slot.base().tts_values.allocator();
    SPI_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let conn = stack.last_mut().unwrap_or_else(|| {
            panic!("spi_printtup called while not connected to SPI")
        });
        let id = conn.tuptable.unwrap_or_else(|| panic!("improper call to spi_printtup"));
        let entry = conn
            .tuptables
            .iter_mut()
            .find(|tt| tt.id == id)
            .expect("current tuptable is registered");
        entry.table.with_mut_mcx(|mcx, data| {
            let tuple = exectuples::exec_copy_slot_heap_tuple(slot, slot_mcx, mcx)?;
            let (ptr, len, tid, oid) =
                (tuple.as_tuple().header_ptr(), tuple.t_len, tuple.t_self, tuple.t_tableOid);
            // C stores bare HeapTuple pointers freed wholesale with tuptabcxt;
            // forget drops the owning wrapper, the image stays in the arena.
            core::mem::forget(tuple);
            // SAFETY: image just copied into this arena, live until the
            // tuptable context drops.
            let tuple = unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) };
            data.vals.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<usize>()))?;
            data.vals.push(tuple);
            Ok(())
        })?;
        Ok(true)
    })
}

pub(crate) fn numvals_of(tuptables: &[TuptabEntry], id: u64) -> u64 {
    tuptables
        .iter()
        .find(|tt| tt.id == id)
        .map(|tt| tt.table.with(|d| d.vals.len() as u64))
        .unwrap_or(0)
}

fn locate(h: TuptabHandle) -> Option<(usize, usize)> {
    SPI_STACK.with(|s| {
        let stack = s.borrow();
        for (ci, conn) in stack.iter().enumerate().rev() {
            if let Some(ti) = conn.tuptables.iter().position(|tt| tt.id == h.0) {
                return Some((ci, ti));
            }
        }
        None
    })
}

/// Read access to a result table; panics on a stale handle (freed table).
pub fn tuptable_with<R>(h: TuptabHandle, f: impl for<'mcx> FnOnce(&TuptabData<'mcx>) -> R) -> R {
    let (ci, ti) = locate(h).unwrap_or_else(|| panic!("SPI tuptable {h:?} is not live"));
    SPI_STACK.with(|s| s.borrow()[ci].tuptables[ti].table.with(f))
}

pub fn SPI_freetuptable(h: TuptabHandle) -> PgResult<()> {
    let removed = SPI_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let Some(conn) = stack.last_mut() else { return None };
        let ti = conn.tuptables.iter().position(|tt| tt.id == h.0)?;
        if conn.tuptable == Some(h.0) {
            conn.tuptable = None;
        }
        Some(conn.tuptables.swap_remove(ti))
    });
    match removed {
        Some(entry) => {
            if crate::SPI_tuptable() == Some(h) {
                set_spi_tuptable(None);
            }
            drop(entry);
            Ok(())
        }
        None => ereport(WARNING)
            .errmsg(format!("attempt to delete invalid SPITupleTable {:#x}", h.0))
            .finish(loc("SPI_freetuptable")),
    }
}
