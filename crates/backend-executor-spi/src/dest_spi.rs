//! The `DestSPI` receiver (`spi.c`: `spi_dest_startup` / `spi_printtup`),
//! routed into the one `backend-tcop-dest` router exactly as printtup / copyto
//! register their vtables.
//!
//! # Owned model
//!
//! C's `spi_dest_startup` allocates a `SPITupleTable` in the procedure context
//! and `spi_printtup` appends `ExecCopySlotHeapTuple(slot)` to it. The eventual
//! consumer reads each column with `SPI_getvalue` (output-function rendering).
//!
//! For the consumer-facing value seams (`spi_execute_select`,
//! `spi_cursor_fetch`, which return owned `SpiResult` = column descriptors +
//! string-rendered rows), we collect the same information directly: at
//! `rStartup` we record the result column descriptors (`name` / `typeid` /
//! `is_dropped`) from the `TupleDesc`, and at each `receiveSlot` we deform the
//! slot and render every non-dropped column through its type's output function
//! (`getTypeOutputInfo` + `OidOutputFunctionCall` — the body of `SPI_getvalue`),
//! storing one `SpiRow` (`Vec<Option<String>>`, `None` for SQL NULL).
//!
//! This is the SPI tuple table specialized to the string-shaped `SpiResult` the
//! xml / cursor consumers want; a `HeapTuple`-retaining `SPITupleTable`
//! (needed by callers that read raw datums via `SPI_getbinval`) is the
//! `SpiPlanPtr`/raw-tuptable keystone tracked in [`crate::exec`].

use core::cell::RefCell;
use mcx::Mcx;
use types_core::Oid;
use types_dest::dest::CommandDest;
use types_error::PgResult;
use types_nodes::nodes::CmdType;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::tuptable::SlotData;
use types_tuple::heaptuple::TupleDescData;
use types_xml::{SpiColumn, SpiResult, SpiRow};

/// One DestSPI receiver's collected state: the result column descriptors (set
/// at `rStartup`) and the string-rendered rows (appended at `receiveSlot`).
struct SpiReceiverState {
    columns: Vec<SpiColumn>,
    rows: Vec<SpiRow>,
}

thread_local! {
    static RECEIVERS: RefCell<Vec<Option<SpiReceiverState>>> = const { RefCell::new(Vec::new()) };
}

/// Allocate a fresh DestSPI receiver slot, returning its 1-based registry index
/// (the router `state` token; 0 is the C NULL sentinel).
fn receiver_register() -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let st = SpiReceiverState {
            columns: Vec::new(),
            rows: Vec::new(),
        };
        if let Some(i) = reg.iter().position(Option::is_none) {
            reg[i] = Some(st);
            (i + 1) as u64
        } else {
            reg.push(Some(st));
            reg.len() as u64
        }
    })
}

fn with_receiver<R>(state: u64, f: impl FnOnce(&mut SpiReceiverState) -> R) -> R {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = reg
            .get_mut((state - 1) as usize)
            .and_then(Option::as_mut)
            .expect("backend-executor-spi: dispatch on an unregistered DestSPI receiver");
        f(slot)
    })
}

/// `CreateDestReceiver(DestSPI)` (spi.c `spi_printtupDR`): allocate a fresh
/// collecting receiver and register its vtable into the tcop-dest router,
/// returning the [`DestReceiverHandle`]. Reached through the
/// `create_spi_dest_receiver` seam the router's `CreateDestReceiver(DestSPI)`
/// arm calls, mirroring printtup's `printtup_create_dr` routing.
pub fn create_spi_dest_receiver() -> DestReceiverHandle {
    let state = receiver_register();
    backend_tcop_dest::register_dest_receiver(
        CommandDest::Spi,
        backend_tcop_dest::ReceiverVtable {
            rStartup: spi_dest_startup,
            receiveSlot: spi_printtup,
            rShutdown: spi_dest_shutdown,
        },
        state,
    )
}

/// `spi_dest_startup(self, operation, typeinfo)` (spi.c:2123): record the result
/// column descriptors. C creates the `SPITupleTable` + copies `typeinfo`; the
/// owned collecting receiver records the per-column `(name, typeid, dropped)`
/// the `SpiResult` descriptor exposes.
fn spi_dest_startup(
    _mcx: Mcx<'_>,
    state: u64,
    _operation: CmdType,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<()> {
    let mut columns: Vec<SpiColumn> = Vec::with_capacity(tupdesc.natts as usize);
    for i in 0..tupdesc.natts {
        let attr = tupdesc.attr(i as usize);
        columns.push(SpiColumn {
            name: String::from_utf8_lossy(attr.attname.name_str()).into_owned(),
            typeid: attr.atttypid,
            is_dropped: attr.attisdropped,
        });
    }
    with_receiver(state, |st| {
        st.columns = columns;
    });
    Ok(())
}

/// `spi_printtup(slot, self)` (spi.c:2171): deform the slot and render every
/// non-dropped column through its type's output function (the `SPI_getvalue`
/// body), appending one [`SpiRow`].
fn spi_printtup<'mcx>(mcx: Mcx<'mcx>, state: u64, slot: &mut SlotData<'mcx>) -> PgResult<bool> {
    // ExecCopySlotHeapTuple(slot) -> deform: get every attribute's (value, isnull).
    let cols = backend_executor_execTuples_seams::slot_getallattrs::call(mcx, slot)?;

    // Render each column to its text form (NULL -> None), skipping dropped
    // columns just as the descriptor does. Snapshot the per-column type OIDs +
    // dropped flags first (immutable borrow), then render outside it.
    let coltypes: Vec<(Oid, bool)> =
        with_receiver(state, |st| st.columns.iter().map(|c| (c.typeid, c.is_dropped)).collect());

    let mut row: SpiRow = Vec::with_capacity(cols.len());
    for (i, (value, isnull)) in cols.iter().enumerate() {
        let (typeid, dropped) = coltypes.get(i).copied().unwrap_or((0u32, false));
        if dropped {
            row.push(None);
            continue;
        }
        if *isnull {
            row.push(None);
            continue;
        }
        // getTypeOutputInfo(typoid, &foutoid, &typisvarlena);
        let (foutoid, _typisvarlena) =
            backend_utils_cache_lsyscache_seams::get_type_output_info::call(typeid)?;
        // OidOutputFunctionCall(foutoid, val) -> text image.
        let bytes =
            backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(mcx, foutoid, value)?;
        row.push(Some(String::from_utf8_lossy(&bytes).into_owned()));
    }

    with_receiver(state, |st| st.rows.push(row));
    Ok(true)
}

/// `rShutdown` — DestSPI has no shutdown work (C's `spi_printtupDR.rShutdown`
/// is `donothingCleanup`).
fn spi_dest_shutdown(_mcx: Mcx<'_>, _state: u64) -> PgResult<()> {
    Ok(())
}

/// Take the collected [`SpiResult`] for `receiver` and free the receiver slot.
/// Called by the SPI execute / cursor-fetch driver once `ExecutorRun` /
/// `PortalRunFetch` has finished pushing tuples (the equivalent of reading
/// `SPI_tuptable` and `SPI_freetuptable`).
pub fn take_spi_result(receiver: DestReceiverHandle) -> SpiResult {
    let state = backend_tcop_dest::dest_receiver_state_token(receiver);
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        let slot = (state - 1) as usize;
        let taken = reg
            .get_mut(slot)
            .and_then(Option::take)
            .expect("backend-executor-spi: take_spi_result on an unregistered DestSPI receiver");
        SpiResult {
            columns: taken.columns,
            rows: taken.rows,
        }
    })
}

/// Read just the result column descriptors collected at `rStartup` (for
/// `spi_query_tupdesc` / `spi_cursor_tupdesc`, which need the descriptor even
/// when no rows were fetched), without consuming the receiver.
pub fn spi_result_columns(receiver: DestReceiverHandle) -> Vec<SpiColumn> {
    let state = backend_tcop_dest::dest_receiver_state_token(receiver);
    with_receiver(state, |st| st.columns.clone())
}
