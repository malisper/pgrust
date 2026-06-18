//! SPI cursor operations over the Portal machinery (`spi.c`:
//! `SPI_cursor_find` / `SPI_cursor_fetch` / `_SPI_cursor_operation`),
//! specialized to the read-forward consumer-facing fetch the xml `cursor_to_xml`
//! / `cursor_to_xmlschema` family uses.
//!
//! The cursor portal is opened externally (a SQL `DECLARE CURSOR`, found by
//! name in the portal hash table); these entries `SPI_connect()`, look the
//! portal up, run a forward fetch through `PortalRunFetch` into a DestSPI
//! receiver, collect the rendered rows / descriptor, and `SPI_finish()`.
//!
//! # Scope
//!
//! Only the forward fetch of an already-open SELECT cursor is wired — what the
//! xml consumers need. Opening a cursor from an `SpiPlanPtr`
//! (`SPI_cursor_open`) is part of the prepared-plan keystone tracked in
//! [`crate::exec`]; backward / scrollable fetch reaches the same portal
//! machinery but is not exercised by these consumers.

use backend_utils_error::ereport;
use types_error::{PgResult, ERROR};
use types_portal::FetchDirection;
use types_xml::{SpiColumn, SpiResult};

use crate::backbone::{SPI_connect, SPI_finish};
use crate::dest_spi::{create_spi_dest_receiver, spi_result_columns, take_spi_result};

use backend_tcop_pquery as pquery;
use backend_utils_mmgr_portalmem_seams as portalmem;

/// `spi_cursor_fetch(name, count)` — find the open cursor `name` and fetch up to
/// `count` rows forward into a DestSPI receiver, returning the rendered
/// [`SpiResult`].
///
/// C: `SPI_connect(); portal = SPI_cursor_find(name);
/// SPI_cursor_fetch(portal, true, count); …; SPI_finish();`
/// (`_SPI_cursor_operation` with `CreateDestReceiver(DestSPI)`).
pub fn spi_cursor_fetch(name: &str, count: i32) -> PgResult<SpiResult> {
    SPI_connect()?;
    let res = do_cursor_fetch(name, count);
    SPI_finish()?;
    res
}

fn do_cursor_fetch(name: &str, count: i32) -> PgResult<SpiResult> {
    // portal = GetPortalByName(name); if invalid -> elog(ERROR).
    let portal = portalmem::get_portal_by_name::call(name)?
        .ok_or_else(cursor_not_found)?;

    let receiver = create_spi_dest_receiver();

    // PortalRunFetch(portal, FETCH_FORWARD, count, DestSPI).
    pquery::portal_run_fetch(&portal, FetchDirection::FETCH_FORWARD, count as i64, receiver)?;

    Ok(take_spi_result(receiver))
}

/// `spi_cursor_tupdesc(name)` — the tuple descriptor of an open cursor
/// (`SPI_cursor_find(name)->tupDesc`). C reads `portal->tupDesc` directly; the
/// owned Portal carries `tupDesc`.
pub fn spi_cursor_tupdesc(name: &str) -> PgResult<Vec<SpiColumn>> {
    SPI_connect()?;
    let res = do_cursor_tupdesc(name);
    SPI_finish()?;
    res
}

fn do_cursor_tupdesc(name: &str) -> PgResult<Vec<SpiColumn>> {
    let portal = portalmem::get_portal_by_name::call(name)?
        .ok_or_else(cursor_not_found)?;

    // portal->tupDesc.
    let p = portal.borrow();
    let mut cols: Vec<SpiColumn> = Vec::new();
    if let Some(td) = p.tupDesc.as_ref() {
        for i in 0..td.natts {
            let attr = td.attr(i as usize);
            cols.push(SpiColumn {
                name: String::from_utf8_lossy(attr.attname.name_str()).into_owned(),
                typeid: attr.atttypid,
                is_dropped: attr.attisdropped,
            });
        }
    }
    Ok(cols)
}

/// Read the column descriptors of an open DestSPI receiver (re-export of the
/// dest-receiver helper for the descriptor path; used when the descriptor must
/// be read after a fetch rather than from the portal directly).
#[allow(dead_code)]
pub(crate) fn receiver_columns(
    receiver: types_nodes::parsestmt::DestReceiverHandle,
) -> Vec<SpiColumn> {
    spi_result_columns(receiver)
}

fn cursor_not_found() -> types_error::PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_UNDEFINED_CURSOR)
        .errmsg("cursor does not exist")
        .into_error()
}
