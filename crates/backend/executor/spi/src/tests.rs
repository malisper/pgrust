use super::*;

fn reset() {
    SPI_STACK.with(|s| {
        let drained: Vec<_> = s.borrow_mut().drain(..).collect();
        for conn in drained {
            crate::teardown_connection(conn);
        }
    });
    crate::sync_connected();
    set_spi_processed(0);
    set_spi_result(0);
    set_spi_tuptable(None);
}

#[test]
fn connect_finish_roundtrip() {
    reset();
    assert_eq!(SPI_connect().unwrap(), SPI_OK_CONNECT);
    assert_eq!(debug_stack_depth(), 1);
    assert_eq!(SPI_finish().unwrap(), SPI_OK_FINISH);
    assert_eq!(debug_stack_depth(), 0);
    assert_eq!(SPI_finish().unwrap(), SPI_ERROR_UNCONNECTED);
}

#[test]
fn nesting_preserves_outer_globals() {
    reset();
    SPI_connect().unwrap();
    set_spi_processed(7);
    set_spi_result(SPI_OK_SELECT);
    SPI_connect().unwrap();
    assert_eq!(SPI_processed(), 0);
    assert_eq!(SPI_result(), 0);
    SPI_finish().unwrap();
    assert_eq!(SPI_processed(), 7);
    assert_eq!(SPI_result(), SPI_OK_SELECT);
    SPI_finish().unwrap();
}

#[test]
fn at_eoxact_pops_leaked_levels() {
    reset();
    SPI_connect().unwrap();
    SPI_connect().unwrap();
    AtEOXact_SPI(false).unwrap();
    assert_eq!(debug_stack_depth(), 0);
    assert_eq!(debug_live_counts(), (0, 0));
}

#[test]
fn at_eosubxact_pops_only_matching_subid() {
    reset();
    SPI_connect().unwrap();
    let cur = xact::GetCurrentSubTransactionId();
    AtEOSubXact_SPI(false, cur + 1).unwrap();
    assert_eq!(debug_stack_depth(), 1);
    AtEOSubXact_SPI(false, cur).unwrap();
    assert_eq!(debug_stack_depth(), 0);
}

#[test]
fn empty_stack_seam_arms() {
    reset();
    init_seams();
    assert!(!spi_seams::spi_inside_nonatomic_context::call());
    spi_seams::at_eoxact_spi::call(true).unwrap();
    spi_seams::at_eoxact_spi::call(false).unwrap();
    spi_seams::at_eosubxact_spi::call(false, 2).unwrap();
}

#[test]
fn begin_end_call_exec_discipline() {
    reset();
    assert_eq!(_SPI_begin_call(true), SPI_ERROR_UNCONNECTED);
    SPI_connect().unwrap();
    assert_eq!(_SPI_begin_call(true), 0);
    let subid = with_current(|c| c.exec_subid).unwrap();
    assert_eq!(subid, xact::GetCurrentSubTransactionId());
    _SPI_end_call(true);
    let subid = with_current(|c| c.exec_subid).unwrap();
    assert_eq!(subid, types_core::InvalidSubTransactionId);
    SPI_finish().unwrap();
}
