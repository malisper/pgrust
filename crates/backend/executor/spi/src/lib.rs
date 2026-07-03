#![allow(non_snake_case)]

use std::cell::Cell;

use types_core::SubTransactionId;
use types_error::PgResult;

thread_local! {
    // spi.c's _SPI_connected; SPI_connect (unported) is the only writer, so
    // the stack is provably empty (-1).
    static SPI_CONNECTED: Cell<i32> = const { Cell::new(-1) };
}

#[cold]
fn unported_stack() -> ! {
    panic!("SPI stack is nonempty but executor/spi.c is not ported (SPI_connect must not have run)");
}

pub fn SPI_inside_nonatomic_context() -> bool {
    if SPI_CONNECTED.with(|c| c.get()) < 0 {
        return false;
    }
    unported_stack();
}

pub fn AtEOXact_SPI(_is_commit: bool) -> PgResult<()> {
    if SPI_CONNECTED.with(|c| c.get()) >= 0 {
        unported_stack();
    }
    Ok(())
}

pub fn AtEOSubXact_SPI(_is_commit: bool, _my_subid: SubTransactionId) -> PgResult<()> {
    if SPI_CONNECTED.with(|c| c.get()) >= 0 {
        unported_stack();
    }
    Ok(())
}

pub fn init_seams() {
    spi_seams::spi_inside_nonatomic_context::set(SPI_inside_nonatomic_context);
    spi_seams::at_eoxact_spi::set(AtEOXact_SPI);
    spi_seams::at_eosubxact_spi::set(AtEOSubXact_SPI);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stack_arms() {
        init_seams();
        assert!(!spi_seams::spi_inside_nonatomic_context::call());
        spi_seams::at_eoxact_spi::call(true).unwrap();
        spi_seams::at_eoxact_spi::call(false).unwrap();
        spi_seams::at_eosubxact_spi::call(false, 2).unwrap();
    }
}
