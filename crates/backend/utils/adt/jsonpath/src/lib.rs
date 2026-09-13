//! jsonpath type I/O: scanner + grammar (parse tree), the flatten/reader/
//! printer over the on-disk JsonPath varlena, and the fmgr in/out/recv/send
//! surface. The executor lives in adt_jsonpath_exec.

pub mod builtins;
pub mod gram;
pub mod mutability;
pub mod path;
pub mod scan;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod vectors;

pub fn init_seams() {}

// CHECK_FOR_INTERRUPTS(): the InterruptPending pre-check keeps seamless
// contexts (unit tests) off the tcop seam.
pub(crate) fn check_for_interrupts() -> types_error::PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}
