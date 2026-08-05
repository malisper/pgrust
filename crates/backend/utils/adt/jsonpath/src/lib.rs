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
