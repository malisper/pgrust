//! jsonpath type I/O: scanner + grammar (parse tree), the flatten/reader/
//! printer over the on-disk JsonPath varlena, and the fmgr in/out/recv/send
//! surface. The executor (jsonpath_exec.c: @@/@?/jsonb_path_* ) stays loud
//! via unported OIDs.

pub mod builtins;
pub mod gram;
pub mod path;
pub mod scan;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod vectors;

pub fn init_seams() {}
