pub use crate::include::access::detoast::*;

// :HACK: External/compressed datum fetch is not wired into heap scans yet.
// The PG-style module boundary exists now so later TOAST work can attach
// fetch/decompress behavior here instead of spreading it through exec_tuples.
