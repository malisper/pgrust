use ::fmgr::PGFunction;
use ::types_core::Oid;

// Append-only, strictly OID-ascending; every OID must exist in CANONICAL
// (compile-asserted). An OID absent here resolves to a loud not-ported panic.
pub const PORTED: &[(Oid, PGFunction)] = &[];
