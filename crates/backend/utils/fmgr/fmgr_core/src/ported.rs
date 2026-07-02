use ::adt_int::builtins::INT_BUILTINS;
use ::fmgr::PGFunction;
use ::types_core::Oid;

const N: usize = INT_BUILTINS.len();

// CANONICAL wants strict OID order; the adt tables keep pg_proc.dat grouping.
const fn oid_sorted() -> [(Oid, PGFunction); N] {
    let mut t: [(Oid, PGFunction); N] = [(0, INT_BUILTINS[0].func); N];
    let mut i = 0;
    while i < N {
        t[i] = (INT_BUILTINS[i].foid, INT_BUILTINS[i].func);
        i += 1;
    }
    let mut i = 1;
    while i < N {
        let mut j = i;
        while j > 0 && t[j - 1].0 > t[j].0 {
            let tmp = t[j - 1];
            t[j - 1] = t[j];
            t[j] = tmp;
            j -= 1;
        }
        i += 1;
    }
    t
}

const SORTED: [(Oid, PGFunction); N] = oid_sorted();

// Strictly OID-ascending; every OID must exist in CANONICAL (compile-asserted).
// An OID absent here resolves to a loud not-ported panic.
pub const PORTED: &[(Oid, PGFunction)] = &SORTED;
