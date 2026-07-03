use ::fmgr::{FmgrBuiltin, PGFunction};
use ::types_core::Oid;

const TABLES: &[&[FmgrBuiltin]] = &[
    ::adt_bool::builtins::BOOL_BUILTINS,
    ::arrayfuncs::builtins::ARRAYFUNCS_BUILTINS,
    ::adt_cash::builtins::CASH_BUILTINS,
    ::adt_char::builtins::CHAR_BUILTINS,
    ::adt_date::builtins::DATE_BUILTINS,
    ::adt_encode::builtins::ENCODE_BUILTINS,
    ::adt_float::builtins::FLOAT_BUILTINS,
    ::adt_formatting::fmgr_builtins::FORMATTING_BUILTINS,
    ::adt_int::builtins::INT_BUILTINS,
    ::adt_int8::builtins::INT8_BUILTINS,
    ::adt_json::builtins::JSON_BUILTINS,
    ::adt_jsonb::builtins::JSONB_BUILTINS,
    ::adt_like::builtins::LIKE_BUILTINS,
    ::adt_mac::builtins::MAC_BUILTINS,
    ::adt_mac8::builtins::MAC8_BUILTINS,
    ::adt_numeric::builtins::NUMERIC_BUILTINS,
    ::adt_oracle_compat::builtins::ORACLE_COMPAT_BUILTINS,
    ::adt_pseudotypes::builtins::PSEUDOTYPES_BUILTINS,
    ::adt_scalar::builtins::SCALAR_BUILTINS,
    ::adt_timestamp::builtins::TIMESTAMP_BUILTINS,
    ::name::builtins::NAME_BUILTINS,
    ::nbt_compare::builtins::NBT_BUILTINS,
    ::varlena::builtins::VARLENA_BUILTINS,
    ::adt_windowfuncs::WINDOWFUNCS_BUILTINS,
    ::commands_async::builtins::ASYNC_BUILTINS,
];

const fn total() -> usize {
    let mut n = 0;
    let mut i = 0;
    while i < TABLES.len() {
        n += TABLES[i].len();
        i += 1;
    }
    n
}

const N: usize = total();

// CANONICAL wants strict OID order; the adt tables keep pg_proc.dat grouping.
const fn oid_sorted() -> [(Oid, PGFunction); N] {
    let mut t: [(Oid, PGFunction); N] = [(0, TABLES[0][0].func); N];
    let mut n = 0;
    let mut ti = 0;
    while ti < TABLES.len() {
        let table = TABLES[ti];
        let mut i = 0;
        while i < table.len() {
            t[n] = (table[i].foid, table[i].func);
            n += 1;
            i += 1;
        }
        ti += 1;
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
