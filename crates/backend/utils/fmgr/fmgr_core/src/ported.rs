use ::fmgr::{FmgrBuiltin, PGFunction};
use ::types_core::Oid;

const TABLES: &[&[FmgrBuiltin]] = &[
    ::adt_bool::builtins::BOOL_BUILTINS,
    ::arrayfuncs::builtins::ARRAYFUNCS_BUILTINS,
    ::array_userfuncs::builtins::ARRAY_USERFUNCS_BUILTINS,
    ::adt_cash::builtins::CASH_BUILTINS,
    ::adt_char::builtins::CHAR_BUILTINS,
    ::adt_domains::DOMAINS_BUILTINS,
    ::adt_date::builtins::DATE_BUILTINS,
    ::adt_encode::builtins::ENCODE_BUILTINS,
    ::adt_enum::builtins::ENUM_BUILTINS,
    ::adt_float::builtins::FLOAT_BUILTINS,
    ::adt_formatting::fmgr_builtins::FORMATTING_BUILTINS,
    ::adt_int::builtins::INT_BUILTINS,
    ::adt_int8::builtins::INT8_BUILTINS,
    ::adt_json::builtins::JSON_BUILTINS,
    ::adt_jsonb::builtins::JSONB_BUILTINS,
    ::adt_jsonpath::builtins::JSONPATH_BUILTINS,
    ::adt_jsonpath_exec::builtins::JSONPATH_EXEC_BUILTINS,
    ::adt_like::builtins::LIKE_BUILTINS,
    ::adt_regexp::builtins::REGEXP_BUILTINS,
    ::hbafuncs::HBAFUNCS_BUILTINS,
    ::adt_mac::builtins::MAC_BUILTINS,
    ::adt_mac8::builtins::MAC8_BUILTINS,
    ::adt_pg_lsn::builtins::PG_LSN_BUILTINS,
    ::adt_xml::builtins::XML_BUILTINS,
    ::xid8funcs::builtins::XID8FUNCS_BUILTINS,
    ::dbsize::builtins::DBSIZE_BUILTINS,
    ::adt_uuid::builtins::UUID_BUILTINS,
    ::adt_tsvector_core::builtins::TSVECTOR_BUILTINS,
    ::adt_tsquery_core::builtins::TSQUERY_BUILTINS,
    ::adt_tsrank::builtins::TSRANK_BUILTINS,
    ::wparser_def::builtins::WPARSER_BUILTINS,
    ::ts_parse::builtins::TSPARSE_BUILTINS,
    ::tsearch_dict::builtins::DICT_BUILTINS,
    ::tsearch_spell::builtins::SPELL_BUILTINS,
    ::to_tsany::builtins::TO_TSANY_BUILTINS,
    ::dict_snowball::builtins::SNOWBALL_BUILTINS,
    ::ts_cache::builtins::TS_CACHE_BUILTINS,
    ::adt_varbit::VARBIT_BUILTINS,
    ::adt_network::builtins::NETWORK_BUILTINS,
    ::adt_rangetypes::builtins::RANGETYPES_BUILTINS,
    ::adt_multirangetypes::builtins::MULTIRANGETYPES_BUILTINS,
    ::adt_regproc::builtins::REGPROC_BUILTINS,
    ::adt_numeric::builtins::NUMERIC_BUILTINS,
    ::adt_oracle_compat::builtins::ORACLE_COMPAT_BUILTINS,
    ::adt_pseudotypes::builtins::PSEUDOTYPES_BUILTINS,
    ::adt_quote::builtins::QUOTE_BUILTINS,
    ::adt_varchar::builtins::VARCHAR_BUILTINS,
    ::pgstatfuncs::PGSTATFUNCS_BUILTINS,
    ::portalmem::PORTALMEM_BUILTINS,
    ::adt_scalar::builtins::SCALAR_BUILTINS,
    ::adt_timestamp::builtins::TIMESTAMP_BUILTINS,
    ::sequence_seams::builtins::SEQUENCE_BUILTINS,
    ::extension_seams::builtins::EXTENSION_BUILTINS,
    ::foreigncmds_seams::builtins::FOREIGN_BUILTINS,
    ::name::builtins::NAME_BUILTINS,
    ::nbt_compare::builtins::NBT_BUILTINS,
    ::varlena::builtins::VARLENA_BUILTINS,
    ::adt_windowfuncs::WINDOWFUNCS_BUILTINS,
    ::commands_async::builtins::ASYNC_BUILTINS,
    ::cryptohashfuncs::CRYPTOHASH_BUILTINS,
    ::adt_ascii::ASCII_BUILTINS,
    ::adt_amutils::AMUTILS_BUILTINS,
    ::adt_mcxtfuncs::MCXTFUNCS_BUILTINS,
    ::waitfuncs::WAITFUNCS_BUILTINS,
    ::signalfuncs::SIGNALFUNCS_BUILTINS,
    ::lockfuncs::LOCKFUNCS_BUILTINS,
    ::pseudorandomfuncs::builtins::PSEUDORANDOM_BUILTINS,
    ::trigfuncs::TRIGFUNCS_BUILTINS,
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
