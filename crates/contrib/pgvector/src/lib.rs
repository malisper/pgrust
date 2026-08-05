//! pgvector 0.8.5 (github.com/pgvector/pgvector @ 159b79a) — the vector type,
//! distance/arithmetic functions, and aggregates. v1 scope: vector type + hnsw
//! (halfvec/sparsevec/bit opclasses and ivfflat are unported; the shipped
//! extension script is trimmed accordingly).

pub mod funcs;
pub mod vec;

use types_fmgr::PGFunction;

const LIBRARY: &str = "vector";

fn lookup(function: &str) -> Option<PGFunction> {
    use funcs::*;
    Some(match function {
        "vector_in" => fc_vector_in,
        "vector_out" => fc_vector_out,
        "vector_typmod_in" => fc_vector_typmod_in,
        "vector_recv" => fc_vector_recv,
        "vector_send" => fc_vector_send,
        "vector" => fc_vector,
        "array_to_vector" => fc_array_to_vector,
        "vector_to_float4" => fc_vector_to_float4,
        "l2_distance" => fc_l2_distance,
        "vector_l2_squared_distance" => fc_vector_l2_squared_distance,
        "inner_product" => fc_inner_product,
        "vector_negative_inner_product" => fc_vector_negative_inner_product,
        "cosine_distance" => fc_cosine_distance,
        "vector_spherical_distance" => fc_vector_spherical_distance,
        "l1_distance" => fc_l1_distance,
        "vector_dims" => fc_vector_dims,
        "vector_norm" => fc_vector_norm,
        "l2_normalize" => fc_l2_normalize,
        "vector_add" => fc_vector_add,
        "vector_sub" => fc_vector_sub,
        "vector_mul" => fc_vector_mul,
        "vector_concat" => fc_vector_concat,
        "binary_quantize" => fc_binary_quantize,
        "subvector" => fc_subvector,
        "vector_lt" => fc_vector_lt,
        "vector_le" => fc_vector_le,
        "vector_eq" => fc_vector_eq,
        "vector_ne" => fc_vector_ne,
        "vector_ge" => fc_vector_ge,
        "vector_gt" => fc_vector_gt,
        "vector_cmp" => fc_vector_cmp,
        "vector_accum" => fc_vector_accum,
        "vector_combine" => fc_vector_combine,
        "vector_avg" => fc_vector_avg,
        "hnswhandler" => fc_hnswhandler,
        _ => return None,
    })
}

// CREATE FUNCTION validation target only; the closed AM set dispatches via
// IndexAmKind, never through fmgr.
fn fc_hnswhandler(
    _f: Option<&mut types_fmgr::FmgrInfo>,
    _fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum> {
    panic!("hnswhandler: the closed AM set dispatches via IndexAmKind, never through fmgr")
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
