//! pgvector 0.8.5 (github.com/pgvector/pgvector @ 159b79a) — the vector type,
//! distance/arithmetic functions, and aggregates. v1 scope: vector type + hnsw
//! (halfvec/sparsevec/bit opclasses and ivfflat are unported; the shipped
//! extension script is trimmed accordingly).

pub mod funcs;
pub mod half;
pub mod halfutils;
pub mod halfvec_funcs;
pub mod vec;

use types_fmgr::PGFunction;

const LIBRARY: &str = "vector";

fn lookup(function: &str) -> Option<PGFunction> {
    use funcs::*;
    use halfvec_funcs::*;
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
        "halfvec_in" => fc_halfvec_in,
        "halfvec_out" => fc_halfvec_out,
        "halfvec_typmod_in" => fc_halfvec_typmod_in,
        "halfvec_recv" => fc_halfvec_recv,
        "halfvec_send" => fc_halfvec_send,
        "halfvec" => fc_halfvec,
        "array_to_halfvec" => fc_array_to_halfvec,
        "halfvec_to_float4" => fc_halfvec_to_float4,
        "vector_to_halfvec" => fc_vector_to_halfvec,
        "halfvec_to_vector" => fc_halfvec_to_vector,
        "halfvec_l2_distance" => fc_halfvec_l2_distance,
        "halfvec_l2_squared_distance" => fc_halfvec_l2_squared_distance,
        "halfvec_inner_product" => fc_halfvec_inner_product,
        "halfvec_negative_inner_product" => fc_halfvec_negative_inner_product,
        "halfvec_cosine_distance" => fc_halfvec_cosine_distance,
        "halfvec_spherical_distance" => fc_halfvec_spherical_distance,
        "halfvec_l1_distance" => fc_halfvec_l1_distance,
        "halfvec_vector_dims" => fc_halfvec_vector_dims,
        "halfvec_l2_norm" => fc_halfvec_l2_norm,
        "halfvec_l2_normalize" => fc_halfvec_l2_normalize,
        "halfvec_add" => fc_halfvec_add,
        "halfvec_sub" => fc_halfvec_sub,
        "halfvec_mul" => fc_halfvec_mul,
        "halfvec_concat" => fc_halfvec_concat,
        "halfvec_binary_quantize" => fc_halfvec_binary_quantize,
        "halfvec_subvector" => fc_halfvec_subvector,
        "halfvec_lt" => fc_halfvec_lt,
        "halfvec_le" => fc_halfvec_le,
        "halfvec_eq" => fc_halfvec_eq,
        "halfvec_ne" => fc_halfvec_ne,
        "halfvec_ge" => fc_halfvec_ge,
        "halfvec_gt" => fc_halfvec_gt,
        "halfvec_cmp" => fc_halfvec_cmp,
        "halfvec_accum" => fc_halfvec_accum,
        "halfvec_avg" => fc_halfvec_avg,
        "hnsw_halfvec_support" => fc_hnsw_halfvec_support,
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
