// std (not no_std): the array_out cstring result path uses alloc; the crate
// builds varlena images into caller-owned mcx buffers (PgVec), never std Vec.
extern crate alloc;

pub mod build;
pub mod builtins;
pub mod construct;
pub mod element;
pub mod expanded;
pub mod foundation;
pub mod io;
pub mod ops;

#[cfg(test)]
mod tests;

pub use build::{
    accum_array_result, accum_array_result_any, accum_array_result_arr, init_array_result,
    init_array_result_any, init_array_result_arr, make_array_result, make_array_result_any,
    make_array_result_arr, make_md_array_result,
};
pub use construct::{
    array_contains_nulls, array_get_integer_typmods, construct_array, construct_empty_array,
    construct_md_array, deconstruct_array, deconstruct_array_builtin,
};
pub use foundation::{
    arr_data_offset, arr_dim, arr_elemtype, arr_hasnull, arr_lbound, arr_ndim, arr_size,
    read_dims_lbounds, MAXDIM,
};
pub use element::{array_get_element, array_get_slice, array_set_element, array_set_slice};
pub use expanded::{
    datum_get_expanded_array, datum_get_expanded_array_x, deconstruct_expanded_array,
    expand_array, ArrayMetaState, ExpandedArrayHeader, EA_MAGIC,
};
pub use io::{array_in, array_out, array_recv, array_send, ArrayIoMeta};
pub use ops::{array_cmp, array_eq_internal};

std::thread_local! {
    // C's Array_nulls backing (bool Array_nulls = true, arrayfuncs.c);
    // session-scoped, so TLS under the thread-per-backend model.
    static ARRAY_NULLS: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

pub(crate) fn array_nulls() -> bool {
    ARRAY_NULLS.with(|c| c.get())
}

fn set_array_nulls(v: bool) {
    ARRAY_NULLS.with(|c| c.set(v));
}

pub fn init_seams() {
    guc_tables::vars::Array_nulls.install(guc_tables::GucVarAccessors {
        get: array_nulls,
        set: set_array_nulls,
    });
}
