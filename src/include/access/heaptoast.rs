use crate::backend::storage::page::bufpage::{ITEM_ID_SIZE, MAXALIGN, SIZE_OF_PAGE_HEADER_DATA};
use crate::backend::storage::smgr::BLCKSZ;
use crate::include::access::htup::SIZEOF_HEAP_TUPLE_HEADER;
use crate::include::varatt::VARHDRSZ;

pub const TOAST_TUPLES_PER_PAGE: usize = 4;
pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
pub const EXTERN_TUPLES_PER_PAGE: usize = 4;

pub const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> usize {
    let item_space = SIZE_OF_PAGE_HEADER_DATA + tuples_per_page * ITEM_ID_SIZE;
    let maxaligned = (item_space + (MAXALIGN - 1)) & !(MAXALIGN - 1);
    let available = BLCKSZ - maxaligned;
    available / tuples_per_page
}

pub const TOAST_TUPLE_THRESHOLD: usize = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE);
pub const TOAST_TUPLE_TARGET: usize = TOAST_TUPLE_THRESHOLD;
pub const TOAST_TUPLE_TARGET_MAIN: usize = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE_MAIN);

pub const EXTERN_TUPLE_MAX_SIZE: usize = maximum_bytes_per_tuple(EXTERN_TUPLES_PER_PAGE);
pub const TOAST_MAX_CHUNK_SIZE: usize = EXTERN_TUPLE_MAX_SIZE
    - ((SIZEOF_HEAP_TUPLE_HEADER + (MAXALIGN - 1)) & !(MAXALIGN - 1))
    - std::mem::size_of::<u32>()
    - std::mem::size_of::<i32>()
    - VARHDRSZ;
