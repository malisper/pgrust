//! `backend-access-brin-tuple` — methods for tuples in BRIN indexes
//! (`src/backend/access/brin/brin_tuple.c`, PostgreSQL 18.3).
//!
//! Code outside this crate deals only in [`types_brin::BrinMemTuple`]s; the
//! on-disk byte image ([`BrinTupleImage`]) is produced and consumed here.
//!
//! A BRIN tuple is similar to a heap tuple, with a few key differences. The
//! tuple header is much simpler — only a `bt_blkno` and a small `bt_info` flag
//! byte. For each indexed column there are two null bits, `allnulls` (all values
//! in the page range null) and `hasnulls` (some null). When `allnulls` is set,
//! the data area holds no values for that column; when `hasnulls` is set, it
//! does. The null bitmask is a *double-width* bitmap: the first half is the
//! `allnulls` bits, the second the `hasnulls` bits. This module reverses the
//! sense of the null bits relative to `att_isnull` (1 == null).
//!
//! The bit-twiddling of the null bitmaps, the `bt_info` layout, the length
//! accounting, and the alignment field-walk are all in-crate and identical to
//! C. The genuinely-external operations go through their owners' seam crates:
//! the opclass `bv_serialize` callback (`backend-access-brin-entry-seams`),
//! `detoast_external_attr` (`backend-access-common-detoast-seams`),
//! `toast_compress_datum` (`backend-access-common-toast-internals-seams`),
//! `datumCopy` (`backend-utils-adt-scalar-seams`). The on-disk data area is
//! filled / sized by `backend-access-common-heaptuple`'s real
//! `heap_fill_tuple` / `heap_compute_data_size` (direct dependency).

#![no_std]
#![allow(non_snake_case)]
#![forbid(unsafe_code)]

extern crate alloc;

pub mod internal;
pub mod tuple;

pub use internal::{
    att_isnull, bitmaplen, brin_tuple_data_offset, brin_tuple_get_blkno, brin_tuple_get_info,
    brin_tuple_has_nulls, brin_tuple_is_empty_range, brin_tuple_is_placeholder,
    brtuple_disk_tupdesc, maxalign, varatt_is_extended, varatt_is_external, varsize, varsize_any,
    BrinTupleImage, HIGHBIT, MAXIMUM_ALIGNOF,
};
pub use tuple::{
    brin_copy_tuple, brin_deform_tuple, brin_form_placeholder_tuple, brin_form_tuple,
    brin_free_tuple, brin_memtuple_initialize, brin_new_memtuple, brin_tuples_equal,
    INVALID_COMPRESSION_METHOD, TOAST_INDEX_TARGET,
};

#[cfg(test)]
mod tests;
