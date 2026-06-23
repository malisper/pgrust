//! On-disk / Datum ABI structs and constants for the TOAST subsystem.
//!
//! These mirror PostgreSQL 18.3 `src/include/varatt.h`,
//! `src/include/access/toast_compression.h`, `src/include/access/detoast.h`,
//! `src/include/access/heaptoast.h`, and `src/include/utils/expandeddatum.h`.
//! The `#[repr(C)]` structs below carry compile-time size/offset assertions in
//! the test module so that on-disk and Datum layout cannot drift from the C ABI.

use core::ffi::{c_char, c_int};

use crate::{
    uint32, uint8, varlena, HeapTupleHeaderData, ItemIdData, MemoryContext, Oid, PageHeaderData,
    Size, BLCKSZ, VARHDRSZ,
};

/// `int32` mirrors PostgreSQL's signed 32-bit type (`c.h`).
pub type int32 = i32;

/// `offsetof(varattrib_1b_e, va_data)` from varatt.h: the `va_header` and
/// `va_tag` bytes preceding the type-specific TOAST-pointer payload.
pub const VARHDRSZ_EXTERNAL: usize = 2;

/// `enum vartag_external` from varatt.h. The peculiar value for `VARTAG_ONDISK`
/// is mandated by on-disk compatibility (it used to be the pointer datum's
/// length field).
pub type vartag_external = uint8;
pub const VARTAG_INDIRECT: vartag_external = 1;
pub const VARTAG_EXPANDED_RO: vartag_external = 2;
pub const VARTAG_EXPANDED_RW: vartag_external = 3;
pub const VARTAG_ONDISK: vartag_external = 18;

/// `struct varatt_external` — the data stored in an on-disk TOAST pointer
/// (`VARTAG_ONDISK`). varatt.h documents that this struct must contain no
/// padding, so all four fields are 4 bytes and the total is exactly 16 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct varatt_external {
    /// Original data size (includes header).
    pub va_rawsize: int32,
    /// External saved size (without header) and compression method.
    pub va_extinfo: uint32,
    /// Unique ID of value within TOAST table.
    pub va_valueid: Oid,
    /// RelID of TOAST table containing it.
    pub va_toastrelid: Oid,
}

// Compile-time ABI guards: `varatt_external` must be exactly 16 bytes with no
// padding, and its four 4-byte fields must sit at offsets 0/4/8/12 (varatt.h
// documents that this on-disk struct contains no padding). A layout regression
// fails the build, not just the test run.
const _: () = assert!(core::mem::size_of::<varatt_external>() == 16);
const _: () = assert!(core::mem::offset_of!(varatt_external, va_rawsize) == 0);
const _: () = assert!(core::mem::offset_of!(varatt_external, va_extinfo) == 4);
const _: () = assert!(core::mem::offset_of!(varatt_external, va_valueid) == 8);
const _: () = assert!(core::mem::offset_of!(varatt_external, va_toastrelid) == 12);

/// `struct varattrib_1b` from varatt.h: a short (1-byte header) inline varlena.
/// The `va_data` flexible array member is modeled as a zero-length array so the
/// struct's size is exactly the 1-byte header.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct varattrib_1b {
    pub va_header: uint8,
    pub va_data: [c_char; 0],
}

/// `struct varattrib_1b_e` from varatt.h: an "external" TOAST pointer datum. The
/// `va_header` byte (always `0x01`) and the `va_tag` byte precede the
/// type-specific TOAST-pointer payload at `va_data`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct varattrib_1b_e {
    pub va_header: uint8,
    pub va_tag: uint8,
    pub va_data: [c_char; 0],
}

// Compile-time ABI guards for the short/external varlena headers.
const _: () = assert!(core::mem::offset_of!(varattrib_1b, va_data) == 1);
const _: () = assert!(core::mem::offset_of!(varattrib_1b_e, va_data) == VARHDRSZ_EXTERNAL);

/// `struct varatt_indirect` — an in-memory indirect TOAST pointer
/// (`VARTAG_INDIRECT`); it just points at another `varlena`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct varatt_indirect {
    /// Pointer to in-memory varlena.
    pub pointer: *mut varlena,
}

/// `struct varatt_expanded` — an in-memory pointer to an expanded object
/// (`VARTAG_EXPANDED_RO` / `VARTAG_EXPANDED_RW`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct varatt_expanded {
    pub eohptr: *mut ExpandedObjectHeader,
}

/// `ExpandedObjectMethods` from expandeddatum.h: the flatten callbacks an
/// expanded object exposes for `detoast_external_attr`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExpandedObjectMethods {
    pub get_flat_size: EOM_get_flat_size_method,
    pub flatten_into: EOM_flatten_into_method,
}

pub type EOM_get_flat_size_method =
    Option<unsafe extern "C" fn(eohptr: *mut ExpandedObjectHeader) -> Size>;
pub type EOM_flatten_into_method = Option<
    unsafe extern "C" fn(
        eohptr: *mut ExpandedObjectHeader,
        result: *mut core::ffi::c_void,
        allocated_size: Size,
    ),
>;

/// `ExpandedObjectHeader` from expandeddatum.h. The two embedded TOAST-pointer
/// buffers are `EXPANDED_POINTER_SIZE` bytes each (`VARHDRSZ_EXTERNAL +
/// sizeof(varatt_expanded)`), so the working layout matches the C struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExpandedObjectHeader {
    pub vl_len_: int32,
    pub eoh_methods: *const ExpandedObjectMethods,
    pub eoh_context: MemoryContext,
    pub eoh_rw_ptr: [c_char; EXPANDED_POINTER_SIZE],
    pub eoh_ro_ptr: [c_char; EXPANDED_POINTER_SIZE],
}

/// `#define EOH_HEADER_MAGIC (-1)` from expandeddatum.h: the sentinel stored in
/// `ExpandedObjectHeader.vl_len_` so an expanded object can be told apart from a
/// 4-byte-header flat varlena.
pub const EOH_HEADER_MAGIC: int32 = -1;

/// `EXPANDED_POINTER_SIZE` from expandeddatum.h.
pub const EXPANDED_POINTER_SIZE: usize =
    VARHDRSZ_EXTERNAL + core::mem::size_of::<varatt_expanded>();

/// `TOAST_POINTER_SIZE` from detoast.h: total size of an on-disk TOAST pointer
/// datum (`VARHDRSZ_EXTERNAL + sizeof(varatt_external)`).
pub const TOAST_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + core::mem::size_of::<varatt_external>();

// Compile-time ABI guards for the in-memory indirect/expanded payloads and the
// derived pointer sizes.
const _: () =
    assert!(core::mem::size_of::<varatt_indirect>() == core::mem::size_of::<*mut varlena>());
const _: () = assert!(
    core::mem::size_of::<varatt_expanded>() == core::mem::size_of::<*mut ExpandedObjectHeader>()
);
const _: () = assert!(TOAST_POINTER_SIZE == VARHDRSZ_EXTERNAL + 16);
const _: () = assert!(TOAST_POINTER_SIZE == 18);
const _: () =
    assert!(EXPANDED_POINTER_SIZE == VARHDRSZ_EXTERNAL + core::mem::size_of::<*mut varlena>());

/// `enum ToastCompressionId` from toast_compression.h. Used to identify the
/// compression method embedded in a compressed datum's header.
pub type ToastCompressionId = c_int;
pub const TOAST_PGLZ_COMPRESSION_ID: ToastCompressionId = 0;
pub const TOAST_LZ4_COMPRESSION_ID: ToastCompressionId = 1;
pub const TOAST_INVALID_COMPRESSION_ID: ToastCompressionId = 2;

/// `#define TOAST_PGLZ_COMPRESSION 'p'` / `TOAST_LZ4_COMPRESSION 'l'` — the
/// per-attribute `attcompression` characters from toast_compression.h.
pub const TOAST_PGLZ_COMPRESSION: c_char = b'p' as c_char;
pub const TOAST_LZ4_COMPRESSION: c_char = b'l' as c_char;

/// `MAXALIGN` of `size` against `MAXIMUM_ALIGNOF` (8 on supported platforms).
const MAXIMUM_ALIGNOF: usize = 8;
const fn maxalign(size: usize) -> usize {
    (size + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN_DOWN(LEN)` from c.h: round `len` down to `MAXIMUM_ALIGNOF`.
const fn maxalign_down(size: usize) -> usize {
    size & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeofHeapTupleHeader` == `offsetof(HeapTupleHeaderData, t_bits)`.
const SIZEOF_HEAP_TUPLE_HEADER: usize = core::mem::offset_of!(HeapTupleHeaderData, t_bits);

/// `SizeOfPageHeaderData` == `offsetof(PageHeaderData, pd_linp)`.
const SIZEOF_PAGE_HEADER_DATA: usize = core::mem::offset_of!(PageHeaderData, pd_linp);

/// `MaxHeapTupleSize` from htup_details.h.
const MAX_HEAP_TUPLE_SIZE: usize =
    BLCKSZ - maxalign(SIZEOF_PAGE_HEADER_DATA + core::mem::size_of::<ItemIdData>());

/// `MaximumBytesPerTuple(tuplesPerPage)` from heaptoast.h.
const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> usize {
    maxalign_down(
        (BLCKSZ
            - maxalign(
                SIZEOF_PAGE_HEADER_DATA + tuples_per_page * core::mem::size_of::<ItemIdData>(),
            ))
            / tuples_per_page,
    )
}

/// `EXTERN_TUPLES_PER_PAGE` from heaptoast.h.
pub const EXTERN_TUPLES_PER_PAGE: usize = 4;

/// `EXTERN_TUPLE_MAX_SIZE` from heaptoast.h.
const EXTERN_TUPLE_MAX_SIZE: usize = maximum_bytes_per_tuple(EXTERN_TUPLES_PER_PAGE);

/// `TOAST_MAX_CHUNK_SIZE` from heaptoast.h: maximum data bytes per TOAST chunk.
/// On the default 8 KiB page this evaluates to 1996.
pub const TOAST_MAX_CHUNK_SIZE: int32 = (EXTERN_TUPLE_MAX_SIZE
    - maxalign(SIZEOF_HEAP_TUPLE_HEADER)
    - core::mem::size_of::<Oid>()
    - core::mem::size_of::<int32>()
    - VARHDRSZ) as int32;

/// `MaxHeapTupleSize` re-exported for callers that compute toast thresholds.
pub const MaxHeapTupleSize: usize = MAX_HEAP_TUPLE_SIZE;

/// `VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)` from varatt.h: the external data
/// payload size, with the top two compression bits masked out.
#[inline]
pub const fn VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer: varatt_external) -> int32 {
    (toast_pointer.va_extinfo & VARLENA_EXTSIZE_MASK) as int32
}

/// `VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)` from varatt.h: the external
/// data length (`va_extinfo` payload) is strictly less than the raw datum size
/// minus `VARHDRSZ`. varatt.h uses `<` (compression is only ever used when it
/// saves space, so we expect either equality or less-than); match it exactly
/// rather than `!=`.
#[inline]
pub const fn VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer: varatt_external) -> bool {
    VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer) < (toast_pointer.va_rawsize - VARHDRSZ as int32)
}

/// `VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer)` from varatt.h.
#[inline]
pub const fn VARATT_EXTERNAL_GET_COMPRESS_METHOD(
    toast_pointer: varatt_external,
) -> ToastCompressionId {
    (toast_pointer.va_extinfo >> VARLENA_EXTSIZE_BITS) as ToastCompressionId
}

/// `VARLENA_EXTSIZE_BITS` / `VARLENA_EXTSIZE_MASK` from varatt.h.
pub const VARLENA_EXTSIZE_BITS: u32 = 30;
pub const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/// `VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm)`.
#[inline]
pub fn VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(
    toast_pointer: &mut varatt_external,
    len: int32,
    cm: ToastCompressionId,
) {
    debug_assert!(cm == TOAST_PGLZ_COMPRESSION_ID || cm == TOAST_LZ4_COMPRESSION_ID);
    toast_pointer.va_extinfo = (len as u32) | ((cm as u32) << VARLENA_EXTSIZE_BITS);
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn toast_abi_layout_matches_c() {
        // varatt_external: four 4-byte fields, no padding.
        assert_eq!(size_of::<varatt_external>(), 16);
        assert_eq!(offset_of!(varatt_external, va_rawsize), 0);
        assert_eq!(offset_of!(varatt_external, va_extinfo), 4);
        assert_eq!(offset_of!(varatt_external, va_valueid), 8);
        assert_eq!(offset_of!(varatt_external, va_toastrelid), 12);

        // Pointer-sized indirect/expanded payloads.
        assert_eq!(size_of::<varatt_indirect>(), size_of::<*mut varlena>());
        assert_eq!(
            size_of::<varatt_expanded>(),
            size_of::<*mut ExpandedObjectHeader>()
        );

        // Derived sizes.
        assert_eq!(TOAST_POINTER_SIZE, VARHDRSZ_EXTERNAL + 16);
        assert_eq!(TOAST_POINTER_SIZE, 18);
        assert_eq!(
            EXPANDED_POINTER_SIZE,
            VARHDRSZ_EXTERNAL + size_of::<*mut varlena>()
        );
    }

    #[test]
    fn toast_compression_id_values() {
        assert_eq!(TOAST_PGLZ_COMPRESSION_ID, 0);
        assert_eq!(TOAST_LZ4_COMPRESSION_ID, 1);
        assert_eq!(TOAST_INVALID_COMPRESSION_ID, 2);
        assert_eq!(TOAST_PGLZ_COMPRESSION, b'p' as c_char);
        assert_eq!(TOAST_LZ4_COMPRESSION, b'l' as c_char);
    }

    #[test]
    fn toast_max_chunk_size_is_1996_on_default_page() {
        // BLCKSZ == 8192 default build: TOAST_MAX_CHUNK_SIZE == 1996.
        assert_eq!(BLCKSZ, 8192);
        assert_eq!(TOAST_MAX_CHUNK_SIZE, 1996);
    }

    #[test]
    fn expanded_object_header_alignment() {
        assert_eq!(align_of::<ExpandedObjectHeader>(), align_of::<*mut ()>());
    }
}
