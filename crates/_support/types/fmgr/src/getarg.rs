use core::alloc::Layout;
use core::ffi::CStr;
use core::marker::PhantomData;

use ::mcx::{Allocator, Mcx};
use ::types_error::PgResult;
use ::types_tuple::varatt;

use crate::fcinfo::FunctionCallInfoBaseData;

pub const UUID_LEN: usize = 16;
pub const TID_LEN: usize = 6;
pub const NAME_LEN: usize = 64;
pub const INTERVAL_LEN: usize = 16;
pub const TIMETZ_LEN: usize = 12;
pub const ACLITEM_LEN: usize = 16;
pub const MACADDR_LEN: usize = 6;
pub const MACADDR8_LEN: usize = 8;

/// Borrowed packed varlena (C's `*_PP` view): 1B-short or 4B-uncompressed.
#[derive(Clone, Copy)]
pub struct PackedVarlena<'a> {
    ptr: *const u8,
    _image: PhantomData<&'a [u8]>,
}

impl<'a> PackedVarlena<'a> {
    /// Safety: `p` is a live varlena image readable for its full VARSIZE_ANY, unwritten for `'a`.
    #[inline]
    pub unsafe fn from_ptr(p: *const u8) -> PackedVarlena<'a> {
        // SAFETY: caller contract — header readable. PG_GETARG_*_PP would
        // detoast external/compressed: loud panic until the detoast unit lands.
        unsafe {
            if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p))
            {
                panic!("fmgr: external/compressed varlena argument requires the detoast unit (not ported)");
            }
        }
        PackedVarlena {
            ptr: p,
            _image: PhantomData,
        }
    }

    #[inline]
    pub fn as_ptr(self) -> *const u8 {
        self.ptr
    }

    #[inline]
    pub fn size(self) -> usize {
        // SAFETY: from_ptr contract — image readable through its header.
        unsafe {
            if varatt::varatt_is_1b(self.ptr) {
                varatt::varsize_1b(self.ptr)
            } else {
                varatt::varsize_4b(self.ptr)
            }
        }
    }

    #[inline]
    pub fn is_short(self) -> bool {
        // SAFETY: from_ptr contract — header readable.
        unsafe { varatt::varatt_is_1b(self.ptr) }
    }

    /// C's detoast_attr short-header arm (PG_DETOAST_DATUM): copy the payload
    /// into `mcx` at palloc alignment (8) so aligned payloads (numeric digits)
    /// stay readable; the arming context's reset reclaims it, like C's palloc.
    pub fn data_expanded<'m>(self, mcx: Mcx<'m>) -> PgResult<&'m [u8]> {
        let src = self.data();
        let layout = Layout::from_size_align(src.len(), 8).expect("data_expanded layout");
        let dst: core::ptr::NonNull<u8> = mcx
            .allocate(layout)
            .map_err(|_| mcx.oom(layout.size()))?
            .cast();
        // SAFETY: fresh `src.len()`-byte allocation; `src` is a live slice.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len());
            Ok(core::slice::from_raw_parts(dst.as_ptr(), src.len()))
        }
    }

    #[inline]
    pub fn data(self) -> &'a [u8] {
        // SAFETY: from_ptr contract — image readable for its full size.
        unsafe {
            if varatt::varatt_is_1b(self.ptr) {
                core::slice::from_raw_parts(
                    self.ptr.add(varatt::VARHDRSZ_SHORT),
                    varatt::varsize_1b(self.ptr) - varatt::VARHDRSZ_SHORT,
                )
            } else {
                core::slice::from_raw_parts(
                    self.ptr.add(varatt::VARHDRSZ),
                    varatt::varsize_4b(self.ptr) - varatt::VARHDRSZ,
                )
            }
        }
    }
}

// PG_GETARG_* analogs; like C, by-value reads carry no null check. By-ref
// reads are `unsafe`: the callee's catalog arg type is the proof.
impl FunctionCallInfoBaseData {
    #[inline]
    pub fn arg_bool(&self, i: usize) -> bool {
        self.arg(i).as_bool()
    }

    #[inline]
    pub fn arg_i16(&self, i: usize) -> i16 {
        self.arg(i).as_i16()
    }

    #[inline]
    pub fn arg_i32(&self, i: usize) -> i32 {
        self.arg(i).as_i32()
    }

    #[inline]
    pub fn arg_i64(&self, i: usize) -> i64 {
        self.arg(i).as_i64()
    }

    #[inline]
    pub fn arg_oid(&self, i: usize) -> ::types_core::Oid {
        self.arg(i).as_oid()
    }

    #[inline]
    pub fn arg_char(&self, i: usize) -> i8 {
        self.arg(i).as_char()
    }

    #[inline]
    pub fn arg_f32(&self, i: usize) -> f32 {
        self.arg(i).as_f32()
    }

    #[inline]
    pub fn arg_f64(&self, i: usize) -> f64 {
        self.arg(i).as_f64()
    }

    /// Safety: arg `i` is by-reference, non-null, and live for the call.
    #[inline]
    pub unsafe fn arg_ptr(&self, i: usize) -> *const u8 {
        self.arg(i).as_usize() as *const u8
    }

    /// Safety: arg `i` is a non-null varlena (`typlen == -1`), live for the call.
    #[inline]
    pub unsafe fn arg_varlena_packed(&self, i: usize) -> PackedVarlena<'_> {
        // SAFETY: forwarded caller contract.
        unsafe { PackedVarlena::from_ptr(self.arg_ptr(i)) }
    }

    /// Safety: arg `i` is a non-null `cstring` (`typlen == -2`): live, NUL-terminated.
    #[inline]
    pub unsafe fn arg_cstring(&self, i: usize) -> &CStr {
        unsafe { CStr::from_ptr(self.arg_ptr(i).cast()) }
    }

    /// C's `(StringInfo) PG_GETARG_POINTER(0)` (recv arg0).
    /// Safety: arg `i` is a live, unaliased `&mut StringInfo` pointer.
    #[inline]
    pub unsafe fn arg_stringinfo(&self, i: usize) -> &mut ::stringinfo::StringInfo<'_> {
        unsafe { &mut *(self.arg_ptr(i) as *mut ::stringinfo::StringInfo) }
    }

    /// Safety: arg `i` is non-null with catalog `typlen == n`, live for the call.
    #[inline]
    pub unsafe fn arg_fixed(&self, i: usize, n: usize) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.arg_ptr(i), n) }
    }

    /// Safety: as [`Self::arg_fixed`] with `n == UUID_LEN`.
    #[inline]
    pub unsafe fn arg_uuid(&self, i: usize) -> &[u8; UUID_LEN] {
        unsafe { &*self.arg_ptr(i).cast() }
    }

    /// Safety: as [`Self::arg_fixed`] with `n == TID_LEN`.
    #[inline]
    pub unsafe fn arg_tid(&self, i: usize) -> &[u8; TID_LEN] {
        unsafe { &*self.arg_ptr(i).cast() }
    }

    /// Safety: as [`Self::arg_fixed`] with `n == NAME_LEN`.
    #[inline]
    pub unsafe fn arg_name(&self, i: usize) -> &[u8; NAME_LEN] {
        unsafe { &*self.arg_ptr(i).cast() }
    }

    /// Safety: as [`Self::arg_fixed`] with `n == INTERVAL_LEN`.
    #[inline]
    pub unsafe fn arg_interval(&self, i: usize) -> &[u8; INTERVAL_LEN] {
        unsafe { &*self.arg_ptr(i).cast() }
    }
}
