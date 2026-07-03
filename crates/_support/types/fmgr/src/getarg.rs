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
    /// Safety: `p` is a live inline varlena image readable for its full VARSIZE_ANY, unwritten for `'a`.
    #[inline]
    pub unsafe fn from_ptr(p: *const u8) -> PackedVarlena<'a> {
        // SAFETY: caller contract — header readable.
        unsafe {
            if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p))
            {
                panic!("fmgr: external/compressed varlena where the caller guaranteed an inline image");
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
    pub fn image(self) -> &'a [u8] {
        // SAFETY: from_ptr contract.
        unsafe { core::slice::from_raw_parts(self.ptr, self.size()) }
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

    /// C's detoast_attr short-header arm: payload copy at palloc alignment
    /// (8), reclaimed by the arming context's reset (numeric digits need it).
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

// Detoast copies leak into the arena (C pallocs in CurrentMemoryContext).
#[cold]
#[inline(never)]
unsafe fn detoast_arg<'m>(p: *const u8, mcx: Mcx<'m>) -> PgResult<PackedVarlena<'m>> {
    // SAFETY: caller contract — image readable for its full VARSIZE_ANY.
    let image = unsafe { core::slice::from_raw_parts(p, varatt::varsize_any(p)) };
    let flat = detoast_seams::detoast_attr::call(mcx, image)?;
    Ok(PackedVarlena {
        ptr: flat.leak().as_ptr(),
        _image: PhantomData,
    })
}

/// C `pg_detoast_datum_packed` over a bare Datum (executor step paths that
/// carry no fcinfo). Safety: `d` is a non-null live varlena datum.
pub unsafe fn datum_varlena_packed<'m>(d: ::datum::Datum, mcx: Mcx<'m>) -> PgResult<PackedVarlena<'m>> {
    // SAFETY: forwarded caller contract — header readable.
    unsafe {
        let p = d.as_usize() as *const u8;
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            return detoast_arg(p, mcx);
        }
        Ok(PackedVarlena { ptr: p, _image: PhantomData })
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

    /// C `pg_detoast_datum_packed`: external/compressed args detoast into the
    /// armed result mcx. Safety: arg `i` is a non-null live varlena.
    #[inline]
    pub unsafe fn arg_varlena_packed(&self, i: usize) -> PgResult<PackedVarlena<'_>> {
        // SAFETY: forwarded caller contract — header readable.
        unsafe {
            let p = self.arg_ptr(i);
            if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p))
            {
                return detoast_arg(p, self.result_mcx());
            }
            Ok(PackedVarlena {
                ptr: p,
                _image: PhantomData,
            })
        }
    }

    /// Raw image, any header form (C's raw datum for slice-fetch paths).
    /// Safety: arg `i` is a non-null varlena readable for its full VARSIZE_ANY.
    #[inline]
    pub unsafe fn arg_varlena_raw(&self, i: usize) -> &[u8] {
        // SAFETY: forwarded caller contract.
        unsafe {
            let p = self.arg_ptr(i);
            core::slice::from_raw_parts(p, varatt::varsize_any(p))
        }
    }

    /// Safety: arg `i` is a non-null `cstring` (`typlen == -2`): live, NUL-terminated.
    #[inline]
    pub unsafe fn arg_cstring(&self, i: usize) -> &CStr {
        debug_assert!(
            self.arg(i).as_usize() != 0,
            "arg_cstring({i}): NULL cstring pointer (non-strict input convention: NULL rides the pointer, not isnull)"
        );
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
