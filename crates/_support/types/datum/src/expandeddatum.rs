use crate::datum::Datum;
#[cfg(not(target_family = "wasm"))]
use core::mem::offset_of;
use core::mem::size_of;
use mcx::MemoryContext;

pub const VARTAG_EXPANDED_RO: u8 = 2;
pub const VARTAG_EXPANDED_RW: u8 = 3;
pub const VARHDRSZ_EXTERNAL: usize = 2;
pub const EXPANDED_POINTER_SIZE: usize =
    VARHDRSZ_EXTERNAL + size_of::<*mut ExpandedObjectHeader>();
pub const EOH_HEADER_MAGIC: i32 = -1;

#[cfg(target_endian = "little")]
const HEADER_1B_E: u8 = 0x01;
#[cfg(target_endian = "big")]
const HEADER_1B_E: u8 = 0x80;

pub struct ExpandedObjectMethods {
    pub get_flat_size: unsafe fn(eohptr: *mut ExpandedObjectHeader) -> usize,
    pub flatten_into: unsafe fn(eohptr: *mut ExpandedObjectHeader, result: *mut u8, allocated_size: usize),
}

// Self-referential: both images embed this struct's own address, so a header
// never moves after eoh_init_header; every unsafe fn below requires its
// Datum/eohptr argument to reference a live initialized header/image.
#[repr(C)]
pub struct ExpandedObjectHeader {
    vl_len_: i32,
    eoh_methods: Option<&'static ExpandedObjectMethods>,
    eoh_context: *const MemoryContext,
    eoh_rw_ptr: [u8; EXPANDED_POINTER_SIZE],
    eoh_ro_ptr: [u8; EXPANDED_POINTER_SIZE],
}

// 64-bit layout pin. On wasm32 (ILP32 pointers) the two Rust pointer fields
// shrink and the layout differs; it stays internally consistent within the
// target, and the port's SIZEOF_DATUM==8 invariant is unaffected (datum.rs).
#[cfg(not(target_family = "wasm"))]
const _: () = {
    assert!(size_of::<ExpandedObjectHeader>() == 48);
    assert!(offset_of!(ExpandedObjectHeader, eoh_rw_ptr) == 24);
    assert!(offset_of!(ExpandedObjectHeader, eoh_ro_ptr) == 34);
};

impl ExpandedObjectHeader {
    pub const fn empty() -> Self {
        ExpandedObjectHeader {
            vl_len_: 0,
            eoh_methods: None,
            eoh_context: core::ptr::null(),
            eoh_rw_ptr: [0; EXPANDED_POINTER_SIZE],
            eoh_ro_ptr: [0; EXPANDED_POINTER_SIZE],
        }
    }

    pub fn context(&self) -> *const MemoryContext {
        self.eoh_context
    }
}

#[inline]
pub const fn vartag_is_expanded(tag: u8) -> bool {
    (tag & !1) == VARTAG_EXPANDED_RO
}

#[inline]
pub unsafe fn datum_is_external_expanded(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    *p == HEADER_1B_E && vartag_is_expanded(*p.add(1))
}

#[inline]
pub unsafe fn datum_is_external_expanded_rw(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    *p == HEADER_1B_E && *p.add(1) == VARTAG_EXPANDED_RW
}

pub unsafe fn datum_get_eohp(d: Datum) -> *mut ExpandedObjectHeader {
    debug_assert!(datum_is_external_expanded(d));
    let p = d.as_usize() as *const u8;
    // read_unaligned of pointer bytes keeps provenance (C memcpy's the same).
    let eohptr = core::ptr::read_unaligned(p.add(VARHDRSZ_EXTERNAL) as *const *mut ExpandedObjectHeader);
    debug_assert!((*eohptr).vl_len_ == EOH_HEADER_MAGIC);
    eohptr
}

/// SAFETY contract: `eohptr` is writable storage at the header's FINAL
/// address — both TOAST images embed it.
pub unsafe fn eoh_init_header(
    eohptr: *mut ExpandedObjectHeader,
    methods: &'static ExpandedObjectMethods,
    obj_context: *const MemoryContext,
) {
    (*eohptr).vl_len_ = EOH_HEADER_MAGIC;
    (*eohptr).eoh_methods = Some(methods);
    (*eohptr).eoh_context = obj_context;
    let rw = core::ptr::addr_of_mut!((*eohptr).eoh_rw_ptr) as *mut u8;
    let ro = core::ptr::addr_of_mut!((*eohptr).eoh_ro_ptr) as *mut u8;
    for (image, tag) in [(rw, VARTAG_EXPANDED_RW), (ro, VARTAG_EXPANDED_RO)] {
        *image = HEADER_1B_E;
        *image.add(1) = tag;
        core::ptr::write_unaligned(image.add(VARHDRSZ_EXTERNAL) as *mut *mut ExpandedObjectHeader, eohptr);
    }
}

#[inline]
pub unsafe fn eohp_get_rw_datum(eohptr: *const ExpandedObjectHeader) -> Datum {
    Datum::from_usize(core::ptr::addr_of!((*eohptr).eoh_rw_ptr) as usize)
}

#[inline]
pub unsafe fn eohp_get_ro_datum(eohptr: *const ExpandedObjectHeader) -> Datum {
    Datum::from_usize(core::ptr::addr_of!((*eohptr).eoh_ro_ptr) as usize)
}

#[inline]
pub unsafe fn datum_is_read_write_expanded_object(d: Datum, isnull: bool, typlen: i16) -> bool {
    if isnull || typlen != -1 {
        return false;
    }
    datum_is_external_expanded_rw(d)
}

pub unsafe fn make_expanded_object_read_only_internal(d: Datum) -> Datum {
    if !datum_is_external_expanded_rw(d) {
        return d;
    }
    eohp_get_ro_datum(datum_get_eohp(d))
}

#[inline]
pub unsafe fn make_expanded_object_read_only(d: Datum, isnull: bool, typlen: i16) -> Datum {
    if isnull || typlen != -1 {
        return d;
    }
    make_expanded_object_read_only_internal(d)
}

pub unsafe fn eoh_get_flat_size(eohptr: *mut ExpandedObjectHeader) -> usize {
    let methods = eoh_methods(eohptr);
    (methods.get_flat_size)(eohptr)
}

/// Flatteners write exactly `allocated_size` (a preceding [`eoh_get_flat_size`])
/// bytes of a plain 4B-header varlena into `result`.
pub unsafe fn eoh_flatten_into(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut u8,
    allocated_size: usize,
) {
    let methods = eoh_methods(eohptr);
    (methods.flatten_into)(eohptr, result, allocated_size)
}

unsafe fn eoh_methods(eohptr: *mut ExpandedObjectHeader) -> &'static ExpandedObjectMethods {
    match (*eohptr).eoh_methods {
        Some(m) => m,
        None => uninitialized_header(),
    }
}

#[cold]
#[inline(never)]
fn uninitialized_header() -> ! {
    panic!("expandeddatum: expanded-object header has no methods (eoh_init_header not run)")
}

pub fn transfer_expanded_object(_d: Datum) -> ! {
    panic!(
        "TransferExpandedObject: expanded-object context re-parenting unported \
         (mcx has no MemoryContextSetParent)"
    )
}

pub fn delete_expanded_object(_d: Datum) -> ! {
    panic!(
        "DeleteExpandedObject: expanded-object context deletion unported \
         (mcx has no by-pointer MemoryContextDelete)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    extern crate alloc;
    use alloc::boxed::Box;

    #[repr(C)]
    struct FakeExpanded {
        hdr: ExpandedObjectHeader,
        payload: [u8; 8],
    }

    unsafe fn fake_flat_size(eohptr: *mut ExpandedObjectHeader) -> usize {
        let _ = eohptr;
        4 + 8
    }

    unsafe fn fake_flatten(eohptr: *mut ExpandedObjectHeader, result: *mut u8, n: usize) {
        assert_eq!(n, 12);
        let obj = eohptr as *mut FakeExpanded;
        let word = crate::varlena::set_varsize_4b(n);
        core::ptr::copy_nonoverlapping(word.as_ptr(), result, 4);
        core::ptr::copy_nonoverlapping((*obj).payload.as_ptr(), result.add(4), 8);
    }

    static FAKE_METHODS: ExpandedObjectMethods = ExpandedObjectMethods {
        get_flat_size: fake_flat_size,
        flatten_into: fake_flatten,
    };

    fn make_fake() -> *mut FakeExpanded {
        let obj = Box::into_raw(Box::new(FakeExpanded {
            hdr: ExpandedObjectHeader::empty(),
            payload: *b"abcdefgh",
        }));
        unsafe {
            eoh_init_header(
                core::ptr::addr_of_mut!((*obj).hdr),
                &FAKE_METHODS,
                core::ptr::null(),
            )
        };
        obj
    }

    fn free_fake(obj: *mut FakeExpanded) {
        drop(unsafe { Box::from_raw(obj) });
    }

    #[test]
    fn header_images_round_trip() {
        let obj = make_fake();
        unsafe {
            let hdr = core::ptr::addr_of_mut!((*obj).hdr);
            let rw = eohp_get_rw_datum(hdr);
            let ro = eohp_get_ro_datum(hdr);
            assert!(datum_is_external_expanded(rw));
            assert!(datum_is_external_expanded(ro));
            assert!(datum_is_external_expanded_rw(rw));
            assert!(!datum_is_external_expanded_rw(ro));
            assert_eq!(datum_get_eohp(rw), hdr);
            assert_eq!(datum_get_eohp(ro), hdr);
        }
        free_fake(obj);
    }

    #[test]
    fn read_write_truth_table() {
        let obj = make_fake();
        unsafe {
            let hdr = core::ptr::addr_of_mut!((*obj).hdr);
            let rw = eohp_get_rw_datum(hdr);
            let ro = eohp_get_ro_datum(hdr);
            assert!(datum_is_read_write_expanded_object(rw, false, -1));
            assert!(!datum_is_read_write_expanded_object(rw, true, -1));
            assert!(!datum_is_read_write_expanded_object(rw, false, 4));
            assert!(!datum_is_read_write_expanded_object(ro, false, -1));
        }
        free_fake(obj);
    }

    #[test]
    fn make_read_only() {
        let obj = make_fake();
        let mut flat = [0u8; 8];
        flat[..4].copy_from_slice(&crate::varlena::set_varsize_4b(8));
        unsafe {
            let hdr = core::ptr::addr_of_mut!((*obj).hdr);
            let rw = eohp_get_rw_datum(hdr);
            let ro = eohp_get_ro_datum(hdr);
            assert_eq!(make_expanded_object_read_only_internal(rw), ro);
            assert_eq!(make_expanded_object_read_only_internal(ro), ro);
            let flat_d = Datum::from_usize(flat.as_ptr() as usize);
            assert_eq!(make_expanded_object_read_only_internal(flat_d), flat_d);
            assert_eq!(make_expanded_object_read_only(rw, true, -1), rw);
            assert_eq!(make_expanded_object_read_only(rw, false, 4), rw);
            assert_eq!(make_expanded_object_read_only(rw, false, -1), ro);
        }
        free_fake(obj);
    }

    #[test]
    fn methods_dispatch() {
        let obj = make_fake();
        unsafe {
            let hdr = core::ptr::addr_of_mut!((*obj).hdr);
            let n = eoh_get_flat_size(hdr);
            assert_eq!(n, 12);
            let mut buf = [0u8; 12];
            eoh_flatten_into(hdr, buf.as_mut_ptr(), n);
            assert_eq!(&buf[4..], b"abcdefgh");
        }
        free_fake(obj);
    }
}
