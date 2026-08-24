use ::datum::Datum;
use ::types_core::AttrNumber;
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_slot::{HeapTupleTableSlot, MinimalTupleTableSlot, SlotBase, SlotData, TTS_FLAG_SLOW};
use ::types_tuple::tupmacs::{
    att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
};
use ::types_tuple::{
    heap_getsysattr, CompactAttribute, HeapTupleData, MinimalTupleData,
    SelfItemPointerAttributeNumber, SizeofMinimalTupleHeader, TableOidAttributeNumber,
    HEAP_HASNULL, MINIMAL_TUPLE_OFFSET,
};

use core::ptr::NonNull;

// One physical-tuple view for the heap and minimal deform lanes: the minimal
// slot deforms its body directly (t_hoff already counts MINIMAL_TUPLE_OFFSET),
// dissolving C's minhdr wrapper pointing 8 bytes before the allocation.
#[derive(Clone, Copy)]
pub(crate) struct TupleImage {
    tp: *const u8,
    bp: *const u8,
    hasnulls: bool,
    tuple_natts: i32,
    // User-data extent at `tp`: t_len - t_hoff (heap) or the minimal-body length.
    // Every offset the deform walk dereferences is bounded against this so a
    // crafted on-disk image (short body with a full natts, or a lying varlena
    // header) cannot over-read past the tuple — see `deform_internal`.
    data_len: usize,
}

impl TupleImage {
    #[inline]
    pub(crate) fn from_heap(t: &HeapTupleData<'_>) -> TupleImage {
        let t_hoff = t.t_data().t_hoff as usize;
        // t_hoff is an attacker-controlled on-disk field; a header claiming a
        // t_hoff past t_len would make getstruct() form an out-of-bounds pointer.
        let data_len = match (t.t_len as usize).checked_sub(t_hoff) {
            Some(len) => len,
            None => deform_corrupt(),
        };
        TupleImage {
            tp: t.getstruct(),
            // SAFETY: in-bounds offset within the image (t_len >= header).
            bp: unsafe { t.header_ptr().add(::types_tuple::SizeofHeapTupleHeader) },
            hasnulls: t.has_nulls(),
            tuple_natts: t.t_data().natts() as i32,
            data_len,
        }
    }

    /// # Safety
    /// `p` points to a live, complete minimal-tuple image.
    #[inline]
    pub(crate) unsafe fn from_minimal(p: NonNull<MinimalTupleData>) -> TupleImage {
        let mt = unsafe { p.as_ref() };
        let base = p.as_ptr().cast::<u8>();
        // The body begins t_hoff - MINIMAL_TUPLE_OFFSET bytes below base; both
        // t_hoff and t_len are attacker-controlled on-disk fields.
        let body_off = match (mt.t_hoff as usize).checked_sub(MINIMAL_TUPLE_OFFSET) {
            Some(off) => off,
            None => deform_corrupt(),
        };
        let data_len = match (mt.t_len as usize).checked_sub(body_off) {
            Some(len) => len,
            None => deform_corrupt(),
        };
        unsafe {
            TupleImage {
                tp: base.add(body_off),
                bp: base.add(SizeofMinimalTupleHeader),
                hasnulls: (mt.t_infomask & HEAP_HASNULL) != 0,
                tuple_natts: mt.natts() as i32,
                data_len,
            }
        }
    }
}

// Cold, divergent: a crafted/mismatched on-disk image walked off the tuple.
// Mirrors types_tuple's deform bound (getattr.rs idx 38): the backend error
// boundary turns the unwind into an aborted transaction rather than an OOB read.
#[cold]
#[inline(never)]
fn deform_corrupt() -> ! {
    panic!("heap tuple data is corrupt: attribute offset exceeds tuple length");
}

// Advance past a NUL-terminated cstring at `tp[off..]`, bounded by data_len.
// Returns the offset just past the terminator; a string with no NUL inside the
// tuple body is corruption (C's strlen would run off the end).
//
// # Safety
// `tp` points to an image readable for at least `data_len` bytes.
#[inline]
unsafe fn cstring_end_bounded(tp: *const u8, off: usize, data_len: usize) -> usize {
    let mut n = off;
    while n < data_len {
        // SAFETY: n < data_len, in range.
        if unsafe { *tp.add(n) } == 0 {
            return n + 1;
        }
        n += 1;
    }
    deform_corrupt();
}

/// # Safety
/// `attnum <= natts <= min(atts.len(), values.len(), isnull.len())`; `img`
/// points at a live tuple image whose attributes match `atts`; `*offp` is a
/// valid resume offset for `attnum` (C slot_deform_heap_tuple_internal
/// contract). Always-inline so literal slow/hasnulls fold like C's.
/// The walk must stay call-free (an in-loop call spills the walk state around
/// every call site): the cstring arm exits with `.1 == true` instead.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
unsafe fn deform_internal(
    values: &mut [Datum],
    isnull: &mut [bool],
    atts: &[CompactAttribute],
    img: TupleImage,
    mut attnum: usize,
    natts: usize,
    slow: bool,
    hasnulls: bool,
    offp: &mut usize,
    slowp: &mut bool,
) -> (usize, bool) {
    let mut slownext = false;
    let tp = img.tp;

    while attnum < natts {
        // SAFETY: attnum < natts, caller contract.
        let thisatt = unsafe { atts.get_unchecked(attnum) };

        // SAFETY: bitmap covers tuple_natts >= natts bits.
        if hasnulls && unsafe { att_isnull(attnum, img.bp) } {
            unsafe {
                *values.get_unchecked_mut(attnum) = Datum::null();
                *isnull.get_unchecked_mut(attnum) = true;
            }
            if !slow {
                *slowp = true;
                return (attnum + 1, false);
            }
            attnum += 1;
            continue;
        }

        // SAFETY: attnum < natts.
        unsafe { *isnull.get_unchecked_mut(attnum) = false };

        // Locals: the attcacheoff Cell makes the struct non-readonly to LLVM.
        let attlen = thisatt.attlen as i32;
        let attbyval = thisatt.attbyval;
        let attalignby = thisatt.attalignby;

        // SAFETY: offsets walk attributes present in the tuple (caller contract);
        // every image dereference below is bounded by img.data_len, so a lying
        // on-page length turns an OOB read into a deterministic corruption error.
        unsafe {
            if !slow && thisatt.attcacheoff.get() >= 0 {
                *offp = thisatt.attcacheoff.get() as usize;
            } else if attlen == -1 {
                // Cacheable only when already aligned (valid packed or not).
                if !slow && *offp == att_nominal_alignby(*offp, attalignby) {
                    thisatt.attcacheoff.set(*offp as i32);
                } else {
                    // att_pointer_alignby peeks tp[off] (the pad byte).
                    if *offp >= img.data_len {
                        deform_corrupt();
                    }
                    *offp = att_pointer_alignby(*offp, attalignby, -1, tp.add(*offp));
                    if !slow {
                        slownext = true;
                    }
                }
            } else {
                *offp = att_nominal_alignby(*offp, attalignby);
                if !slow {
                    thisatt.attcacheoff.set(*offp as i32);
                }
            }

            // Bound the read at the finalized offset. Fixed-width reads span
            // attlen bytes; varlena/cstring dereference the header/first byte
            // (their full length is bounded at the advance step below).
            if attlen > 0 {
                if *offp + attlen as usize > img.data_len {
                    deform_corrupt();
                }
            } else if *offp >= img.data_len {
                deform_corrupt();
            }

            *values.get_unchecked_mut(attnum) = fetch_att(tp.add(*offp), attbyval, attlen);

            if attlen > 0 {
                *offp += attlen as usize;
            } else if attlen == -1 {
                // *offp < data_len (checked above), so avail >= 1.
                match ::types_tuple::varatt::varsize_bounded(tp.add(*offp), img.data_len - *offp) {
                    Some(vlen) => *offp += vlen,
                    None => deform_corrupt(),
                }
            } else {
                debug_assert!(attlen == -2);
                *slowp = true;
                return (attnum, true);
            }
        }

        if !slow && (slownext || attlen <= 0) {
            *slowp = true;
            return (attnum + 1, false);
        }
        attnum += 1;
    }

    (natts, false)
}

/// # Safety
/// As [`deform_internal`], resumed at its `(attnum, true)` exit: datum stored,
/// `*offp` at the cstring; slow is provably true, cacheoff branches drop out.
#[inline]
unsafe fn deform_cstring_rest(
    values: &mut [Datum],
    isnull: &mut [bool],
    atts: &[CompactAttribute],
    img: TupleImage,
    attnum: usize,
    natts: usize,
    offp: &mut usize,
) -> usize {
    let tp = img.tp;
    let data_len = img.data_len;
    // SAFETY: caller contract — the walk covers attributes present in the tuple;
    // every deref below is bounded by data_len.
    unsafe {
        // Finish the cstring attribute already staged in deform_internal: its
        // NUL terminator must lie within the tuple body (no unbounded strlen).
        *offp = cstring_end_bounded(tp, *offp, data_len);
        for i in attnum + 1..natts {
            let thisatt = atts.get_unchecked(i);
            if img.hasnulls && att_isnull(i, img.bp) {
                *values.get_unchecked_mut(i) = Datum::null();
                *isnull.get_unchecked_mut(i) = true;
                continue;
            }
            *isnull.get_unchecked_mut(i) = false;
            let attlen = thisatt.attlen as i32;
            if attlen == -1 {
                if *offp >= data_len {
                    deform_corrupt();
                }
                *offp = att_pointer_alignby(*offp, thisatt.attalignby, -1, tp.add(*offp));
            } else {
                *offp = att_nominal_alignby(*offp, thisatt.attalignby);
            }
            if attlen > 0 {
                if *offp + attlen as usize > data_len {
                    deform_corrupt();
                }
            } else if *offp >= data_len {
                deform_corrupt();
            }
            *values.get_unchecked_mut(i) = fetch_att(tp.add(*offp), thisatt.attbyval, attlen);
            if attlen > 0 {
                *offp += attlen as usize;
            } else if attlen == -1 {
                match ::types_tuple::varatt::varsize_bounded(tp.add(*offp), data_len - *offp) {
                    Some(vlen) => *offp += vlen,
                    None => deform_corrupt(),
                }
            } else {
                debug_assert!(attlen == -2);
                *offp = cstring_end_bounded(tp, *offp, data_len);
            }
        }
    }
    natts
}

// Always-inline as C: offp/kind monomorphize into each getsomeattrs entry.
#[inline(always)]
pub(crate) fn slot_deform_heap_tuple(
    base: &mut SlotBase<'_>,
    img: TupleImage,
    offp: &mut u32,
    natts: i32,
) {
    let natts = img.tuple_natts.min(natts) as usize;
    let mut attnum = base.tts_nvalid as usize;
    let (mut off, mut slow) = if attnum == 0 {
        (0usize, false)
    } else {
        (*offp as usize, base.is_slow())
    };

    {
        let SlotBase {
            tts_tupleDescriptor,
            tts_values,
            tts_isnull,
            ..
        } = base;
        let atts: &[CompactAttribute] = &tts_tupleDescriptor
            .as_ref()
            .expect("slot_deform_heap_tuple without descriptor")
            .compact_attrs;
        let values = tts_values.as_mut_slice();
        let isnull = tts_isnull.as_mut_slice();
        debug_assert!(
            natts <= atts.len() && values.len() == atts.len() && isnull.len() == atts.len()
        );

        // SAFETY: natts <= atts.len() == values.len() == isnull.len() (slot
        // invariant + clamp above); img is the slot's live stored tuple.
        unsafe {
            let mut cstring = false;
            if !slow {
                if !img.hasnulls {
                    (attnum, cstring) = deform_internal(
                        values, isnull, atts, img, attnum, natts, false, false, &mut off, &mut slow,
                    );
                } else {
                    (attnum, cstring) = deform_internal(
                        values, isnull, atts, img, attnum, natts, false, true, &mut off, &mut slow,
                    );
                }
            }
            if !cstring && attnum < natts {
                (attnum, cstring) = deform_internal(
                    values,
                    isnull,
                    atts,
                    img,
                    attnum,
                    natts,
                    true,
                    img.hasnulls,
                    &mut off,
                    &mut slow,
                );
            }
            if cstring {
                // Tail exit (finalizes the slot itself): no caller state may
                // survive a call on the hot path.
                return deform_cstring_tail(base, img, offp, attnum, natts, off);
            }
        }
    }

    base.tts_nvalid = attnum as AttrNumber;
    *offp = off as u32;
    if slow {
        base.tts_flags |= TTS_FLAG_SLOW;
    } else {
        base.tts_flags &= !TTS_FLAG_SLOW;
    }
}

#[cold]
#[inline(never)]
fn deform_cstring_tail(
    base: &mut SlotBase<'_>,
    img: TupleImage,
    offp: &mut u32,
    attnum: usize,
    natts: usize,
    mut off: usize,
) {
    {
        let SlotBase {
            tts_tupleDescriptor,
            tts_values,
            tts_isnull,
            ..
        } = base;
        let atts: &[CompactAttribute] = &tts_tupleDescriptor
            .as_ref()
            .expect("deform_cstring_tail without descriptor")
            .compact_attrs;
        // SAFETY: deform_internal's (attnum, true) exit, same slot invariants.
        unsafe {
            deform_cstring_rest(
                tts_values.as_mut_slice(),
                tts_isnull.as_mut_slice(),
                atts,
                img,
                attnum,
                natts,
                &mut off,
            );
        }
    }
    base.tts_nvalid = natts as AttrNumber;
    *offp = off as u32;
    base.tts_flags |= TTS_FLAG_SLOW;
}

pub fn slot_getmissingattrs(base: &mut SlotBase<'_>, start_attnum: i32, last_attnum: i32) {
    let SlotBase {
        tts_tupleDescriptor,
        tts_values,
        tts_isnull,
        ..
    } = base;
    let desc = tts_tupleDescriptor
        .as_ref()
        .expect("slot_getmissingattrs without descriptor");
    let missing = desc
        .constr
        .as_ref()
        .map(|c| c.missing.as_slice())
        .filter(|m| !m.is_empty());

    match missing {
        None => {
            for i in start_attnum as usize..last_attnum as usize {
                tts_values[i] = Datum::null();
                tts_isnull[i] = true;
            }
        }
        Some(attrmiss) => {
            for i in start_attnum as usize..last_attnum as usize {
                tts_values[i] = attrmiss[i].am_value;
                tts_isnull[i] = !attrmiss[i].am_present;
            }
        }
    }
}

#[cold]
#[inline(never)]
fn invalid_attnum(attnum: i32) -> ! {
    panic!("invalid attribute number {attnum}")
}

#[cold]
#[inline(never)]
fn virtual_getsomeattrs() -> ! {
    panic!("getsomeattrs is not required to be called on a virtual tuple table slot")
}

// The missing-attr pad leaves at entry: a call after the walk would pin
// base/attnum in callee-saved registers across the whole deform. The JIT arm
// is outlined BEFORE the walk for the same reason — a kernel call inside
// slot_deform_heap_tuple spills the walk state around it (range lane +0.9%
// instr when tried; docs/optimizations/jit-deform.md).
#[inline(always)]
fn getsome_common(
    base: &mut SlotBase<'_>,
    img: TupleImage,
    offp: &mut u32,
    attnum: i32,
    jit: Option<&jit_deform::DeformKernel>,
) {
    if img.tuple_natts < attnum {
        return getsome_narrow(base, img, offp, attnum);
    }
    if let Some(k) = jit {
        return getsome_jit(base, img, offp, attnum, k);
    }
    slot_deform_heap_tuple(base, img, offp, attnum);
    debug_assert!(base.tts_nvalid as i32 >= attnum);
}

// Fresh null-free deforms run the kernel, then finalize the slot exactly as
// the interpreted walk would at (ncols, end_off, !SLOW); a not-fully-covered
// request resumes slot_deform_heap_tuple from that state. Kernel domain
// misses (hasnulls, partial resume) take the walk whole.
#[inline(never)]
fn getsome_jit(
    base: &mut SlotBase<'_>,
    img: TupleImage,
    offp: &mut u32,
    attnum: i32,
    k: &jit_deform::DeformKernel,
) {
    let ncols = k.ncols() as i32;
    if base.tts_nvalid == 0 && !img.hasnulls && ncols <= img.tuple_natts {
        // SAFETY: slot arrays span descriptor natts >= ncols (the kernel was
        // emitted from this slot's descriptor — arm-site contract); img is
        // the live stored tuple, null-free, with natts >= ncols.
        unsafe {
            k.row(
                img.tp,
                base.tts_values.as_mut_ptr(),
                base.tts_isnull.as_mut_ptr().cast(),
            );
        }
        base.tts_nvalid = ncols as AttrNumber;
        *offp = k.end_off();
        base.tts_flags &= !TTS_FLAG_SLOW;
        if ncols >= attnum {
            return;
        }
    }
    slot_deform_heap_tuple(base, img, offp, attnum);
    debug_assert!(base.tts_nvalid as i32 >= attnum);
}

#[cold]
#[inline(never)]
fn getsome_narrow(base: &mut SlotBase<'_>, img: TupleImage, offp: &mut u32, attnum: i32) {
    slot_deform_heap_tuple(base, img, offp, attnum);
    finish_getsomeattrs(base, attnum);
}

#[inline]
pub(crate) fn heap_getsomeattrs_int(h: &mut HeapTupleTableSlot<'_>, attnum: i32) {
    let HeapTupleTableSlot { base, tuple, off, jit_deform } = h;
    debug_assert!(!base.is_empty());
    check_attnum(base, attnum);
    let img = TupleImage::from_heap(tuple.as_ref().expect("heap slot without tuple"));
    getsome_common(base, img, off, attnum, jit_deform.as_deref());
}

#[inline]
pub(crate) fn minimal_getsomeattrs_int(m: &mut MinimalTupleTableSlot<'_>, attnum: i32) {
    debug_assert!(!m.base.is_empty());
    check_attnum(&m.base, attnum);
    // SAFETY: the stored mintuple is live until the slot is cleared/overwritten
    // (slot invariant).
    let img = unsafe { TupleImage::from_minimal(m.mintuple.expect("minimal slot without tuple")) };
    getsome_common(&mut m.base, img, &mut m.off, attnum, None);
}

#[inline]
fn check_attnum(base: &SlotBase<'_>, attnum: i32) {
    let natts = base
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_getsomeattrs_int without descriptor")
        .natts;
    if attnum > natts {
        invalid_attnum(attnum);
    }
}

// C's post-getsomeattrs pad: a tuple from before ALTER TABLE ADD COLUMN can be
// narrower than the descriptor.
#[inline]
fn finish_getsomeattrs(base: &mut SlotBase<'_>, attnum: i32) {
    if (base.tts_nvalid as i32) < attnum {
        slot_getmissingattrs(base, base.tts_nvalid as i32, attnum);
        base.tts_nvalid = attnum as AttrNumber;
    }
}

pub fn slot_getsomeattrs_int(slot: &mut SlotData<'_>, attnum: i32) {
    debug_assert!((slot.base().tts_nvalid as i32) < attnum);
    debug_assert!(attnum > 0);

    match slot {
        SlotData::Virtual(_) => virtual_getsomeattrs(),
        SlotData::Heap(h) => heap_getsomeattrs_int(h, attnum),
        SlotData::BufferHeap(b) => heap_getsomeattrs_int(&mut b.base, attnum),
        SlotData::Minimal(m) => minimal_getsomeattrs_int(m, attnum),
    }
}

#[inline]
pub fn slot_getsomeattrs(slot: &mut SlotData<'_>, attnum: i32) {
    if (slot.base().tts_nvalid as i32) < attnum {
        slot_getsomeattrs_int(slot, attnum);
    }
}

#[inline]
pub fn slot_getallattrs(slot: &mut SlotData<'_>) {
    let natts = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_getallattrs without descriptor")
        .natts;
    slot_getsomeattrs(slot, natts);
}

// Hit path resolves the enum payload ONCE and returns without rejoining the
// miss path (a shared join forces LLVM to re-derive the payload pointer
// around the deform call).
#[inline]
pub fn slot_getattr(slot: &mut SlotData<'_>, attnum: i32, isnull: &mut bool) -> Datum {
    debug_assert!(attnum > 0);
    let base = slot.base();
    if attnum <= base.tts_nvalid as i32 {
        let i = (attnum - 1) as usize;
        // Checked indexing: the bound is attnum <= tts_nvalid, but tts_nvalid and
        // the vector length live in separate (pub) fields, so a safe caller that
        // desyncs them must get a deterministic panic here, not a release-mode OOB
        // read (idx 256). Matches C's Assert-in-debug + defined-in-release.
        *isnull = base.tts_isnull[i];
        return base.tts_values[i];
    }
    slot_getattr_miss(slot, attnum, isnull)
}

fn slot_getattr_miss(slot: &mut SlotData<'_>, attnum: i32, isnull: &mut bool) -> Datum {
    slot_getsomeattrs_int(slot, attnum);
    let base = slot.base();
    let i = (attnum - 1) as usize;
    // Checked indexing (idx 256): getsomeattrs postcondition is tts_nvalid >= attnum,
    // but release-safety must not depend on it — a desynced slot panics, not UB.
    *isnull = base.tts_isnull[i];
    base.tts_values[i]
}

#[inline]
pub fn slot_attisnull(slot: &mut SlotData<'_>, attnum: i32) -> bool {
    debug_assert!(attnum > 0);
    let base = slot.base();
    if attnum <= base.tts_nvalid as i32 {
        // Checked indexing (idx 256): release-safe, panics on a desynced slot.
        return base.tts_isnull[(attnum - 1) as usize];
    }
    slot_getsomeattrs_int(slot, attnum);
    slot.base().tts_isnull[(attnum - 1) as usize]
}

// Monomorphized fast lanes for callers that hold the concrete slot kind: the
// deform kernel is a direct call, no SlotData dispatch (types_slot design).
#[inline]
pub fn heap_slot_getattr(h: &mut HeapTupleTableSlot<'_>, attnum: i32, isnull: &mut bool) -> Datum {
    let HeapTupleTableSlot { base, tuple, off, jit_deform } = h;
    base.slot_getattr(attnum, isnull, |b, n| {
        check_attnum(b, n);
        let img = TupleImage::from_heap(tuple.as_ref().expect("heap slot without tuple"));
        getsome_common(b, img, off, n, jit_deform.as_deref());
    })
}

#[inline]
pub fn minimal_slot_getattr(
    m: &mut MinimalTupleTableSlot<'_>,
    attnum: i32,
    isnull: &mut bool,
) -> Datum {
    let MinimalTupleTableSlot {
        base,
        mintuple,
        off,
        ..
    } = m;
    base.slot_getattr(attnum, isnull, |b, n| {
        check_attnum(b, n);
        // SAFETY: the stored mintuple is live until cleared/overwritten.
        let img =
            unsafe { TupleImage::from_minimal(mintuple.expect("minimal slot without tuple")) };
        getsome_common(b, img, off, n, None);
    })
}

#[cold]
#[inline(never)]
fn no_system_columns() -> alloc::boxed::Box<PgError> {
    alloc::boxed::Box::new(
        PgError::error("cannot retrieve a system column in this context")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn slot_getsysattr(slot: &SlotData<'_>, attnum: i32, isnull: &mut bool) -> PgResult<Datum> {
    debug_assert!(attnum < 0);
    let base = slot.base();
    if attnum == TableOidAttributeNumber {
        *isnull = false;
        return Ok(Datum::from_oid(base.tts_tableOid));
    }
    if attnum == SelfItemPointerAttributeNumber {
        *isnull = false;
        return Ok(Datum::from_usize(&base.tts_tid as *const _ as usize));
    }

    debug_assert!(!base.is_empty());
    let tuple = match slot {
        SlotData::Heap(h) => h.tuple.as_ref(),
        SlotData::BufferHeap(b) => b.base.tuple.as_ref(),
        SlotData::Virtual(_) | SlotData::Minimal(_) => None,
    };
    match tuple {
        Some(t) => Ok(heap_getsysattr(t, attnum, isnull)),
        None => Err(no_system_columns()),
    }
}

#[cfg(test)]
mod deform_bounds_tests {
    use super::*;
    use ::types_tuple::varatt::set_varsize_4b_word;
    use core::cell::Cell;

    // 8-byte aligned scratch image (varlena/fixed reads want MAXALIGN).
    #[repr(align(8))]
    struct Image([u8; 64]);

    fn att(attlen: i16, attbyval: bool, attalignby: u8) -> CompactAttribute {
        CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen,
            attbyval,
            attispackable: attlen < 0,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby,
        }
    }

    // Drive the deform WALK directly over a crafted null-free single-attribute
    // image whose user-data extent is `data_len` bytes — exactly the offsets
    // slot_deform_heap_tuple feeds deform_internal from a stored tuple.
    fn walk_one(image: &Image, data_len: usize, a: CompactAttribute) {
        let img = TupleImage {
            tp: image.0.as_ptr(),
            bp: image.0.as_ptr(),
            hasnulls: false,
            tuple_natts: 1,
            data_len,
        };
        let atts = [a];
        let mut values = [Datum::null(); 1];
        let mut isnull = [false; 1];
        let mut off = 0usize;
        let mut slow = false;
        // SAFETY: attnum 0 < natts 1 == atts.len() == values.len() == isnull.len();
        // img is the crafted image with a matching data_len.
        unsafe {
            deform_internal(
                &mut values, &mut isnull, &atts, img, 0, 1, false, false, &mut off, &mut slow,
            );
        }
    }

    // A varlena whose 4-byte header declares 100 bytes over an 8-byte body must
    // raise a deterministic corruption error, not advance the walk ~100 bytes
    // past the tuple (the idx 97 out-of-bounds read).
    #[test]
    #[should_panic(expected = "corrupt")]
    fn lying_varlena_header_is_bounded() {
        let mut image = Image([0u8; 64]);
        // SAFETY: writing the 4-byte varlena header within the data area.
        unsafe {
            image
                .0
                .as_mut_ptr()
                .cast::<u32>()
                .write_unaligned(set_varsize_4b_word(100));
        }
        walk_one(&image, 8, att(-1, false, 4));
    }

    // A fixed-width (int8) attribute wider than the data area must be rejected
    // rather than reading the 4 bytes past the tuple body.
    #[test]
    #[should_panic(expected = "corrupt")]
    fn truncated_fixed_attribute_is_bounded() {
        let image = Image([0u8; 64]);
        walk_one(&image, 4, att(8, true, 8));
    }

    // A well-formed image deforms without error: guards never fire on a valid
    // tuple (one int8 in an 8-byte body).
    #[test]
    fn well_formed_fixed_attribute_deforms() {
        let image = Image([0u8; 64]);
        walk_one(&image, 8, att(8, true, 8));
    }
}
