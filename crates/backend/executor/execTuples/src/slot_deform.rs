//! Family: slot deform — `slot_deform_heap_tuple` and the
//! `slot_getsomeattrs[_int]` / `slot_getmissingattrs` / `slot_getattr` /
//! `slot_getallattrs` deconstruction entry points (execTuples.c).
//!
//! Deforming detoasts and fills the slot's `tts_values`/`tts_isnull` arrays up
//! to a watermark, so these are fallible (`elog(ERROR)` on a too-short tuple or
//! detoast failure).
//!
//! # Faithful byte engine vs. the owned payload model
//!
//! C's `slot_deform_heap_tuple` walks the physical tuple's bytes
//! (`tp = (char *) tup + tup->t_hoff`) attribute by attribute, resuming from
//! the saved `off`/`TTS_SLOW` state. We port that incremental state machine
//! 1:1 below (the `slot_deform_heap_tuple_internal` helper inlined under its
//! `slow`/`hasnulls` const-specializations, then the
//! `slot_deform_heap_tuple` driver).
//!
//! In this codebase a heap tuple's user-data area is a separate byte slice
//! (`heaptuple::FormedTuple::data`), not bytes hanging
//! off the `HeapTupleData` header. After the keystone the heap slot carries the
//! body-bearing [`FormedTuple`], so the one place this engine needs
//! `(char *) tup + t_hoff` — [`heap_slot_body`] — returns the slot's owned data
//! body directly, and the by-reference [`fetchatt`] writes a
//! `Datum::ByRef` over the verbatim on-disk field bytes into the
//! by-reference `tts_values` lane. Everything here is complete.

extern crate alloc;
use alloc::format;

use mcx::{slice_in, Mcx};
use ::types_core::primitive::AttrNumber;
use types_error::{PgError, PgResult};
use ::nodes::tuptable::{
    HeapTupleTableSlot, SlotData, TTS_FLAG_SLOW,
};
// The canonical value enum; `Datum` is its transitional alias.
use ::types_tuple::heaptuple::{Datum};
use ::types_tuple::heaptuple::{CompactAttribute, HeapTupleHeaderGetNatts};

use crate::slot_ops_vtables;

// The alignment / varlena-length helpers below mirror `tupmacs.h` /
// `varatt.h` exactly (and match the implementations in
// `backend-access-common-heaptuple`).

/// `att_isnull(ATT, BITS)` (tupmacs.h): a 0 bit in the null bitmap means NULL.
#[inline]
fn att_isnull(att: usize, bits: &[u8]) -> bool {
    (bits[att >> 3] & (1u8 << (att & 0x07))) == 0
}

/// `TYPEALIGN(ALIGNVAL, LEN)` (c.h).
#[inline]
fn type_align(alignval: usize, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}

/// `att_nominal_alignby(cur_offset, attalignby)` (tupmacs.h):
/// `TYPEALIGN(attalignby, cur_offset)`.
#[inline]
fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    type_align(attalignby as usize, cur_offset)
}

// --- varlena length decode (varatt.h), needed by att_addlength_pointer ------

#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}
#[inline]
fn varsize_1b(b: &[u8]) -> usize {
    ((b[0] >> 1) & 0x7F) as usize
}
#[inline]
fn varsize_4b(b: &[u8]) -> usize {
    let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    ((hdr >> 2) & 0x3FFF_FFFF) as usize
}
#[inline]
fn vartag_size(tag: u8) -> usize {
    const VARTAG_INDIRECT: u8 = 1;
    const VARTAG_EXPANDED_RO: u8 = 2;
    const VARTAG_ONDISK: u8 = 18;
    if tag == VARTAG_INDIRECT {
        8
    } else if (tag & !1) == VARTAG_EXPANDED_RO {
        8
    } else if tag == VARTAG_ONDISK {
        16
    } else {
        0
    }
}
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    2 + vartag_size(b[1])
}
/// `VARSIZE_ANY(ptr)` (varatt.h) for an in-line varlena starting at `b[0]`.
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b)
    } else {
        varsize_4b(b)
    }
}

/// `att_pointer_alignby(cur_offset, attalignby, attlen, attptr)` (tupmacs.h):
/// `VARATT_NOT_PAD_BYTE(ptr)` is `*(ptr) != 0` — no alignment when a varlena
/// field's first byte is not a pad byte, else align.
#[inline]
fn att_pointer_alignby(cur_offset: usize, attalignby: u8, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen == -1 && data[off] != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

/// `att_addlength_pointer(cur_offset, attlen, attptr=&data[off..])` (tupmacs.h).
#[inline]
fn att_addlength_pointer(cur_offset: usize, attlen: i16, data: &[u8], off: usize) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + varsize_any(&data[off..])
    } else {
        debug_assert_eq!(attlen, -2);
        let mut len = 0usize;
        while data[off + len] != 0 {
            len += 1;
        }
        cur_offset + len + 1
    }
}

/// `fetch_att(T, attbyval, attlen)` (tupmacs.h) for a pass-by-value att — read
/// the scalar from `data[off..]` as a Datum word, sign/zero handling matching
/// the C `*(intN *)` reads on a little-endian 64-bit build.
#[inline]
fn fetch_att_byval<'mcx>(data: &[u8], off: usize, attlen: i16) -> Datum<'mcx> {
    match attlen {
        1 => Datum::from_usize(data[off] as usize),
        2 => Datum::from_usize(u16::from_ne_bytes([data[off], data[off + 1]]) as usize),
        4 => Datum::from_usize(
            u32::from_ne_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]) as usize,
        ),
        8 => Datum::from_usize(u64::from_ne_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
            data[off + 4],
            data[off + 5],
            data[off + 6],
            data[off + 7],
        ]) as usize),
        other => panic!("fetch_att: unsupported by-value attlen {other}"),
    }
}

/// `values[attnum] = fetchatt(thisatt, tp + *offp)` (tupmacs.h `fetchatt`).
///
/// For a by-value att, read the scalar word (`Datum::ByVal`). For a
/// by-reference att, C yields `PointerGetDatum(tp + off)` — a pointer into the
/// tuple data; the faithful idiomatic carrier is `Datum::ByRef` over the
/// verbatim on-disk bytes the field spans (the C contract that the pointer
/// "points into the given tuple" is preserved by copying the exact bytes). The
/// field's length is the same one the byte engine advances `off` by, computed
/// here via `att_addlength_pointer` — `[off, end)` is the field's span.
#[inline]
fn fetchatt<'mcx>(
    mcx: Mcx<'mcx>,
    att: &CompactAttribute,
    data: &[u8],
    off: usize,
) -> PgResult<Datum<'mcx>> {
    if att.attbyval {
        Ok(fetch_att_byval(data, off, att.attlen))
    } else {
        // C: PointerGetDatum(tp + off). Copy out the exact byte span the field
        // occupies: end == att_addlength_pointer(off, attlen, tp, off), the very
        // advance the deform loop applies to `off` right after this fetch.
        let end = att_addlength_pointer(off, att.attlen, data, off);
        Ok(Datum::ByRef(slice_in(mcx, &data[off..end])?))
    }
}

/// `slot_deform_heap_tuple_internal(slot, tuple, attnum, natts, slow, hasnulls,
/// offp, slowp)` (execTuples.c:1019) — the always-inline byte-deform helper.
///
/// Ported with the `slow`/`hasnulls` parameters passed as runtime bools (C uses
/// them as compile-time constants only to let the optimizer specialize; the
/// computed result is identical either way). Returns the next attnum to deform
/// (== `natts` when all requested were deformed); `off` and `slow` are in/out.
///
/// `data` is the tuple's user-data area (`(char *) tup + tup->t_hoff`).
#[allow(clippy::too_many_arguments)]
fn slot_deform_heap_tuple_internal<'mcx>(
    mcx: Mcx<'mcx>,
    values: &mut [Datum<'mcx>],
    isnull: &mut [bool],
    compact_attrs: &[CompactAttribute],
    bp: &[u8],
    data: &[u8],
    mut attnum: i32,
    natts: i32,
    slow: bool,
    hasnulls: bool,
    off: &mut usize,
    slowp: &mut bool,
) -> PgResult<i32> {
    let mut slownext = false;

    while attnum < natts {
        let thisatt = &compact_attrs[attnum as usize];

        if hasnulls && att_isnull(attnum as usize, bp) {
            values[attnum as usize] = Datum::null();
            isnull[attnum as usize] = true;
            if !slow {
                *slowp = true;
                return Ok(attnum + 1);
            } else {
                attnum += 1;
                continue;
            }
        }

        isnull[attnum as usize] = false;

        // calculate the offset of this attribute
        if !slow && thisatt.attcacheoff >= 0 {
            *off = thisatt.attcacheoff as usize;
        } else if thisatt.attlen == -1 {
            // We can only cache the offset for a varlena attribute if the
            // offset is already suitably aligned, so that there would be no
            // pad bytes in any case: then the offset will be valid for either
            // an aligned or unaligned value.
            if !slow && *off == att_nominal_alignby(*off, thisatt.attalignby) {
                // C sets thisatt->attcacheoff = *off here. The descriptor is
                // borrowed immutably (the compact attrs are read-only here),
                // so the cache write is omitted; the computed offsets are
                // identical — this matches heap_deform_tuple's port.
            } else {
                *off = att_pointer_alignby(*off, thisatt.attalignby, -1, data, *off);
                if !slow {
                    slownext = true;
                }
            }
        } else {
            // not varlena, so safe to use att_nominal_alignby
            *off = att_nominal_alignby(*off, thisatt.attalignby);
            // if (!slow) thisatt->attcacheoff = *off; (cache write omitted)
        }

        values[attnum as usize] = fetchatt(mcx, thisatt, data, *off)?;

        *off = att_addlength_pointer(*off, thisatt.attlen, data, *off);

        // check if we need to switch to slow mode
        if !slow {
            // We're unable to deform any further if the above code set
            // 'slownext', or if this isn't a fixed-width attribute.
            if slownext || thisatt.attlen <= 0 {
                *slowp = true;
                return Ok(attnum + 1);
            }
        }

        attnum += 1;
    }

    Ok(natts)
}

/// `slot_deform_heap_tuple(slot, tuple, &offp, natts)` (execTuples.c:1122): the
/// incremental byte-deform engine — fill `tts_values`/`tts_isnull` for the
/// first `natts` attributes from the slot's physical heap tuple, resuming from
/// the saved `off`/`TTS_SLOW` state.
///
/// The C caller (`tts_heap_getsomeattrs` etc.) passes `&hslot->off`; here the
/// heap slot owns that `off` field directly, so we read/write `slot.off`.
pub fn slot_deform_heap_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut HeapTupleTableSlot<'mcx>,
    mut natts: i32,
) -> PgResult<()> {
    let tuple = slot
        .tuple
        .as_ref()
        .ok_or_else(|| PgError::error("slot_deform_heap_tuple: slot has no physical tuple"))?;
    let tup = tuple
        .tuple
        .t_data
        .as_ref()
        .ok_or_else(|| PgError::error("slot_deform_heap_tuple: tuple has no t_data"))?;

    let hasnulls = (tup.t_infomask & ::types_tuple::heaptuple::HEAP_HASNULL) != 0;

    // We can only fetch as many attributes as the tuple has.
    let tuple_natts = HeapTupleHeaderGetNatts(tup) as i32;
    if tuple_natts < natts {
        natts = tuple_natts;
    }

    // The null bitmap (tup->t_bits) and the user-data area
    // ((char *) tup + tup->t_hoff). The body byte carrier is owned by
    // slot_payload_model and not yet landed (see module docs / heap_slot_body).
    let bp_owned: alloc::vec::Vec<u8> = tup.t_bits.iter().copied().collect();
    let data: alloc::vec::Vec<u8> = heap_slot_body(slot);

    // Snapshot descriptor compact attrs (read-only) before borrowing tts arrays.
    let compact_attrs: alloc::vec::Vec<CompactAttribute> = slot
        .base
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_deform_heap_tuple: slot has no tuple descriptor")
        .compact_attrs
        .iter()
        .copied()
        .collect();

    // Check whether the first call for this tuple, and initialize or restore
    // loop state.
    let mut attnum = slot.base.tts_nvalid as i32;
    let mut off: usize;
    let mut slow: bool;
    if attnum == 0 {
        // Start from the first attribute
        off = 0;
        slow = false;
    } else {
        // Restore state from previous execution
        off = slot.off as usize;
        slow = slot.base.tts_flags & TTS_FLAG_SLOW != 0;
    }

    {
        let values = slot.base.tts_values.as_mut_slice();
        let isnull = slot.base.tts_isnull.as_mut_slice();
        let mut slowp = false;

        // If 'slow' isn't set, try deforming using deforming code that does not
        // contain any of the extra checks required for non-fixed offset
        // deforming. C inlines the internal helper twice with hasnulls const.
        if !slow {
            if !hasnulls {
                attnum = slot_deform_heap_tuple_internal(
                    mcx,
                    values,
                    isnull,
                    &compact_attrs,
                    &bp_owned,
                    &data,
                    attnum,
                    natts,
                    false, // slow
                    false, // hasnulls
                    &mut off,
                    &mut slowp,
                )?;
            } else {
                attnum = slot_deform_heap_tuple_internal(
                    mcx,
                    values,
                    isnull,
                    &compact_attrs,
                    &bp_owned,
                    &data,
                    attnum,
                    natts,
                    false, // slow
                    true, // hasnulls
                    &mut off,
                    &mut slowp,
                )?;
            }
            // slowp reflects whether a switch to slow mode is now required.
            slow = slowp;
        }

        // If there's still work to do then we must be in slow mode
        if attnum < natts {
            attnum = slot_deform_heap_tuple_internal(
                mcx,
                values,
                isnull,
                &compact_attrs,
                &bp_owned,
                &data,
                attnum,
                natts,
                true, // slow
                hasnulls,
                &mut off,
                &mut slowp,
            )?;
            slow = slowp;
        }
    }

    // Save state for next execution
    trace::trace!(
        trace::Category::Slot,
        "slot_deform_heap_tuple nvalid {} -> {} (off={})",
        slot.base.tts_nvalid,
        attnum,
        off
    );
    slot.base.tts_nvalid = attnum as AttrNumber;
    slot.off = off as u32;
    if slow {
        slot.base.tts_flags |= TTS_FLAG_SLOW;
    } else {
        slot.base.tts_flags &= !TTS_FLAG_SLOW;
    }

    Ok(())
}

/// `(char *) tup + tup->t_hoff` — the heap slot's user-data byte area.
///
/// In C this is a pointer into the contiguous `HeapTupleHeaderData` chunk just
/// past the (aligned, null-bitmap-bearing) header. In this codebase the body
/// bytes travel separately from the `HeapTupleData` header as the heap slot's
/// owned [`FormedTuple::data`] (`= (char *) tup + t_hoff`), set up when the
/// tuple was stored. Hand the deform engine that owned body slice directly.
///
/// The caller has already established the slot has a physical tuple (it reads
/// `slot.tuple` for the header/natts just above), so the body carrier is
/// present too.
fn heap_slot_body(slot: &HeapTupleTableSlot) -> alloc::vec::Vec<u8> {
    slot.tuple
        .as_ref()
        .expect("heap_slot_body: heap slot has no physical tuple")
        .data
        .iter()
        .copied()
        .collect()
}

/// `slot_getmissingattrs(slot, startAttNum, lastAttNum)` (execTuples.c:2056):
/// fill the `[startAttNum, lastAttNum)` range of `tts_values`/`tts_isnull` from
/// the descriptor's attribute missing-value defaults (or NULL).
pub fn slot_getmissingattrs<'mcx>(
    _mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    start_att_num: i32,
    last_att_num: i32,
) -> PgResult<()> {
    let base = slot.base_mut();

    // attrmiss = slot->tts_tupleDescriptor->constr ?
    //            slot->tts_tupleDescriptor->constr->missing : NULL;
    let desc = base
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_getmissingattrs: slot has no tuple descriptor");
    let has_missing = desc
        .constr
        .as_ref()
        .map(|c| !c.missing.is_empty())
        .unwrap_or(false);

    if !has_missing {
        // no missing values array at all, so just fill everything in as NULL:
        //   memset(tts_values + start, 0, (last - start) * sizeof(Datum));
        //   memset(tts_isnull + start, 1, (last - start) * sizeof(bool));
        for i in start_att_num..last_att_num {
            base.tts_values[i as usize] = Datum::null();
            base.tts_isnull[i as usize] = true;
        }
    } else {
        // if there is a missing values array we must process them one by one:
        //   tts_values[missattnum] = attrmiss[missattnum].am_value;
        //   tts_isnull[missattnum] = !attrmiss[missattnum].am_present;
        let desc = base
            .tts_tupleDescriptor
            .as_ref()
            .expect("slot_getmissingattrs: slot has no tuple descriptor");
        let constr = desc.constr.as_ref().unwrap();
        // Snapshot the (value, present) pairs to avoid borrowing the descriptor
        // and the tts arrays simultaneously. With the expanded tts_values
        // (`Datum`), the missing value — by-value or by-reference — is
        // carried verbatim: C's `tts_values[missattnum] = attrmiss->am_value`.
        let mut pairs: alloc::vec::Vec<(Datum<'mcx>, bool)> = alloc::vec::Vec::new();
        for missattnum in start_att_num..last_att_num {
            let am = &constr.missing[missattnum as usize];
            pairs.push((am.am_value.clone(), !am.am_present));
        }
        let base = slot.base_mut();
        for (idx, missattnum) in (start_att_num..last_att_num).enumerate() {
            base.tts_values[missattnum as usize] = pairs[idx].0.clone();
            base.tts_isnull[missattnum as usize] = pairs[idx].1;
        }
    }

    Ok(())
}

/// `slot_getsomeattrs_int(slot, attnum)` (execTuples.c:2091): the slow path of
/// `slot_getsomeattrs` — call the slot-ops `getsomeattrs` and pad any
/// remaining requested attributes with `slot_getmissingattrs`.
pub fn slot_getsomeattrs_int<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    attnum: i32,
) -> PgResult<()> {
    // Assert(slot->tts_nvalid < attnum);  /* checked in slot_getsomeattrs */
    debug_assert!((slot.base().tts_nvalid as i32) < attnum);
    // Assert(attnum > 0);
    debug_assert!(attnum > 0);

    // if (unlikely(attnum > slot->tts_tupleDescriptor->natts))
    //     elog(ERROR, "invalid attribute number %d", attnum);
    let desc_natts = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_getsomeattrs_int: slot has no tuple descriptor")
        .natts;
    if attnum > desc_natts {
        return Err(PgError::error(format!(
            "invalid attribute number {attnum}"
        )));
    }

    // Fetch as many attributes as possible from the underlying tuple.
    //   slot->tts_ops->getsomeattrs(slot, attnum);
    slot_ops_vtables::slot_ops_getsomeattrs(mcx, slot, attnum)?;

    // If the underlying tuple doesn't have enough attributes, the tuple
    // descriptor must have the missing attributes.
    if (slot.base().tts_nvalid as i32) < attnum {
        let nvalid = slot.base().tts_nvalid as i32;
        slot_getmissingattrs(mcx, slot, nvalid, attnum)?;
        slot.base_mut().tts_nvalid = attnum as AttrNumber;
    }

    Ok(())
}

/// `slot_getsomeattrs(slot, attnum)` (tuptable.h:359 inline): ensure the first
/// `attnum` attributes of the slot are deconstructed into
/// `tts_values`/`tts_isnull` (fast path checks `tts_nvalid`).
pub fn slot_getsomeattrs<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    attnum: i32,
) -> PgResult<()> {
    if (slot.base().tts_nvalid as i32) < attnum {
        slot_getsomeattrs_int(mcx, slot, attnum)?;
    }
    Ok(())
}

/// `slot_getallattrs(slot)` (tuptable.h:372 inline): deconstruct all attributes
/// of the slot's descriptor.
pub fn slot_getallattrs<'mcx>(mcx: Mcx<'mcx>, slot: &mut SlotData<'mcx>) -> PgResult<()> {
    let natts = slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("slot_getallattrs: slot has no tuple descriptor")
        .natts;
    slot_getsomeattrs(mcx, slot, natts)
}

/// `slot_getattr(slot, attnum, &isnull)` (tuptable.h:399 inline): fetch a single
/// attribute as `(datum, isnull)`, deforming as needed.
///
/// The C inline asserts `attnum > 0` and handles only regular attributes; a
/// negative (system) attribute is fetched through `slot_getsysattr`
/// (tuptable.h:420). We dispatch on the sign here so the single entry point
/// covers both, matching the scaffold signature.
pub fn slot_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    attnum: AttrNumber,
) -> PgResult<(Datum<'mcx>, bool)> {
    if attnum > 0 {
        // if (attnum > slot->tts_nvalid)
        //     slot_getsomeattrs(slot, attnum);
        if attnum > slot.base().tts_nvalid {
            slot_getsomeattrs(mcx, slot, attnum as i32)?;
        }
        // *isnull = slot->tts_isnull[attnum - 1];
        // return slot->tts_values[attnum - 1];
        //
        // The stored column is already the canonical unified value: a by-value
        // column is `ByVal` (C's scalar Datum), a by-reference column is `ByRef`
        // over the owned tuple bytes (the unified value type's faithful stand-in
        // for C's `PointerGetDatum(tp + off)`). Hand the caller a copy in its
        // own `mcx`, mirroring C's `return slot->tts_values[attnum - 1]`.
        let base = slot.base();
        let isnull = base.tts_isnull[(attnum - 1) as usize];
        let value = base.tts_values[(attnum - 1) as usize].clone_in(mcx)?;
        Ok((value, isnull))
    } else {
        // slot_getsysattr(slot, attnum, &isnull) (tuptable.h:420):
        //   TableOid / SelfItemPointer are handled by the dispatch helper,
        //   everything else goes through slot->tts_ops->getsysattr.
        slot_ops_vtables::slot_getsysattr(mcx, slot, attnum)
    }
}
