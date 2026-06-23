//! `src/backend/utils/adt/trigfuncs.c` (postgres-18.3) — builtin functions for
//! useful trigger support.
//!
//! The single function `trigfuncs.c` defines,
//! `suppress_redundant_updates_trigger`, is ported here 1:1: the control flow,
//! the four trigger-protocol checks (with their exact SQLSTATE and message
//! text), and the field-by-field "is the NEW tuple identical to the OLD"
//! comparison — including the comparison of the tuple payload past
//! `SizeofHeapTupleHeader` (the C `memcmp`, which in the owned-tree
//! representation is the `t_bits` flexible-array tail of the header).
//!
//! The only genuinely-external coupling is the fmgr / `TriggerData` call
//! boundary: the trigger is invoked as `Datum f(PG_FUNCTION_ARGS)`, fetches its
//! `TriggerData` out of `fcinfo->context` (gated by the `CALLED_AS_TRIGGER`
//! node-tag test), and returns the surviving tuple via `PointerGetDatum`. The
//! trigger manager (`commands/trigger.c`) owns that state and is not yet ported,
//! so the unwrapping routes through the centralized seams in
//! [`trigger_seams`] (`called_as_trigger` / `tg_event` /
//! `tg_trigtuple` / `tg_newtuple`); the protocol checks and tuple comparison are
//! the owned logic, ported verbatim.

use ::mcx::Mcx;
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED};
use ::types_ri_triggers::TriggerDataRef;
use ::types_tuple::heaptuple::FormedTuple;
use ::types_tuple::heap::SizeofHeapTupleHeader;
use ::types_tuple::heaptuple::{
    HeapTupleData, HeapTupleHeaderData, HeapTupleHeaderGetNatts, HEAP_XACT_MASK,
};

use trigger_seams as trigger;

// ---------------------------------------------------------------------------
// Trigger-event bit tests (commands/trigger.h) — the three this file uses.
// ---------------------------------------------------------------------------

/// `TRIGGER_EVENT_UPDATE`.
pub const TRIGGER_EVENT_UPDATE: u32 = 0x0002;
/// `TRIGGER_EVENT_OPMASK`.
pub const TRIGGER_EVENT_OPMASK: u32 = 0x0003;
/// `TRIGGER_EVENT_ROW`.
pub const TRIGGER_EVENT_ROW: u32 = 0x0004;
/// `TRIGGER_EVENT_BEFORE`.
pub const TRIGGER_EVENT_BEFORE: u32 = 0x0008;
/// `TRIGGER_EVENT_TIMINGMASK`.
pub const TRIGGER_EVENT_TIMINGMASK: u32 = 0x0018;

/// `TRIGGER_FIRED_BY_UPDATE(event)`.
#[inline]
pub fn trigger_fired_by_update(event: u32) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
/// `TRIGGER_FIRED_BEFORE(event)`.
#[inline]
pub fn trigger_fired_before(event: u32) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE
}
/// `TRIGGER_FIRED_FOR_ROW(event)`.
#[inline]
pub fn trigger_fired_for_row(event: u32) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}

// ---------------------------------------------------------------------------
// suppress_redundant_updates_trigger
// ---------------------------------------------------------------------------

/// `suppress_redundant_updates_trigger` (trigfuncs.c:27).
///
/// This trigger function inhibits an UPDATE from being done if the OLD and NEW
/// records are identical. Returns the new tuple to proceed with the update
/// (`Ok(Some(newtuple))`), or `Ok(None)` (C `PointerGetDatum(NULL)`) to suppress
/// it; a trigger-protocol violation returns the matching `ERROR` [`PgError`].
///
/// `trigdata` is the `TriggerData *` handle from `fcinfo->context` — opaque,
/// owned by the (unported) trigger manager, read through the trigger seams. The
/// surviving NEW tuple is materialised in `mcx` (the C trigger result lives in
/// the executor's per-tuple context).
pub fn suppress_redundant_updates_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    trigdata: TriggerDataRef,
) -> PgResult<Option<HeapTupleData<'mcx>>> {
    // make sure it's called as a trigger
    if !trigger::called_as_trigger::call(trigdata) {
        return Err(PgError::error(
            "suppress_redundant_updates_trigger: must be called as trigger",
        )
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
    }

    let event = trigger::tg_event::call(trigdata);

    // and that it's called on update
    if !trigger_fired_by_update(event) {
        return Err(
            PgError::error("suppress_redundant_updates_trigger: must be called on update")
                .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
        );
    }

    // and that it's called before update
    if !trigger_fired_before(event) {
        return Err(PgError::error(
            "suppress_redundant_updates_trigger: must be called before update",
        )
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
    }

    // and that it's called for each row
    if !trigger_fired_for_row(event) {
        return Err(PgError::error(
            "suppress_redundant_updates_trigger: must be called for each row",
        )
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
    }

    // get tuple data, set default result
    // rettuple = newtuple = trigdata->tg_newtuple;
    // oldtuple = trigdata->tg_trigtuple;
    //
    // C compares the two HeapTuples byte-for-byte over their full on-disk image
    // (a single contiguous palloc block: fixed header, NULL bitmap, padding, and
    // the user-data area).  In this tree's owned tuple representation those parts
    // are split — the header (`HeapTupleData`) carries only the fixed fields and
    // the NULL bitmap (`t_bits`), while the user-data area lives separately in the
    // `FormedTuple::data` vector.  So the comparison is driven off the fully-formed
    // OLD/NEW `FormedTuple`s (header + user data) fetched off the trigger slot
    // side-channel, reconstructing the C `memcmp` tail (`bitmap || padding || data`).
    let newslot = trigger::tg_newslot::call(trigdata);
    let oldslot = trigger::tg_trigslot::call(trigdata);
    let newtuple = trigger::tg_slot_formed_tuple::call(mcx, newslot)?;
    let oldtuple = trigger::tg_slot_formed_tuple::call(mcx, oldslot)?;

    decide(newtuple, oldtuple)
}

/// The post-validation body of `suppress_redundant_updates_trigger`:
///
/// ```c
/// rettuple = newtuple = trigdata->tg_newtuple;
/// oldtuple = trigdata->tg_trigtuple;
/// newheader = newtuple->t_data;
/// oldheader = oldtuple->t_data;
/// if (... payload is the same ...) rettuple = NULL;
/// return PointerGetDatum(rettuple);
/// ```
///
/// The trigger manager guarantees `tg_newtuple` / `tg_trigtuple` (and their
/// `t_data` headers) are present for a BEFORE-UPDATE-FOR-EACH-ROW firing — the
/// same assumption the C code relies on when it dereferences them
/// unconditionally. A missing tuple/header is a trigger-manager contract
/// violation rather than a recoverable user error, so it surfaces as an `ERROR`
/// instead of silently pretending the tuples differ.
fn decide<'mcx>(
    newtuple: Option<FormedTuple<'mcx>>,
    oldtuple: Option<FormedTuple<'mcx>>,
) -> PgResult<Option<HeapTupleData<'mcx>>> {
    let newtuple = newtuple.ok_or_else(missing_tuple)?;
    let oldtuple = oldtuple.ok_or_else(missing_tuple)?;

    // newheader = newtuple->t_data;  oldheader = oldtuple->t_data;
    let newheader: &HeapTupleHeaderData =
        newtuple.tuple.t_data.as_deref().ok_or_else(missing_tuple)?;
    let oldheader: &HeapTupleHeaderData =
        oldtuple.tuple.t_data.as_deref().ok_or_else(missing_tuple)?;

    // if the tuple payload is the same ... then suppress the update.
    if tuples_identical(&newtuple, &oldtuple, newheader, oldheader) {
        // ... then suppress the update.  rettuple = NULL; return PointerGetDatum(NULL).
        return Ok(None);
    }

    // rettuple = newtuple (the surviving NEW tuple).
    Ok(Some(::mcx::box_into_inner_leak(newtuple.tuple)))
}

/// Internal error for a trigger-manager contract violation (a tuple/header the
/// manager promised to supply was absent). Mirrors the C code dereferencing the
/// pointers unconditionally — we just refuse to fabricate a result.
fn missing_tuple() -> PgError {
    PgError::error("suppress_redundant_updates_trigger: trigger tuple data unexpectedly missing")
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED)
}

/// The body of the big `if` in `suppress_redundant_updates_trigger`: whether the
/// NEW and OLD tuples are equal modulo the transaction-status infomask bits.
/// Mirrors the C condition exactly, including evaluation order (short-circuit
/// `&&`) and the trailing payload comparison over everything past
/// `SizeofHeapTupleHeader`.
fn tuples_identical(
    newtuple: &FormedTuple,
    oldtuple: &FormedTuple,
    newheader: &HeapTupleHeaderData,
    oldheader: &HeapTupleHeaderData,
) -> bool {
    // newtuple->t_len == oldtuple->t_len
    newtuple.tuple.t_len == oldtuple.tuple.t_len
        // newheader->t_hoff == oldheader->t_hoff
        && newheader.t_hoff == oldheader.t_hoff
        // HeapTupleHeaderGetNatts(newheader) == HeapTupleHeaderGetNatts(oldheader)
        && HeapTupleHeaderGetNatts(newheader) == HeapTupleHeaderGetNatts(oldheader)
        // (newheader->t_infomask & ~HEAP_XACT_MASK) == (oldheader->t_infomask & ~HEAP_XACT_MASK)
        && (newheader.t_infomask & !HEAP_XACT_MASK) == (oldheader.t_infomask & !HEAP_XACT_MASK)
        // memcmp(((char *) newheader) + SizeofHeapTupleHeader,
        //        ((char *) oldheader) + SizeofHeapTupleHeader,
        //        newtuple->t_len - SizeofHeapTupleHeader) == 0
        && payload_eq(newtuple, oldtuple, newheader, oldheader)
}

/// `memcmp((char*)newheader + SizeofHeapTupleHeader,
///         (char*)oldheader + SizeofHeapTupleHeader,
///         newtuple->t_len - SizeofHeapTupleHeader) == 0`
///
/// In the C ABI, `HeapTupleHeaderData` is fixed-size up to its
/// `t_bits[FLEXIBLE_ARRAY_MEMBER]` (`SizeofHeapTupleHeader == offsetof(.., t_bits)`),
/// and the flexible array is the entire variable tail of the tuple: the NULL
/// bitmap, alignment padding, and the attribute data, laid out contiguously after
/// the fixed header. So `memcmp` from offset `SizeofHeapTupleHeader` over
/// `newtuple->t_len - SizeofHeapTupleHeader` bytes is a comparison of exactly that
/// tail. In the owned-tree representation that whole tail is modelled by
/// `HeapTupleHeaderData::t_bits` (the FAM member, by definition starting at
/// `SizeofHeapTupleHeader`).
///
/// The C `memcmp` length is taken from `newtuple` specifically (not `oldtuple`),
/// even though the caller has already checked `newtuple.t_len == oldtuple.t_len`.
/// A well-formed trigger tuple has a tail of exactly
/// `t_len - SizeofHeapTupleHeader` bytes; if either tail is shorter than `len`
/// (a malformed/partial owned tuple, which C — reading a contiguous block —
/// never produces) we report "not equal" rather than reading past the end.
fn payload_eq(
    newtuple: &FormedTuple,
    oldtuple: &FormedTuple,
    newheader: &HeapTupleHeaderData,
    oldheader: &HeapTupleHeaderData,
) -> bool {
    // newtuple->t_len - SizeofHeapTupleHeader (the C memcmp length, from newtuple).
    // saturating_sub mirrors that a header-sized-or-smaller tuple compares zero
    // payload bytes without panicking on the subtraction.
    let len = (newtuple.tuple.t_len as usize).saturating_sub(SizeofHeapTupleHeader);

    // Reconstruct the contiguous C tail (everything from offset
    // SizeofHeapTupleHeader to t_len) for each tuple: the NULL bitmap (`t_bits`),
    // then zero padding up to `t_hoff`, then the user-data area (`data`).
    let new_tail = reconstruct_tail(newtuple, newheader);
    let old_tail = reconstruct_tail(oldtuple, oldheader);

    // C reads `len` bytes past each header. Require both tails to actually hold
    // those bytes (true for any well-formed tuple), then compare exactly `len`.
    new_tail.len() >= len && old_tail.len() >= len && new_tail[..len] == old_tail[..len]
}

/// Build the contiguous byte tail C `memcmp`s — everything past the fixed header
/// (`SizeofHeapTupleHeader == 23`) up to `t_len`: the NULL bitmap (`t_bits`),
/// zero padding up to `t_hoff`, then the user-data area (`FormedTuple::data`).
fn reconstruct_tail(tuple: &FormedTuple, header: &HeapTupleHeaderData) -> Vec<u8> {
    let t_hoff = header.t_hoff as usize;
    // The tail begins at offset SizeofHeapTupleHeader; the header section of the
    // tail (bitmap + padding) is `t_hoff - SizeofHeapTupleHeader` bytes.
    let header_tail_len = t_hoff.saturating_sub(SizeofHeapTupleHeader);
    let mut tail = vec![0u8; header_tail_len];
    let copy = core::cmp::min(header.t_bits.len(), header_tail_len);
    tail[..copy].copy_from_slice(&header.t_bits[..copy]);
    tail.extend_from_slice(&tuple.data);
    tail
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::{slice_in, MemoryContext, PgBox};
    use std::sync::Once;
    use ::types_tuple::heaptuple::{
        bits8, HeapTupleFields, HeapTupleHeaderChoice, ItemPointerData, HEAP_NATTS_MASK,
    };

    const HEAP_HASNULL_TEST: u16 = 0x0001;
    const BEFORE_ROW_UPDATE: u32 =
        TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;

    fn make_header<'mcx>(
        mcx: Mcx<'mcx>,
        t_hoff: u8,
        natts: u16,
        infomask: u16,
        t_bits: &[bits8],
    ) -> HeapTupleHeaderData<'mcx> {
        HeapTupleHeaderData {
            t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields::default()),
            t_ctid: ItemPointerData::default(),
            t_infomask2: natts & HEAP_NATTS_MASK,
            t_infomask: infomask,
            t_hoff,
            t_bits: slice_in(mcx, t_bits).unwrap(),
        }
    }

    /// Build a `FormedTuple` for the comparison core: `header` carries the fixed
    /// header + NULL bitmap (`t_bits`, the bytes up to `t_hoff`); `data` is the
    /// separately-stored user-data area. `t_len = t_hoff + data.len()`, matching
    /// the on-disk contract.
    fn make_formed<'mcx>(
        mcx: Mcx<'mcx>,
        header: HeapTupleHeaderData<'mcx>,
        data: &[u8],
    ) -> FormedTuple<'mcx> {
        let t_len = header.t_hoff as u32 + data.len() as u32;
        let tuple = HeapTupleData {
            t_len,
            t_self: ItemPointerData::default(),
            t_tableOid: 0,
            t_data: Some(PgBox::new_in(header, mcx)),
        };
        FormedTuple {
            tuple: PgBox::new_in(tuple, mcx),
            data: slice_in(mcx, data).unwrap(),
        }
    }

    /// Convenience: build a `FormedTuple` with no separate user-data area (the
    /// whole tail being the NULL bitmap, `t_len = SizeofHeapTupleHeader + bitmap`).
    fn make_tuple<'mcx>(mcx: Mcx<'mcx>, header: HeapTupleHeaderData<'mcx>) -> FormedTuple<'mcx> {
        // t_hoff = SizeofHeapTupleHeader + bitmap len so t_len = t_hoff with no data.
        let header = HeapTupleHeaderData {
            t_hoff: (SizeofHeapTupleHeader + header.t_bits.len()) as u8,
            ..header
        };
        make_formed(mcx, header, &[])
    }

    /// Drive the owned post-validation core directly.
    fn decide_pair<'mcx>(
        newt: FormedTuple<'mcx>,
        oldt: FormedTuple<'mcx>,
    ) -> PgResult<Option<HeapTupleData<'mcx>>> {
        decide(Some(newt), Some(oldt))
    }

    #[test]
    fn macro_event_predicates_match_c() {
        assert!(trigger_fired_by_update(TRIGGER_EVENT_UPDATE));
        assert!(!trigger_fired_by_update(0x0000)); // INSERT
        assert!(!trigger_fired_by_update(0x0001)); // DELETE
        assert!(trigger_fired_before(TRIGGER_EVENT_BEFORE));
        assert!(!trigger_fired_before(0x0000)); // AFTER
        assert!(!trigger_fired_before(0x0010)); // INSTEAD
        assert!(trigger_fired_for_row(TRIGGER_EVENT_ROW));
        assert!(!trigger_fired_for_row(0));
    }

    #[test]
    fn identical_tuples_suppress_update() {
        let ctx = MemoryContext::new("trig-identical");
        let mcx = ctx.mcx();
        let tail = [0b0000_0011u8, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00];
        let newt = make_tuple(mcx, make_header(mcx, 24, 2, HEAP_HASNULL_TEST, &tail));
        let oldt = make_tuple(mcx, make_header(mcx, 24, 2, HEAP_HASNULL_TEST, &tail));
        assert!(decide_pair(newt, oldt).unwrap().is_none());
    }

    #[test]
    fn identical_modulo_xact_bits_still_suppresses() {
        let ctx = MemoryContext::new("trig-xact");
        let mcx = ctx.mcx();
        let tail = [0b0000_0011u8, 0x00, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x00];
        // 0x0100 is HEAP_XMIN_COMMITTED, inside HEAP_XACT_MASK -> masked out.
        let newt = make_tuple(mcx, make_header(mcx, 24, 2, 0x0100 | HEAP_HASNULL_TEST, &tail));
        let oldt = make_tuple(mcx, make_header(mcx, 24, 2, HEAP_HASNULL_TEST, &tail));
        assert!(decide_pair(newt, oldt).unwrap().is_none());
    }

    #[test]
    fn differing_len_keeps_update() {
        let ctx = MemoryContext::new("trig-len");
        let mcx = ctx.mcx();
        // Same header but the NEW user-data area is shorter than the OLD one.
        let newt = make_formed(mcx, make_header(mcx, 24, 2, 0, &[0b0000_0011]), &[0x01]);
        let oldt = make_formed(
            mcx,
            make_header(mcx, 24, 2, 0, &[0b0000_0011]),
            &[0x01, 0x02, 0x03],
        );
        let kept = decide_pair(newt, oldt).unwrap().expect("kept");
        assert_eq!(kept.t_len, 24 + 1);
    }

    #[test]
    fn differing_payload_past_bitmap_keeps_update() {
        let ctx = MemoryContext::new("trig-payload");
        let mcx = ctx.mcx();
        // Same NULL bitmap, attribute byte differs in the user-data area.
        let new_data = [0xde, 0xad, 0xbe, 0xef, 0x00];
        let old_data = [0xde, 0xad, 0xbe, 0xef, 0x01];
        let newt = make_formed(mcx, make_header(mcx, 24, 1, HEAP_HASNULL_TEST, &[0b1]), &new_data);
        let oldt = make_formed(mcx, make_header(mcx, 24, 1, HEAP_HASNULL_TEST, &[0b1]), &old_data);
        assert!(decide_pair(newt, oldt).unwrap().is_some());
    }

    #[test]
    fn differing_natts_hoff_infomask_keep_update() {
        let ctx = MemoryContext::new("trig-fields");
        let mcx = ctx.mcx();
        // natts diff
        let n = make_formed(mcx, make_header(mcx, 24, 2, 0, &[0b1]), &[]);
        let o = make_formed(mcx, make_header(mcx, 24, 3, 0, &[0b1]), &[]);
        assert!(decide_pair(n, o).unwrap().is_some());
        // hoff diff (different t_hoff -> different t_len, both with no user data)
        let n = make_formed(mcx, make_header(mcx, 24, 2, 0, &[0b11]), &[]);
        let o = make_formed(mcx, make_header(mcx, 32, 2, 0, &[0b11]), &[]);
        assert!(decide_pair(n, o).unwrap().is_some());
        // non-xact infomask diff (HEAP_HASVARWIDTH 0x0002, outside HEAP_XACT_MASK)
        let n = make_formed(mcx, make_header(mcx, 24, 2, 0x0002, &[0b1]), &[]);
        let o = make_formed(mcx, make_header(mcx, 24, 2, 0x0000, &[0b1]), &[]);
        assert!(decide_pair(n, o).unwrap().is_some());
    }

    // Full top-level path through the trigger seams (installed once for the test).
    static ONCE: Once = Once::new();

    fn install_test_seams() {
        ONCE.call_once(|| {
            // TriggerDataRef(1) => not-a-trigger; (2) => valid BEFORE-ROW-UPDATE
            // with identical NEW/OLD tuples; (3) => valid event but tg_event
            // not-update.
            trigger::called_as_trigger::set(|td| td.0 != 1);
            trigger::tg_event::set(|td| match td.0 {
                3 => TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE, // INSERT timing
                _ => BEFORE_ROW_UPDATE,
            });
            // tg_newslot/tg_trigslot resolve to distinct slot markers; the formed
            // tuple seam returns identical NEW/OLD tuples for both.
            trigger::tg_newslot::set(|_td| ::types_ri_triggers::TupleTableSlotRef(1));
            trigger::tg_trigslot::set(|_td| ::types_ri_triggers::TupleTableSlotRef(2));
            fn mk_formed(
                mcx: Mcx<'_>,
                _slot: ::types_ri_triggers::TupleTableSlotRef,
            ) -> PgResult<Option<FormedTuple<'_>>> {
                let data = [0x2a, 0x00];
                Ok(Some(make_formed(mcx, make_header(mcx, 24, 1, 0, &[0b1]), &data)))
            }
            trigger::tg_slot_formed_tuple::set(mk_formed);
        });
    }

    #[test]
    fn top_level_protocol_and_suppression() {
        install_test_seams();
        let ctx = MemoryContext::new("trig-top");
        let mcx = ctx.mcx();

        // not a trigger
        let err = suppress_redundant_updates_trigger(mcx, TriggerDataRef(1)).unwrap_err();
        assert_eq!(
            err.message(),
            "suppress_redundant_updates_trigger: must be called as trigger"
        );
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);

        // not an update
        let err = suppress_redundant_updates_trigger(mcx, TriggerDataRef(3)).unwrap_err();
        assert_eq!(
            err.message(),
            "suppress_redundant_updates_trigger: must be called on update"
        );

        // valid + identical tuples => suppress (None)
        let res = suppress_redundant_updates_trigger(mcx, TriggerDataRef(2)).unwrap();
        assert!(res.is_none());
    }
}
