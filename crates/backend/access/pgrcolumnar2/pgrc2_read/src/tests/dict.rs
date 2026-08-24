//! Dict-handle gates (§5 M3-F): lazy ensure-frame/ensure-code (fault-count
//! witnessed), the generation-stable payload-region pin (the StrView
//! zero-copy dependency), the varlena-shaped entry presentation pin
//! (StrView §7b — this crate's chartered pinning test), byte-rank order,
//! epoch semantics, and the publishability facts M3-G's O-6 posture reads.

use pgrc2_format::part::{SectionKind, StreamRole};
use pgrc2_format::wire::{crc32c, varlena_4b_u_payload_len};

use crate::dicthandle::DictHandle;
use crate::openpart::{FaultTag, OpenPart, PartExpect};
use crate::registry::{PartPin, PartRegistry};
use crate::testpart::{build_part, text_col, BuiltPart, DictSpec, PartSpec};
use crate::ReadError;

use super::open_built;

fn dict_entries(n: usize) -> Vec<Vec<u8>> {
    // Byte-rank-sorted by construction (zero-padded numeric suffix).
    (0..n).map(|i| format!("entry-{i:05}").into_bytes()).collect()
}

fn dict_part(entries: usize, nulls: bool) -> BuiltPart {
    let rows = 9_000u64;
    let mut col = text_col(1, rows, if nulls { Some(6) } else { None });
    col.dict = Some(DictSpec {
        entries: dict_entries(entries),
    });
    col.dict_exec_flag = true;
    build_part(&PartSpec::new(rows, vec![col]))
}

fn dict_stream_faults(part: &OpenPart) -> Vec<u8> {
    part.faults()
        .iter()
        .filter_map(|f| match f.tag {
            FaultTag::StreamExtent { role, .. }
                if role == StreamRole::DictIndex.as_u8()
                    || role == StreamRole::DictPayload.as_u8() =>
            {
                Some(role)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn handle_open_is_lazy_lengths_fault_index_only_entries_fault_payload() {
    let b = dict_part(2500, false);
    let part = open_built(&b, 200, 1);
    let h = DictHandle::open(part.clone(), None, &[], 1, 0).expect("handle");
    assert_eq!(h.ncodes(), 2500);
    assert!(
        dict_stream_faults(&part).is_empty(),
        "handle open must fault no dict sections (lazy contract)"
    );
    // Stored lengths are a table lookup: INDEX only, no payload fault.
    let (bl, cl) = h.lengths(7).expect("lengths");
    assert_eq!(bl, b"entry-00007".len() as u32);
    assert_eq!(cl, 11);
    assert_eq!(
        dict_stream_faults(&part),
        vec![StreamRole::DictIndex.as_u8()],
        "lengths() faults the index stream only"
    );
    // First entry() faults the payload.
    let e = h.entry(7).expect("entry");
    assert_eq!(e.bytes, b"entry-00007");
    assert_eq!(
        dict_stream_faults(&part),
        vec![
            StreamRole::DictIndex.as_u8(),
            StreamRole::DictPayload.as_u8()
        ]
    );
    // SB-7 (M3-L3): the realized fault grain is the FRAME. entry(7)
    // faulted frame 0 only; frame 2 is a NEW fault; frame 0 re-ensures
    // are free.
    h.ensure_frame(h.frame_of_code(2499)).expect("frame");
    assert_eq!(
        dict_stream_faults(&part).len(),
        3,
        "a new frame is a new (single-extent) fault"
    );
    h.ensure_code(0).expect("code");
    h.entry(2499).expect("entry in the faulted frame");
    assert_eq!(
        dict_stream_faults(&part).len(),
        3,
        "resident frames re-ensure without faults"
    );
}

#[test]
fn frame_lazy_first_touch_bounds_residency() {
    // The q23-class lever (SB-7 stakes: 3.01/2.62/2.27GB payloads resident
    // whole on first touch in v3): first entry touch faults ONE frame's
    // extent (plus the header-bearing extent 0), never the whole payload.
    let b = dict_part(5000, false); // 5 frames at DICT_FRAME_ENTRIES=1024
    let part = open_built(&b, 210, 1);
    let h = DictHandle::open(part.clone(), None, &[], 1, 0).expect("handle");
    assert_eq!(h.frame_lazy_frames(), 5, "frame-lazy arm elected");
    assert_eq!(h.resident_payload_bytes(), 0);
    h.entry(7).expect("entry");
    let after_first = h.resident_payload_bytes();
    assert!(after_first > 0);
    // Touch the last frame: residency grows by one frame, still bounded.
    h.entry(4999).expect("entry");
    let after_last = h.resident_payload_bytes();
    assert!(after_first < after_last);
    // The fault log agrees: payload extents faulted are exactly {0, 4}
    // (collected BEFORE the whole-fault twin below pollutes the shared
    // part-level log).
    let mut ext: Vec<u32> = part
        .faults()
        .iter()
        .filter_map(|f| match f.tag {
            FaultTag::StreamExtent { role, extent, .. }
                if role == StreamRole::DictPayload.as_u8() =>
            {
                Some(extent)
            }
            _ => None,
        })
        .collect();
    ext.sort_unstable();
    ext.dedup();
    assert_eq!(ext, vec![0, 4], "exactly the touched frames fault");
    // Whole-payload size: fault everything on a twin handle and compare.
    let twin = DictHandle::open_with_mode(
        part.clone(),
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::WholeFault,
    )
    .expect("twin");
    twin.entry(0).expect("entry");
    let whole = twin.resident_payload_bytes();
    assert!(
        after_last < whole,
        "partial residency {after_last} must stay BELOW the whole region {whole}"
    );
}

#[test]
fn frame_lazy_entries_are_byte_identical_and_generation_stable_across_frames() {
    // Frame-grain faulting must change WHEN bytes arrive, never WHAT they
    // are or WHERE they live: entries equal the whole-fault twin
    // byte-for-byte, and pointers taken before later frame faults never
    // move.
    let b = dict_part(3000, false);
    let part = open_built(&b, 211, 1);
    let lazy = DictHandle::open(part.clone(), None, &[], 1, 0).expect("lazy");
    let whole = DictHandle::open_with_mode(
        part,
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::WholeFault,
    )
    .expect("whole");
    let early = lazy.entry(3).expect("entry");
    let early_ptr = early.image.as_ptr();
    let early_bytes = early.bytes.to_vec();
    for code in [0u32, 3, 1023, 1024, 2047, 2048, 2999] {
        let a = lazy.entry(code).expect("lazy entry");
        let bref = whole.entry(code).expect("whole entry");
        assert_eq!(a.bytes, bref.bytes, "code {code} byte identity across arms");
        assert_eq!(a.byte_len, bref.byte_len);
    }
    // Generation stability across later frame faults (StrView §7b).
    let again = lazy.entry(3).expect("entry again");
    assert_eq!(again.image.as_ptr(), early_ptr, "pointer never moves");
    assert_eq!(again.bytes, &early_bytes[..]);
}

#[test]
fn whole_fault_kill_switch_reproduces_the_pre_fix_arm() {
    // The born-RED direction of the residency probe: the WholeFault arm
    // (PGRUST_PGRC2_FRAME_LAZY=0 / open_with_mode) faults the ENTIRE
    // payload on first touch — the difference the probe witnesses.
    let b = dict_part(5000, false);
    let part = open_built(&b, 212, 1);
    let h = DictHandle::open_with_mode(
        part.clone(),
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::WholeFault,
    )
    .expect("handle");
    assert_eq!(h.frame_lazy_frames(), 0, "whole-fault arm elected");
    h.entry(0).expect("entry");
    let resident = h.resident_payload_bytes();
    // All 5 payload extents fault on the first touch.
    let payload_faults = part
        .faults()
        .iter()
        .filter(|f| {
            matches!(f.tag, FaultTag::StreamExtent { role, .. }
                if role == StreamRole::DictPayload.as_u8())
        })
        .count();
    assert_eq!(payload_faults, 5, "whole-fault arm faults every frame extent");
    // And the frame-lazy arm on the same part stays strictly below it.
    let lazy = DictHandle::open(part, None, &[], 1, 0).expect("lazy");
    lazy.entry(0).expect("entry");
    assert!(lazy.resident_payload_bytes() < resident);
}

#[test]
fn entry_presentation_is_varlena_shaped_and_8_aligned_on_the_copy_arm() {
    // The StrView §7b STRICT-form pin, scoped to the frame-lazy COPY arm
    // (the `PGRUST_PGRC2_FRAME_DIRECT=0` kill-switch arm): a dict entry is
    // presented as a valid PG datum image — 4B-U header, 8-aligned pointer,
    // payload bytes exactly the stored image. The frame-direct production
    // default forfeits ONLY the 8-align clause (next test).
    let b = dict_part(1200, false);
    let part = open_built(&b, 201, 1);
    let h = DictHandle::open_with_arms(
        part,
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::FrameLazy,
        crate::dicthandle::DictFrameServe::Copy,
    )
    .expect("handle");
    for code in [0u32, 1, 511, 1023, 1024, 1199] {
        let e = h.entry(code).expect("entry");
        assert_eq!(
            e.image.as_ptr() as usize % 8,
            0,
            "entry image 8-aligned (spec §1)"
        );
        let header = u32::from_le_bytes(e.image[..4].try_into().expect("4"));
        let plen = varlena_4b_u_payload_len(header, "§7b").expect("valid 4B-U header");
        assert_eq!(plen as usize, e.bytes.len());
        assert_eq!(&e.image[4..], e.bytes, "image = header + payload");
        assert_eq!(e.byte_len as usize, e.bytes.len());
    }
}

#[test]
fn frame_direct_entries_are_varlena_shaped_byte_identical_and_stable() {
    // The frame-direct (production default) presentation pin: entries are
    // valid 4B-U varlena images byte-identical to the copy arm's, with
    // generation-stable pointers — everything of §7b EXCEPT the 8-align
    // clause, which this arm forfeits by design (frame cuts land at the
    // unpadded end of the previous frame; consumers are byte-addressable
    // per the 2026-08-18 audit).
    let b = dict_part(3000, false);
    let part = open_built(&b, 214, 1);
    let direct = DictHandle::open_with_arms(
        part.clone(),
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::FrameLazy,
        crate::dicthandle::DictFrameServe::Direct,
    )
    .expect("direct");
    let copy = DictHandle::open_with_arms(
        part,
        None,
        &[],
        1,
        0,
        crate::dicthandle::DictFaultMode::FrameLazy,
        crate::dicthandle::DictFrameServe::Copy,
    )
    .expect("copy");
    let early = direct.entry(3).expect("entry");
    let early_ptr = early.image.as_ptr();
    for code in [0u32, 3, 1023, 1024, 2047, 2048, 2999] {
        let d = direct.entry(code).expect("direct entry");
        let c = copy.entry(code).expect("copy entry");
        assert_eq!(d.image, c.image, "code {code} image identity across arms");
        assert_eq!(d.bytes, c.bytes, "code {code} byte identity across arms");
        assert_eq!(d.byte_len, c.byte_len);
        assert_eq!(d.char_len, c.char_len);
        let header = u32::from_le_bytes(d.image[..4].try_into().expect("4"));
        let plen = varlena_4b_u_payload_len(header, "§7b").expect("valid 4B-U header");
        assert_eq!(plen as usize, d.bytes.len());
    }
    // Generation stability across later frame faults holds on this arm
    // (SegBufs are write-once in per-extent OnceLocks, handle-lifetime).
    let again = direct.entry(3).expect("entry again");
    assert_eq!(again.image.as_ptr(), early_ptr, "pointer never moves");
}

#[test]
fn payload_region_is_generation_stable() {
    // The zero-copy dependency: entry pointers NEVER move for the handle's
    // life — across registry churn, budget-zero eviction, and re-opens of
    // the same part elsewhere.
    let b = dict_part(800, false);
    let reg = PartRegistry::new(u64::MAX);
    let pin = reg
        .open_pinned(&PartExpect::none(), {
            let io = b.mem_io(5, 60);
            move || Ok(Box::new(io) as Box<dyn crate::io::PartIo>)
        })
        .expect("open");
    let part = pin.part().clone();
    let h = DictHandle::open(part.clone(), Some(pin), &[], 1, 0).expect("handle");
    let p0 = h.entry(5).expect("entry").image.as_ptr();
    let b0 = h.entry(5).expect("entry").bytes.to_vec();
    // Churn: other parts in and out, budget to zero, maintain.
    for i in 0..5u64 {
        let other = reg
            .open_pinned(&PartExpect::none(), {
                let io = b.mem_io(5, 100 + i);
                move || Ok(Box::new(io) as Box<dyn crate::io::PartIo>)
            })
            .expect("churn open");
        other.part().prefault_all_sections().expect("warm");
    }
    reg.set_budget(0);
    reg.maintain();
    let p1 = h.entry(5).expect("entry").image.as_ptr();
    assert_eq!(p0, p1, "generation-stable payload region pin");
    assert_eq!(h.entry(5).expect("entry").bytes, &b0[..], "bytes stable");
}

#[test]
fn byte_rank_order_is_contractual() {
    let b = dict_part(600, false);
    let part = open_built(&b, 202, 1);
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    let mut prev: Option<Vec<u8>> = None;
    for code in 0..h.ncodes() {
        let e = h.entry(code).expect("entry");
        if let Some(p) = &prev {
            assert!(
                p[..] < e.bytes[..],
                "global code order == byte order (spec §7)"
            );
        }
        prev = Some(e.bytes.to_vec());
    }
}

#[test]
fn epoch_semantics() {
    let b = dict_part(64, false);
    // Same open-part instance ⇒ same epoch; distinct instances ⇒ distinct
    // epochs (conservative — Law A).
    let part1 = open_built(&b, 203, 1);
    let h1a = DictHandle::open(part1.clone(), None, &[], 1, 0).expect("h1a");
    let h1b = DictHandle::open(part1.clone(), None, &[], 1, 0).expect("h1b");
    assert_eq!(h1a.epoch64(), h1b.epoch64(), "one instance, one epoch");
    let part2 = open_built(&b, 203, 1);
    let h2 = DictHandle::open(part2, None, &[], 1, 0).expect("h2");
    assert_ne!(
        h1a.epoch64(),
        h2.epoch64(),
        "separate instances mint separate epochs"
    );
    // The STRUCTURAL identity is equal for equal part identity (spec §7).
    assert_eq!(h1a.epoch_key(), h2.epoch_key());
    assert_ne!(h1a.epoch64(), 0, "0 is never a minted epoch");
}

#[test]
fn publishability_facts_for_the_o6_posture() {
    let clean = dict_part(64, false);
    let part = open_built(&clean, 204, 1);
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    assert!(h.exec_publishable(), "DICT_EXEC flag surfaces");
    assert!(h.column_all_valid(), "zero-null proof: no validity stream");
    assert!(h.byte_rank_sorted());

    let nulled = dict_part(64, true);
    let part = open_built(&nulled, 205, 1);
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    assert!(
        !h.column_all_valid(),
        "a nulled column cannot present the zero-null proof"
    );
}

#[test]
fn code_bounds_and_corrupt_index_are_typed() {
    let b = dict_part(64, false);
    let part = open_built(&b, 206, 1);
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    assert!(matches!(
        h.entry(64).expect_err("oob code"),
        ReadError::Format(_)
    ));
    assert!(matches!(
        h.ensure_code(9999).expect_err("oob ensure"),
        ReadError::Format(_)
    ));
    // Corrupt a stored byte_len (index entry field) with a fixed CRC chain:
    // the cross-check against the payload image must refuse typed.
    let mut bytes = b.bytes.clone();
    let idx_sec = b
        .find_stream_section(1, 0, StreamRole::DictIndex)
        .expect("index section");
    let s = b.sections[idx_sec];
    // Entry 3's byte_len lives at section payload + 3*12 + 4; payload
    // starts at the 32-B stream-section header.
    let field = s.off as usize + 32 + 3 * 12 + 4;
    let bad = (b"entry-00003".len() as u32 + 2).to_le_bytes();
    bytes[field..field + 4].copy_from_slice(&bad);
    // Fix the section CRC in the section table + chain (mirrors refusal.rs).
    let fo = b.footer_off as usize;
    let st_off = u64::from_le_bytes(bytes[fo + 32..fo + 40].try_into().expect("8")) as usize;
    let e = st_off + idx_sec * 32;
    let crc = crc32c(&bytes[s.off as usize..(s.off + s.len) as usize]);
    bytes[e + 28..e + 32].copy_from_slice(&crc.to_le_bytes());
    let count = u32::from_le_bytes(bytes[fo + 24..fo + 28].try_into().expect("4")) as usize;
    let st_crc = crc32c(&bytes[st_off..st_off + count * 32]);
    bytes[fo + 40..fo + 44].copy_from_slice(&st_crc.to_le_bytes());
    let f_crc = crc32c(&bytes[fo..fo + 92]);
    bytes[fo + 92..fo + 96].copy_from_slice(&f_crc.to_le_bytes());
    // The extent record CRC (inside StreamDir) also covers this section;
    // patch it too so the corruption is CRC-consistent and only the
    // SEMANTIC cross-check can catch it.
    let sd = b
        .find_section(SectionKind::StreamDir, 0, 0)
        .expect("stream dir");
    let sd_info = b.sections[sd];
    // Find the extent record whose file_off == s.off and fix its crc.
    let sd_bytes_start = sd_info.off as usize;
    let mut fixed_extent = false;
    let mut off = sd_bytes_start;
    while off + 40 <= (sd_info.off + sd_info.len) as usize {
        let fo64 = u64::from_le_bytes(bytes[off..off + 8].try_into().expect("8"));
        let len64 = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().expect("8"));
        if fo64 == s.off && len64 == s.len {
            bytes[off + 32..off + 36].copy_from_slice(&crc.to_le_bytes());
            fixed_extent = true;
        }
        off += 4; // scan on a 4-byte lattice; records are 8-aligned anyway
    }
    assert!(fixed_extent, "patched the extent record crc");
    // StreamDir section bytes changed: fix ITS crcs.
    let sd_crc = crc32c(&bytes[sd_bytes_start..(sd_info.off + sd_info.len) as usize]);
    let esd = st_off + sd * 32;
    bytes[esd + 28..esd + 32].copy_from_slice(&sd_crc.to_le_bytes());
    let st_crc = crc32c(&bytes[st_off..st_off + count * 32]);
    bytes[fo + 40..fo + 44].copy_from_slice(&st_crc.to_le_bytes());
    let f_crc = crc32c(&bytes[fo..fo + 92]);
    bytes[fo + 92..fo + 96].copy_from_slice(&f_crc.to_le_bytes());

    let part = std::sync::Arc::new(
        OpenPart::open(
            Box::new(crate::io::MemPartIo::new(bytes, 9, 9)),
            &PartExpect::none(),
        )
        .expect("open"),
    );
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    let e = h.entry(3).expect_err("byte_len cross-check");
    assert!(matches!(e, ReadError::Format(_)), "typed: {e}");
}

#[test]
fn in_range_code_with_out_of_range_offset_is_typed_not_panic() {
    // Finding idx 178: a hostile-but-checksummed dict index whose stored
    // payload OFFSET is in-range-by-code (code 3 < ncodes 64) yet points out
    // of range must refuse TYPED — never `.expect()`-panic when the scan
    // lane consumes it. This is the honesty the `ScanDictSpace`
    // prepare_frames entry-honesty gate leans on: it resolves every licensed
    // code via this same `entry(code)?`, so a corrupt offset surfaces as a
    // catchable corruption error at the fallible gate, not a panic in an
    // infallible face. (CRC-fix sequence mirrors
    // `code_bounds_and_corrupt_index_are_typed`.)
    let b = dict_part(64, false);
    let mut bytes = b.bytes.clone();
    let idx_sec = b
        .find_stream_section(1, 0, StreamRole::DictIndex)
        .expect("index section");
    let s = b.sections[idx_sec];
    // Entry 3's payload offset lives at section payload + 3*12 + 0; payload
    // starts at the 32-B stream-section header. Point it far out of range.
    let field = s.off as usize + 32 + 3 * 12;
    bytes[field..field + 4].copy_from_slice(&0x7FFF_FFFFu32.to_le_bytes());
    // Fix the section CRC in the section table + chain (mirrors refusal.rs).
    let fo = b.footer_off as usize;
    let st_off = u64::from_le_bytes(bytes[fo + 32..fo + 40].try_into().expect("8")) as usize;
    let e = st_off + idx_sec * 32;
    let crc = crc32c(&bytes[s.off as usize..(s.off + s.len) as usize]);
    bytes[e + 28..e + 32].copy_from_slice(&crc.to_le_bytes());
    let count = u32::from_le_bytes(bytes[fo + 24..fo + 28].try_into().expect("4")) as usize;
    let st_crc = crc32c(&bytes[st_off..st_off + count * 32]);
    bytes[fo + 40..fo + 44].copy_from_slice(&st_crc.to_le_bytes());
    let f_crc = crc32c(&bytes[fo..fo + 92]);
    bytes[fo + 92..fo + 96].copy_from_slice(&f_crc.to_le_bytes());
    // The extent record CRC (inside StreamDir) also covers this section;
    // patch it too so the corruption is CRC-consistent and only the SEMANTIC
    // bounds check can catch it.
    let sd = b
        .find_section(SectionKind::StreamDir, 0, 0)
        .expect("stream dir");
    let sd_info = b.sections[sd];
    let sd_bytes_start = sd_info.off as usize;
    let mut fixed_extent = false;
    let mut off = sd_bytes_start;
    while off + 40 <= (sd_info.off + sd_info.len) as usize {
        let fo64 = u64::from_le_bytes(bytes[off..off + 8].try_into().expect("8"));
        let len64 = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().expect("8"));
        if fo64 == s.off && len64 == s.len {
            bytes[off + 32..off + 36].copy_from_slice(&crc.to_le_bytes());
            fixed_extent = true;
        }
        off += 4;
    }
    assert!(fixed_extent, "patched the extent record crc");
    let sd_crc = crc32c(&bytes[sd_bytes_start..(sd_info.off + sd_info.len) as usize]);
    let esd = st_off + sd * 32;
    bytes[esd + 28..esd + 32].copy_from_slice(&sd_crc.to_le_bytes());
    let st_crc = crc32c(&bytes[st_off..st_off + count * 32]);
    bytes[fo + 40..fo + 44].copy_from_slice(&st_crc.to_le_bytes());
    let f_crc = crc32c(&bytes[fo..fo + 92]);
    bytes[fo + 92..fo + 96].copy_from_slice(&f_crc.to_le_bytes());

    let part = std::sync::Arc::new(
        OpenPart::open(
            Box::new(crate::io::MemPartIo::new(bytes, 9, 10)),
            &PartExpect::none(),
        )
        .expect("open"),
    );
    let h = DictHandle::open(part, None, &[], 1, 0).expect("handle");
    // The code is in range (below ncodes) yet its entry is dishonest: the
    // resolver must refuse TYPED, not panic.
    assert!(h.ncodes() > 3, "code 3 is in range");
    let err = h.entry(3).expect_err("out-of-range offset must refuse typed");
    assert!(matches!(err, ReadError::Format(_)), "typed corruption: {err}");
}

#[test]
fn pinned_handle_composes_with_the_registry() {
    let b = dict_part(64, false);
    let reg = PartRegistry::new(u64::MAX);
    let pin = reg
        .open_pinned(&PartExpect::none(), {
            let io = b.mem_io(6, 70);
            move || Ok(Box::new(io) as Box<dyn crate::io::PartIo>)
        })
        .expect("open");
    let key = {
        let i = pin.part().ident();
        (i.dev, i.ino, i.len)
    };
    let part = pin.part().clone();
    let h = DictHandle::open(part, Some(PartPin::pin(pin.part())), &[], 1, 0).expect("handle");
    drop(pin);
    // The handle's own pin keeps the entry resident under budget zero.
    reg.set_budget(0);
    reg.maintain();
    assert!(
        reg.get(key).is_some(),
        "a live dict handle pins its part in the registry"
    );
    drop(h);
    reg.maintain();
    assert!(reg.get(key).is_none(), "handle drop releases the pin");
}
