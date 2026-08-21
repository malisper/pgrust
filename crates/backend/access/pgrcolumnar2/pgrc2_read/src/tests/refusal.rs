//! The corruption/truncation refusal battery (§5 M3-F: "per-section CRC
//! refusal on every section kind (fuzz-seeded corruption ⇒ typed error)")
//! plus the structural and dispatch-time typed refusals. Every check here is
//! a tooth on a bounds/CRC/adjudication site in the read path; the seeded
//! byte-flip battery at the end is the M3-K read-fuzzer's PR-local floor
//! (no panic on hostile bytes, ever).

use std::panic::{catch_unwind, AssertUnwindSafe};

use pgrc2_format::abi::{ByteArena, DecodeOut};
use pgrc2_format::part::{SectionKind, StreamRole, SECTION_OPTIONAL};
use pgrc2_format::wire::crc32c;
use pgrc2_format::FormatError;

use crate::cursor::{reference_binding_leaked, StreamCursor};
use crate::io::MemPartIo;
use crate::openpart::{OpenPart, PartExpect};
use crate::testpart::{build_part, seq_i64_col, text_col, BuiltPart, PartSpec};
use crate::ReadError;

use super::{open_built, ArenaBuf, XorShift};

/// The all-kinds part for the CRC battery.
fn kinds_part() -> BuiltPart {
    let rows = 17_000u64;
    let mut a = text_col(1, rows, Some(4)); // validity stream
    a.with_stats = true;
    a.with_psma = true;
    a.with_bloom = true;
    a.with_ndv = true;
    let mut lane = text_col(1, rows, None);
    lane.path_ord = 1;
    let b = seq_i64_col(2, rows);
    let mut spec = PartSpec::new(rows, vec![a, lane, b]);
    spec.path_table = vec!["$.lane1".to_string()];
    spec.with_sort_key = true;
    spec.with_sidecar_dir = true;
    build_part(&spec)
}

fn open_bytes(bytes: Vec<u8>) -> Result<OpenPart, ReadError> {
    OpenPart::open(
        Box::new(MemPartIo::new(bytes, 7, 7)),
        &PartExpect::none(),
    )
}

// -- byte-patch helpers (fix the CRC chain after a deliberate edit) --------

fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().expect("len 8"))
}

fn put_at(b: &mut [u8], off: usize, v: &[u8]) {
    b[off..off + v.len()].copy_from_slice(v);
}

/// Recompute section crc (table slot `sidx`), the table crc, and the footer
/// crc after editing section bytes or the entry itself.
fn fix_crcs(bytes: &mut [u8], footer_off: usize, sidx: usize, recompute_section: bool) {
    let st_off = u64_at(bytes, footer_off + 32) as usize;
    let e = st_off + sidx * 32;
    if recompute_section {
        let off = u64_at(bytes, e) as usize;
        let len = u64_at(bytes, e + 8) as usize;
        let crc = crc32c(&bytes[off..off + len]);
        put_at(bytes, e + 28, &crc.to_le_bytes());
    }
    let count =
        u32::from_le_bytes(bytes[footer_off + 24..footer_off + 28].try_into().expect("4")) as usize;
    let st_crc = crc32c(&bytes[st_off..st_off + count * 32]);
    put_at(bytes, footer_off + 40, &st_crc.to_le_bytes());
    let f_crc = crc32c(&bytes[footer_off..footer_off + 92]);
    put_at(bytes, footer_off + 92, &f_crc.to_le_bytes());
}

/// Patch one field of stream-dir entry `slot`, fixing the CRC chain.
fn patch_stream_entry(b: &BuiltPart, slot: usize, field_off: usize, v: &[u8]) -> Vec<u8> {
    let mut bytes = b.bytes.clone();
    let sd = b
        .find_section(SectionKind::StreamDir, 0, 0)
        .expect("stream dir");
    let sd_off = b.sections[sd].off as usize;
    put_at(&mut bytes, sd_off + slot * 48 + field_off, v);
    fix_crcs(&mut bytes, b.footer_off as usize, sd, true);
    bytes
}

// -- the battery -----------------------------------------------------------

#[test]
fn crc_refusal_on_every_section_kind() {
    let b = kinds_part();
    let mut kinds_hit: Vec<u16> = Vec::new();
    for (idx, s) in b.sections.iter().enumerate() {
        if s.len == 0 {
            continue;
        }
        let mut bytes = b.bytes.clone();
        let mid = (s.off + s.len / 2) as usize;
        bytes[mid] ^= 0xA5;
        // Sections are lazy: the open path itself must still succeed…
        let part = open_bytes(bytes).expect("open survives lazy-section corruption");
        // …and the corrupted section must refuse typed on first fault.
        let e = part.section_bytes(idx).expect_err("corrupt section read");
        assert!(
            matches!(e, ReadError::Format(FormatError::CrcMismatch { .. })),
            "kind {} refused with {:?}, want CrcMismatch",
            s.kind,
            e
        );
        // Fault isolation: an untouched sibling still reads.
        let other = (idx + 1) % b.sections.len();
        if other != idx {
            part.section_bytes(other).expect("untouched section reads");
        }
        kinds_hit.push(s.kind);
    }
    for want in [
        SectionKind::StreamDir,
        SectionKind::Stream,
        SectionKind::PathTable,
        SectionKind::Stats,
        SectionKind::Psma,
        SectionKind::Bloom,
        SectionKind::NdvRegisters,
        SectionKind::SortKey,
        SectionKind::SidecarDir,
    ] {
        assert!(
            kinds_hit.contains(&want.as_u16()),
            "battery must cover kind {want:?}"
        );
    }
}

#[test]
fn corrupt_stream_extent_refuses_through_the_cursor() {
    let b = kinds_part();
    let vs = b
        .find_stream_section(2, 0, StreamRole::Values)
        .expect("values section");
    let s = b.sections[vs];
    let mut bytes = b.bytes.clone();
    bytes[(s.off + s.len / 2) as usize] ^= 0xA5;
    let part = std::sync::Arc::new(open_bytes(bytes).expect("open"));
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part, binding, 2, 0).expect("cursor open");
    let mut d = vec![0u64; 8192];
    let mut ab = ArenaBuf::new(1 << 20);
    let mut out = DecodeOut {
        datums: &mut d,
        arena: ByteArena::new(ab.bytes_mut()),
    };
    let e = cur.decode_full(0, &mut out).expect_err("corrupt extent");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::CrcMismatch { .. })
    ));
}

#[test]
fn truncation_battery() {
    let b = kinds_part();
    for cut in [
        0usize,
        100,
        175, // below the structural minimum
        b.bytes.len() - 8,
        b.footer_off as usize + 40,
        b.bytes.len() - 17, // tail straddled
    ] {
        let bytes = b.bytes[..cut.min(b.bytes.len())].to_vec();
        let e = open_bytes(bytes).expect_err("truncated part must refuse");
        assert!(matches!(e, ReadError::Format(_)), "typed refusal, got {e}");
    }
}

#[test]
fn tail_refusals() {
    let b = kinds_part();
    let n = b.bytes.len();
    // Bad magic.
    let mut bytes = b.bytes.clone();
    put_at(&mut bytes, n - 4, &0xDEAD_BEEFu32.to_le_bytes());
    assert!(matches!(
        open_bytes(bytes).expect_err("tail magic"),
        ReadError::Format(FormatError::BadMagic { .. })
    ));
    // footer_len echo.
    let mut bytes = b.bytes.clone();
    put_at(&mut bytes, n - 8, &95u32.to_le_bytes());
    assert!(matches!(
        open_bytes(bytes).expect_err("footer_len"),
        ReadError::Format(FormatError::Corrupt { .. })
    ));
    // footer_off out of bounds.
    let mut bytes = b.bytes.clone();
    put_at(&mut bytes, n - 16, &(u64::MAX - 7).to_le_bytes());
    assert!(matches!(
        open_bytes(bytes).expect_err("footer_off"),
        ReadError::Format(FormatError::Bounds { .. })
    ));
}

#[test]
fn footer_crc_and_geometry_echo_refusals() {
    let b = kinds_part();
    let fo = b.footer_off as usize;
    // Plain corruption.
    let mut bytes = b.bytes.clone();
    bytes[fo + 10] ^= 0xA5;
    assert!(matches!(
        open_bytes(bytes).expect_err("footer crc"),
        ReadError::Format(FormatError::CrcMismatch { .. })
    ));
    // Geometry echo with a VALID crc: the echo check must have its own
    // tooth beyond the checksum.
    let mut bytes = b.bytes.clone();
    let bad_gc = u32::from_le_bytes(bytes[fo + 16..fo + 20].try_into().expect("4")) + 1;
    put_at(&mut bytes, fo + 16, &bad_gc.to_le_bytes());
    let f_crc = crc32c(&bytes[fo..fo + 92]);
    put_at(&mut bytes, fo + 92, &f_crc.to_le_bytes());
    assert!(matches!(
        open_bytes(bytes).expect_err("geometry echo"),
        ReadError::Format(FormatError::Corrupt { .. })
    ));
}

#[test]
fn header_crc_and_echo_refusals() {
    let b = kinds_part();
    let mut bytes = b.bytes.clone();
    bytes[20] ^= 0xA5;
    assert!(matches!(
        open_bytes(bytes).expect_err("header crc"),
        ReadError::Format(FormatError::CrcMismatch { .. })
    ));
    // part_no echo with valid header crc.
    let mut bytes = b.bytes.clone();
    let bad_pn = u32::from_le_bytes(bytes[16..20].try_into().expect("4")) + 1;
    put_at(&mut bytes, 16, &bad_pn.to_le_bytes());
    let h_crc = crc32c(&bytes[..60]);
    put_at(&mut bytes, 60, &h_crc.to_le_bytes());
    assert!(matches!(
        open_bytes(bytes).expect_err("part_no echo"),
        ReadError::Format(FormatError::Corrupt { .. })
    ));
}

#[test]
fn section_table_refusals() {
    let b = kinds_part();
    let fo = b.footer_off as usize;
    let st_off = u64_at(&b.bytes, fo + 32) as usize;
    // Table corruption.
    let mut bytes = b.bytes.clone();
    bytes[st_off + 5] ^= 0xA5;
    assert!(matches!(
        open_bytes(bytes).expect_err("table crc"),
        ReadError::Format(FormatError::CrcMismatch { .. })
    ));
    // Unknown required kind refuses at open…
    let mut bytes = b.bytes.clone();
    put_at(&mut bytes, st_off + 16, &33u16.to_le_bytes());
    fix_crcs(&mut bytes, fo, 0, false);
    assert!(matches!(
        open_bytes(bytes).expect_err("unknown kind"),
        ReadError::Format(FormatError::UnknownSectionKind { kind: 33 })
    ));
    // …but an unknown OPTIONAL kind is skipped (forward compatibility) and
    // still CRC-validates on explicit read.
    let mut bytes = b.bytes.clone();
    put_at(&mut bytes, st_off + 16, &33u16.to_le_bytes());
    put_at(&mut bytes, st_off + 18, &SECTION_OPTIONAL.to_le_bytes());
    fix_crcs(&mut bytes, fo, 0, false);
    let part = open_bytes(bytes).expect("optional unknown kind opens");
    assert_eq!(part.section_kind(0), None, "adjudicated as skip");
    part.section_bytes(0).expect("optional section still reads");
    // Overlapping sections refuse structurally.
    let mut bytes = b.bytes.clone();
    let s0_off = u64_at(&b.bytes, st_off) ;
    put_at(&mut bytes, st_off + 32, &s0_off.to_le_bytes());
    fix_crcs(&mut bytes, fo, 1, false);
    let e = open_bytes(bytes).expect_err("overlap");
    assert!(
        matches!(
            e,
            ReadError::Format(FormatError::Corrupt { at: "section overlap" })
                | ReadError::Format(FormatError::CrcMismatch { .. })
        ),
        "got {e}"
    );
}

#[test]
fn open_expectation_mismatches_are_typed() {
    let b = kinds_part();
    let cases: Vec<(&'static str, PartExpect)> = vec![
        (
            "part_no",
            PartExpect {
                part_no: Some(b.spec.part_no + 1),
                ..PartExpect::none()
            },
        ),
        (
            "rows",
            PartExpect {
                rows: Some(b.spec.rows + 1),
                ..PartExpect::none()
            },
        ),
        (
            "file_len",
            PartExpect {
                file_len: Some(b.bytes.len() as u64 + 1),
                ..PartExpect::none()
            },
        ),
        (
            "footer_off",
            PartExpect {
                footer_off: Some(b.footer_off + 8),
                ..PartExpect::none()
            },
        ),
        (
            "schema_fingerprint",
            PartExpect {
                schema_fingerprint: Some(b.spec.schema_fingerprint ^ 1),
                ..PartExpect::none()
            },
        ),
        (
            "relfilenumber",
            PartExpect {
                relfilenumber: Some(b.spec.relfilenumber + 1),
                ..PartExpect::none()
            },
        ),
        (
            "spc/db",
            PartExpect {
                spc_db: Some((b.spec.spc, b.spec.db + 1)),
                ..PartExpect::none()
            },
        ),
    ];
    for (field, expect) in cases {
        let e = OpenPart::open(Box::new(b.mem_io(7, 7)), &expect)
            .err()
            .unwrap_or_else(|| panic!("{field} mismatch must refuse"));
        assert!(
            matches!(e, ReadError::OpenMismatch { .. }),
            "{field}: got {e}"
        );
    }
}

#[test]
fn dispatch_refusals_unknown_reserved_missing_wrapper_role_class() {
    let rows = 9_000u64;
    let b = build_part(&PartSpec::new(rows, vec![seq_i64_col(1, rows)]));
    let binding = reference_binding_leaked();
    // Slot 0 = the only stream (values). Field offsets per spec §6.3.
    let open_cursor = |bytes: Vec<u8>| {
        let part = std::sync::Arc::new(open_bytes(bytes).expect("open"));
        StreamCursor::open(part.clone(), binding, 1, 0).map(|c| (part, c))
    };
    // Unknown encoding id.
    let e = open_cursor(patch_stream_entry(&b, 0, 36, &0xBEEFu16.to_le_bytes()))
        .err()
        .expect("unknown encoding");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::UnknownEncoding { id: 0xBEEF })
    ));
    // FSST (12) is FIRST-CLASS in v4 (ledger SB-4; ENC_FSST_RESERVED
    // retired to historical): it RESOLVES, and on this word-class stream the
    // reference binding carries no (Fsst, word) kernel — KernelMissing, the
    // same binding-decides-coverage law as the arm below. The old
    // unknown-vs-reserved distinction now lives in the 13..=127 band
    // (UnknownEncoding), pinned in pgrc2_format tests/refusal.rs.
    let e = open_cursor(patch_stream_entry(&b, 0, 36, &12u16.to_le_bytes()))
        .err()
        .expect("fsst on word-class stream: kernel missing, not reserved");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::KernelMissing { encoding: 12, .. })
    ));
    // Assigned encoding with no kernel in this binding: KernelMissing (the
    // M3-C slot-in seam — binding, not reader, decides coverage).
    let e = open_cursor(patch_stream_entry(&b, 0, 36, &8u16.to_le_bytes()))
        .err()
        .expect("kernel missing");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::KernelMissing { encoding: 8, .. })
    ));
    // Wrapped stream without an unwrapper: typed refusal at cursor open,
    // BEFORE any payload fault.
    let bytes = patch_stream_entry(&b, 0, 43, &[1u8]);
    let part = std::sync::Arc::new(open_bytes(bytes).expect("open"));
    let before = part.faults().len();
    let e = StreamCursor::open(part.clone(), binding, 1, 0).expect_err("wrapper");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::WrapperUnsupported { wrapper: 1 })
    ));
    assert_eq!(
        part.faults().len(),
        before + 1, // StreamDir only
        "wrapper refusal must precede any payload fault"
    );
    // The Zstd twin (CMP-A): a binding without the arm refuses the SAME
    // typed way — the old-binary posture, arm id carried in the error.
    let bytes = patch_stream_entry(&b, 0, 43, &[2u8]);
    let part = std::sync::Arc::new(open_bytes(bytes).expect("open"));
    let before = part.faults().len();
    let e = StreamCursor::open(part.clone(), binding, 1, 0).expect_err("zstd wrapper");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::WrapperUnsupported { wrapper: 2 })
    ));
    assert_eq!(
        part.faults().len(),
        before + 1, // StreamDir only
        "zstd wrapper refusal must precede any payload fault"
    );
    // Unknown wrapper id: adjudication refuses typed before any fault.
    let e = open_cursor(patch_stream_entry(&b, 0, 43, &[3u8]))
        .err()
        .expect("unknown wrapper");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::Corrupt { at: "Wrapper" })
    ));
    // Unknown role byte.
    let e = open_cursor(patch_stream_entry(&b, 0, 40, &[9u8]))
        .err()
        .expect("role");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::UnknownStreamRole { role: 9 })
    ));
    // Unknown storage class byte.
    let e = open_cursor(patch_stream_entry(&b, 0, 41, &[6u8]))
        .err()
        .expect("class");
    assert!(matches!(
        e,
        ReadError::Format(FormatError::UnknownStorageClass { class: 6 })
    ));
    // Stream values vs extent sum mismatch.
    let e = open_cursor(patch_stream_entry(&b, 0, 8, &(rows + 1).to_le_bytes()))
        .err()
        .expect("values sum");
    assert!(matches!(e, ReadError::Format(FormatError::Corrupt { .. })));
}

#[test]
fn seeded_byte_flip_battery_never_panics() {
    // The M3-K read-fuzzer floor: hostile bytes anywhere in the part must
    // produce typed errors or correct data — never a panic, never UB.
    let b = kinds_part();
    let binding = reference_binding_leaked();
    let mut rng = XorShift(0xF0F0_1234_5678_9ABC);
    for i in 0..400u32 {
        let mut bytes = b.bytes.clone();
        let off = rng.below(bytes.len() as u64) as usize;
        bytes[off] ^= (1 + rng.below(255)) as u8;
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let Ok(part) = open_bytes(bytes) else {
                return;
            };
            let part = std::sync::Arc::new(part);
            let _ = part.prefault_all_sections();
            let Ok(mut cur) = StreamCursor::open(part.clone(), binding, 1, 0) else {
                return;
            };
            let mut d = vec![0u64; 8192];
            let mut ab = ArenaBuf::new(4 << 20);
            let mut out = DecodeOut {
                datums: &mut d,
                arena: ByteArena::new(ab.bytes_mut()),
            };
            let _ = cur.decode_full(0, &mut out);
            let mut mask = vec![0u64; 128];
            let _ = cur.validity(0, &mut mask);
        }));
        assert!(
            outcome.is_ok(),
            "panic on hostile bytes (iteration {i}, offset {off})"
        );
    }
}

#[test]
fn identical_bytes_reopen_is_deterministic() {
    // Equal identity ⇒ identical bytes ⇒ identical parse: two opens of the
    // same image agree on every open-time fact.
    let b = kinds_part();
    let p1 = open_built(&b, 7, 7);
    let p2 = open_built(&b, 7, 7);
    assert_eq!(p1.uuid(), p2.uuid());
    assert_eq!(p1.footer(), p2.footer());
    assert_eq!(p1.header(), p2.header());
    assert_eq!(p1.sections(), p2.sections());
}
