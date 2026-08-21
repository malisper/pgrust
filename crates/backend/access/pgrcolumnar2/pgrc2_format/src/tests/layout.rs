//! Layout pins (issue-#69 template): every on-disk struct's size is pinned
//! HERE so a change is a visible diff, never silent drift. Wire order ==
//! declaration order for all of these (spec, throughout); the `*_LEN`
//! constants are the wire sizes the encoders/decoders enforce.

use core::mem::{align_of, size_of};

use crate::dict::{DictIndexEntry, DICT_INDEX_ENTRY_LEN};
use crate::dml::{
    DvGranuleBlockHdr, DvHeader, DV_BITMAP_BYTES, DV_GRANULE_BLOCK_HDR_LEN, DV_HEADER_LEN,
};
use crate::geom::*;
use crate::manifest::{
    CommitPointer, ManifestHeader, PartRecord, COMMIT_POINTER_LEN, MANIFEST_HEADER_LEN,
    PART_RECORD_LEN,
};
use crate::meta::{
    BloomHdr, NdvRegistersHdr, PsmaEntry, StatsRecord, BLOOM_HDR_LEN, NDV_REGISTERS_HDR_LEN,
    PSMA_BLOCK_LEN, PSMA_ENTRIES, PSMA_ENTRY_LEN, STATS_RECORD_LEN,
};
use crate::part::{
    ExtentRecord, FooterFixed, PartHeader, PartTail, SectionEntry, StreamEntry, StreamSectionHdr,
    EXTENT_RECORD_LEN, FOOTER_FIXED_LEN, PART_HEADER_LEN, PART_TAIL_LEN, SECTION_ENTRY_LEN,
    STREAM_ENTRY_LEN, STREAM_SECTION_HDR_LEN,
};
use crate::rowid::*;
use crate::sidecar::{
    SidecarFileHeader, SidecarSlotRecord, SIDECAR_FILE_HEADER_LEN, SIDECAR_SLOT_RECORD_LEN,
};
use crate::sortkey::{SortKeyEntry, SORT_KEY_ENTRY_LEN};
use crate::wal::SealWalRecordShape;

#[test]
fn on_disk_struct_sizes_pinned() {
    // Part file (spec §5).
    assert_eq!(size_of::<PartHeader>(), PART_HEADER_LEN);
    assert_eq!(size_of::<PartHeader>(), 64);
    assert_eq!(size_of::<SectionEntry>(), SECTION_ENTRY_LEN);
    assert_eq!(size_of::<SectionEntry>(), 32);
    assert_eq!(size_of::<FooterFixed>(), FOOTER_FIXED_LEN);
    assert_eq!(size_of::<FooterFixed>(), 96);
    assert_eq!(size_of::<PartTail>(), PART_TAIL_LEN);
    assert_eq!(size_of::<PartTail>(), 16);
    // Stream directory (spec §6).
    assert_eq!(size_of::<StreamEntry>(), STREAM_ENTRY_LEN);
    assert_eq!(size_of::<StreamEntry>(), 48);
    assert_eq!(size_of::<ExtentRecord>(), EXTENT_RECORD_LEN);
    assert_eq!(size_of::<ExtentRecord>(), 40);
    assert_eq!(size_of::<StreamSectionHdr>(), STREAM_SECTION_HDR_LEN);
    assert_eq!(size_of::<StreamSectionHdr>(), 32);
    // Manifest + commit pointer (spec §13).
    assert_eq!(size_of::<ManifestHeader>(), MANIFEST_HEADER_LEN);
    assert_eq!(size_of::<ManifestHeader>(), 72);
    assert_eq!(size_of::<PartRecord>(), PART_RECORD_LEN);
    assert_eq!(size_of::<PartRecord>(), 64);
    assert_eq!(size_of::<CommitPointer>(), COMMIT_POINTER_LEN);
    assert_eq!(size_of::<CommitPointer>(), 32);
    // Metadata plane (spec §8).
    assert_eq!(size_of::<StatsRecord>(), STATS_RECORD_LEN);
    assert_eq!(size_of::<StatsRecord>(), 80);
    assert_eq!(align_of::<StatsRecord>(), 16); // i128 field
    assert_eq!(size_of::<PsmaEntry>(), PSMA_ENTRY_LEN);
    assert_eq!(PSMA_ENTRIES, 256);
    assert_eq!(PSMA_BLOCK_LEN, 1024);
    assert_eq!(size_of::<BloomHdr>(), BLOOM_HDR_LEN);
    assert_eq!(size_of::<NdvRegistersHdr>(), NDV_REGISTERS_HDR_LEN);
    // Dictionary (spec §7).
    assert_eq!(size_of::<DictIndexEntry>(), DICT_INDEX_ENTRY_LEN);
    assert_eq!(size_of::<DictIndexEntry>(), 12);
    // Tombstones/DV (spec §15).
    assert_eq!(size_of::<DvHeader>(), DV_HEADER_LEN);
    assert_eq!(size_of::<DvHeader>(), 32);
    assert_eq!(size_of::<DvGranuleBlockHdr>(), DV_GRANULE_BLOCK_HDR_LEN);
    assert_eq!(size_of::<DvGranuleBlockHdr>(), 8);
    assert_eq!(DV_BITMAP_BYTES, 1024);
    // Sidecars (spec §16).
    assert_eq!(size_of::<SidecarSlotRecord>(), SIDECAR_SLOT_RECORD_LEN);
    assert_eq!(size_of::<SidecarSlotRecord>(), 48);
    assert_eq!(size_of::<SidecarFileHeader>(), SIDECAR_FILE_HEADER_LEN);
    assert_eq!(size_of::<SidecarFileHeader>(), 48);
    // Sort key (spec §9).
    assert_eq!(size_of::<SortKeyEntry>(), SORT_KEY_ENTRY_LEN);
    assert_eq!(size_of::<SortKeyEntry>(), 8);
    // The reserved seal-WAL shape (spec §14) — paper, but its shape is
    // pinned so the reservation cannot drift silently.
    assert_eq!(size_of::<SealWalRecordShape>(), 64);
}

#[test]
fn geometry_constants_pinned() {
    // M0-S1/M0-S4 frozen geometry (spec §2). Reopen conditions live in the
    // spike docs — never edit these pins without a new banked verdict.
    assert_eq!(BATCH_ROWS, 1024);
    assert_eq!(FRAME_VALUES, 1024);
    assert_eq!(GRANULE_ROWS, 8192);
    assert_eq!(BAND_ROWS, 65_536);
    assert_eq!(FRAMES_PER_GRANULE, 8);
    assert_eq!(GRANULES_PER_BAND, 8);
    assert_eq!(DICT_FRAME_ENTRIES, 1024);
    assert_eq!(OVERSIZE_THRESHOLD, 32 * 1024);
    assert_eq!(EXTENT_MAX_LEN, u32::MAX as u64);
    // Closed forms.
    assert_eq!(granule_count(0), 0);
    assert_eq!(granule_count(1), 1);
    assert_eq!(granule_count(8192), 1);
    assert_eq!(granule_count(8193), 2);
    assert_eq!(band_count(65_536), 1);
    assert_eq!(band_count(65_537), 2);
    assert_eq!(rows_in_granule(8193, 0), 8192);
    assert_eq!(rows_in_granule(8193, 1), 1);
    assert_eq!(rows_in_granule(8193, 2), 0);
    assert_eq!(rows_in_band(100_000, 1), 100_000 - 65_536);
    assert_eq!(frames_in_granule(8192), 8);
    assert_eq!(frames_in_granule(1), 1);
    assert_eq!(values_in_frame(2500, 2), 452);
    assert_eq!(band_of_granule(7), 0);
    assert_eq!(band_of_granule(8), 1);
}

#[test]
fn grain_ladder_pinned_and_closed_forms_hold_at_every_grain() {
    // SB-10 (OD-13 RULED): the ladder, largest first; DEFAULT == max.
    assert_eq!(GRAIN_LADDER, [8192, 4096, 2048, 1024]);
    assert_eq!(GranuleGrain::DEFAULT.rows(), GRANULE_ROWS);
    assert!(GranuleGrain::DEFAULT.is_default());
    // The byte-bound constant is PROVISIONAL (QA-1 sizes the final value);
    // this pin makes any re-cut a visible diff, not a silent drift.
    assert_eq!(GRANULE_BYTE_BOUND_PROVISIONAL, 10 * 1024 * 1024);
    // Off-ladder grains refuse typed.
    assert!(GranuleGrain::from_rows(0).is_err());
    assert!(GranuleGrain::from_rows(512).is_err());
    assert!(GranuleGrain::from_rows(3000).is_err());
    assert!(GranuleGrain::from_rows(16_384).is_err());
    for &g in &GRAIN_LADDER {
        let grain = GranuleGrain::from_rows(g).expect("ladder grain");
        assert_eq!(grain.rows(), g);
        assert_eq!(grain.band_rows(), g * GRANULES_PER_BAND);
        assert_eq!(grain.frames_per_granule(), g / FRAME_VALUES);
        // Counts (exact fit, one-over, one-under).
        assert_eq!(granule_count_at(0, grain), 0);
        assert_eq!(granule_count_at(g as u64, grain), 1);
        assert_eq!(granule_count_at(g as u64 + 1, grain), 2);
        assert_eq!(granule_count_at(g as u64 - 1, grain), 1);
        assert_eq!(band_count_at(grain.band_rows() as u64, grain), 1);
        assert_eq!(band_count_at(grain.band_rows() as u64 + 1, grain), 2);
        // Last-short-granule math.
        let rows = 3 * g as u64 + 7;
        assert_eq!(granule_count_at(rows, grain), 4);
        assert_eq!(rows_in_granule_at(rows, grain, 0), g);
        assert_eq!(rows_in_granule_at(rows, grain, 2), g);
        assert_eq!(rows_in_granule_at(rows, grain, 3), 7);
        assert_eq!(rows_in_granule_at(rows, grain, 4), 0);
        // Band math (band = 8 granules of THIS grain).
        let band_rows = grain.band_rows() as u64;
        let rows2 = band_rows + 13;
        assert_eq!(band_count_at(rows2, grain), 2);
        assert_eq!(rows_in_band_at(rows2, grain, 0), grain.band_rows());
        assert_eq!(rows_in_band_at(rows2, grain, 1), 13);
        assert_eq!(rows_in_band_at(rows2, grain, 2), 0);
        // Frames at this grain.
        assert_eq!(frames_in_granule_at(g, grain), g / FRAME_VALUES);
        assert_eq!(frames_in_granule_at(1, grain), 1);
    }
    // The default forms delegate to the `_at` forms exactly.
    assert_eq!(granule_count(100_000), granule_count_at(100_000, GranuleGrain::DEFAULT));
    assert_eq!(band_count(100_000), band_count_at(100_000, GranuleGrain::DEFAULT));
    assert_eq!(
        rows_in_granule(8193, 1),
        rows_in_granule_at(8193, GranuleGrain::DEFAULT, 1)
    );
}

#[test]
fn rowid_round_trips_at_ladder_grains() {
    // The split never moves: pack_rowid_at packs identically at every
    // grain; extraction is grain-independent (totality, AB-5.2).
    for &g in &[1024u32, 8192] {
        let grain = GranuleGrain::from_rows(g).expect("ladder grain");
        for &(part, gran, row) in &[(0u32, 0u32, 0u32), (7, 3, g - 1), (u32::MAX, 511, 1)] {
            let id = pack_rowid_at(part, gran, row, grain);
            assert_eq!(id, pack_rowid(part, gran, row));
            assert_eq!(rowid_part(id), part);
            assert_eq!(rowid_granule(id), gran);
            assert_eq!(rowid_row(id), row);
        }
    }
    // Order-embedding within a part holds at the small grain too: granule
    // advances dominate row ordinals regardless of grain.
    let grain = GranuleGrain::from_rows(1024).expect("ladder grain");
    assert!(pack_rowid_at(5, 3, 1023, grain) < pack_rowid_at(5, 4, 0, grain));
}

#[test]
fn footer_carries_the_grain_and_validates_it() {
    // SB-10: the footer's granule_rows field (the v3 pad slot) round-trips
    // at every ladder grain, and the geometry echo is checked AT THAT GRAIN.
    let rows: u64 = 20_000;
    for &g in &GRAIN_LADDER {
        let grain = GranuleGrain::from_rows(g).expect("ladder grain");
        let mut f = FooterFixed {
            magic: crate::part::FOOTER_MAGIC,
            format_version: crate::FORMAT_VERSION,
            rows,
            granule_count: granule_count_at(rows, grain),
            band_count: band_count_at(rows, grain),
            section_count: 0,
            flags: 0,
            section_table_off: 64,
            section_table_crc: 0,
            part_no: 3,
            schema_fingerprint: 0xF00D,
            stream_count: 0,
            granule_rows: g,
            reserved: [0; 28],
            footer_crc: 0,
        };
        let mut enc = Vec::new();
        f.encode_into(&mut enc);
        let back = FooterFixed::decode(&enc).expect("footer decodes at ladder grain");
        f.footer_crc = back.footer_crc;
        assert_eq!(back, f);
        assert_eq!(back.grain().expect("valid grain").rows(), g);
    }
    // Off-ladder grain refuses typed (re-encoded so the CRC is valid — the
    // grain check itself must fire, not the checksum).
    let grain = GranuleGrain::DEFAULT;
    let mut bad = FooterFixed {
        magic: crate::part::FOOTER_MAGIC,
        format_version: crate::FORMAT_VERSION,
        rows,
        granule_count: granule_count_at(rows, grain),
        band_count: band_count_at(rows, grain),
        section_count: 0,
        flags: 0,
        section_table_off: 64,
        section_table_crc: 0,
        part_no: 3,
        schema_fingerprint: 0xF00D,
        stream_count: 0,
        granule_rows: 4096, // echoes below are DEFAULT-grain: mismatch
        reserved: [0; 28],
        footer_crc: 0,
    };
    let mut enc = Vec::new();
    bad.encode_into(&mut enc);
    assert!(FooterFixed::decode(&enc).is_err(), "grain/echo skew refuses");
    bad.granule_rows = 512; // not on the ladder at all
    let mut enc2 = Vec::new();
    bad.encode_into(&mut enc2);
    assert_eq!(
        FooterFixed::decode(&enc2),
        Err(crate::FormatError::Corrupt {
            at: "granule grain not on the ladder",
        })
    );
}

#[test]
fn rowid_bit_budget_pinned() {
    // The 10B-row law (M3-A exit slice): degenerate one-band-per-part still
    // addresses ≥ 10^10 rows; the const asserts in rowid.rs are the
    // compile-time enforcement, these are the readable witnesses.
    assert_eq!(ROWID_PART_BITS + ROWID_GRANULE_BITS + ROWID_ROW_BITS, 64);
    assert_eq!(1u64 << ROWID_ROW_BITS, GRANULE_ROWS as u64);
    assert!((1u128 << ROWID_PART_BITS) * BAND_ROWS as u128 >= 10_000_000_000);
    assert_eq!(
        (1u128 << ROWID_GRANULE_BITS) * GRANULE_ROWS as u128,
        1u128 << 32
    );
    // Order-embedding within a part.
    assert!(pack_rowid(1, 0, 0) > pack_rowid(0, u32::MAX >> 13, 8191));
    assert!(pack_rowid(5, 3, 100) < pack_rowid(5, 3, 101));
    assert!(pack_rowid(5, 3, 8191) < pack_rowid(5, 4, 0));
    // Pack/unpack inverse.
    let id = pack_rowid(0xDEAD_BEEF, 0x7_FFFF, 0x1FFF);
    assert_eq!(rowid_part(id), 0xDEAD_BEEF);
    assert_eq!(rowid_granule(id), 0x7_FFFF);
    assert_eq!(rowid_row(id), 0x1FFF);
    // DV-local u32 packing (spec §15).
    let l = dv_local(0x7_FFFF, 0x1FFF);
    assert_eq!(l, u32::MAX);
    assert_eq!(dv_local_granule(l), 0x7_FFFF);
    assert_eq!(dv_local_row(l), 0x1FFF);
}

#[cfg(target_pointer_width = "64")]
mod abi_pins {
    use core::mem::size_of;

    use crate::abi::{CodecVtable, KernelKey};

    #[test]
    fn abi_struct_sizes_pinned() {
        // 64-bit layout pins; wasm32 (ILP32) shrinks fn pointers.
        assert_eq!(size_of::<KernelKey>(), 4);
        // key (4) + padding (4) + six fn pointers.
        assert_eq!(size_of::<CodecVtable>(), 56);
    }
}
