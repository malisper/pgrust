//! Unit tests for the pure (non-seam) helpers of the hash WAL redo port: the
//! hashutil.c splitpoint bit math, page-layout constants, and the offset/bitmap
//! arithmetic. The seam-dispatched redo handlers are exercised end-to-end by
//! the recovery integration once the buffer-manager / xlogutils owners land.

use super::*;

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
    assert_eq!(maxalign(SizeOfPageHeaderData), 24);
}

#[test]
fn pg_ceil_log2_32_matches_c() {
    assert_eq!(pg_ceil_log2_32(0), 0);
    assert_eq!(pg_ceil_log2_32(1), 0);
    assert_eq!(pg_ceil_log2_32(2), 1);
    assert_eq!(pg_ceil_log2_32(3), 2);
    assert_eq!(pg_ceil_log2_32(4), 2);
    assert_eq!(pg_ceil_log2_32(5), 3);
    assert_eq!(pg_ceil_log2_32(1024), 10);
    assert_eq!(pg_ceil_log2_32(1025), 11);
}

#[test]
fn pg_nextpower2_32_matches_c() {
    assert_eq!(pg_nextpower2_32(1), 1);
    assert_eq!(pg_nextpower2_32(2), 2);
    assert_eq!(pg_nextpower2_32(3), 4);
    assert_eq!(pg_nextpower2_32(5), 8);
    assert_eq!(pg_nextpower2_32(1024), 1024);
    assert_eq!(pg_nextpower2_32(1025), 2048);
}

#[test]
fn pg_leftmost_one_pos32_matches_c() {
    assert_eq!(pg_leftmost_one_pos32(1), 0);
    assert_eq!(pg_leftmost_one_pos32(2), 1);
    assert_eq!(pg_leftmost_one_pos32(3), 1);
    assert_eq!(pg_leftmost_one_pos32(0x80000000), 31);
}

#[test]
fn spareindex_first_groups_are_identity() {
    // For splitpoint groups < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE the spare
    // index equals pg_ceil_log2_32(num_bucket).
    for nb in [1u32, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
        assert_eq!(_hash_spareindex(nb), pg_ceil_log2_32(nb));
    }
}

#[test]
fn totalbuckets_inverts_initial_phases() {
    // In the single-phase region, total buckets == 2^phase.
    for phase in 0..HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        assert_eq!(_hash_get_totalbuckets(phase), 1 << phase);
    }
}

#[test]
fn meta_field_offsets_are_packed_as_c() {
    // Field offsets relative to PageGetContents (mirrors HashMetaPageData).
    assert_eq!(META_OFF_MAGIC, 0);
    assert_eq!(META_OFF_NTUPLES, 8);
    assert_eq!(META_OFF_MAXBUCKET, 24);
    assert_eq!(META_OFF_PROCID, 48);
    assert_eq!(META_OFF_SPARES, 52);
    // spares[HASH_MAX_SPLITPOINTS] then mapp[HASH_MAX_BITMAPS].
    assert_eq!(META_OFF_MAPP, 52 + HASH_MAX_SPLITPOINTS * 4);
}

#[test]
fn hash_max_splitpoints_value() {
    // ((32 - 10) * 4) + 10 == 98.
    assert_eq!(HASH_MAX_SPLITPOINTS, 98);
}

#[test]
fn hash_max_bitmaps_value() {
    // Min(8192 / 8, 1024) == 1024.
    assert_eq!(HASH_MAX_BITMAPS, 1024);
}

#[test]
fn bitmap_set_and_clear_bit_roundtrip() {
    let mut page = alloc::vec![0u8; BLCKSZ];
    bitmap_setbit(&mut page, 0);
    bitmap_setbit(&mut page, 33);
    bitmap_setbit(&mut page, 64);

    fn read(page: &[u8], n: u32) -> bool {
        let word = (n / BITS_PER_MAP) as usize;
        let bit = n % BITS_PER_MAP;
        let o = CONTENTS_OFFSET + word * 4;
        let v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
        (v & (1 << bit)) != 0
    }
    assert!(read(&page, 0));
    assert!(read(&page, 33));
    assert!(read(&page, 64));
    assert!(!read(&page, 1));

    bitmap_clrbit(&mut page, 33);
    assert!(!read(&page, 33));
    assert!(read(&page, 0));
}

#[test]
fn index_tuple_size_reads_size_bits() {
    // t_info is the uint16 at offset 6; size bits are the low 13.
    let mut itup = [0u8; 8];
    let t_info: u16 = 0xE000 | 24; // top flag bits set, size 24
    itup[6..8].copy_from_slice(&t_info.to_ne_bytes());
    assert_eq!(index_tuple_size(&itup), 24);
}

#[test]
fn opcode_dispatch_values_match_c() {
    assert_eq!(XLOG_HASH_INIT_META_PAGE, 0x00);
    assert_eq!(XLOG_HASH_VACUUM_ONE_PAGE, 0xC0);
}

#[test]
fn init_seams_installs_callbacks() {
    // Installing this unit's owned seams must not panic; the rmgr table
    // dispatches hash_redo / hash_mask through the now-installed seams.
    super::init_seams();
}
