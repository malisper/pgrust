use super::*;

const NIBBLE_ONES: [u8; 16] = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4];

#[test]
fn number_of_ones_table() {
    for b in 0..256usize {
        assert_eq!(
            PG_NUMBER_OF_ONES[b],
            NIBBLE_ONES[b >> 4] + NIBBLE_ONES[b & 15],
            "byte {b}"
        );
    }
}

#[test]
fn word_popcounts() {
    assert_eq!(pg_popcount32(0), 0);
    assert_eq!(pg_popcount32(1), 1);
    assert_eq!(pg_popcount32(0x8000_0001), 2);
    assert_eq!(pg_popcount32(u32::MAX), 32);
    assert_eq!(pg_popcount64(0), 0);
    assert_eq!(pg_popcount64(0x8000_0000_0000_0001), 2);
    assert_eq!(pg_popcount64(u64::MAX), 64);
}

#[test]
fn one_positions() {
    for i in 0..32 {
        assert_eq!(pg_leftmost_one_pos32(1u32 << i), i as i32);
        assert_eq!(pg_rightmost_one_pos32(1u32 << i), i as i32);
        if i > 0 {
            assert_eq!(pg_leftmost_one_pos32((1u32 << i) | 1), i as i32);
            assert_eq!(pg_rightmost_one_pos32((1u32 << i) | (1u32 << (i - 1))), (i - 1) as i32);
        }
    }
    for i in 0..64 {
        assert_eq!(pg_leftmost_one_pos64(1u64 << i), i as i32);
        assert_eq!(pg_rightmost_one_pos64(1u64 << i), i as i32);
        if i > 0 {
            assert_eq!(pg_leftmost_one_pos64((1u64 << i) | 1), i as i32);
        }
    }
}

#[test]
fn powers_and_logs() {
    for (num, next) in [
        (1u32, 1u32),
        (2, 2),
        (3, 4),
        (4, 4),
        (5, 8),
        (7, 8),
        (8, 8),
        (9, 16),
        (1023, 1024),
        (1024, 1024),
        (1025, 2048),
        (0x4000_0000, 0x4000_0000),
        (0x4000_0001, 0x8000_0000),
        (0x8000_0000, 0x8000_0000),
    ] {
        assert_eq!(pg_nextpower2_32(num), next, "nextpower2_32({num})");
        assert_eq!(pg_nextpower2_64(num as u64), next as u64);
    }
    assert_eq!(pg_nextpower2_64(0x4000_0000_0000_0001), 0x8000_0000_0000_0000);
    assert_eq!(pg_prevpower2_32(1), 1);
    assert_eq!(pg_prevpower2_32(3), 2);
    assert_eq!(pg_prevpower2_32(1025), 1024);
    assert_eq!(pg_prevpower2_64(u64::MAX), 1u64 << 63);
    for (num, log) in [(0u32, 0u32), (1, 0), (2, 1), (3, 2), (4, 2), (5, 3), (1024, 10), (1025, 11)] {
        assert_eq!(pg_ceil_log2_32(num), log, "ceil_log2_32({num})");
        assert_eq!(pg_ceil_log2_64(num as u64), log as u64);
    }
}

#[test]
fn rotates() {
    assert_eq!(pg_rotate_right32(0x8000_0001, 1), 0xC000_0000);
    assert_eq!(pg_rotate_left32(0x8000_0001, 1), 0x0000_0003);
    assert_eq!(pg_rotate_right32(0x1234_5678, 8), 0x7812_3456);
    assert_eq!(pg_rotate_left32(0x1234_5678, 8), 0x3456_7812);
}

#[test]
fn popcount_fixed_vectors() {
    assert_eq!(pg_popcount(&[]), 0);
    assert_eq!(pg_popcount(&[0u8; 200]), 0);
    assert_eq!(pg_popcount(&[0xFFu8; 200]), 1600);
    assert_eq!(pg_popcount(&[0x01u8; 129]), 129);
    assert_eq!(pg_popcount(&[0xF0u8; 65]), 260);
    assert_eq!(pg_popcount_masked(&[0xFFu8; 200], 0x55), 800);
    assert_eq!(pg_popcount_masked(&[0xFFu8; 200], 0x00), 0);
    assert_eq!(pg_popcount_masked(&[0xAAu8; 129], 0x55), 0);
    assert_eq!(pg_popcount_masked(&[0xAAu8; 129], 0xAA), 516);
}

// Table-summed expectation is independent of the SIMD path (table pinned by
// number_of_ones_table above).
#[test]
fn popcount_every_boundary() {
    let mut buf = [0u8; 300];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i.wrapping_mul(7).wrapping_add(3) & 0xFF) as u8;
    }
    for len in 0..=300 {
        let s = &buf[..len];
        let expect: u64 = s.iter().map(|&b| PG_NUMBER_OF_ONES[b as usize] as u64).sum();
        assert_eq!(pg_popcount(s), expect, "len {len}");
        for mask in [0x01u8, 0x55, 0xAA, 0xFF] {
            let expect_m: u64 = s
                .iter()
                .map(|&b| PG_NUMBER_OF_ONES[(b & mask) as usize] as u64)
                .sum();
            assert_eq!(pg_popcount_masked(s, mask), expect_m, "len {len} mask {mask:#x}");
        }
    }
}

#[test]
fn popcount_unaligned_starts() {
    let mut buf = [0u8; 8300];
    let mut s = 0x243F_6A88_85A3_08D3u64;
    for b in buf.iter_mut() {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *b = (s >> 56) as u8;
    }
    for off in 0..16 {
        let sl = &buf[off..off + 8192];
        let expect: u64 = sl.iter().map(|&b| PG_NUMBER_OF_ONES[b as usize] as u64).sum();
        assert_eq!(pg_popcount(sl), expect, "off {off}");
        let expect_m: u64 = sl
            .iter()
            .map(|&b| PG_NUMBER_OF_ONES[(b & 0x55) as usize] as u64)
            .sum();
        assert_eq!(pg_popcount_masked(sl, 0x55), expect_m, "off {off}");
    }
}
