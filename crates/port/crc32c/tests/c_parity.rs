mod c_vectors {
    include!("data/c_vectors.rs");
}
use c_vectors::COMP_CRC32C;

#[repr(align(16))]
struct Aligned([u8; 8256]);

fn fill_buf() -> Box<Aligned> {
    let mut prng: u64 = 0x243f6a8885a308d3;
    let mut buf = Box::new(Aligned([0; 8256]));
    for b in buf.0.iter_mut() {
        prng = prng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *b = (prng >> 56) as u8;
    }
    buf
}

#[test]
fn sb8_matches_c() {
    let buf = fill_buf();
    assert!(COMP_CRC32C.len() >= 77 * 8 * 2);
    for &(len, off, seed, expect) in COMP_CRC32C {
        assert_eq!(
            crc32c::pg_comp_crc32c_sb8(seed, &buf.0[off..off + len]),
            expect,
            "len={len} off={off} seed={seed:#x}"
        );
    }
}

#[test]
fn dispatch_matches_c() {
    let buf = fill_buf();
    for &(len, off, seed, expect) in COMP_CRC32C {
        assert_eq!(
            crc32c::pg_comp_crc32c(seed, &buf.0[off..off + len]),
            expect,
            "len={len} off={off} seed={seed:#x}"
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn sse42_matches_c() {
    assert!(
        std::arch::is_x86_feature_detected!("sse4.2"),
        "x86_64 host without SSE4.2: hardware CRC gate cannot run"
    );
    let buf = fill_buf();
    for &(len, off, seed, expect) in COMP_CRC32C {
        // SAFETY: SSE4.2 presence asserted above.
        let got = unsafe { crc32c::pg_comp_crc32c_sse42(seed, &buf.0[off..off + len]) };
        assert_eq!(got, expect, "len={len} off={off} seed={seed:#x}");
    }
    for off in 0..16 {
        for len in 0..=64 {
            let s = &buf.0[off..off + len];
            // SAFETY: SSE4.2 presence asserted above.
            let got = unsafe { crc32c::pg_comp_crc32c_sse42(crc32c::CRC32C_INIT, s) };
            assert_eq!(got, crc32c::pg_comp_crc32c_sb8(crc32c::CRC32C_INIT, s));
        }
    }
}

fn crc32c_oneshot(f: impl Fn(u32, &[u8]) -> u32, data: &[u8]) -> u32 {
    crc32c::fin_crc32c(f(crc32c::CRC32C_INIT, data))
}

// RFC 3720 B.4 test cases plus the classic check value.
#[test]
fn rfc3720_vectors() {
    let mut iscsi_read = [0u8; 48];
    iscsi_read[0] = 0x01;
    iscsi_read[1] = 0xc0;
    iscsi_read[16] = 0x14;
    iscsi_read[22] = 0x04;
    iscsi_read[27] = 0x14;
    iscsi_read[31] = 0x18;
    iscsi_read[32] = 0x28;
    iscsi_read[40] = 0x02;

    let mut ascending = [0u8; 32];
    let mut descending = [0u8; 32];
    for i in 0..32 {
        ascending[i] = i as u8;
        descending[i] = (31 - i) as u8;
    }

    let cases: &[(&[u8], u32)] = &[
        (b"", 0x0000_0000),
        (b"123456789", 0xE306_9283),
        (b"iSCSI", 0x54AA_B3D4),
        (&[0u8; 32], 0x8A91_36AA),
        (&[0xFFu8; 32], 0x62A8_AB43),
        (&ascending, 0x46DD_794E),
        (&descending, 0x113F_DB5C),
        (&iscsi_read, 0xD996_3A56),
    ];
    for &(data, expect) in cases {
        assert_eq!(crc32c_oneshot(crc32c::pg_comp_crc32c_sb8, data), expect);
        assert_eq!(crc32c_oneshot(crc32c::pg_comp_crc32c, data), expect);
    }
}

// Every length 0..=64 at every offset 0..16: the dispatched path (hardware
// where the host has it) must agree with sb8 bit-for-bit.
#[test]
fn all_short_lengths_dispatch_matches_sb8() {
    let buf = fill_buf();
    for off in 0..16 {
        for len in 0..=64 {
            let s = &buf.0[off..off + len];
            for seed in [crc32c::CRC32C_INIT, 0, 0xDEAD_BEEF] {
                assert_eq!(
                    crc32c::pg_comp_crc32c(seed, s),
                    crc32c::pg_comp_crc32c_sb8(seed, s),
                    "len={len} off={off} seed={seed:#x}"
                );
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[test]
fn armv8_matches_c() {
    assert!(
        std::arch::is_aarch64_feature_detected!("crc"),
        "aarch64 host without FEAT_CRC32: hardware CRC gate cannot run"
    );
    let buf = fill_buf();
    for &(len, off, seed, expect) in COMP_CRC32C {
        // SAFETY: FEAT_CRC32 presence asserted above.
        let got = unsafe { crc32c::pg_comp_crc32c_armv8(seed, &buf.0[off..off + len]) };
        assert_eq!(got, expect, "len={len} off={off} seed={seed:#x}");
    }
}

#[test]
fn init_fin_round_trip() {
    let mut crc = crc32c::CRC32C_INIT;
    crc = crc32c::pg_comp_crc32c(crc, b"123456789");
    assert_eq!(crc32c::fin_crc32c(crc), 0xE306_9283);
}

#[test]
fn split_accumulation_matches_whole() {
    let buf = fill_buf();
    for cut in [1usize, 3, 5, 8, 13, 64, 100] {
        for f in [crc32c::pg_comp_crc32c, crc32c::pg_comp_crc32c_sb8] {
            let whole = f(crc32c::CRC32C_INIT, &buf.0[..200]);
            let split = f(f(crc32c::CRC32C_INIT, &buf.0[..cut]), &buf.0[cut..200]);
            assert_eq!(whole, split, "cut={cut}");
        }
    }
}

#[test]
fn legacy_check_vectors() {
    assert_eq!(crc32c::legacy_crc32_lexeme(b"123456789"), 0xC40E_D0B0);
    assert_eq!(crc32c::legacy_crc32_lexeme(b""), 0);
    assert_eq!(crc32c::traditional_crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32c::traditional_crc32(b""), 0);
}
