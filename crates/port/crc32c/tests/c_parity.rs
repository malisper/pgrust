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
