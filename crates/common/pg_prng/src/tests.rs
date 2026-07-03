// KATs from compiled C pg_prng.c (REL_18_3, arm64); doubles as raw bits.
use super::*;

#[test]
fn seed_and_u64_stream_match_c() {
    let mut s = PgPrng::seeded(42);
    assert_eq!(s.raw(), (0xbdd732262feb6e95, 0x28efe333b266f103));
    assert_eq!(s.next_u64(), 0x69e85b3631381baa);
    assert_eq!(s.next_u64(), 0x3bc32c541d626e1d);
    assert_eq!(s.next_u64(), 0x3e35de64b3b378d8);
    assert_eq!(s.next_u64(), 0x106e3c0092b088bf);
}

#[test]
fn typed_draw_sequence_matches_c() {
    let mut s = PgPrng::seeded(0xdeadbeefcafebabe);
    assert_eq!(s.raw(), (0x0d7d93560d1929d2, 0x491dfb740e50d43f));
    assert_eq!(
        [s.next_u32(), s.next_u32(), s.next_u32(), s.next_u32()],
        [0x89731026, 0xfb8ad0ec, 0x107922fc, 0x651df9e6]
    );
    assert_eq!([s.next_i32(), s.next_i32()], [-661281187, -1342010035]);
    assert_eq!(
        [s.next_nonnegative_i32(), s.next_nonnegative_i32()],
        [1950826024, 1685390619]
    );
    assert_eq!(
        [s.next_i64(), s.next_i64()],
        [-328153444023275653, 6899194418642041191]
    );
    assert_eq!(
        [s.next_nonnegative_i64(), s.next_nonnegative_i64()],
        [2025493334285799759, 7065612945206188005]
    );
    assert_eq!(
        [s.next_bool(), s.next_bool(), s.next_bool(), s.next_bool()],
        [true, false, true, true]
    );
    assert_eq!(
        [
            s.next_f64().to_bits(),
            s.next_f64().to_bits(),
            s.next_f64().to_bits(),
        ],
        [0x3fe7102389f2027e, 0x3fece7c21d9a7a30, 0x3fef2479d7df3a12]
    );
    let nrm = [s.normal_f64(), s.normal_f64(), s.normal_f64()];
    let c_nrm = [
        f64::from_bits(0xbfedc1307eb06237),
        f64::from_bits(0x3fd7f1ac9d5d1e46),
        f64::from_bits(0xc0016b2246b456c1),
    ];
    for (got, want) in nrm.iter().zip(c_nrm) {
        assert!((got - want).abs() <= want.abs() * 1e-14, "{got} vs {want}");
    }
    assert_eq!(s.raw(), (0x8a6fe4562ac344ff, 0x05e77e1c321c04f9));
}

#[test]
fn ranges_match_c() {
    let mut s = PgPrng::seeded(7);
    let u: Vec<u64> = (0..6).map(|_| s.u64_range(1000, 1000000)).collect();
    assert_eq!(u, [440721, 895227, 654123, 869895, 5554, 677954]);
    let i: Vec<i64> = (0..6).map(|_| s.i64_range(-500, 500)).collect();
    assert_eq!(i, [296, 483, -287, 200, -258, 471]);
    assert_eq!(s.u64_range(9, 9), 9);
    assert_eq!(s.i64_range(-7, -7), -7);
    assert_eq!(s.i64_range(i64::MIN, i64::MAX), 4247590932269477632);
    assert_eq!(s.raw(), (0xe941dc5a865a568a, 0xd3ebd8ab43770989));
}

#[test]
fn fseed_matches_c() {
    let mut s = PgPrng::default();
    s.fseed(-0.375);
    assert_eq!(s.raw(), (0x3bc87a2eeaf873e4, 0xd83dd2493533db98));
    assert_eq!(s.next_u64(), 0x1ebd1fa6d62f8abd);
    s.fseed(0.999999);
    assert_eq!(s.raw(), (0xb68b80aa2e22e51e, 0xaafa163495385df2));
    assert_eq!(s.next_u64(), 0x42cef50e11232588);
}

#[test]
fn zero_state_gets_fallback_matching_c() {
    let mut s = PgPrng::from_raw(0, 0);
    assert!(s.ensure_seeded());
    assert_eq!(s.raw(), (FALLBACK_S0, FALLBACK_S1));
    assert_eq!(s.next_u64(), 0x33f5fb3b23ad77bc);
}

#[test]
fn seams_draw_from_thread_local_global() {
    init_seams();
    global_prng(|s| s.seed(42));
    assert_eq!(
        pg_prng_seams::global_prng_uint32::call(),
        (0x69e85b3631381baau64 >> 32) as u32
    );
    let d = global_prng(PgPrng::next_f64);
    assert!((0.0..1.0).contains(&d));
    assert_ne!(global_prng(|s| s.raw()), PgPrng::seeded(42).raw());
    std::thread::spawn(|| {
        assert_eq!(global_prng(|s| s.raw()), (0, 0));
    })
    .join()
    .unwrap();
}
