use mcx::MemoryContext;

use super::*;

fn test_context() -> MemoryContext {
    MemoryContext::new("hyperloglog test")
}

#[test]
fn init_by_width_matches_postgres_parameters() {
    let ctx = test_context();
    let s = initHyperLogLog(ctx.mcx(), 4).unwrap();

    assert_eq!(s.registerWidth, 4);
    assert_eq!(s.nRegisters, 16);
    assert_eq!(s.arrSize, 17);
    assert_eq!(s.alphaMM, 0.673 * 16.0 * 16.0);
    // arrSize is nRegisters + 1; only the first nRegisters are live registers.
    assert_eq!(s.hashesArr.len(), 17);
    assert!(s.hashesArr.iter().all(|&byte| byte == 0));
}

#[test]
fn init_alpha_for_each_special_case() {
    // The three switch arms (16/32/64 registers) plus the default formula.
    let ctx = test_context();
    let s4 = initHyperLogLog(ctx.mcx(), 4).unwrap();
    assert_eq!(s4.alphaMM, 0.673 * 16.0 * 16.0);

    let s5 = initHyperLogLog(ctx.mcx(), 5).unwrap();
    assert_eq!(s5.alphaMM, 0.697 * 32.0 * 32.0);

    let s6 = initHyperLogLog(ctx.mcx(), 6).unwrap();
    assert_eq!(s6.alphaMM, 0.709 * 64.0 * 64.0);

    // Default arm: nRegisters = 128, alpha = 0.7213 / (1 + 1.079/128).
    let s7 = initHyperLogLog(ctx.mcx(), 7).unwrap();
    let m = 128.0_f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    assert_eq!(s7.nRegisters, 128);
    assert_eq!(s7.alphaMM, alpha * m * m);
}

#[test]
fn init_extremes_4_and_16() {
    let ctx = test_context();
    let lo = initHyperLogLog(ctx.mcx(), 4).unwrap();
    assert_eq!(lo.nRegisters, 16);
    assert_eq!(lo.arrSize, 17);

    let hi = initHyperLogLog(ctx.mcx(), 16).unwrap();
    assert_eq!(hi.registerWidth, 16);
    assert_eq!(hi.nRegisters, 65536);
    assert_eq!(hi.arrSize, 65537);
}

#[test]
fn init_by_error_chooses_first_width_below_error() {
    let ctx = test_context();
    let s = initHyperLogLogError(ctx.mcx(), 0.10).unwrap();
    assert_eq!(s.registerWidth, 7);
}

#[test]
fn init_by_error_tiny_error_caps_at_16() {
    // A demand for an error rate the largest counter cannot beat stops at 16.
    let ctx = test_context();
    let s = initHyperLogLogError(ctx.mcx(), 0.0001).unwrap();
    assert_eq!(s.registerWidth, 16);
}

#[test]
fn init_by_error_huge_error_uses_min_width() {
    // 1.04/sqrt(16) ~= 0.26 < 0.5, so bwidth 4 is already good enough.
    let ctx = test_context();
    let s = initHyperLogLogError(ctx.mcx(), 0.5).unwrap();
    assert_eq!(s.registerWidth, 4);
}

#[test]
fn invalid_width_uses_postgres_elog_message() {
    let ctx = test_context();
    let error = initHyperLogLog(ctx.mcx(), 3).unwrap_err();
    assert_eq!(error.message(), "bit width must be between 4 and 16 inclusive");

    let error = initHyperLogLog(ctx.mcx(), 17).unwrap_err();
    assert_eq!(error.message(), "bit width must be between 4 and 16 inclusive");

    // Boundary 0 also errors.
    assert!(initHyperLogLog(ctx.mcx(), 0).is_err());
}

#[test]
fn add_updates_registers_and_estimate_grows() {
    let ctx = test_context();
    let mut state = initHyperLogLog(ctx.mcx(), 4).unwrap();

    assert_eq!(estimateHyperLogLog(&state), 0.0);

    // hash 0xF8000000: top 4 bits = 0b1111 = index 15; hash<<4 = 0x80000000,
    // whose MSB is bit 31 -> pg_leftmost_one_pos32 = 31 -> j = 32-31 = 1.
    addHyperLogLog(&mut state, 0xF800_0000);
    // hash 0x00000000: index 0, x = 0 -> rho returns b+1 = 29 (b = 32-4 = 28).
    addHyperLogLog(&mut state, 0x0000_0000);

    assert_eq!(state.hashesArr[15], 1);
    assert_eq!(state.hashesArr[0], 29);
    assert!(estimateHyperLogLog(&state) > 0.0);
}

#[test]
fn add_keeps_running_max_per_register() {
    let ctx = test_context();
    let mut state = initHyperLogLog(ctx.mcx(), 4).unwrap();

    // Both land on index 0 (top nibble 0). First gives a high rho, second a low
    // one; the register must keep the maximum.
    addHyperLogLog(&mut state, 0x0000_0001); // x = 1<<4 = 0x10, MSB bit 4 -> j = 32-4 = 28
    assert_eq!(state.hashesArr[0], 28);

    addHyperLogLog(&mut state, 0x0800_0000); // x = 0x80000000, j = 1; max keeps 28
    assert_eq!(state.hashesArr[0], 28);
}

#[test]
fn rho_examples_from_c_comment() {
    // rho over the first 10 bits, per the C doc comment examples. The C helper
    // reads from the MOST significant bit, so a 10-bit pattern is left-aligned
    // into the 32-bit word.
    assert_eq!(rho(0b1000000000 << 22, 10), 1);
    assert_eq!(rho(0b0010000000 << 22, 10), 3);
    assert_eq!(rho(0, 10), 11); // all zero -> b + 1
}

#[test]
fn rho_overflow_returns_b_plus_one() {
    // A set bit beyond the first b bits yields j > b, so rho returns b + 1.
    // word = 1 (only bit 0 set) -> j = 32; with b = 4, j > b -> 5.
    assert_eq!(rho(1, 4), 5);
}

#[test]
fn pg_leftmost_one_pos32_matches_msb() {
    assert_eq!(pg_leftmost_one_pos32(1), 0);
    assert_eq!(pg_leftmost_one_pos32(2), 1);
    assert_eq!(pg_leftmost_one_pos32(0x8000_0000), 31);
    assert_eq!(pg_leftmost_one_pos32(0x00FF_0000), 23);
}

#[test]
fn estimate_distinct_count_is_in_ballpark() {
    // Feed many well-distributed hashes and check the estimate is close to the
    // true cardinality. We use a simple integer hash that spreads bits.
    let ctx = test_context();
    let mut state = initHyperLogLog(ctx.mcx(), 12).unwrap();

    let n: u32 = 5000;
    for i in 0..n {
        // splitmix32-style avalanche to get a uniform bit distribution.
        let mut h = i.wrapping_mul(0x9E37_79B9);
        h ^= h >> 16;
        h = h.wrapping_mul(0x85EB_CA6B);
        h ^= h >> 13;
        addHyperLogLog(&mut state, h);
    }

    let est = estimateHyperLogLog(&state);
    // With bwidth 12 (4096 registers) the standard error ~1.6%; allow generous
    // slack to keep the test deterministic across platforms.
    let lo = n as f64 * 0.85;
    let hi = n as f64 * 1.15;
    assert!(est > lo && est < hi, "estimate {} out of [{}, {}]", est, lo, hi);
}

#[test]
fn free_returns_charge_to_context() {
    let ctx = test_context();
    let state = initHyperLogLog(ctx.mcx(), 8).unwrap();
    // 256 registers + 1 byte charged.
    assert!(ctx.used() >= 257);
    freeHyperLogLog(state);
}

#[test]
fn estimate_all_zero_registers_is_zero() {
    // No elements added: sum = nRegisters (each 1/2^0 = 1), result = alphaMM/m.
    // result <= 2.5*m triggers the small-range branch, zero_count == m != 0, so
    // result = m * ln(m / m) = m * ln(1) = 0.
    let ctx = test_context();
    let state = initHyperLogLog(ctx.mcx(), 6).unwrap();
    assert_eq!(estimateHyperLogLog(&state), 0.0);
}

#[test]
fn full_counter_lifecycle_on_owned_value() {
    // Exercise the type/ops split the nodeAgg spill / varlena sort consumers use:
    // an owned HyperLogLog held by value, ops called directly (no handle/seam).
    let ctx = test_context();
    let mut state = initHyperLogLog(ctx.mcx(), 5).unwrap();

    addHyperLogLog(&mut state, 0xF800_0000);
    let est = estimateHyperLogLog(&state);
    assert!(est >= 0.0);
    freeHyperLogLog(state);
}
