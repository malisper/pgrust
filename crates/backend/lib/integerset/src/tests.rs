use super::*;
use std::collections::BTreeSet;
use std::vec::Vec;

fn with_set<R>(f: impl FnOnce(&mut IntegerSet) -> R) -> R {
    let ctx = mcx::MemoryContext::new("integer set test");
    let mut set = IntegerSet::create(ctx.mcx());
    f(&mut set)
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn prng_range(state: &mut u64, max_inclusive: u64) -> u64 {
    if max_inclusive == u64::MAX {
        return splitmix64(state);
    }
    splitmix64(state) % (max_inclusive + 1)
}

fn collect(set: &mut IntegerSet) -> Vec<u64> {
    set.begin_iterate();
    let mut values = Vec::new();
    while let Some(v) = set.iterate_next() {
        values.push(v);
    }
    values
}

#[test]
fn test_empty() {
    with_set(|set| {
        assert!(!set.is_member(0));
        assert!(!set.is_member(1));
        assert!(!set.is_member(u64::MAX));

        set.begin_iterate();
        assert_eq!(set.iterate_next(), None);
    });
}

#[test]
fn test_single_value() {
    for value in [0u64, 1, u64::MAX - 1, u64::MAX] {
        with_set(|set| {
            set.add_member(value).unwrap();
            assert_eq!(set.num_entries(), 1);

            assert_eq!(set.is_member(0), value == 0);
            assert_eq!(set.is_member(1), value == 1);
            assert_eq!(set.is_member(u64::MAX), value == u64::MAX);
            assert!(set.is_member(value));

            set.begin_iterate();
            assert_eq!(set.iterate_next(), Some(value));
            assert_eq!(set.iterate_next(), None);
        });
    }
}

fn check_with_filler(set: &IntegerSet, x: u64, value: u64, filler_min: u64, filler_max: u64) {
    let expected = x == value || (filler_min <= x && x < filler_max);
    assert_eq!(set.is_member(x), expected, "is_member({x})");
}

fn run_single_value_and_filler(value: u64, filler_min: u64, filler_max: u64) {
    with_set(|set| {
        let mut expected = Vec::new();
        if value < filler_min {
            set.add_member(value).unwrap();
            expected.push(value);
        }
        for x in filler_min..filler_max {
            set.add_member(x).unwrap();
            expected.push(x);
        }
        if value >= filler_max {
            set.add_member(value).unwrap();
            expected.push(value);
        }

        assert_eq!(set.num_entries(), expected.len() as u64);

        for x in [
            0,
            1,
            filler_min.wrapping_sub(1),
            filler_min,
            filler_min + 1,
            value.wrapping_sub(1),
            value,
            value.wrapping_add(1),
            filler_max - 1,
            filler_max,
            filler_max + 1,
            u64::MAX - 1,
            u64::MAX,
        ] {
            check_with_filler(set, x, value, filler_min, filler_max);
        }

        assert_eq!(collect(set), expected);

        let mem_usage = set.memory_usage();
        assert!(
            (5000..500_000_000).contains(&mem_usage),
            "suspicious memory_usage {mem_usage}"
        );
    });
}

#[test]
fn test_single_value_and_filler() {
    run_single_value_and_filler(0, 1000, 2000);
    run_single_value_and_filler(1, 1000, 2000);
    run_single_value_and_filler(1, 1000, 2000000);
    run_single_value_and_filler(u64::MAX - 1, 1000, 2000);
    run_single_value_and_filler(u64::MAX, 1000, 2000);
}

#[test]
fn test_huge_distances() {
    const P60: u64 = 1152921504606846976;
    let mut values: Vec<u64> = Vec::new();
    let mut val: u64 = 0;
    values.push(val);
    for delta in [
        P60 - 1,
        P60 - 1,
        P60,
        P60,
        P60,
        P60 + 1,
        P60 + 1,
        P60 + 1,
        P60 + 2,
        P60 + 2,
        P60,
    ] {
        val += delta;
        values.push(val);
    }
    let mut rng = 0xDEAD_BEEF_u64;
    while values.len() < 1000 {
        val += (splitmix64(&mut rng) as u32 as u64).max(1);
        values.push(val);
    }

    with_set(|set| {
        for &v in &values {
            set.add_member(v).unwrap();
        }

        for (i, &y) in values.iter().enumerate() {
            if y > 0 {
                let expected = i > 0 && values[i - 1] == y - 1;
                assert_eq!(set.is_member(y - 1), expected, "probe {}", y - 1);
            }
            assert!(set.is_member(y), "probe {y}");
            let expected = i != values.len() - 1 && values[i + 1] == y + 1;
            assert_eq!(set.is_member(y + 1), expected, "probe {}", y + 1);
        }

        assert_eq!(collect(set), values);
    });
}

struct TestSpec {
    pattern_str: &'static str,
    spacing: u64,
    num_values: u64,
}

// The C test_specs vectors, with num_values scaled down (10M/100M -> 300k/600k)
// to keep runtime sane; spacing and patterns are the C values verbatim.
const TEST_SPECS: &[TestSpec] = &[
    TestSpec { pattern_str: "1111111111", spacing: 10, num_values: 300_000 },
    TestSpec { pattern_str: "0101010101", spacing: 10, num_values: 300_000 },
    TestSpec { pattern_str: "1111111111", spacing: 10000, num_values: 300_000 },
    TestSpec {
        pattern_str: "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
        spacing: 10000,
        num_values: 600_000,
    },
    TestSpec { pattern_str: "1", spacing: 65536, num_values: 300_000 },
    TestSpec {
        pattern_str: "100000000000000000000000000000001",
        spacing: 10000000,
        num_values: 300_000,
    },
    TestSpec { pattern_str: "1", spacing: 10000000000, num_values: 300_000 },
    TestSpec { pattern_str: "10101010", spacing: 10000000000, num_values: 300_000 },
    TestSpec { pattern_str: "10101010", spacing: 2000000000000000000, num_values: 23 },
];

fn run_pattern(spec: &TestSpec, rng: &mut u64) {
    let pattern_values: Vec<u64> = spec
        .pattern_str
        .bytes()
        .enumerate()
        .filter(|&(_, b)| b == b'1')
        .map(|(i, _)| i as u64)
        .collect();
    let patternlen = spec.pattern_str.len() as u64;

    with_set(|set| {
        let mut n: u64 = 0;
        let mut last_int: u64 = 0;
        while n < spec.num_values {
            for &pv in &pattern_values {
                if n >= spec.num_values {
                    break;
                }
                set.add_member(last_int + pv).unwrap();
                n += 1;
            }
            last_int += spec.spacing;
        }

        assert_eq!(set.num_entries(), spec.num_values);

        for _ in 0..20_000 {
            let x = prng_range(rng, last_int + 1000);
            let expected = if x >= last_int {
                false
            } else {
                let idx = x % spec.spacing;
                idx < patternlen && spec.pattern_str.as_bytes()[idx as usize] == b'1'
            };
            assert_eq!(set.is_member(x), expected, "probe {x}");
        }

        set.begin_iterate();
        let mut n: u64 = 0;
        let mut last_int: u64 = 0;
        'outer: while n < spec.num_values {
            for &pv in &pattern_values {
                if n >= spec.num_values {
                    break;
                }
                let expected = last_int + pv;
                match set.iterate_next() {
                    Some(x) => assert_eq!(x, expected, "iterate at {n}"),
                    None => break 'outer,
                }
                n += 1;
            }
            last_int += spec.spacing;
        }
        assert_eq!(n, spec.num_values, "iterator entry count");
        assert_eq!(set.iterate_next(), None);
    });
}

#[test]
fn test_patterns() {
    let mut rng = 0x1234_5678_9ABC_DEF0_u64;
    for spec in TEST_SPECS {
        run_pattern(spec, &mut rng);
    }
}

// Selectors of the flushed leaf items, EMPTY_CODEWORD tracked separately
// (it would otherwise read as selector 0).
fn selectors_used(set: &IntegerSet) -> (BTreeSet<u64>, bool) {
    let mut selectors = BTreeSet::new();
    let mut saw_empty = false;
    for node in set.leaf_nodes.iter() {
        for item in &node.items[..node.num_items as usize] {
            if item.codeword == EMPTY_CODEWORD {
                saw_empty = true;
            } else {
                selectors.insert(item.codeword >> 60);
            }
        }
    }
    (selectors, saw_empty)
}

// Forces every multi-bit Simple-8b mode: a constant delta of 2^(bits-1)+1
// makes delta-minus-one need exactly `bits` bits, so some codeword must use
// the mode whose width is exactly `bits`. Big-delta counts are capped to stay
// inside u64 and padded with delta-1 values so the buffer actually flushes.
#[test]
fn test_codeword_modes() {
    let mode_for_bits = |bits: u32| {
        SIMPLE8B_MODES
            .iter()
            .position(|m| m.bits_per_int == bits && m.num_ints != 0)
            .unwrap() as u64
    };

    for bits in [1u32, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60] {
        let delta = (1u64 << (bits - 1)) + 1;
        with_set(|set| {
            let mut values = Vec::new();
            let mut model = BTreeSet::new();
            let mut v: u64 = 1;
            let n_big = 600.min((u64::MAX - v) / delta - 1);
            for _ in 0..n_big {
                values.push(v);
                set.add_member(v).unwrap();
                model.insert(v);
                v += delta;
            }
            while values.len() < 1200 {
                values.push(v);
                set.add_member(v).unwrap();
                model.insert(v);
                v += 1;
            }

            let (selectors, _) = selectors_used(set);
            assert!(
                selectors.contains(&mode_for_bits(bits)),
                "bits {bits}: expected mode absent, got {selectors:?}"
            );

            for &x in &values {
                assert!(set.is_member(x), "bits {bits} member {x}");
                assert_eq!(set.is_member(x + 1), model.contains(&(x + 1)), "bits {bits} probe");
                assert_eq!(set.is_member(x - 1), model.contains(&(x - 1)), "bits {bits} probe");
            }
            assert_eq!(collect(set), values, "bits {bits} iteration");
        });
    }
}

// Modes 0 and 1: zero-diff runs. A long consecutive run yields 240-zero
// codewords; blocks of 122 consecutive values before a larger gap leave
// 120..239 zeroes pending when the gap is hit, which is exactly mode 1.
#[test]
fn test_codeword_zero_run_modes() {
    with_set(|set| {
        for v in 100..100_000u64 {
            set.add_member(v).unwrap();
        }
        let (selectors, saw_empty) = selectors_used(set);
        assert!(selectors.contains(&0), "mode 0 absent: {selectors:?}");
        assert!(!saw_empty);

        assert!(!set.is_member(99));
        assert!(set.is_member(100));
        assert!(set.is_member(99_999));
        assert!(!set.is_member(100_000));
        assert_eq!(collect(set), (100..100_000).collect::<Vec<u64>>());
    });

    with_set(|set| {
        let mut values = Vec::new();
        let mut v: u64 = 0;
        for _ in 0..50 {
            for _ in 0..122 {
                values.push(v);
                set.add_member(v).unwrap();
                v += 1;
            }
            v += 100;
        }
        let (selectors, _) = selectors_used(set);
        assert!(selectors.contains(&1), "mode 1 absent: {selectors:?}");
        assert_eq!(collect(set), values);
    });
}

// EMPTY_CODEWORD path: a leaf item whose successor is more than 2^60 away
// carries the magic empty codeword. Mirrors the C huge-distances shape:
// a few >2^60 jumps, then enough dense values to force the flush.
#[test]
fn test_empty_codeword_gaps() {
    with_set(|set| {
        let mut values = Vec::new();
        let mut model = BTreeSet::new();
        let mut v: u64 = 0;
        for k in 0..13u64 {
            values.push(v);
            set.add_member(v).unwrap();
            model.insert(v);
            v += (1u64 << 60) + 1 + k;
        }
        while values.len() < 1200 {
            values.push(v);
            set.add_member(v).unwrap();
            model.insert(v);
            v += 3;
        }

        let (_, saw_empty) = selectors_used(set);
        assert!(saw_empty, "no EMPTY_CODEWORD item was produced");

        for &x in &values {
            assert!(set.is_member(x));
            assert_eq!(set.is_member(x + 1), model.contains(&(x + 1)));
            if x > 0 {
                assert_eq!(set.is_member(x - 1), model.contains(&(x - 1)));
            }
        }
        assert_eq!(collect(set), values);
    });
}

#[test]
fn test_add_ordering_errors() {
    with_set(|set| {
        set.add_member(10).unwrap();
        let err = set.add_member(10).unwrap_err();
        assert_eq!(err.message(), "cannot add value to integer set out of order");
        let err = set.add_member(9).unwrap_err();
        assert_eq!(err.message(), "cannot add value to integer set out of order");

        set.begin_iterate();
        let err = set.add_member(11).unwrap_err();
        assert_eq!(
            err.message(),
            "cannot add new values to integer set while iteration is in progress"
        );
    });
}

#[test]
fn test_zero_is_addable_first() {
    with_set(|set| {
        set.add_member(0).unwrap();
        assert!(set.is_member(0));
        assert_eq!(collect(set), std::vec![0]);
    });
}

#[test]
fn property_vs_btreeset() {
    for seed in 1..=8u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut values = Vec::new();
        let mut model = BTreeSet::new();
        let mut v: u64 = prng_range(&mut rng, 1000);
        for _ in 0..3000 {
            values.push(v);
            model.insert(v);
            let gap = match splitmix64(&mut rng) % 5 {
                0 => 1,
                1 => 1 + splitmix64(&mut rng) % 16,
                2 => 1 + splitmix64(&mut rng) % (1 << 20),
                3 => 1 + splitmix64(&mut rng) % (1 << 40),
                _ => (1u64 << 60) + splitmix64(&mut rng) % (1 << 30),
            };
            match v.checked_add(gap) {
                Some(next) => v = next,
                None => break,
            }
        }

        with_set(|set| {
            for &x in &values {
                set.add_member(x).unwrap();
            }
            assert_eq!(set.num_entries(), values.len() as u64);

            for &x in &values {
                assert!(set.is_member(x), "seed {seed} member {x}");
            }
            let hi = *values.last().unwrap();
            for _ in 0..5000 {
                let x = prng_range(&mut rng, hi.saturating_add(1000));
                assert_eq!(set.is_member(x), model.contains(&x), "seed {seed} probe {x}");
            }

            let iterated = collect(set);
            assert_eq!(iterated, values, "seed {seed} iteration");
        });
    }
}
