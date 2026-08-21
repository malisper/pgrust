//! Deterministic jsonb document generators for shredding tests — the C0
//! proof-test corpus (S4-shaped + adversarial: non-object roots, path
//! explosion, deep chains, duplicate keys, unicode keys, dscale-rich
//! numbers, non-canonical uuids), extracted for reuse by storage-side
//! integration tests (the C1 writer's round-trip suites are the first
//! external consumer).
//!
//! Compiled for this crate's own tests and for dependents that enable the
//! `testgen` feature; nothing here ships in a production build.

use crate::elect::PathHint;
use crate::manifest::Lane;
use crate::path::JsonPath;

pub fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 11
}

pub fn pick<'a, T>(st: &mut u64, xs: &'a [T]) -> &'a T {
    &xs[(lcg(st) % xs.len() as u64) as usize]
}

/// form 0 = canonical lowercase-hyphenated; 1 = uppercase (non-canonical);
/// 2+ = braced (non-canonical).
pub fn gen_uuid(st: &mut u64, form: u8) -> String {
    let (a, b) = (lcg(st), lcg(st));
    let bytes: Vec<u8> = a
        .to_le_bytes()
        .iter()
        .chain(b.to_le_bytes().iter())
        .copied()
        .collect();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let canon = format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    );
    match form {
        0 => canon,
        1 => canon.to_uppercase(),
        _ => format!("{{{canon}}}"),
    }
}

pub const STR_VALUES: &[&str] = &[
    r#""""#,
    r#""sequence""#,
    r#""manual""#,
    r#""a b c""#,
    r#""quote\"inside""#,
    r#""back\\slash""#,
    r#""tab\tnl\n""#,
    r#""unicode é ü 中文 🚀""#,
    r#""é中""#,
    r#""null""#,
    r#""fairly-long-string-value-0123456789-0123456789-0123456789""#,
];

pub const NUM_VALUES: &[&str] = &[
    "0",
    "-0",
    "1",
    "42",
    "-7",
    "1.50",
    "0.10",
    "-2.25",
    "3.14159",
    "1e-15",
    "1E+5",
    "123456789012345678901234567890123456",
    "0.000000000000000000000000000001",
    "99999999999999999999.99",
    "-360287970189639.67",
    "2.5e10",
];

pub const KEYS: &[&str] = &[
    "a",
    "b",
    "c",
    "source",
    "status",
    "n",
    "num",
    "flag",
    "id",
    "key with space",
    "日本語",
    "ключ",
    "🚀",
    "x1",
    "deep",
    "longer_key_name_for_variety",
];

pub fn gen_scalar(st: &mut u64) -> String {
    match lcg(st) % 100 {
        0..=34 => (*pick(st, STR_VALUES)).to_string(),
        35..=64 => (*pick(st, NUM_VALUES)).to_string(),
        65..=79 => if lcg(st) % 2 == 0 { "true" } else { "false" }.to_string(),
        80..=89 => "null".to_string(),
        _ => {
            let form = (lcg(st) % 3) as u8;
            format!("\"{}\"", gen_uuid(st, form))
        }
    }
}

pub fn gen_value(st: &mut u64, depth: u32) -> String {
    if depth >= 3 {
        return gen_scalar(st);
    }
    match lcg(st) % 100 {
        0..=54 => gen_scalar(st),
        55..=74 => {
            let n = lcg(st) % 5;
            let body: Vec<String> = (0..n)
                .map(|_| format!("\"{}\": {}", pick(st, KEYS), gen_value(st, depth + 1)))
                .collect();
            format!("{{{}}}", body.join(", "))
        }
        75..=89 => {
            let n = lcg(st) % 4;
            let body: Vec<String> = (0..n).map(|_| gen_value(st, depth + 1)).collect();
            format!("[{}]", body.join(", "))
        }
        _ => if lcg(st) % 2 == 0 { "{}" } else { "[]" }.to_string(),
    }
}

/// The S4 task_source shape of record (datagen.py:344-371): 80% sequence
/// (three canonical uuids + a step key), 15% manual, 5% import.
pub fn gen_s4(st: &mut u64) -> String {
    match lcg(st) % 100 {
        0..=79 => format!(
            r#"{{"source": "sequence", "sequenceId": "{}", "sequenceStateId": "{}", "sequenceStepId": "step_{}"}}"#,
            gen_uuid(st, 0),
            gen_uuid(st, 0),
            lcg(st) % 40
        ),
        80..=94 => r#"{"source": "manual"}"#.to_string(),
        _ => format!(r#"{{"source": "import", "importId": "{}"}}"#, gen_uuid(st, 0)),
    }
}

/// The adversarial mixed corpus: S4 rows, non-object roots, path explosion,
/// deep chains, duplicate input keys, and generic small objects.
pub fn gen_doc(st: &mut u64) -> String {
    match lcg(st) % 100 {
        0..=7 => gen_s4(st),
        // Non-object roots: whole-document residual at the empty path.
        8..=13 => match lcg(st) % 3 {
            0 => gen_scalar(st),
            1 => format!("[{}, {}]", gen_value(st, 1), gen_value(st, 1)),
            _ => "[]".to_string(),
        },
        // Path explosion: unique keys, elects little to nothing.
        14..=19 => {
            let n = 10 + lcg(st) % 21;
            let salt = lcg(st);
            let body: Vec<String> = (0..n)
                .map(|i| format!("\"k{salt}_{i}\": {}", gen_scalar(st)))
                .collect();
            format!("{{{}}}", body.join(", "))
        }
        // Deep chains: subtree residual at the depth budget.
        20..=25 => {
            let d = 6 + lcg(st) % 5;
            let mut s = gen_scalar(st);
            for _ in 0..d {
                s = format!("{{\"deep\": {s}}}");
            }
            s
        }
        // Duplicate keys in the input text (jsonb_in last-wins normalizes
        // before the shredder ever sees bytes).
        26..=31 => {
            let n = 2 + lcg(st) % 5;
            let small = ["a", "b", "c"];
            let body: Vec<String> = (0..n)
                .map(|_| format!("\"{}\": {}", pick(st, &small), gen_value(st, 1)))
                .collect();
            format!("{{{}}}", body.join(", "))
        }
        _ => {
            let n = 2 + lcg(st) % 7;
            let body: Vec<String> = (0..n)
                .map(|_| format!("\"{}\": {}", pick(st, KEYS), gen_value(st, 1)))
                .collect();
            format!("{{{}}}", body.join(", "))
        }
    }
}

/// Hints exercising every lane class over the [`gen_doc`] key vocabulary.
pub fn volume_hints() -> Vec<PathHint> {
    vec![
        PathHint {
            path: JsonPath::from_dotted("source"),
            lane: Lane::Text,
        },
        PathHint {
            path: JsonPath::from_dotted("id"),
            lane: Lane::Uuid16,
        },
        PathHint {
            path: JsonPath::from_dotted("n"),
            lane: Lane::NumericFs,
        },
        PathHint {
            path: JsonPath::from_dotted("flag"),
            lane: Lane::Bool,
        },
        PathHint {
            path: JsonPath::from_dotted("deep.deep"),
            lane: Lane::Text,
        },
    ]
}
