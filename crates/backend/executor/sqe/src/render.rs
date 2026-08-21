//! render — the ONE typed-answer -> text seam: escape law, date /
//! PG-exact timestamp renders, PG-exact numeric average, `{:.6}` floats,
//! NULL as the empty field, the `\t` join, and the witness trailer
//! constructors. Answer values never meet `format!` outside this module.

use crate::answer::{AnswerSet, ColData};
use crate::typmeta::oids;

pub fn esc(b: &[u8]) -> String {
    let mut s = String::new();
    for &c in b {
        match c {
            b'\\' => s.push_str("\\\\"),
            b'\t' => s.push_str("\\t"),
            b'\n' => s.push_str("\\n"),
            0x20..=0x7e => s.push(c as char),
            _ => s.push_str(&format!("\\x{c:02x}")),
        }
    }
    s
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn date_str(pg_days: i64) -> String {
    let (y, m, d) = civil_from_days(pg_days + 10957);
    format!("{y:04}-{m:02}-{d:02}")
}

/// PG-exact `timestamp_out` for the whole-second/micros domain: full
/// seconds, fractional micros trimmed of trailing zeros (absent when 0).
/// Supersedes the truncated-minute render (`fmt_minute`), which zeroed
/// the seconds field of ANY timestamp — minute-grain GROUP-BY keys are
/// floored upstream (`trunc_minute` at the stencil, e.g. dense_domain),
/// so they render identically here; raw-second values (the q23 SELECT *
/// class) previously diverged from the server's byte-law answer.
pub fn fmt_timestamp(us: i64) -> String {
    let days = us.div_euclid(86_400_000_000);
    let rem = us.rem_euclid(86_400_000_000);
    let (y, m, d) = civil_from_days(days + 10957);
    let secs = rem / 1_000_000;
    let mut out = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    );
    let frac = rem % 1_000_000;
    if frac != 0 {
        let f = format!("{frac:06}");
        out.push('.');
        out.push_str(f.trim_end_matches('0'));
    }
    out
}

/// AVG over an integer-domain `{sum, count}` pair, rendered exactly as
/// PG's int8 avg finisher does (`numeric_poly_avg` -> `select_div_scale`
/// division -> `numeric_out`): the server's AvgNumeric arm and this seam
/// finish through the same adt_numeric entry, so the two arms are
/// byte-identical by construction.
pub fn avg_numeric(sum: i128, n: i64) -> String {
    assert!(n > 0);
    let img = adt_numeric::int128_avg_div(sum, n)
        .expect("integer avg over a positive count cannot overflow numeric");
    let mut out = Vec::new();
    adt_numeric::numeric_out_into(img.num(), &mut out);
    String::from_utf8(out).expect("numeric text is ASCII")
}

// Trailer/header law: stencils and rig verification share these.
pub fn footer_rows(n: u64) -> String {
    format!("-- {n} rows")
}

pub fn footer_groups(groups: u64, rows: u64) -> String {
    format!("-- groups={groups} rows={rows}")
}

pub fn head_matches(n: u64) -> String {
    format!("-- predicate matches: {n}")
}

/// The variance-family finisher at the text seam: PG's own ported
/// numeric_poly_stddev_internal over the exact {n, Σx, Σx²} triple —
/// byte-identical numeric rendering (rig builds only; the server seam
/// finishes through the same adt_numeric entry in execmain).
#[cfg(feature = "rig")]
pub fn moments_str(kind: crate::answer::MomentKind, n: i64, sum: i128, sumsq: i128) -> String {
    let (variance, sample) = kind.flags();
    let state = adt_numeric::Int128AggState { calc_sum_x2: true, n, sum_x: sum, sum_x2: sumsq };
    let img = adt_numeric::aggregates::numeric_poly_stddev_internal(Some(&state), variance, sample)
        .expect("stddev finisher over exact i128 sums cannot fail")
        .expect("NULL rows are masked by the validity leg before render");
    let mut out = Vec::new();
    adt_numeric::numeric_out_into(img.num(), &mut out);
    String::from_utf8(out).expect("numeric text is ASCII")
}

fn field(a: &AnswerSet, ci: usize, row: usize) -> String {
    let col = &a.cols[ci];
    if !col.validity.is_valid(row) {
        return String::new();
    }
    match &col.data {
        ColData::I64(v) => match col.ty.oid {
            oids::DATE => date_str(v[row]),
            oids::TIMESTAMP => fmt_timestamp(v[row]),
            oids::BOOL => (if v[row] != 0 { "t" } else { "f" }).to_string(),
            _ => format!("{}", v[row]),
        },
        ColData::F64(v) => format!("{:.6}", v[row]),
        ColData::I128(v) => format!("{}", v[row]),
        #[cfg(feature = "rig")]
        ColData::Moments { kind, trips } => {
            let (n, sum, sumsq) = trips[row];
            moments_str(*kind, n, sum, sumsq)
        }
        #[cfg(not(feature = "rig"))]
        ColData::Moments { .. } => {
            unreachable!("Moments text render is rig-only (the server finishes via adt_numeric)")
        }
        ColData::Ratio { pairs, .. } => {
            let (sum, count) = pairs[row];
            avg_numeric(sum, count)
        }
        ColData::Bytes { .. } if col.ty.oid == oids::UUID => {
            let b = col.data.bytes_at(row);
            debug_assert_eq!(b.len(), 16);
            let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
        }
        ColData::Bytes { .. } => esc(col.data.bytes_at(row)),
        // [sortgrp v1] PG array_out over the two-level List currency:
        // 1-D `{e1,e2,...}`; NULL elements print bare NULL; text elements
        // quote when empty / containing `{ } , " \` / whitespace / equal
        // to NULL case-insensitively, with `"`/`\` backslash-escaped.
        ColData::List { elems, offs } => {
            let (s, e) = (offs[row] as usize, offs[row + 1] as usize);
            let mut out = String::from("{");
            for i in s..e {
                if i > s {
                    out.push(',');
                }
                if !elems.validity.is_valid(i) {
                    out.push_str("NULL");
                    continue;
                }
                let txt = match &elems.data {
                    ColData::I64(v) => match elems.ty.oid {
                        oids::DATE => date_str(v[i]),
                        oids::BOOL => (if v[i] != 0 { "t" } else { "f" }).to_string(),
                        _ => format!("{}", v[i]),
                    },
                    ColData::Bytes { .. } => esc(elems.data.bytes_at(i)),
                    other => unreachable!("List element class {other:?} (bug)"),
                };
                let quote = txt.is_empty()
                    || txt.eq_ignore_ascii_case("null")
                    || txt.bytes().any(|b| {
                        matches!(b, b'{' | b'}' | b',' | b'"' | b'\\') || b.is_ascii_whitespace()
                    });
                if quote {
                    out.push('"');
                    for c in txt.chars() {
                        if c == '"' || c == '\\' {
                            out.push('\\');
                        }
                        out.push(c);
                    }
                    out.push('"');
                } else {
                    out.push_str(&txt);
                }
            }
            out.push('}');
            out
        }
    }
}

pub fn field_at(a: &AnswerSet, ci: usize, row: usize) -> String {
    field(a, ci, row)
}

/// The rig's byte-identity surface: one `\t`-joined line per answer row.
pub fn to_lines(a: &AnswerSet) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(h) = &a.head_note {
        out.push(h.clone());
    }
    out.extend((0..a.nrows)
        .map(|row| {
            (0..a.cols.len())
                .map(|ci| field(a, ci, row))
                .collect::<Vec<_>>()
                .join("\t")
        })
        );
    if let Some(n) = &a.note {
        out.push(n.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{avg_numeric, fmt_timestamp};

    #[test]
    fn timestamp_render_carries_full_seconds() {
        // The q23 byte-law pin (tax cell srvrig-perfcheck-v6-20260820T190812Z):
        // the truncated-minute render zeroed the seconds field of every
        // TIMESTAMP — RED under fmt_minute, green under the PG-exact render.
        // Micros are PG epoch (2000-01-01); expected strings are the stock-PG
        // psql outputs from the tax cell's server-arm q23 answer.
        let cases: &[(i64, &str)] = &[
            (426_075_622_000_000, "2013-07-02 10:20:22"),
            (426_081_491_000_000, "2013-07-02 11:58:11"),
            (426_027_922_000_000, "2013-07-01 21:05:22"),
            (426_097_704_000_000, "2013-07-02 16:28:24"),
            // minute-floored values (the GROUP-BY-minute key domain) are
            // unchanged by the seconds fix
            (426_075_600_000_000, "2013-07-02 10:20:00"),
            (0, "2000-01-01 00:00:00"),
            // fractional micros: PG trims trailing zeros
            (1_500_000, "2000-01-01 00:00:01.5"),
            (1_000_001, "2000-01-01 00:00:01.000001"),
            (-1_000_000, "1999-12-31 23:59:59"),
        ];
        for &(us, want) in cases {
            assert_eq!(fmt_timestamp(us), want, "fmt_timestamp({us})");
        }
    }

    #[test]
    fn avg_render_matches_pg_numeric_division() {
        // Expected strings are stock PG outputs of `sum::numeric / count`
        // (int avg finisher): rscale from select_div_scale, round half away.
        let cases: &[(i128, i64, &str)] = &[
            (10, 4, "2.5000000000000000"),
            (4, 4, "1.00000000000000000000"),
            (7, 3, "2.3333333333333333"),
            (-7, 3, "-2.3333333333333333"),
            (2, 3, "0.66666666666666666667"),
            (1, 3, "0.33333333333333333333"),
            (0, 5, "0.00000000000000000000"),
            (123_456_789, 1000, "123456.789000000000"),
            (1_000_000_000_000_000_000_000, 7, "142857142857142857143"),
        ];
        for &(sum, n, want) in cases {
            assert_eq!(avg_numeric(sum, n), want, "avg({sum}/{n})");
        }
    }
}
