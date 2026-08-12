//! Boundary-value literal banks: the standing corpus fix for the mutation
//! pilot's headline blind spot (docs/fuzzing/mutation-testing-pilot.md).
//!
//! The pilot measured that the random generator produced only well-formed
//! mid-range values — its 400-statement batch killed ZERO mutants the fixed
//! deck missed — while 13 of 26 differential misses were corpus gaps at
//! boundaries: numeric `Infinity`/`NaN`, boundary-adjacent time values
//! (`24:01:00`), the `12:00 AM` midnight fold, uncommon interval unit words
//! (decade/century/millennium/week) and qualified interval forms.
//!
//! Every entry is a SELF-DELIMITING SQL literal expression pinned to its
//! type (cast/keyword form), exactly what `ExprGen::gen_literal` returns.
//! `gen_literal` draws from these banks through the weighted
//! `lit:boundary` production (default 1 boundary : 2 mid-range), so every
//! ordinary differential run exercises boundaries — not just hand decks.
//!
//! REJECT-spelling entries (e.g. `time '24:01:00'`, `date '2019-02-29'`)
//! are deliberately included: the differential compares error messages
//! too, and several pilot survivors (#36, #49) are only distinguishable by
//! WHICH error a boundary-adjacent input raises. They error identically on
//! both sides, so they are baseline-clean.
//!
//! Composition note: the library-level fuzz bank `fuzz/core/src/edge.rs`
//! (lane EDGE) carries the same categories as raw Rust values for the
//! crate-root decoder drivers; this module is the SQL-literal projection of
//! those categories for the statement generator. fuzzgen cannot depend on
//! `decoder_fuzz` (separate workspace, backend-crate deps), so the two
//! banks are kept category-aligned by comment rather than by import.

use crate::catalog::SqlType;

/// Boundary literals for `ty`. Empty slice = no bank for this type (the
/// caller falls back to the mid-range generator).
pub fn bank(ty: SqlType) -> &'static [&'static str] {
    match ty {
        // Integer MIN/MAX/0/-1 plus the just-past-the-edge rejects.
        SqlType::Int2 => &[
            "(0)::int2",
            "(-1)::int2",
            "(32767)::int2",
            "(-32768)::int2",
            "('32768'::int4)::int2",  // overflow via runtime cast
            "(16384)::int2",          // high bit of the positive range
        ],
        SqlType::Int4 => &[
            "(0)::int4",
            "(-1)::int4",
            "(2147483647)::int4",
            "(-2147483648)::int4",
            "('2147483648'::int8)::int4", // overflow via runtime cast
            "(65536)::int4",
        ],
        SqlType::Int8 => &[
            "(0)::int8",
            "(-1)::int8",
            "(9223372036854775807)::int8",
            "(-9223372036854775808)::int8",
            "(4294967296)::int8",
            "(2147483648)::int8", // just past int4
        ],
        // Float specials: infinities, NaN, signed zero, subnormals,
        // max/min finite — for BOTH widths.
        SqlType::Float4 => &[
            "('Infinity')::float4",
            "('-Infinity')::float4",
            "('NaN')::float4",
            "('-0')::float4",
            "('3.4028235e38')::float4",  // max finite
            "('-3.4028235e38')::float4",
            "('1.1754944e-38')::float4", // min normal
            "('1e-45')::float4",         // subnormal
            "('-1e-45')::float4",
        ],
        SqlType::Float8 => &[
            "('Infinity')::float8",
            "('-Infinity')::float8",
            "('NaN')::float8",
            "('-0')::float8",
            "('1.7976931348623157e308')::float8", // max finite
            "('-1.7976931348623157e308')::float8",
            "('2.2250738585072014e-308')::float8", // min normal
            "('5e-324')::float8",                  // min subnormal
            "('1e-310')::float8",                  // mid subnormal
        ],
        // Numeric specials — the pilot's #1 gap: the corpus had NO
        // Infinity numerics at all, so every `is_special()` branch was
        // dead. NaN alone is not enough.
        SqlType::Numeric => &[
            "('Infinity')::numeric",
            "('-Infinity')::numeric",
            "('NaN')::numeric",
            "('0')::numeric",
            "('-0.0')::numeric",
            "('1e-16383')::numeric",  // min display scale region
            "('9.999999999e131071')::numeric", // near max weight
            "('0.00000000000000000001')::numeric",
            "('-1')::numeric",
        ],
        // Empty string, quote/escape shapes, padded, long.
        SqlType::Text | SqlType::Varchar => &[
            "('')::text",
            "(' ')::text",
            "('''')::text",
            "(repeat('a', 10000))::text",
            "(E'a\\nb')::text",
        ],
        SqlType::Bool => &["TRUE", "FALSE"],
        // Dates: epoch, leap-day rules (valid AND reject), BC/AD,
        // infinities, range ends.
        SqlType::Date => &[
            "('epoch')::date",
            "('infinity')::date",
            "('-infinity')::date",
            "DATE '2000-02-29'",           // century leap day (valid)
            "DATE '2004-02-29'",           // ordinary leap day
            "('1900-02-29'::text)::date",  // century non-leap: REJECT
            "('2019-02-29'::text)::date",  // non-leap year: REJECT
            "DATE 'February 29, 2000'",    // month-name spelling
            "DATE '0001-01-01 BC'",
            "DATE '0001-01-01 AD'",
            "DATE '4714-11-24 BC'",        // Julian day 0
            "DATE '5874897-12-31'",        // max
        ],
        SqlType::Timestamp => &[
            "('epoch')::timestamp",
            "('infinity')::timestamp",
            "('-infinity')::timestamp",
            "TIMESTAMP '2000-06-15 12:00 AM'", // midnight fold
            "TIMESTAMP '2000-06-15 12:00 PM'", // noon fold
            "TIMESTAMP '1999-12-31 23:59:60'", // leap second (folds to 00:00:00)
            "TIMESTAMP '4714-11-24 00:00:00 BC'",
            "TIMESTAMP '294276-12-31 23:59:59.999999'", // max
            "TIMESTAMP '2000-01-01 00:00:00.000001'",
        ],
        // Times: the 24:00 boundary AND just past it (nonzero later
        // field), AM/PM folds both ways, leap second.
        SqlType::Time => &[
            "TIME '24:00:00'",
            "('24:00:01'::text)::time",   // REJECT: past the boundary
            "('24:01:00'::text)::time",   // REJECT: nonzero minute
            "('25:00:00'::text)::time",   // REJECT
            "TIME '12:00 AM'",            // -> 00:00:00
            "TIME '12:00 PM'",            // -> 12:00:00
            "TIME '12:00:00.000001 AM'",
            "TIME '11:59:59.999999 PM'",
            "TIME '23:59:60'",            // leap second folds to 24:00:00
            "TIME '00:00:00'",
            "TIME '23:59:59.999999'",
        ],
        SqlType::Timetz => &[
            "TIMETZ '24:00:00+00'",
            "('24:00:01+00'::text)::timetz", // REJECT
            "TIMETZ '12:00 AM +05:45'",
            "TIMETZ '12:00 PM -08'",
            "TIMETZ '23:59:60+00'",          // leap second
            "TIMETZ '15:00:00+15:59:59'",    // max zone offset
            "TIMETZ '15:00:00-15:59:59'",
        ],
        // Intervals: uncommon unit words, qualified (`HOUR TO MINUTE`)
        // forms, ISO-8601 overflow spellings, `ago`.
        SqlType::Interval => &[
            "('1 decade')::interval",
            "('1 century')::interval",
            "('1 centuries')::interval",
            "('1 millennium')::interval",
            "('2 weeks')::interval",
            "('1 decade 1 century 1 millennium')::interval",
            "(INTERVAL '2:03' HOUR TO MINUTE)",
            "(INTERVAL '2:03:04' MINUTE TO SECOND)",
            "(INTERVAL '1-2' YEAR TO MONTH)",
            "(INTERVAL '5 4:03:02.1' DAY TO SECOND)",
            "(INTERVAL '100' SECOND (3))",
            "('P100000000000000D'::text)::interval",   // REJECT: ISO overflow
            "('PT2562047788:00:54.775807')::interval", // near max time part
            "('@ 1 decade 2 centuries ago')::interval",
            "('178000000 years')::interval",
            "('-178000000 years')::interval",
        ],
        SqlType::Json | SqlType::Jsonb => &[
            "('{}')::jsonb",
            "('[]')::jsonb",
            "('null')::jsonb",
            "('\"\"')::jsonb",
            "('0')::jsonb",
            "('-0.0')::jsonb",
            "('1e308')::jsonb",
        ],
        SqlType::Uuid => &[
            "('00000000-0000-0000-0000-000000000000')::uuid",
            "('ffffffff-ffff-ffff-ffff-ffffffffffff')::uuid",
        ],
        SqlType::Bytea => &[
            "('\\x')::bytea",   // empty
            "('\\x00')::bytea",
            "('\\xff')::bytea",
            "('\\000'::text)::bytea",  // octal-escape spelling
            "('\\\\')::bytea",         // escaped-backslash spelling
        ],
        // Empty / NULL-bearing / duplicated arrays.
        SqlType::TextArr => &[
            "('{}')::text[]",
            "('{NULL}')::text[]",
            "('{\"\"}')::text[]",
            "(ARRAY[]::text[])",
        ],
        SqlType::Int4Arr => &[
            "('{}')::int4[]",
            "('{NULL}')::int4[]",
            "(ARRAY[2147483647,-2147483648,0,-1])",
            "(ARRAY[]::int4[])",
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ALL_TYPES;

    #[test]
    fn every_generic_type_has_a_bank() {
        // Every type the generic machinery draws must have boundary
        // representation; json/jsonb are reached via explicit productions
        // but still carry a bank.
        for &ty in ALL_TYPES {
            assert!(
                !bank(ty).is_empty(),
                "no boundary bank for {:?}",
                ty
            );
        }
        assert!(!bank(SqlType::Jsonb).is_empty());
    }

    #[test]
    fn bank_entries_are_self_delimiting() {
        // Cast form `(...)::t`, keyword literal (TIME '...' etc.), or an
        // uppercase keyword (TRUE/FALSE) — never a bare number/string that
        // could re-associate with surrounding operators.
        for &ty in ALL_TYPES {
            for &lit in bank(ty) {
                let ok = lit.starts_with('(')
                    || lit
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_uppercase())
                        .unwrap_or(false);
                assert!(ok, "not self-delimiting: {}", lit);
            }
        }
    }
}
