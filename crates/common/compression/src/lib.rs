//! Port of `src/common/compression.c` (PostgreSQL 18.3): shared code for
//! compression methods and specifications.
//!
//! A compression specification specifies the parameters that should be used
//! when performing compression with a specific algorithm. The simplest
//! possible compression specification is an integer, which sets the
//! compression level.
//!
//! Otherwise, a compression specification is a comma-separated list of items,
//! each having the form keyword or keyword=value. Currently, the supported
//! keywords are "level", "long", and "workers".
//!
//! C's `parse_error` (a palloc'd string checked by the caller) becomes an
//! `Option<String>` on the spec; the error strings are byte-identical to C's
//! so the caller-side `invalid compression specification: %s` report matches.
//!
//! Build-support mapping (C's HAVE_LIBZ / USE_LZ4 / USE_ZSTD):
//! - gzip: always available (miniz_oxide, pure Rust, already in-tree).
//! - lz4: always available (lz4_flex, pure Rust, already in-tree).
//! - zstd: available except on wasm32 (zstd-sys links C); the wasm arm gets
//!   C's exact "this build does not support compression with ZSTD" error.

/// C `pg_compress_algorithm`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PgCompressAlgorithm {
    #[default]
    None,
    Gzip,
    Lz4,
    Zstd,
}

/// C `PG_COMPRESSION_OPTION_WORKERS`.
pub const PG_COMPRESSION_OPTION_WORKERS: u32 = 1 << 0;
/// C `PG_COMPRESSION_OPTION_LONG_DISTANCE`.
pub const PG_COMPRESSION_OPTION_LONG_DISTANCE: u32 = 1 << 1;

/// zlib `Z_DEFAULT_COMPRESSION`.
pub const Z_DEFAULT_COMPRESSION: i32 = -1;

/// zstd's `ZSTD_CLEVEL_DEFAULT` / `ZSTD_minCLevel()` / `ZSTD_maxCLevel()`,
/// read from the linked libzstd where it exists.
#[cfg(not(target_family = "wasm"))]
fn zstd_default_level() -> i32 {
    zstd::zstd_safe::CLEVEL_DEFAULT
}
#[cfg(not(target_family = "wasm"))]
fn zstd_min_level() -> i32 {
    zstd::zstd_safe::min_c_level()
}
#[cfg(not(target_family = "wasm"))]
fn zstd_max_level() -> i32 {
    zstd::zstd_safe::max_c_level()
}

/// C `pg_compress_specification`.
#[derive(Clone, Debug, Default)]
pub struct PgCompressSpecification {
    pub algorithm: PgCompressAlgorithm,
    pub options: u32,
    pub level: i32,
    pub workers: i32,
    pub long_distance: bool,
    pub parse_error: Option<String>,
}

/// C `parse_compress_algorithm`. Look up a compression algorithm by name
/// (case-sensitively, as C's strcmp does). Returns `None` if unrecognized.
pub fn parse_compress_algorithm(name: &str) -> Option<PgCompressAlgorithm> {
    match name {
        "none" => Some(PgCompressAlgorithm::None),
        "gzip" => Some(PgCompressAlgorithm::Gzip),
        "lz4" => Some(PgCompressAlgorithm::Lz4),
        "zstd" => Some(PgCompressAlgorithm::Zstd),
        _ => None,
    }
}

/// C `get_compress_algorithm_name`.
pub fn get_compress_algorithm_name(algorithm: PgCompressAlgorithm) -> &'static str {
    match algorithm {
        PgCompressAlgorithm::None => "none",
        PgCompressAlgorithm::Gzip => "gzip",
        PgCompressAlgorithm::Lz4 => "lz4",
        PgCompressAlgorithm::Zstd => "zstd",
    }
}

/// C's `strtol(s, &endp, 10)` subset used here: optional leading whitespace,
/// optional sign, then digits; returns the parsed value and the number of
/// bytes consumed (0 when no digits were found).
fn strtol10(s: &str) -> (i64, usize) {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && (b[i] == b' ' || b[i] == b'\t' || b[i] == b'\n' || b[i] == b'\r') {
        i += 1;
    }
    let mut sign: i64 = 1;
    let mut j = i;
    if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
        if b[j] == b'-' {
            sign = -1;
        }
        j += 1;
    }
    let digits_start = j;
    let mut val: i64 = 0;
    while j < b.len() && b[j].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((b[j] - b'0') as i64);
        j += 1;
    }
    if j == digits_start {
        // No digits: strtol returns 0 with endp == nptr.
        return (0, 0);
    }
    (sign.saturating_mul(val), j)
}

/// C `expect_integer_value`. On failure sets `parse_error` and returns -1.
fn expect_integer_value(
    keyword: &str,
    value: Option<&str>,
    result: &mut PgCompressSpecification,
) -> i32 {
    let Some(value) = value else {
        result.parse_error = Some(format!("compression option \"{keyword}\" requires a value"));
        return -1;
    };
    let (ivalue, consumed) = strtol10(value);
    if consumed == 0 || consumed != value.len() {
        result.parse_error = Some(format!(
            "value for compression option \"{keyword}\" must be an integer"
        ));
        return -1;
    }
    ivalue as i32
}

/// C `expect_boolean_value`. Valid values: yes, no, on, off, 1, 0 (and a
/// bare keyword means true). On failure sets `parse_error` and returns false.
fn expect_boolean_value(
    keyword: &str,
    value: Option<&str>,
    result: &mut PgCompressSpecification,
) -> bool {
    let Some(value) = value else {
        return true;
    };
    let eq = |a: &str| pgstrcasecmp::pg_strcasecmp(value.as_bytes(), a.as_bytes()) == 0;
    if eq("yes") || eq("on") || eq("1") {
        return true;
    }
    if eq("no") || eq("off") || eq("0") {
        return false;
    }
    result.parse_error = Some(format!(
        "value for compression option \"{keyword}\" must be a Boolean value"
    ));
    false
}

/// C `parse_compress_specification`. Parse a compression specification for a
/// specified algorithm. On return all fields are initialized; `parse_error`
/// is `None` iff no errors occurred during parsing. Even without a parse
/// error the spec might not make sense (e.g. gzip level=12 parses OK); use
/// [`validate_compress_specification`] for that.
pub fn parse_compress_specification(
    algorithm: PgCompressAlgorithm,
    specification: Option<&str>,
) -> PgCompressSpecification {
    let mut result = PgCompressSpecification {
        algorithm,
        options: 0,
        level: 0,
        workers: 0,
        long_distance: false,
        parse_error: None,
    };

    // Assign a default level depending on the compression method. This may
    // be enforced later.
    match algorithm {
        PgCompressAlgorithm::None => result.level = 0,
        PgCompressAlgorithm::Lz4 => {
            result.level = 0; // fast compression mode
        }
        PgCompressAlgorithm::Zstd => {
            #[cfg(not(target_family = "wasm"))]
            {
                result.level = zstd_default_level();
            }
            #[cfg(target_family = "wasm")]
            {
                result.parse_error = Some(
                    "this build does not support compression with ZSTD".to_string(),
                );
            }
        }
        PgCompressAlgorithm::Gzip => {
            result.level = Z_DEFAULT_COMPRESSION;
        }
    }

    // If there is no specification, we're done already.
    let Some(mut spec) = specification else {
        return result;
    };

    // As a special case, the specification can be a bare integer.
    let (bare_level, consumed) = strtol10(spec);
    if consumed > 0 && consumed == spec.len() {
        result.level = bare_level as i32;
        return result;
    }

    // Look for comma-separated keyword or keyword=value entries.
    loop {
        // Figure start, end, and length of next keyword and any value.
        let kwend = spec
            .find(|c| c == ',' || c == '=')
            .unwrap_or(spec.len());
        let keyword = &spec[..kwend];
        let has_value = spec.as_bytes().get(kwend) == Some(&b'=');
        let (value, vend): (Option<&str>, Option<usize>) = if has_value {
            let vstart = kwend + 1;
            let vend = spec[vstart..]
                .find(',')
                .map(|off| vstart + off)
                .unwrap_or(spec.len());
            (Some(&spec[vstart..vend]), Some(vend))
        } else {
            (None, None)
        };

        // Reject empty keyword.
        if keyword.is_empty() {
            result.parse_error =
                Some("found empty string where a compression option was expected".to_string());
            break;
        }

        // Handle whatever keyword we found.
        if keyword == "level" {
            result.level = expect_integer_value(keyword, value, &mut result);
            // No need to set a flag in "options": a default level is always
            // set by the logic above.
        } else if keyword == "workers" {
            result.workers = expect_integer_value(keyword, value, &mut result);
            result.options |= PG_COMPRESSION_OPTION_WORKERS;
        } else if keyword == "long" {
            result.long_distance = expect_boolean_value(keyword, value, &mut result);
            result.options |= PG_COMPRESSION_OPTION_LONG_DISTANCE;
        } else {
            result.parse_error =
                Some(format!("unrecognized compression option: \"{keyword}\""));
        }

        // If we got an error or have reached the end of the string, stop.
        let entry_end = vend.unwrap_or(kwend);
        if result.parse_error.is_some() || entry_end == spec.len() {
            break;
        }

        // Advance to next entry and loop around.
        spec = &spec[entry_end + 1..];
    }

    result
}

/// C `validate_compress_specification`. Returns `None` if the spec is
/// syntactically valid and semantically sensible; otherwise an error message.
/// Does not test whether this build supports the requested method (that is
/// reported through `parse_error`).
pub fn validate_compress_specification(spec: &PgCompressSpecification) -> Option<String> {
    // If it didn't even parse OK, it's definitely no good.
    if let Some(err) = &spec.parse_error {
        return Some(err.clone());
    }

    // Check that the algorithm expects a compression level and it is within
    // the legal range for the algorithm.
    // (min_level is only reassigned by the zstd arm, absent on wasm.)
    #[cfg_attr(target_family = "wasm", allow(unused_mut))]
    let mut min_level: i32 = 1;
    let mut max_level: i32 = 1;
    let mut default_level: i32 = 0;
    match spec.algorithm {
        PgCompressAlgorithm::Gzip => {
            max_level = 9;
            default_level = Z_DEFAULT_COMPRESSION;
        }
        PgCompressAlgorithm::Lz4 => {
            max_level = 12;
            default_level = 0; // fast mode
        }
        PgCompressAlgorithm::Zstd => {
            #[cfg(not(target_family = "wasm"))]
            {
                max_level = zstd_max_level();
                min_level = zstd_min_level();
                default_level = zstd_default_level();
            }
        }
        PgCompressAlgorithm::None => {
            if spec.level != 0 {
                return Some(format!(
                    "compression algorithm \"{}\" does not accept a compression level",
                    get_compress_algorithm_name(spec.algorithm)
                ));
            }
        }
    }

    if (spec.level < min_level || spec.level > max_level) && spec.level != default_level {
        return Some(format!(
            "compression algorithm \"{}\" expects a compression level between {} and {} (default at {})",
            get_compress_algorithm_name(spec.algorithm),
            min_level,
            max_level,
            default_level
        ));
    }

    // Of the compression algorithms that we currently support, only zstd
    // allows parallel workers.
    if (spec.options & PG_COMPRESSION_OPTION_WORKERS) != 0
        && spec.algorithm != PgCompressAlgorithm::Zstd
    {
        return Some(format!(
            "compression algorithm \"{}\" does not accept a worker count",
            get_compress_algorithm_name(spec.algorithm)
        ));
    }

    // Of the compression algorithms that we currently support, only zstd
    // supports long-distance mode.
    if (spec.options & PG_COMPRESSION_OPTION_LONG_DISTANCE) != 0
        && spec.algorithm != PgCompressAlgorithm::Zstd
    {
        return Some(format!(
            "compression algorithm \"{}\" does not support long-distance mode",
            get_compress_algorithm_name(spec.algorithm)
        ));
    }

    None
}

#[cfg(test)]
mod tests;
