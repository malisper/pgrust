//! `config_enum_lookup_by_value` / `config_enum_lookup_by_name` /
//! `config_enum_get_options` from `src/backend/utils/misc/guc.c`.

use crate::model::config_enum;

/// Case-insensitive ASCII compare used by `pg_strcasecmp` on the option names.
/// (`pg_strcasecmp` defers to the locale for high-bit bytes, but GUC enum option
/// names are all ASCII literals, so the ASCII fold is exact for them.)
fn ascii_caseless_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

/// `config_enum_lookup_by_value(record, val)` (guc.c:3022). Returns the option
/// name for the given value. The C version `elog(ERROR)`s on a value that is not
/// in the table (it is only ever called with known-valid values); we return
/// `None` so the caller can surface that as an internal error, never a silent
/// wrong answer.
pub fn config_enum_lookup_by_value(record: &config_enum, val: i32) -> Option<&'static str> {
    for entry in record.entries() {
        // The C loop stops at the first entry with a NULL `name` (the table
        // terminator); our slice has no terminator.
        if entry.val == val {
            return Some(entry.name);
        }
    }
    None
}

/// `config_enum_lookup_by_name(record, value, &retval)` (guc.c:3045). Returns
/// `Some(val)` if found (case-insensitive), `None` otherwise.
pub fn config_enum_lookup_by_name(record: &config_enum, value: &str) -> Option<i32> {
    for entry in record.entries() {
        if ascii_caseless_eq(value, entry.name) {
            return Some(entry.val);
        }
    }
    None
}

/// `config_enum_get_options(record, prefix, suffix, separator)` (guc.c:3071).
/// Returns a string listing all non-hidden options, separated by `separator`,
/// wrapped in `prefix`/`suffix`. Matches the C trailing-separator removal.
pub fn config_enum_get_options(
    record: &config_enum,
    prefix: &str,
    suffix: &str,
    separator: &str,
) -> String {
    let mut retstr = String::new();
    retstr.push_str(prefix);

    let seplen = separator.len();
    for entry in record.entries() {
        if !entry.hidden {
            retstr.push_str(entry.name);
            retstr.push_str(separator);
        }
    }

    // Replace the final separator (C: if retstr.len >= seplen, drop the last
    // `seplen` bytes). All option names + separators are ASCII, so byte-length
    // truncation is character-safe.
    if retstr.len() >= seplen && seplen > 0 {
        retstr.truncate(retstr.len() - seplen);
    }

    retstr.push_str(suffix);
    retstr
}
