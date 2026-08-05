use crate::model::config_enum;

fn ascii_caseless_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| x.to_ascii_lowercase() == y.to_ascii_lowercase())
}

// C elog(ERROR)s on an unknown value; None lets the caller surface that.
pub fn config_enum_lookup_by_value(record: &config_enum, val: i32) -> Option<&'static str> {
    record.entries().iter().find(|e| e.val == val).map(|e| e.name)
}

pub fn config_enum_lookup_by_name(record: &config_enum, value: &str) -> Option<i32> {
    record
        .entries()
        .iter()
        .find(|e| ascii_caseless_eq(value, e.name))
        .map(|e| e.val)
}

pub fn config_enum_get_options(
    record: &config_enum,
    prefix: &str,
    suffix: &str,
    separator: &str,
) -> String {
    let mut retstr = String::new();
    retstr.push_str(prefix);
    for entry in record.entries() {
        if !entry.hidden {
            retstr.push_str(entry.name);
            retstr.push_str(separator);
        }
    }
    let seplen = separator.len();
    if retstr.len() >= seplen && seplen > 0 {
        retstr.truncate(retstr.len() - seplen);
    }
    retstr.push_str(suffix);
    retstr
}
