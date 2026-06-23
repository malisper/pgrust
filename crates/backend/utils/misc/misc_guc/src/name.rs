//! GUC name comparison / hashing and old-name canonicalization from
//! `src/backend/utils/misc/guc.c`: `guc_name_compare`, `guc_name_hash`,
//! `guc_name_match`, `map_old_guc_names`, and
//! `convert_GUC_name_for_parameter_acl`.

use core::cmp::Ordering;

/// `map_old_guc_names[]` (guc.c:190): pairs of (old name, new name) applied to
/// any unrecognized name. Terminated in C by a NULL; here a plain slice.
pub const MAP_OLD_GUC_NAMES: &[(&str, &str)] = &[
    ("sort_mem", "work_mem"),
    ("vacuum_mem", "maintenance_work_mem"),
    ("ssl_ecdh_curve", "ssl_groups"),
];

/// `guc_name_compare(namea, nameb)` (guc.c:1299). ASCII-only case-insensitive
/// comparison (deliberately *not* `strcasecmp`, so the mapping is stable across
/// `setlocale()`). Returns the C tri-state as an `Ordering`.
pub fn guc_name_compare(namea: &str, nameb: &str) -> Ordering {
    let a = namea.as_bytes();
    let b = nameb.as_bytes();
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && a[i] != 0 && j < b.len() && b[j] != 0 {
        let mut cha = a[i];
        let mut chb = b[j];
        i += 1;
        j += 1;
        if cha.is_ascii_uppercase() {
            cha += b'a' - b'A';
        }
        if chb.is_ascii_uppercase() {
            chb += b'a' - b'A';
        }
        if cha != chb {
            return cha.cmp(&chb);
        }
    }
    // C: if (*namea) return 1; if (*nameb) return -1; return 0;
    let a_more = i < a.len() && a[i] != 0;
    let b_more = j < b.len() && b[j] != 0;
    if a_more {
        Ordering::Greater
    } else if b_more {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

/// `guc_name_compare(a, b) == 0` — name equality.
#[inline]
pub fn guc_name_eq(namea: &str, nameb: &str) -> bool {
    guc_name_compare(namea, nameb) == Ordering::Equal
}

/// `pg_rotate_left32(word, n)` (`pg_bitutils.h`): rotate-left a u32.
#[inline]
fn pg_rotate_left32(word: u32, n: u32) -> u32 {
    word.rotate_left(n)
}

/// `guc_name_hash(key, keysize)` (guc.c:1329). Case-folded rolling hash that is
/// compatible with `guc_name_compare`.
pub fn guc_name_hash(name: &str) -> u32 {
    let mut result: u32 = 0;
    for &b in name.as_bytes() {
        if b == 0 {
            break;
        }
        let mut ch = b;
        if ch.is_ascii_uppercase() {
            ch += b'a' - b'A';
        }
        result = pg_rotate_left32(result, 5);
        result ^= ch as u32;
    }
    result
}

/// `guc_name_match(key1, key2, keysize)` (guc.c:1353): dynahash match function,
/// returns `guc_name_compare` (0 == match).
#[inline]
pub fn guc_name_match(name1: &str, name2: &str) -> Ordering {
    guc_name_compare(name1, name2)
}

/// `convert_GUC_name_for_parameter_acl(name)` (guc.c:1373): apply the old-name
/// mapping, then ASCII case-fold to lower (matching `guc_name_compare`). Returns
/// the canonical name as an owned string.
pub fn convert_guc_name_for_parameter_acl(name: &str) -> String {
    let mut canonical = name;
    for (old, new) in MAP_OLD_GUC_NAMES {
        if guc_name_eq(name, old) {
            canonical = new;
            break;
        }
    }

    let mut result = String::with_capacity(canonical.len());
    for &b in canonical.as_bytes() {
        let ch = if b.is_ascii_uppercase() {
            b + (b'a' - b'A')
        } else {
            b
        };
        result.push(ch as char);
    }
    result
}
