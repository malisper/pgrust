//! Format-picture caches for DCH (date/time) and NUM (number) pictures.
//!
//! Faithful port of the cache machinery in formatting.c (DCH_cache_* at
//! :3716-3935, NUM_cache_* at :4896-5072).  The C code keeps fixed-size arrays
//! of entries in `TopMemoryContext`, persistent across transactions, with an
//! aging counter and LRU-ish eviction.  We reproduce the same algorithm with a
//! process-global, mutex-guarded set of entries (the picture caches are
//! genuinely process-wide shared state).
//!
//! Entries store the parsed `FormatNode` list (owned `Vec`), which is what the
//! C struct's fixed `format[]` array holds.

use std::sync::{Mutex, OnceLock};

use types_error::PgResult;

use crate::parse::parse_format;
use crate::tables::*;

struct DchCacheEntry {
    format: Vec<FormatNode>,
    str: Vec<u8>,
    std: bool,
    valid: bool,
    age: i32,
}

struct NumCacheEntry {
    format: Vec<FormatNode>,
    str: Vec<u8>,
    valid: bool,
    age: i32,
    num: NUMDesc,
}

#[derive(Default)]
struct DchCacheState {
    cache: Vec<DchCacheEntry>, // up to DCH_CACHE_ENTRIES
    counter: i32,
}

#[derive(Default)]
struct NumCacheState {
    cache: Vec<NumCacheEntry>,
    counter: i32,
}

fn dch_state() -> &'static Mutex<DchCacheState> {
    static S: OnceLock<Mutex<DchCacheState>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(DchCacheState::default()))
}

fn num_state() -> &'static Mutex<NumCacheState> {
    static S: OnceLock<Mutex<NumCacheState>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(NumCacheState::default()))
}

/// C: `DCH_prevent_counter_overflow` (formatting.c:3723).
fn dch_prevent_counter_overflow(st: &mut DchCacheState) {
    if st.counter >= i32::MAX - 1 {
        for e in st.cache.iter_mut() {
            e.age >>= 1;
        }
        st.counter >>= 1;
    }
}

/// C: `NUM_prevent_counter_overflow` (formatting.c:4897).
fn num_prevent_counter_overflow(st: &mut NumCacheState) {
    if st.counter >= i32::MAX - 1 {
        for e in st.cache.iter_mut() {
            e.age >>= 1;
        }
        st.counter >>= 1;
    }
}

/// Fetch (parse + cache or return cached) the DCH format for `str`.
///
/// Combines `DCH_cache_search` / `DCH_cache_getnew` / `DCH_cache_fetch`
/// (formatting.c:3894-3935): returns the parsed `FormatNode` list.
pub fn dch_cache_fetch(str: &[u8], std: bool) -> PgResult<Vec<FormatNode>> {
    let st = &mut *dch_state().lock().expect("DCH cache lock");

    // DCH_cache_search
    dch_prevent_counter_overflow(st);
    for e in st.cache.iter_mut() {
        if e.valid && e.str == str && e.std == std {
            e.age = {
                st.counter += 1;
                st.counter
            };
            return Ok(e.format.clone());
        }
    }

    // DCH_cache_getnew + parse + mark valid
    let format = parse_format(
        str,
        DCH_KEYWORDS,
        DCH_SUFF,
        &DCH_INDEX,
        DCH_FLAG | if std { STD_FLAG } else { 0 },
        None,
    )?;

    dch_cache_getnew(st, str, std, format.clone());
    Ok(format)
}

/// C: `DCH_cache_getnew` (formatting.c:3834): select/recycle an entry.
fn dch_cache_getnew(st: &mut DchCacheState, str: &[u8], std: bool, format: Vec<FormatNode>) {
    dch_prevent_counter_overflow(st);

    if st.cache.len() >= DCH_CACHE_ENTRIES {
        // Find oldest valid, or first not-valid, entry.
        let mut old_idx = 0usize;
        if st.cache[0].valid {
            for i in 1..DCH_CACHE_ENTRIES {
                if !st.cache[i].valid {
                    old_idx = i;
                    break;
                }
                if st.cache[i].age < st.cache[old_idx].age {
                    old_idx = i;
                }
            }
        }
        st.counter += 1;
        let age = st.counter;
        let e = &mut st.cache[old_idx];
        // C DCH_cache_getnew recycle branch (formatting.c:3869-3873) sets only
        // valid=false, str, age; it does NOT touch `std`.  Only the NEW branch
        // (:3885) sets ent->std.  Match that exactly (the subsequent search
        // compares both str AND std, so the stale `std` is harmless).
        e.valid = true;
        e.str = str.to_vec();
        e.format = format;
        e.age = age;
    } else {
        st.counter += 1;
        let age = st.counter;
        st.cache.push(DchCacheEntry {
            format,
            str: str.to_vec(),
            std,
            valid: true,
            age,
        });
    }
}

/// Fetch (parse + cache or return cached) the NUM format for `str`, returning
/// the parsed nodes and the prepared `NUMDesc`.
///
/// Combines `NUM_cache_search`/`NUM_cache_getnew`/`NUM_cache_fetch`
/// (formatting.c:4968-5011).
pub fn num_cache_fetch(str: &[u8]) -> PgResult<(Vec<FormatNode>, NUMDesc)> {
    let st = &mut *num_state().lock().expect("NUM cache lock");

    num_prevent_counter_overflow(st);
    for e in st.cache.iter_mut() {
        if e.valid && e.str == str {
            e.age = {
                st.counter += 1;
                st.counter
            };
            return Ok((e.format.clone(), e.num.clone()));
        }
    }

    // NUM_cache_getnew + zeroize Num + parse + mark valid.
    let mut num = NUMDesc::default();
    num.zeroize();
    let format = parse_format(str, NUM_KEYWORDS, &[], &NUM_INDEX, NUM_FLAG, Some(&mut num))?;

    num_cache_getnew(st, str, format.clone(), num.clone());
    Ok((format, num))
}

/// C: `NUM_cache_getnew` (formatting.c:4909).
fn num_cache_getnew(st: &mut NumCacheState, str: &[u8], format: Vec<FormatNode>, num: NUMDesc) {
    num_prevent_counter_overflow(st);

    if st.cache.len() >= NUM_CACHE_ENTRIES {
        let mut old_idx = 0usize;
        if st.cache[0].valid {
            for i in 1..NUM_CACHE_ENTRIES {
                if !st.cache[i].valid {
                    old_idx = i;
                    break;
                }
                if st.cache[i].age < st.cache[old_idx].age {
                    old_idx = i;
                }
            }
        }
        st.counter += 1;
        let age = st.counter;
        let e = &mut st.cache[old_idx];
        e.valid = true;
        e.str = str.to_vec();
        e.format = format;
        e.num = num;
        e.age = age;
    } else {
        st.counter += 1;
        let age = st.counter;
        st.cache.push(NumCacheEntry {
            format,
            str: str.to_vec(),
            valid: true,
            age,
            num,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: these tests use separator-free pictures so `parse_format` never
    // reaches the `pg_mblen` seam (which has no installed provider under
    // `cargo test`).

    #[test]
    fn dch_cache_returns_same_parse() {
        let a = dch_cache_fetch(b"YYYYMMDD", false).unwrap();
        let b = dch_cache_fetch(b"YYYYMMDD", false).unwrap();
        assert_eq!(a.len(), b.len());
        assert_eq!(a[0].key, b[0].key);
    }

    #[test]
    fn num_cache_returns_format_and_desc() {
        let (fmt, num) = num_cache_fetch(b"9999.99").unwrap();
        assert_eq!(num.pre, 4);
        assert_eq!(num.post, 2);
        assert!(!fmt.is_empty());
    }
}
