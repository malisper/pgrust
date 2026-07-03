//! DCH/NUM format-picture caches. C keeps fixed arrays in TopMemoryContext,
//! persistent per backend; here they are backend-thread-local. A cache hit
//! returns an `Rc<[FormatNode]>` (refcount bump, no per-row array copy),
//! mirroring C's `format = ent->format` pointer share reused across rows.

use std::cell::RefCell;
use std::rc::Rc;

use ::types_error::PgResult;

use crate::parse::parse_format;
use crate::tables::*;

struct DchCacheEntry {
    format: Rc<[FormatNode]>,
    str: Vec<u8>,
    std: bool,
    valid: bool,
    age: i32,
}

struct NumCacheEntry {
    format: Rc<[FormatNode]>,
    str: Vec<u8>,
    valid: bool,
    age: i32,
    num: NUMDesc,
}

#[derive(Default)]
struct DchCacheState {
    cache: Vec<DchCacheEntry>,
    counter: i32,
}

#[derive(Default)]
struct NumCacheState {
    cache: Vec<NumCacheEntry>,
    counter: i32,
}

thread_local! {
    static DCH: RefCell<DchCacheState> = RefCell::new(DchCacheState::default());
    static NUM: RefCell<NumCacheState> = RefCell::new(NumCacheState::default());
}

fn dch_prevent_counter_overflow(st: &mut DchCacheState) {
    if st.counter >= i32::MAX - 1 {
        for e in st.cache.iter_mut() {
            e.age >>= 1;
        }
        st.counter >>= 1;
    }
}

fn num_prevent_counter_overflow(st: &mut NumCacheState) {
    if st.counter >= i32::MAX - 1 {
        for e in st.cache.iter_mut() {
            e.age >>= 1;
        }
        st.counter >>= 1;
    }
}

pub fn dch_cache_fetch(str: &[u8], std: bool) -> PgResult<Rc<[FormatNode]>> {
    DCH.with(|c| {
        let st = &mut *c.borrow_mut();
        dch_prevent_counter_overflow(st);
        for e in st.cache.iter_mut() {
            if e.valid && e.str == str && e.std == std {
                st.counter += 1;
                e.age = st.counter;
                return Ok(Rc::clone(&e.format));
            }
        }

        let format: Rc<[FormatNode]> = parse_format(
            str,
            DCH_KEYWORDS,
            DCH_SUFF,
            &DCH_INDEX,
            DCH_FLAG | if std { STD_FLAG } else { 0 },
            None,
        )?
        .into();

        dch_cache_getnew(st, str, std, Rc::clone(&format));
        Ok(format)
    })
}

fn dch_cache_getnew(st: &mut DchCacheState, str: &[u8], std: bool, format: Rc<[FormatNode]>) {
    dch_prevent_counter_overflow(st);

    if st.cache.len() >= DCH_CACHE_ENTRIES {
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

pub fn num_cache_fetch(str: &[u8]) -> PgResult<(Rc<[FormatNode]>, NUMDesc)> {
    NUM.with(|c| {
        let st = &mut *c.borrow_mut();
        num_prevent_counter_overflow(st);
        for e in st.cache.iter_mut() {
            if e.valid && e.str == str {
                st.counter += 1;
                e.age = st.counter;
                return Ok((Rc::clone(&e.format), e.num.clone()));
            }
        }

        let mut num = NUMDesc::default();
        num.zeroize();
        let format: Rc<[FormatNode]> =
            parse_format(str, NUM_KEYWORDS, &[], &NUM_INDEX, NUM_FLAG, Some(&mut num))?.into();

        num_cache_getnew(st, str, Rc::clone(&format), num.clone());
        Ok((format, num))
    })
}

fn num_cache_getnew(st: &mut NumCacheState, str: &[u8], format: Rc<[FormatNode]>, num: NUMDesc) {
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
