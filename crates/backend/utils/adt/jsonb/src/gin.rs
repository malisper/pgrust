//! jsonb_gin.c, jsonb_ops half: gin_compare_jsonb / gin_extract_jsonb /
//! gin_extract_jsonb_query / gin_consistent_jsonb / gin_triconsistent_jsonb.
//! jsonb_path_ops and the jsonpath strategies are loud.

extern crate alloc;

use crate::container::JsonbItem;
use crate::iter::{JsonbIterator, WjbToken};
use adt_numeric::{get_str_from_var, Num, NumericVar};
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_error::PgResult;

pub const JsonbContainsStrategyNumber: u16 = 7;
pub const JsonbExistsStrategyNumber: u16 = 9;
pub const JsonbExistsAnyStrategyNumber: u16 = 10;
pub const JsonbExistsAllStrategyNumber: u16 = 11;
pub const JsonbJsonpathExistsStrategyNumber: u16 = 15;
pub const JsonbJsonpathPredicateStrategyNumber: u16 = 16;

const JGINFLAG_KEY: u8 = 0x01;
const JGINFLAG_NULL: u8 = 0x02;
const JGINFLAG_BOOL: u8 = 0x03;
const JGINFLAG_NUM: u8 = 0x04;
const JGINFLAG_STR: u8 = 0x05;
const JGINFLAG_HASHED: u8 = 0x10;
const JGIN_MAXLENGTH: usize = 125;

pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
pub const GIN_SEARCH_MODE_ALL: i32 = 2;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: jsonb_gin {what}")
}

/// gin_compare_jsonb over text payloads (header already stripped). C uses
/// varstr_cmp with C collation == memcmp + length tiebreak.
pub fn gin_compare_jsonb(a: &[u8], b: &[u8]) -> i32 {
    varlena::varstrfastcmp_c(a, b)
}

/// make_text_key: 4-byte-header text datum `flag || str` (hashing overlength
/// keys), allocated in `mcx`.
fn make_text_key<'m>(mcx: Mcx<'m>, mut flag: u8, s: &[u8]) -> PgResult<Datum> {
    let mut hashbuf = [0u8; 8];
    let str_: &[u8] = if s.len() > JGIN_MAXLENGTH {
        let hashval = hashfn::hash_bytes(s);
        let hex = alloc::format!("{hashval:08x}");
        hashbuf.copy_from_slice(hex.as_bytes());
        flag |= JGINFLAG_HASHED;
        &hashbuf
    } else {
        s
    };

    let len = str_.len();
    let total = 4 + len + 1;
    let mut item: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut item, &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut item, &[flag])?;
    mcx::vec_append_bytes(&mut item, str_)?;
    let p = item.as_ptr();
    core::mem::forget(item);
    Ok(Datum::from_usize(p as usize))
}

/// numeric_normalize: strip trailing zeroes, render to text.
fn numeric_normalize(image: &[u8], out: &mut alloc::vec::Vec<u8>) {
    // JsonbItem::Numeric carries the full 4-byte-header numeric image.
    let num = Num::from_payload(&image[4..]);
    let mut var = NumericVar::from_view(num.view());
    var.strip();
    get_str_from_var(var.view(), out);
}

/// make_scalar_key.
fn make_scalar_key<'m>(mcx: Mcx<'m>, v: &JsonbItem<'_>, is_key: bool) -> PgResult<Datum> {
    match v {
        JsonbItem::Null => {
            debug_assert!(!is_key);
            make_text_key(mcx, JGINFLAG_NULL, b"")
        }
        JsonbItem::Bool(b) => {
            debug_assert!(!is_key);
            make_text_key(mcx, JGINFLAG_BOOL, if *b { b"t" } else { b"f" })
        }
        JsonbItem::Numeric(image) => {
            debug_assert!(!is_key);
            let mut cstr = alloc::vec::Vec::new();
            numeric_normalize(image, &mut cstr);
            make_text_key(mcx, JGINFLAG_NUM, &cstr)
        }
        JsonbItem::String(s) => {
            make_text_key(mcx, if is_key { JGINFLAG_KEY } else { JGINFLAG_STR }, s)
        }
        other => panic!("unrecognized jsonb scalar type: {}", other.type_ord()),
    }
}

/// gin_extract_jsonb over a detoasted jsonb payload.
pub fn gin_extract_jsonb<'m>(mcx: Mcx<'m>, payload: &[u8]) -> PgResult<PgVec<'m, Datum>> {
    let mut entries: PgVec<'m, Datum> = mcx::vec_new_in(mcx);

    let mut it = JsonbIterator::init(mcx, payload)?;
    loop {
        let (tok, v) = it.next(false);
        match tok {
            WjbToken::Done => break,
            WjbToken::Key => entries.push(make_scalar_key(mcx, &v, true)?),
            WjbToken::Elem => {
                let is_key = matches!(v, JsonbItem::String(_));
                entries.push(make_scalar_key(mcx, &v, is_key)?);
            }
            WjbToken::Value => entries.push(make_scalar_key(mcx, &v, false)?),
            _ => {}
        }
    }
    Ok(entries)
}

/// gin_extract_jsonb_query. `query_payload` is the detoasted right-hand
/// operand payload (jsonb for @>, text for ?, text[] for ?| / ?&).
pub fn gin_extract_jsonb_query<'m>(
    mcx: Mcx<'m>,
    query_payload: &[u8],
    strategy: u16,
) -> PgResult<(PgVec<'m, Datum>, i32)> {
    let mut search_mode = GIN_SEARCH_MODE_DEFAULT;
    let entries = match strategy {
        JsonbContainsStrategyNumber => {
            let entries = gin_extract_jsonb(mcx, query_payload)?;
            if entries.is_empty() {
                search_mode = GIN_SEARCH_MODE_ALL;
            }
            entries
        }
        JsonbExistsStrategyNumber => {
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, 1)?;
            entries.push(make_text_key(mcx, JGINFLAG_KEY, query_payload)?);
            entries
        }
        JsonbExistsAnyStrategyNumber | JsonbExistsAllStrategyNumber => {
            unported("?| / ?& text[] query extraction (arrays lane)")
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            unported("jsonpath GIN strategies (@? / @@)")
        }
        other => panic!("unrecognized strategy number: {other}"),
    };
    Ok((entries, search_mode))
}

/// gin_consistent_jsonb.
pub fn gin_consistent_jsonb(
    check: &[i8],
    strategy: u16,
    nkeys: usize,
    recheck: &mut bool,
) -> bool {
    match strategy {
        JsonbContainsStrategyNumber => {
            *recheck = true;
            check[..nkeys].iter().all(|&c| c != 0)
        }
        JsonbExistsStrategyNumber | JsonbExistsAnyStrategyNumber => {
            *recheck = true;
            true
        }
        JsonbExistsAllStrategyNumber => {
            *recheck = true;
            check[..nkeys].iter().all(|&c| c != 0)
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            unported("jsonpath GIN strategies (@? / @@)")
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}

/// gin_triconsistent_jsonb: never GIN_TRUE (recheck always required).
pub fn gin_triconsistent_jsonb(check: &[i8], strategy: u16, nkeys: usize) -> i8 {
    const GIN_FALSE: i8 = 0;
    const GIN_TRUE: i8 = 1;
    const GIN_MAYBE: i8 = 2;
    match strategy {
        JsonbContainsStrategyNumber | JsonbExistsAllStrategyNumber => {
            for &c in &check[..nkeys] {
                if c == GIN_FALSE {
                    return GIN_FALSE;
                }
            }
            GIN_MAYBE
        }
        JsonbExistsStrategyNumber | JsonbExistsAnyStrategyNumber => {
            for &c in &check[..nkeys] {
                if c == GIN_TRUE || c == GIN_MAYBE {
                    return GIN_MAYBE;
                }
            }
            GIN_FALSE
        }
        JsonbJsonpathExistsStrategyNumber | JsonbJsonpathPredicateStrategyNumber => {
            unported("jsonpath GIN strategies (@? / @@)")
        }
        other => panic!("unrecognized strategy number: {other}"),
    }
}
