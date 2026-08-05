//! gin_hstore_ops cores (hstore_gin.c), installed on gin_hstore_seams. Each
//! index entry is a text image [VARHDR|flag|bytes], flag 'K' (key), 'V'
//! (value) or 'N' (null value).

use datum::Datum;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::repr::HstoreView;

const HSTORE_CONTAINS_STRATEGY: u16 = 7;
const HSTORE_EXISTS_STRATEGY: u16 = 9;
const HSTORE_EXISTS_ANY_STRATEGY: u16 = 10;
const HSTORE_EXISTS_ALL_STRATEGY: u16 = 11;

const KEYFLAG: u8 = b'K';
const VALFLAG: u8 = b'V';
const NULLFLAG: u8 = b'N';

const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
const GIN_SEARCH_MODE_ALL: i32 = 2;
const GIN_FALSE: i8 = 0;

fn makeitem(s: &[u8], flag: u8) -> Vec<u8> {
    let size = 4 + s.len() + 1;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(size));
    img.push(flag);
    img.extend_from_slice(s);
    img
}

fn varlena_payload(image: &[u8]) -> &[u8] {
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= 4 => &image[4..],
        _ => &[],
    }
}

pub(crate) fn extract_value(hs_image: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let hs = HstoreView::from_vardata(varlena_payload(hs_image));
    let mut entries = Vec::with_capacity(2 * hs.count());
    for i in 0..hs.count() {
        entries.push(makeitem(hs.key(i), KEYFLAG));
        if hs.val_isnull(i) {
            entries.push(makeitem(&[], NULLFLAG));
        } else {
            entries.push(makeitem(hs.val(i), VALFLAG));
        }
    }
    Ok(entries)
}

pub(crate) fn extract_query(query_image: &[u8], strategy: u16) -> PgResult<(Vec<Vec<u8>>, i32)> {
    match strategy {
        HSTORE_CONTAINS_STRATEGY => {
            let entries = extract_value(query_image)?;
            let mode = if entries.is_empty() {
                GIN_SEARCH_MODE_ALL
            } else {
                GIN_SEARCH_MODE_DEFAULT
            };
            Ok((entries, mode))
        }
        HSTORE_EXISTS_STRATEGY => Ok((
            vec![makeitem(varlena_payload(query_image), KEYFLAG)],
            GIN_SEARCH_MODE_DEFAULT,
        )),
        HSTORE_EXISTS_ANY_STRATEGY | HSTORE_EXISTS_ALL_STRATEGY => {
            let scratch = mcx::MemoryContext::new("gin_extract_hstore_query text[]");
            let keys = crate::deconstruct_text_array(scratch.mcx(), query_image)?;
            let entries: Vec<Vec<u8>> = keys
                .iter()
                .flatten()
                .map(|k| makeitem(k, KEYFLAG))
                .collect();
            let mode = if entries.is_empty() && strategy == HSTORE_EXISTS_ALL_STRATEGY {
                GIN_SEARCH_MODE_ALL
            } else {
                GIN_SEARCH_MODE_DEFAULT
            };
            Ok((entries, mode))
        }
        other => Err(PgError::error(format!("unrecognized strategy number: {other}")).into()),
    }
}

pub(crate) fn consistent(check: &[i8], strategy: u16, nkeys: usize) -> PgResult<(bool, bool)> {
    match strategy {
        HSTORE_CONTAINS_STRATEGY => {
            // Key/value correspondence isn't tracked: recheck on the heap.
            Ok(((0..nkeys).all(|i| check[i] != GIN_FALSE), true))
        }
        HSTORE_EXISTS_STRATEGY | HSTORE_EXISTS_ANY_STRATEGY => Ok((true, false)),
        HSTORE_EXISTS_ALL_STRATEGY => {
            Ok(((0..nkeys).all(|i| check[i] != GIN_FALSE), false))
        }
        other => Err(PgError::error(format!("unrecognized strategy number: {other}")).into()),
    }
}

#[track_caller]
#[cold]
fn gin_symbol_via_fmgr(name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "hstore: {name} reached through a raw fmgr call — the GIN core dispatches \
         gin_hstore_ops through its opclass enum (gin_hstore_seams), never via fmgr"
    )))
}

macro_rules! fc_gin_stub {
    ($($fname:ident: $sym:literal;)*) => {$(
        pub fn $fname(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Err(gin_symbol_via_fmgr($sym))
        }
    )*};
}

fc_gin_stub! {
    fc_gin_extract_hstore: "gin_extract_hstore";
    fc_gin_extract_hstore_query: "gin_extract_hstore_query";
    fc_gin_consistent_hstore: "gin_consistent_hstore";
}
