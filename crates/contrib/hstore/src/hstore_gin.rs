//! `contrib/hstore/hstore_gin.c` — the `gin_hstore_ops` GIN opclass support
//! functions (`gin_extract_hstore` / `gin_extract_hstore_query` /
//! `gin_consistent_hstore`), ported over the generic catalog-driven GIN opclass
//! dispatch.
//!
//! The GIN core resolves each opclass support proc into an `FmgrInfo`, and
//! `gin-core-probe`'s `extdispatch` invokes the body here through a real fmgr
//! frame, passing the value/query on the by-ref lane (arg 0) and the
//! `internal`-out-parameter protocol struct in the internal lane
//! ([`::gin::extproc::GIN_EXTPROC_INTERNAL_SLOT`]), exactly as C's
//! `FunctionCallNColl` passes the by-pointer `internal` arguments.
//!
//! ## Key representation
//!
//! GIN indexes both keys AND values of an hstore. Each index entry is a `text`
//! varlena whose first payload byte is a flag — `K` (key), `V` (value), or `N`
//! (null value) — followed by the raw bytes (`makeitem`). The keys cross the
//! protocol as [`::gin::extproc::GinKey::Varlena`] header-ful `text` images.

use ::gin::extproc::GinKey;
use ::gin::GIN_SEARCH_MODE_ALL;
use ::types_error::{PgError, PgResult};

use crate::repr::HstoreView;

// hstore strategy numbers (hstore.h).
const HSTORE_CONTAINS_STRATEGY: u16 = 7;
const HSTORE_EXISTS_STRATEGY: u16 = 9;
const HSTORE_EXISTS_ANY_STRATEGY: u16 = 10;
const HSTORE_EXISTS_ALL_STRATEGY: u16 = 11;

// makeitem flag bytes (hstore_gin.c).
const KEYFLAG: u8 = b'K';
const VALFLAG: u8 = b'V';
const NULLFLAG: u8 = b'N';

/// `SET_VARSIZE(ptr, size)` — the 4-byte ("4B-U") varlena length word.
fn varsize_header(size: usize) -> [u8; 4] {
    ((size as u32) << 2).to_ne_bytes()
}

/// `makeitem(str, len, flag)` — build an indexable `text` value:
/// `[ VARHDR(4) | flag(1) | str ]` (`VARSIZE = VARHDRSZ + len + 1`).
fn makeitem(s: &[u8], flag: u8) -> Vec<u8> {
    let size = 4 + s.len() + 1;
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&varsize_header(size));
    img.push(flag);
    img.extend_from_slice(s);
    img
}

/// `gin_extract_hstore(hs, &nentries) RETURNS internal` — extract 2 keys per
/// pair (the key, and the value or the null marker).
///
/// `hs_image` is the FULL header-ful hstore varlena (the by-ref lane image);
/// returns the GIN key list (`*nentries == keys.len()`).
pub fn gin_extract_hstore(hs_image: &[u8]) -> Vec<GinKey> {
    let body = crate::varlena_payload(hs_image);
    let hs = HstoreView::from_vardata(body);
    let count = hs.count();
    let mut entries = Vec::with_capacity(2 * count);
    for i in 0..count {
        entries.push(GinKey::Varlena(makeitem(hs.key(i), KEYFLAG)));
        if hs.val_isnull(i) {
            entries.push(GinKey::Varlena(makeitem(&[], NULLFLAG)));
        } else {
            entries.push(GinKey::Varlena(makeitem(hs.val(i), VALFLAG)));
        }
    }
    entries
}

/// `gin_extract_hstore_query(query, &nentries, strategy, ..., &searchMode)
/// RETURNS internal`. Returns `(keys, search_mode_override)` where
/// `search_mode_override` is `Some(GIN_SEARCH_MODE_ALL)` when a full index scan
/// is required.
///
/// `query_image` is the FULL header-ful query varlena; its shape depends on the
/// strategy (an hstore for Contains; a `text` for Exists; a `text[]` for
/// ExistsAny/ExistsAll).
pub fn gin_extract_hstore_query(
    query_image: &[u8],
    strategy: u16,
) -> PgResult<(Vec<GinKey>, Option<i32>)> {
    match strategy {
        HSTORE_CONTAINS_STRATEGY => {
            // Query is an hstore — just apply gin_extract_hstore.
            let entries = gin_extract_hstore(query_image);
            // "contains {}" requires a full index scan.
            let search_mode = if entries.is_empty() {
                Some(GIN_SEARCH_MODE_ALL)
            } else {
                None
            };
            Ok((entries, search_mode))
        }
        HSTORE_EXISTS_STRATEGY => {
            // Query is a text key.
            let key = crate::varlena_payload(query_image);
            Ok((vec![GinKey::Varlena(makeitem(key, KEYFLAG))], None))
        }
        HSTORE_EXISTS_ANY_STRATEGY | HSTORE_EXISTS_ALL_STRATEGY => {
            // Query is a text[]; nulls are ignored (cf hstoreArrayToPairs).
            let keys = deconstruct_text_array_local(query_image)?;
            let mut entries = Vec::with_capacity(keys.len());
            for k in &keys {
                if let Some(b) = k {
                    entries.push(GinKey::Varlena(makeitem(b, KEYFLAG)));
                }
            }
            // ExistsAll with no keys should match everything.
            let search_mode = if entries.is_empty() && strategy == HSTORE_EXISTS_ALL_STRATEGY {
                Some(GIN_SEARCH_MODE_ALL)
            } else {
                None
            };
            Ok((entries, search_mode))
        }
        other => Err(PgError::error(format!(
            "unrecognized strategy number: {other}"
        ))),
    }
}

/// `gin_consistent_hstore(check, strategy, query, nkeys, ..., &recheck)
/// RETURNS bool`. Returns `(matched, recheck)`.
pub fn gin_consistent_hstore(check: &[bool], strategy: u16, nkeys: i32) -> PgResult<(bool, bool)> {
    let nkeys = nkeys.max(0) as usize;
    match strategy {
        HSTORE_CONTAINS_STRATEGY => {
            // The index doesn't track key/value correspondence, so we recheck;
            // but if not all keys are present we can fail at once.
            let recheck = true;
            let mut res = true;
            for &c in check.iter().take(nkeys) {
                if !c {
                    res = false;
                    break;
                }
            }
            Ok((res, recheck))
        }
        HSTORE_EXISTS_STRATEGY | HSTORE_EXISTS_ANY_STRATEGY => {
            // Existence of key is guaranteed in default search mode.
            Ok((true, false))
        }
        HSTORE_EXISTS_ALL_STRATEGY => {
            // Testing for all keys present gives an exact result.
            let mut res = true;
            for &c in check.iter().take(nkeys) {
                if !c {
                    res = false;
                    break;
                }
            }
            Ok((res, false))
        }
        other => Err(PgError::error(format!(
            "unrecognized strategy number: {other}"
        ))),
    }
}

/// `deconstruct_array_builtin(query, TEXTOID, ...)` over the header-ful array
/// image — returns each element as `Option<Vec<u8>>` (None == SQL NULL).
fn deconstruct_text_array_local(image: &[u8]) -> PgResult<Vec<Option<Vec<u8>>>> {
    let scratch = ::mcx::MemoryContext::new("gin_extract_hstore_query text[]");
    let mcx = scratch.mcx();
    let v = arrayfuncs::construct::deconstruct_text_array_nullable(mcx, image)?;
    Ok(v.iter()
        .map(|o| o.as_ref().map(|s| s.as_str().as_bytes().to_vec()))
        .collect())
}
