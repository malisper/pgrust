//! The pass-by-reference `Datum` payload registry — the safe stand-in for the
//! backend heap a by-reference `Datum`'s pointer addresses in C.
//!
//! A PostgreSQL `Datum` is a single machine word. For a pass-by-value type the
//! word IS the value; for a pass-by-reference type (`text`/`bytea`/`numeric`/
//! every varlena, `cstring`, fixed-by-ref like `name`/`tid`) the C word is a
//! `palloc`'d pointer and the real payload lives behind it. The idiomatic
//! rewrite forbids raw pointers, so a by-reference `Datum` word here is a 1-based
//! **token** into a per-backend [`RefPayload`] table: word `n >= 1` addresses the
//! `n - 1`-th registered payload, and word `0` stays the C `NULL` /  SQL-NULL
//! sentinel. The table is a `thread_local!` — one backend == one thread here, so
//! no cross-thread aliasing to guard.
//!
//! C `PointerGetDatum(palloc(...))` writes are [`register`]; `DatumGetPointer`
//! reads are [`fetch`] and the typed `fetch_*` helpers. A registered payload
//! lives until [`reset`] (the C analogue is the owning context being
//! reset/deleted — at which point every pointer-`Datum` into it is dangling).

use std::cell::RefCell;

use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use types_fmgr::boundary::RefPayload;

thread_local! {
    /// The per-backend by-reference payload table.
    static REF_TABLE: RefCell<Vec<RefPayload>> = const { RefCell::new(Vec::new()) };
    /// The high-water mark: max table length since the last [`reset`].
    static HIGH_WATER: core::cell::Cell<usize> = const { core::cell::Cell::new(0) };
}

/// C: `ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid Datum
/// pointer")))` — `datum.c` raises this when a by-reference `Datum`'s pointer is
/// NULL. A token of `0`, or past the table end (a stale / fabricated word), is
/// the safe-port equivalent of dereferencing a bad pointer.
#[inline]
fn invalid_datum_pointer() -> PgError {
    PgError::error("invalid Datum pointer").with_sqlstate(ERRCODE_DATA_EXCEPTION)
}

/// Mint a by-reference `Datum` referencing `payload` (C: `resultptr = palloc(n);
/// memcpy(...); PointerGetDatum(resultptr)`). Distinct registrations of equal
/// bytes yield distinct tokens — a copy is a distinct "allocation".
pub fn register(payload: RefPayload) -> Datum {
    REF_TABLE.with(|t| {
        let mut tbl = t.borrow_mut();
        tbl.push(payload);
        let len = tbl.len();
        HIGH_WATER.with(|c| {
            if len > c.get() {
                c.set(len);
            }
        });
        // Word `0` is NULL, so the token is the new length (== index + 1).
        Datum::from_usize(len)
    })
}

/// Mint a by-reference `Datum` over an owned byte image (the
/// [`RefPayload::Varlena`] arm).
pub fn register_bytes(bytes: Vec<u8>) -> Datum {
    register(RefPayload::Varlena(bytes))
}

/// Borrow-and-clone the [`RefPayload`] a by-reference `Datum` references. A token
/// of `0` (NULL) or out of range raises [`invalid_datum_pointer`].
pub fn fetch(value: Datum) -> PgResult<RefPayload> {
    let word = value.as_usize();
    if word == 0 {
        return Err(invalid_datum_pointer());
    }
    REF_TABLE.with(|t| {
        let tbl = t.borrow();
        tbl.get(word - 1)
            .map(|p| p.clone_flat())
            .ok_or_else(invalid_datum_pointer)
    })
}

/// Read the flat byte image a by-reference `Datum` references (`Varlena` ->
/// bytes; `Cstring` -> UTF-8 bytes; `Expanded` -> flattened).
pub fn fetch_varlena(value: Datum) -> PgResult<Vec<u8>> {
    Ok(fetch(value)?.flatten())
}

/// Read the NUL-terminated C-string image a by-reference `Datum` references,
/// *including* its trailing NUL (the `datum_get_cstring` contract: returned
/// length is C's `strlen(s) + 1`).
pub fn fetch_cstring(value: Datum) -> PgResult<Vec<u8>> {
    match fetch(value)? {
        RefPayload::Cstring(s) => {
            let mut bytes = s.into_bytes();
            bytes.push(0);
            Ok(bytes)
        }
        other => Ok(other.flatten()),
    }
}

/// Read the `typ_len` significant bytes a fixed-length by-reference `Datum`
/// references (truncated to `typ_len`, as C copies exactly `realSize == typLen`).
pub fn fetch_fixed(value: Datum, typ_len: i32) -> PgResult<Vec<u8>> {
    let mut bytes = fetch(value)?.flatten();
    if typ_len >= 0 {
        bytes.truncate(typ_len as usize);
    }
    Ok(bytes)
}

/// Borrow the LIVE [`RefPayload`] a by-reference `Datum` references, without the
/// [`fetch`] `clone_flat` (which flattens an `Expanded` and loses the live
/// object). C analogue: dereferencing the Datum's pointer in place.
pub fn with_payload<R>(value: Datum, f: impl FnOnce(&RefPayload) -> PgResult<R>) -> PgResult<R> {
    let word = value.as_usize();
    if word == 0 {
        return Err(invalid_datum_pointer());
    }
    REF_TABLE.with(|t| {
        let tbl = t.borrow();
        let payload = tbl.get(word - 1).ok_or_else(invalid_datum_pointer)?;
        f(payload)
    })
}

/// Clear the per-backend payload table (C analogue: the owning context being
/// reset/deleted). Used at backend teardown and by tests.
pub fn reset() {
    REF_TABLE.with(|t| t.borrow_mut().clear());
    HIGH_WATER.with(|c| c.set(0));
}

/// The number of live registered payloads (observability helper; no C analogue).
pub fn len() -> usize {
    REF_TABLE.with(|t| t.borrow().len())
}

/// The high-water mark: max simultaneously-live payloads since the last
/// [`reset`] (observability; no C analogue).
pub fn high_water() -> usize {
    HIGH_WATER.with(|c| c.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_word_is_invalid_pointer() {
        reset();
        assert!(fetch(Datum::null()).is_err());
        assert!(fetch_varlena(Datum::null()).is_err());
    }

    #[test]
    fn register_then_fetch_roundtrips_varlena() {
        reset();
        let d = register_bytes(b"\x14\x00\x00\x00hello".to_vec());
        assert_ne!(d, Datum::null(), "a by-ref token is never the NULL word");
        assert_eq!(fetch_varlena(d).unwrap(), b"\x14\x00\x00\x00hello");
    }

    #[test]
    fn distinct_registrations_are_distinct_tokens() {
        reset();
        let a = register_bytes(b"same".to_vec());
        let b = register_bytes(b"same".to_vec());
        assert_ne!(a, b, "a copy must be a distinct allocation (distinct token)");
        assert_eq!(fetch_varlena(a).unwrap(), fetch_varlena(b).unwrap());
    }

    #[test]
    fn cstring_fetch_appends_trailing_nul() {
        reset();
        let d = register(RefPayload::Cstring("abc".to_string()));
        assert_eq!(fetch_cstring(d).unwrap(), b"abc\0");
    }

    #[test]
    fn fixed_fetch_truncates_to_typlen() {
        reset();
        let d = register_bytes(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(fetch_fixed(d, 4).unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn out_of_range_token_is_invalid_pointer() {
        reset();
        let _ = register_bytes(vec![9]);
        assert!(fetch(Datum::from_usize(2)).is_err());
    }
}
