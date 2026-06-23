//! Port of PostgreSQL's `jsonb_op.c` — the special jsonb-only operators
//! (existence, containment, B-Tree comparison, and hashing) used by the
//! various index access methods.
//!
//! Mirrors `postgres-18.3/src/backend/utils/adt/jsonb_op.c`.
//!
//! Every top-level function is a thin SQL-facing operator on top of the jsonb
//! container search / compare / hash engine in
//! [`jsonb_util`], reached through a normal path dependency
//! (no cycle, sibling within-port crate).
//!
//! Jsonb arguments arrive as the on-disk container bytes starting at the root
//! `JsonbContainer` header (C: `&jb->root`), matching the `&[u8]` convention of
//! `jsonb_util`.  The `?|` / `?&` operators receive their
//! `text[]` argument as the detoasted array varlena bytes, flattened through
//! the `deconstruct_text_array` seam owned by `backend-utils-adt-arrayfuncs`.
//!
//! Bare-word `PGFunction` registry entry points are deferred; these are the
//! plain Rust workers the dispatcher will call.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use ::jsonb_util::{
    compareJsonbContainers, findJsonbValueFromContainer, jbvType, json_container_is_object,
    json_container_size, JsonbDeepContains, JsonbHashScalarValue, JsonbHashScalarValueExtended,
    JsonbIteratorInit, JsonbIteratorNext, JsonbIteratorToken, JsonbValue, JsonbValueData,
    JB_FARRAY, JB_FOBJECT,
};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};

// ---------------------------------------------------------------------------
// Root-container helpers (C: JB_ROOT_COUNT / JB_ROOT_IS_OBJECT).
//
// These read the leading `JsonbContainer.header` word from the on-disk root
// bytes (the first 4 bytes), matching the `&jb->root` slice the engine
// consumes.
// ---------------------------------------------------------------------------

/// Read the leading `JsonbContainer.header` word from the root bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

/// C: `JB_ROOT_COUNT(jbp)` == `JsonContainerSize(&(jbp)->root)`.
#[inline]
fn jb_root_count(root: &[u8]) -> u32 {
    json_container_size(container_header(root))
}

/// C: `JB_ROOT_IS_OBJECT(jbp)` == `JsonContainerIsObject(&(jbp)->root)`.
#[inline]
fn jb_root_is_object(root: &[u8]) -> bool {
    json_container_is_object(container_header(root))
}

/// Build a `jbvString` [`JsonbValue`] over a copy of the given key bytes
/// (C: the `kval.type = jbvString; kval.val.string.{val,len}` assignment).
#[inline]
fn jbv_string(bytes: &[u8]) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(bytes.to_vec()),
    }
}

// ---------------------------------------------------------------------------
// Existence operators
// ---------------------------------------------------------------------------

/// C: `jsonb_exists(PG_FUNCTION_ARGS)`.
///
/// `jb_root` is the jsonb argument's root container bytes; `key` is the text
/// key's detoasted payload bytes (C: `VARDATA_ANY`/`VARSIZE_ANY_EXHDR`).
///
/// We only match Object keys (which are naturally always Strings), or string
/// elements in arrays.  In particular, we do not match non-string scalar
/// elements.  Existence of a key/element is only considered at the top level.
/// No recursion occurs.
pub fn jsonb_exists(jb_root: &[u8], key: &[u8]) -> PgResult<bool> {
    let kval = jbv_string(key);

    let v = findJsonbValueFromContainer(jb_root, JB_FOBJECT | JB_FARRAY, &kval)?;

    Ok(v.is_some())
}

/// C: `jsonb_exists_any(PG_FUNCTION_ARGS)`.
///
/// `keys` is the detoasted `text[]` array varlena bytes, flattened through the
/// `deconstruct_text_array` seam.
pub fn jsonb_exists_any(jb_root: &[u8], keys: &[u8]) -> PgResult<bool> {
    let key_elems = array_more_seams::deconstruct_text_array::call(keys)?;

    for elem in &key_elems {
        if elem.is_null {
            continue;
        }

        // We rely on the array elements not being toasted.
        let str_val = jbv_string(&elem.value);

        if findJsonbValueFromContainer(jb_root, JB_FOBJECT | JB_FARRAY, &str_val)?.is_some() {
            return Ok(true);
        }
    }

    Ok(false)
}

/// C: `jsonb_exists_all(PG_FUNCTION_ARGS)`.
pub fn jsonb_exists_all(jb_root: &[u8], keys: &[u8]) -> PgResult<bool> {
    let key_elems = array_more_seams::deconstruct_text_array::call(keys)?;

    for elem in &key_elems {
        if elem.is_null {
            continue;
        }

        // We rely on the array elements not being toasted.
        let str_val = jbv_string(&elem.value);

        if findJsonbValueFromContainer(jb_root, JB_FOBJECT | JB_FARRAY, &str_val)?.is_none() {
            return Ok(false);
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Containment operators
// ---------------------------------------------------------------------------

/// C: `jsonb_contains(PG_FUNCTION_ARGS)`.
///
/// `val_root` / `tmpl_root` are the root container bytes of the two jsonb
/// arguments.
pub fn jsonb_contains(val_root: &[u8], tmpl_root: &[u8]) -> PgResult<bool> {
    if jb_root_is_object(val_root) != jb_root_is_object(tmpl_root) {
        return Ok(false);
    }

    let mut it1 = JsonbIteratorInit(val_root);
    let mut it2 = JsonbIteratorInit(tmpl_root);

    JsonbDeepContains(&mut it1, &mut it2)
}

/// C: `jsonb_contained(PG_FUNCTION_ARGS)` — commutator of "contains".
///
/// C arg 0 is `tmpl`, arg 1 is `val`.
pub fn jsonb_contained(tmpl_root: &[u8], val_root: &[u8]) -> PgResult<bool> {
    if jb_root_is_object(val_root) != jb_root_is_object(tmpl_root) {
        return Ok(false);
    }

    let mut it1 = JsonbIteratorInit(val_root);
    let mut it2 = JsonbIteratorInit(tmpl_root);

    JsonbDeepContains(&mut it1, &mut it2)
}

// ---------------------------------------------------------------------------
// B-Tree operator class operators + support function
// ---------------------------------------------------------------------------

/// C: `jsonb_ne(PG_FUNCTION_ARGS)`.
pub fn jsonb_ne(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? != 0)
}

/// C: `jsonb_lt(PG_FUNCTION_ARGS)`.
pub fn jsonb_lt(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? < 0)
}

/// C: `jsonb_gt(PG_FUNCTION_ARGS)`.
pub fn jsonb_gt(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? > 0)
}

/// C: `jsonb_le(PG_FUNCTION_ARGS)`.
pub fn jsonb_le(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? <= 0)
}

/// C: `jsonb_ge(PG_FUNCTION_ARGS)`.
pub fn jsonb_ge(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? >= 0)
}

/// C: `jsonb_eq(PG_FUNCTION_ARGS)`.
pub fn jsonb_eq(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<bool> {
    Ok(compareJsonbContainers(jba_root, jbb_root)? == 0)
}

/// C: `jsonb_cmp(PG_FUNCTION_ARGS)`.
pub fn jsonb_cmp(jba_root: &[u8], jbb_root: &[u8]) -> PgResult<i32> {
    compareJsonbContainers(jba_root, jbb_root)
}

// ---------------------------------------------------------------------------
// Hash operator class jsonb hashing functions
// ---------------------------------------------------------------------------

/// C: `jsonb_hash(PG_FUNCTION_ARGS)`.
pub fn jsonb_hash(jb_root: &[u8]) -> PgResult<i32> {
    let mut hash: u32 = 0;

    if jb_root_count(jb_root) == 0 {
        return Ok(0);
    }

    let mut it = JsonbIteratorInit(jb_root);
    let mut v = JsonbValue::null();

    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, false)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        match r {
            // Rotation is left to JsonbHashScalarValue().
            JsonbIteratorToken::WJB_BEGIN_ARRAY => {
                hash ^= JB_FARRAY;
            }
            JsonbIteratorToken::WJB_BEGIN_OBJECT => {
                hash ^= JB_FOBJECT;
            }
            JsonbIteratorToken::WJB_KEY
            | JsonbIteratorToken::WJB_VALUE
            | JsonbIteratorToken::WJB_ELEM => {
                JsonbHashScalarValue(&v, &mut hash)?;
            }
            JsonbIteratorToken::WJB_END_ARRAY | JsonbIteratorToken::WJB_END_OBJECT => {}
            _ => {
                return Err(invalid_iterator_rc(r));
            }
        }
    }

    Ok(hash as i32)
}

/// C: `jsonb_hash_extended(PG_FUNCTION_ARGS)`.
pub fn jsonb_hash_extended(jb_root: &[u8], seed: u64) -> PgResult<u64> {
    let mut hash: u64 = 0;

    if jb_root_count(jb_root) == 0 {
        return Ok(seed);
    }

    let mut it = JsonbIteratorInit(jb_root);
    let mut v = JsonbValue::null();

    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, false)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        match r {
            // Rotation is left to JsonbHashScalarValueExtended().
            JsonbIteratorToken::WJB_BEGIN_ARRAY => {
                hash ^= ((JB_FARRAY as u64) << 32) | JB_FARRAY as u64;
            }
            JsonbIteratorToken::WJB_BEGIN_OBJECT => {
                hash ^= ((JB_FOBJECT as u64) << 32) | JB_FOBJECT as u64;
            }
            JsonbIteratorToken::WJB_KEY
            | JsonbIteratorToken::WJB_VALUE
            | JsonbIteratorToken::WJB_ELEM => {
                JsonbHashScalarValueExtended(&v, &mut hash, seed)?;
            }
            JsonbIteratorToken::WJB_END_ARRAY | JsonbIteratorToken::WJB_END_OBJECT => {}
            _ => {
                return Err(invalid_iterator_rc(r));
            }
        }
    }

    Ok(hash)
}

/// C: `elog(ERROR, "invalid JsonbIteratorNext rc: %d", (int) r)`.
fn invalid_iterator_rc(r: JsonbIteratorToken) -> PgError {
    use alloc::format;
    PgError::error(format!("invalid JsonbIteratorNext rc: {}", r as i32))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

#[cfg(test)]
mod tests;
