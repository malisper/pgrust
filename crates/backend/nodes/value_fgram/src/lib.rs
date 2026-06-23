#![allow(non_snake_case)]
// `clippy::result_large_err`: every fallible constructor returns the shared
// `::error_fgram::PgResult` (== `Result<_, PgError>`). `PgError`'s size is
// fixed by that crate and is the project-wide error contract these ports must
// match; boxing it locally would diverge from every sibling crate's signatures.
#![allow(clippy::result_large_err)]

//! Safe Rust port of PostgreSQL's `value.c` (`src/backend/nodes/value.c`):
//! the constructors for the five "value" nodes used by the grammar and parse
//! tree — [`makeInteger`], [`makeFloat`], [`makeBoolean`], [`makeString`] and
//! [`makeBitString`].
//!
//! # Behaviour preserved
//!
//! Each function is `makeNode(<T>)` followed by setting the single payload
//! field, exactly as in C. `makeNode(T)` expands to
//! `newNode(sizeof(T), T_<T>)`, i.e. a `palloc0` in the **current** memory
//! context with the leading [`NodeTag`] stamped in. The payload-pointer
//! variants (`Float`, `String`, `BitString`) take ownership of a `palloc`'d
//! C string supplied by the caller — they store the pointer verbatim, never
//! copying it, matching the C "Caller is responsible for passing a palloc'd
//! string" contract.
//!
//! # Memory ownership
//!
//! The returned node is owned by the `CurrentMemoryContext` (palloc
//! semantics: freed only on context reset/delete, never by Rust `Drop`), so
//! the functions hand back raw `*mut` pointers in the node ABI representation,
//! the same way `makeNode` does in the C backend. No `Box`/`Drop` ownership is
//! introduced for node storage.

use core::ffi::{c_char, c_int};

use ::error_fgram::PgResult;
use ::mmgr_fgram::{palloc0, MemoryContextScope, PgMemoryContext};
use ::pg_ffi_fgram::{BitString, Boolean, Float, Integer, StringNode};

/// A scope on the *current* PostgreSQL memory context, where `makeNode`
/// allocates. The returned `'static` scope is sound here because every node it
/// allocates is owned by the memory context (palloc semantics), not by the
/// scope, and the current context outlives the call.
fn current_scope() -> PgResult<MemoryContextScope<'static>> {
    let context = PgMemoryContext::current()?;
    Ok(unsafe { MemoryContextScope::from_context_unchecked(context) })
}

/// `makeNode(T)` == `newNode(sizeof(T), T_<T>)`: `palloc0` a node of `size_of`
/// `T` bytes in the current context and initialise it to `value` (whose leading
/// field is the correct [`NodeTag`], set by the `T::new` constructor). Returns
/// a raw pointer owned by the memory context.
fn make_value_node<T: Copy>(value: T) -> PgResult<*mut T> {
    let scope = current_scope()?;
    let node = palloc0(&scope, core::mem::size_of::<T>())?
        .into_raw()
        .cast::<T>();
    // newNode palloc0's then stamps the tag; the constructed `value` already
    // carries the correct NodeTag in its leading field, so a single write of
    // the fully-initialised node reproduces makeNode + the field assignment.
    unsafe { node.write(value) };
    Ok(node)
}

/// `makeInteger(i)` — create an [`Integer`] value node holding `i`.
pub fn makeInteger(i: c_int) -> PgResult<*mut Integer> {
    // Integer *v = makeNode(Integer);  v->ival = i;  return v;
    make_value_node(Integer::new(i))
}

/// `makeFloat(numericStr)` — create a [`Float`] value node holding the given
/// numeric string. The caller is responsible for passing a `palloc`'d string;
/// it is stored verbatim, not copied.
pub fn makeFloat(numericStr: *mut c_char) -> PgResult<*mut Float> {
    // Float *v = makeNode(Float);  v->fval = numericStr;  return v;
    make_value_node(Float::new(numericStr))
}

/// `makeBoolean(val)` — create a [`Boolean`] value node holding `val`.
pub fn makeBoolean(val: bool) -> PgResult<*mut Boolean> {
    // Boolean *v = makeNode(Boolean);  v->boolval = val;  return v;
    make_value_node(Boolean::new(val))
}

/// `makeString(str)` — create a [`StringNode`] value node holding the given
/// string. The caller is responsible for passing a `palloc`'d string; it is
/// stored verbatim, not copied.
pub fn makeString(str: *mut c_char) -> PgResult<*mut StringNode> {
    // String *v = makeNode(String);  v->sval = str;  return v;
    make_value_node(StringNode::new(str))
}

/// `makeBitString(str)` — create a [`BitString`] value node holding the given
/// string. The caller is responsible for passing a `palloc`'d string; it is
/// stored verbatim, not copied.
pub fn makeBitString(str: *mut c_char) -> PgResult<*mut BitString> {
    // BitString *v = makeNode(BitString);  v->bsval = str;  return v;
    make_value_node(BitString::new(str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mmgr_fgram::{MemoryContextSwitchTo, OwnedMemoryContext, PgMemoryContext};
    use ::pg_ffi_fgram::{T_BitString, T_Boolean, T_Float, T_Integer, T_String};

    fn with_context<R>(f: impl FnOnce() -> R) -> R {
        let context = OwnedMemoryContext::alloc_set(
            Some(PgMemoryContext::top().unwrap()),
            "value test",
            0,
            1024,
            8192,
        )
        .unwrap();
        let old = MemoryContextSwitchTo(context.as_context()).unwrap();
        let r = f();
        MemoryContextSwitchTo(old).unwrap();
        r
    }

    #[test]
    fn makers_set_tag_and_payload() {
        with_context(|| {
            let i = makeInteger(42).unwrap();
            assert_eq!(unsafe { (*i).node_tag() }, T_Integer);
            assert_eq!(unsafe { (*i).ival() }, 42);

            let b = makeBoolean(true).unwrap();
            assert_eq!(unsafe { (*b).node_tag() }, T_Boolean);
            assert!(unsafe { (*b).boolval() });

            let p = 0x1234usize as *mut c_char;
            let f = makeFloat(p).unwrap();
            assert_eq!(unsafe { (*f).node_tag() }, T_Float);
            assert_eq!(unsafe { (*f).fval() }, p);

            let s = makeString(p).unwrap();
            assert_eq!(unsafe { (*s).node_tag() }, T_String);
            assert_eq!(unsafe { (*s).sval() }, p);

            let bs = makeBitString(p).unwrap();
            assert_eq!(unsafe { (*bs).node_tag() }, T_BitString);
            assert_eq!(unsafe { (*bs).bsval() }, p);
        });
    }
}
