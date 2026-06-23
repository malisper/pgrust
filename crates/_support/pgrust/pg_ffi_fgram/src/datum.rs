//! Canonical `Datum` conversion helpers (`postgres.h` `*GetDatum` / `DatumGet*`
//! macros).
//!
//! `Datum` is a `usize` (`src/types.rs`).  These are the pure numeric/pointer
//! casts PostgreSQL's `postgres.h` defines for moving fixed-width scalars and raw
//! pointers in and out of a `Datum`.  They allocate nothing and read nothing
//! through any pointer, so this module is a safe canonical home in the
//! foundational `pgrust-pg-ffi` crate (it gains no dependency on the `adt`
//! crates).
//!
//! # Why the pointer form of `CStringGetDatum` lives here
//!
//! There are two shapes in the tree: a safe `Option<&CStr>` form used for
//! *lookups* (it borrows), and the raw `*mut c_char` form used on the *result*
//! path of an output function (the function `palloc`s an owned cstring and the
//! `Datum` carries that raw pointer).  This module provides the raw-pointer pair
//! (`CStringGetDatum`/`DatumGetCString`); collapsing it into the `&CStr` form
//! would make it impossible to carry an output function's palloc'd result.

use core::ffi::c_char;

use crate::types::{Datum, Oid};

/// C: `Int32GetDatum(X)` — `(Datum) (uint32) (X)` (mask to 32 bits, then widen).
#[inline]
pub fn Int32GetDatum(value: i32) -> Datum {
    value as u32 as Datum
}

/// C: `DatumGetInt32(X)` — `(int32) (X)`.
#[inline]
pub fn DatumGetInt32(value: Datum) -> i32 {
    value as u32 as i32
}

/// C: `ObjectIdGetDatum(X)` — `(Datum) (uint32) (X)`.
#[inline]
pub fn ObjectIdGetDatum(value: Oid) -> Datum {
    value as Datum
}

/// C: `DatumGetObjectId(X)` — `(Oid) (X)`.
#[inline]
pub fn DatumGetObjectId(value: Datum) -> Oid {
    value as Oid
}

/// C: `PointerGetDatum(X)` — `(Datum) (X)`.
#[inline]
pub fn PointerGetDatum<T>(value: *mut T) -> Datum {
    value as Datum
}

/// C: `DatumGetPointer(X)` — `(Pointer) (X)`.
#[inline]
pub fn DatumGetPointer<T>(value: Datum) -> *mut T {
    value as *mut T
}

/// C: `CStringGetDatum(X)` — `PointerGetDatum(X)` on a `char *`.
///
/// Raw-pointer form: carries an owned/palloc'd cstring pointer into a `Datum`.
#[inline]
pub fn CStringGetDatum(value: *mut c_char) -> Datum {
    value as Datum
}

/// C: `DatumGetCString(X)` — `(char *) DatumGetPointer(X)`.
#[inline]
pub fn DatumGetCString(value: Datum) -> *mut c_char {
    value as *mut c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int32_round_trips_including_negative() {
        assert_eq!(DatumGetInt32(Int32GetDatum(42)), 42);
        assert_eq!(DatumGetInt32(Int32GetDatum(-1)), -1);
        assert_eq!(DatumGetInt32(Int32GetDatum(i32::MIN)), i32::MIN);
        // Int32GetDatum masks to 32 bits via uint32.
        assert_eq!(Int32GetDatum(1), 1_usize);
    }

    #[test]
    fn objectid_round_trips() {
        assert_eq!(DatumGetObjectId(ObjectIdGetDatum(700)), 700);
    }

    #[test]
    fn pointer_and_cstring_round_trip() {
        let mut byte: c_char = 7;
        let p: *mut c_char = &mut byte;
        let d = CStringGetDatum(p);
        assert_eq!(DatumGetCString(d), p);
        let d2 = PointerGetDatum(p);
        assert_eq!(DatumGetPointer::<c_char>(d2), p);
    }
}
