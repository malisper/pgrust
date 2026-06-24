//! `contrib/ltree/_ltree_op.c` array iteration support: walk an `ltree[]`
//! (or `lquery[]`) container into its per-element varlena byte-slices.
//!
//! The container arrives at the fmgr boundary already detoasted as a flat,
//! 4-byte-header `ArrayType` image: the C `array.h` header
//! (`vl_len_, ndim, dataoffset, elemtype`, then the per-dim `dims`/`lbound`,
//! then an optional null bitmap, then the MAXALIGN-padded element data). ltree
//! / lquery elements are by-reference varlenas, stepped via
//! `INTALIGN(VARSIZE(x))` exactly as `_ltree_op.c`'s `NEXTVAL`.

// `ndim`/`nitems` accessors are part of the complete array surface but only
// some callers use them; allow dead for the unused accessors.
#![allow(dead_code)]

use ::types_error::error::{ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_NULL_VALUE_NOT_ALLOWED};
use ::types_error::PgError;

use crate::repr::{intalign, read_i32, varsize};

const ARR_HDR_NDIM: usize = 4;
const ARR_HDR_DATAOFFSET: usize = 8;
// elemtype at offset 12; dims/lbound start at 16.
const ARR_DIMS_START: usize = 16;

/// Parsed array container giving access to per-element byte-slices.
pub struct LtreeArray<'a> {
    buf: &'a [u8],
    ndim: i32,
    nitems: usize,
    data_start: usize,
    has_null: bool,
    null_bitmap_start: usize,
}

impl<'a> LtreeArray<'a> {
    /// Parse the header. Mirrors `ArrayGetNItems` + `ARR_NDIM`/`ARR_DATA_PTR`.
    pub fn parse(buf: &'a [u8]) -> LtreeArray<'a> {
        let ndim = read_i32(buf, ARR_HDR_NDIM);
        let dataoffset = read_i32(buf, ARR_HDR_DATAOFFSET);
        let has_null = dataoffset != 0;

        let nd = ndim.max(0) as usize;
        // nitems = product of dims (each dim is an i32 starting at ARR_DIMS_START).
        let mut nitems: usize = if nd == 0 { 0 } else { 1 };
        for d in 0..nd {
            let dim = read_i32(buf, ARR_DIMS_START + d * 4);
            nitems = nitems.saturating_mul(dim.max(0) as usize);
        }

        // dims + lbound occupy 2*nd int32s after the 16-byte fixed header.
        let dims_lbound_end = ARR_DIMS_START + 2 * nd * 4;
        let null_bitmap_start = dims_lbound_end;

        let data_start = if has_null {
            // dataoffset is the byte offset (MAXALIGN'd) of the data from the
            // start of the array varlena.
            dataoffset as usize
        } else {
            // ARR_OVERHEAD_NONULLS: MAXALIGN(dims_lbound_end).
            (dims_lbound_end + 7) & !7
        };

        LtreeArray {
            buf,
            ndim,
            nitems,
            data_start,
            has_null,
            null_bitmap_start,
        }
    }

    pub fn ndim(&self) -> i32 {
        self.ndim
    }
    pub fn nitems(&self) -> usize {
        self.nitems
    }

    /// `array_contains_nulls(arr)` — true if any null bit is 0.
    pub fn contains_nulls(&self) -> bool {
        if !self.has_null {
            return false;
        }
        for i in 0..self.nitems {
            let byte = self.buf[self.null_bitmap_start + i / 8];
            if (byte >> (i % 8)) & 1 == 0 {
                return true;
            }
        }
        false
    }

    /// Iterate each element's varlena byte-slice (header-ful image), assuming
    /// no nulls (the C `array_iterator` checks `array_contains_nulls` first).
    pub fn elements(&self) -> ElementIter<'a> {
        ElementIter {
            buf: self.buf,
            off: self.data_start,
            remaining: self.nitems,
        }
    }

    /// The `ARR_NDIM > 1 || contains_nulls` guards C does before iterating.
    pub fn check_1d_no_nulls(&self) -> Result<(), PgError> {
        if self.ndim > 1 {
            return Err(PgError::error("array must be one-dimensional")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
        if self.contains_nulls() {
            return Err(PgError::error("array must not contain nulls")
                .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        Ok(())
    }
}

pub struct ElementIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for ElementIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        let sz = varsize(&self.buf[self.off..]);
        let elem = &self.buf[self.off..self.off + sz];
        // NEXTVAL: INTALIGN(VARSIZE(x))
        self.off += intalign(sz);
        self.remaining -= 1;
        Some(elem)
    }
}
