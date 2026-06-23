//! Flat `MinimalTuple` byte-blob codec (`access/common/heaptuple.c` /
//! `access/htup_details.h` `MinimalTupleData` layout).
//!
//! The tuplestore/tuplesort boundary speaks a `MinimalTuple` as its
//! contiguous C byte image — a context-allocated `PgVec<u8>` whose first four
//! bytes are the tuple's `t_len` — while this crate's structured
//! producers/consumers speak [`FormedMinimalTuple`] (owned `MinimalTupleData`
//! header + the user-data bytes alongside).  This module is the single source
//! of truth for the flat layout, exactly the bytes C holds at
//! `(char *) mtup .. + mtup->t_len`:
//!
//! ```text
//! offset  0: t_len        (uint32, 4 bytes)
//! offset  4: mt_padding   (6 bytes, always zero on a formed tuple)
//! offset 10: t_infomask2  (uint16)
//! offset 12: t_infomask   (uint16)
//! offset 14: t_hoff       (uint8; includes the MINIMAL_TUPLE_OFFSET bias)
//! offset 15: t_bits[]     (BITMAPLEN(natts) bytes, iff HEAP_HASNULL)
//!        ..: zero pad to (t_hoff - MINIMAL_TUPLE_OFFSET)  (MAXALIGN slack)
//!        ..: user data    (t_len - (t_hoff - MINIMAL_TUPLE_OFFSET) bytes)
//! ```
//!
//! The data area begins at `hoff = t_hoff - MINIMAL_TUPLE_OFFSET` within the
//! blob because a `MinimalTupleData` is a `HeapTupleHeaderData` with its first
//! `MINIMAL_TUPLE_OFFSET` bytes (the t_choice/t_ctid system-column region)
//! chopped off, and `t_hoff` keeps the *full* header offset (see
//! `heap_form_minimal_tuple`, heaptuple.c:1452).
//!
//! Decoding validates every structural invariant (length word vs. blob length,
//! `t_hoff` bounds, null-bitmap fit) and fails loudly on a corrupt blob —
//! bytes are never fabricated.

use mcx::{alloc_in, slice_in, vec_with_capacity_in, Mcx, PgVec};
use ::types_error::PgError;

use ::types_tuple::heaptuple::{
    MinimalTupleData, TupleDescData, BITMAPLEN, HEAP_HASNULL, HEAP_NATTS_MASK,
    MINIMAL_TUPLE_OFFSET,
};

use crate::{
    heap_tuple_from_minimal_tuple, DeformedColumn, FormedMinimalTuple, FormedTuple,
    HeapTupleError, Datum, SIZEOF_MINIMAL_TUPLE_HEADER,
};

/// A structural inconsistency found while decoding (or assembling) a flat
/// `MinimalTuple` blob.  Each variant corresponds to a violated layout
/// invariant; none of these can arise from a blob produced by
/// [`minimal_tuple_to_flat`] over a well-formed [`FormedMinimalTuple`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MinimalTupleFlatError {
    /// The blob is shorter than `SizeofMinimalTupleHeader` (15 bytes).
    TooShort { len: usize },
    /// The leading `t_len` word disagrees with the blob's actual byte length.
    LengthMismatch { t_len: u32, blob_len: usize },
    /// `t_hoff` is out of bounds: below the minimum header size or past the
    /// end of the tuple.
    BadHoff { t_hoff: u8, t_len: u32 },
    /// `HEAP_HASNULL` is set but the null bitmap (`BITMAPLEN(natts)` bytes)
    /// does not fit between the fixed header and the data offset.
    BitmapOverrun { natts: u16, t_hoff: u8 },
    /// (encode from a heap tuple) the tuple has no `t_data` header.
    MissingHeader,
    /// (encode from a heap tuple) the carried [`FormedTuple::data`] byte count
    /// does not match `t_len - t_hoff`.
    UserDataLength { expected: usize, actual: usize },
    /// An `ereport(ERROR)` from the allocation path (out of memory in the
    /// target context).
    Pg(PgError),
}

impl From<PgError> for MinimalTupleFlatError {
    fn from(err: PgError) -> Self {
        MinimalTupleFlatError::Pg(err)
    }
}

/// Serialize a [`FormedMinimalTuple`] into its canonical contiguous C byte
/// image (the `PgVec<u8>` blob the tuplestore seams carry), allocated in
/// `mcx`.
///
/// The header fields are written verbatim (this is a serializer, not a
/// header-policy function).  Panics (loudly) if the structured tuple is
/// internally inconsistent — a `t_hoff`/`t_bits`/`data` combination that no
/// `heap_form_minimal_tuple` output can have.
pub fn minimal_tuple_to_flat<'mcx>(
    mcx: Mcx<'mcx>,
    mtup: &FormedMinimalTuple<'_>,
) -> Result<PgVec<'mcx, u8>, MinimalTupleFlatError> {
    let t = &*mtup.tuple;
    let t_hoff = t.t_hoff as usize;
    assert!(
        t_hoff >= MINIMAL_TUPLE_OFFSET + SIZEOF_MINIMAL_TUPLE_HEADER,
        "minimal_tuple_to_flat: t_hoff {t_hoff} below minimal header"
    );
    // The in-blob data offset (C: the data lives at mtup + t_hoff - MINIMAL_TUPLE_OFFSET).
    let hoff = t_hoff - MINIMAL_TUPLE_OFFSET;
    assert_eq!(
        t.t_len as usize,
        hoff + mtup.data.len(),
        "minimal_tuple_to_flat: t_len disagrees with hoff + data len"
    );

    let mut blob: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, t.t_len as usize)?;
    blob.extend_from_slice(&t.t_len.to_ne_bytes());
    for b in t.mt_padding {
        blob.push(b as u8);
    }
    blob.extend_from_slice(&t.t_infomask2.to_ne_bytes());
    blob.extend_from_slice(&t.t_infomask.to_ne_bytes());
    blob.push(t.t_hoff);
    debug_assert_eq!(blob.len(), SIZEOF_MINIMAL_TUPLE_HEADER);

    // Null bitmap, present iff HEAP_HASNULL (heap_fill_tuple laid it down).
    if (t.t_infomask & HEAP_HASNULL) != 0 {
        assert!(
            SIZEOF_MINIMAL_TUPLE_HEADER + t.t_bits.len() <= hoff,
            "minimal_tuple_to_flat: null bitmap overruns t_hoff"
        );
        blob.extend_from_slice(&t.t_bits);
    }

    // Zero pad to the data offset (the MAXALIGN slack C's palloc0 zeroed).
    blob.resize(hoff, 0);

    blob.extend_from_slice(&mtup.data);
    debug_assert_eq!(blob.len(), t.t_len as usize);
    Ok(blob)
}

/// Decode a flat `MinimalTuple` blob back into the structured
/// [`FormedMinimalTuple`] (allocated in `mcx`), validating every layout
/// invariant.
pub fn minimal_tuple_from_flat<'mcx>(
    mcx: Mcx<'mcx>,
    blob: &[u8],
) -> Result<FormedMinimalTuple<'mcx>, MinimalTupleFlatError> {
    if blob.len() < SIZEOF_MINIMAL_TUPLE_HEADER {
        return Err(MinimalTupleFlatError::TooShort { len: blob.len() });
    }

    let t_len = u32::from_ne_bytes([blob[0], blob[1], blob[2], blob[3]]);
    if t_len as usize != blob.len() {
        return Err(MinimalTupleFlatError::LengthMismatch {
            t_len,
            blob_len: blob.len(),
        });
    }

    let mut mt_padding = [0i8; 6];
    for (i, p) in mt_padding.iter_mut().enumerate() {
        *p = blob[4 + i] as i8;
    }
    let t_infomask2 = u16::from_ne_bytes([blob[10], blob[11]]);
    let t_infomask = u16::from_ne_bytes([blob[12], blob[13]]);
    let t_hoff = blob[14];

    let t_hoff_usize = t_hoff as usize;
    if t_hoff_usize < MINIMAL_TUPLE_OFFSET + SIZEOF_MINIMAL_TUPLE_HEADER
        || t_hoff_usize - MINIMAL_TUPLE_OFFSET > blob.len()
    {
        return Err(MinimalTupleFlatError::BadHoff { t_hoff, t_len });
    }
    let hoff = t_hoff_usize - MINIMAL_TUPLE_OFFSET;

    let t_bits: PgVec<'mcx, u8> = if (t_infomask & HEAP_HASNULL) != 0 {
        let natts = t_infomask2 & HEAP_NATTS_MASK;
        let bitmap_len = BITMAPLEN(natts as i32) as usize;
        if SIZEOF_MINIMAL_TUPLE_HEADER + bitmap_len > hoff {
            return Err(MinimalTupleFlatError::BitmapOverrun { natts, t_hoff });
        }
        slice_in(
            mcx,
            &blob[SIZEOF_MINIMAL_TUPLE_HEADER..SIZEOF_MINIMAL_TUPLE_HEADER + bitmap_len],
        )?
    } else {
        PgVec::new_in(mcx)
    };

    Ok(FormedMinimalTuple {
        tuple: alloc_in(
            mcx,
            MinimalTupleData {
                t_len,
                mt_padding,
                t_infomask2,
                t_infomask,
                t_hoff,
                t_bits,
            },
        )?,
        data: slice_in(mcx, &blob[hoff..])?,
    })
}

/// `heap_form_minimal_tuple(tdesc, values, isnull, 0)` returning the flat blob
/// — the shape the tuplestore/tuplesort boundary consumes.
pub fn heap_form_minimal_tuple_flat<'mcx>(
    mcx: Mcx<'mcx>,
    tuple_descriptor: &TupleDescData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
) -> Result<PgVec<'mcx, u8>, HeapTupleError> {
    let formed = crate::heap_form_minimal_tuple(mcx, tuple_descriptor, values, isnull, 0)?;
    match minimal_tuple_to_flat(mcx, &formed) {
        Ok(blob) => Ok(blob),
        Err(MinimalTupleFlatError::Pg(err)) => Err(HeapTupleError::Pg(err)),
        // Structural variants cannot arise from a freshly formed tuple.
        Err(other) => panic!("minimal_tuple_to_flat on a fresh tuple failed: {other:?}"),
    }
}

/// `minimal_tuple_from_heap_tuple(htup, 0)` returning the flat blob — the
/// shape the tuplestore/tuplesort boundary consumes.
///
/// The heap tuple is the [`FormedTuple`] carrier (owned header + column bytes
/// in [`FormedTuple::data`]); the byte count must be exactly `t_len - t_hoff`.
/// C copies `(char *) htup->t_data + MINIMAL_TUPLE_OFFSET` for
/// `t_len - MINIMAL_TUPLE_OFFSET` bytes and rewrites `t_len`; structurally
/// that is: drop the t_choice/t_ctid region, keep the shared
/// `t_infomask2 .. t_bits` tail + pad + data.
pub fn minimal_tuple_from_heap_tuple_flat<'mcx>(
    mcx: Mcx<'mcx>,
    htup: &FormedTuple<'_>,
) -> Result<PgVec<'mcx, u8>, MinimalTupleFlatError> {
    let header = htup
        .tuple
        .t_data
        .as_ref()
        .ok_or(MinimalTupleFlatError::MissingHeader)?;

    // Assert(htup->t_len > MINIMAL_TUPLE_OFFSET);
    if (htup.tuple.t_len as usize) <= MINIMAL_TUPLE_OFFSET
        || (header.t_hoff as usize) > htup.tuple.t_len as usize
    {
        return Err(MinimalTupleFlatError::BadHoff {
            t_hoff: header.t_hoff,
            t_len: htup.tuple.t_len,
        });
    }

    let expected = htup.tuple.t_len as usize - header.t_hoff as usize;
    let data: &[u8] = &htup.data;
    if data.len() != expected {
        return Err(MinimalTupleFlatError::UserDataLength {
            expected,
            actual: data.len(),
        });
    }

    let len = htup.tuple.t_len as usize - MINIMAL_TUPLE_OFFSET;
    let mtup = FormedMinimalTuple {
        tuple: alloc_in(
            mcx,
            MinimalTupleData {
                t_len: len as u32,
                mt_padding: [0; 6],
                t_infomask2: header.t_infomask2,
                t_infomask: header.t_infomask,
                // The minimal t_hoff already carries the MINIMAL_TUPLE_OFFSET bias
                // and equals the heap tuple's full header offset (see
                // minimal_tuple_from_heap_tuple, heaptuple.c:1586).
                t_hoff: header.t_hoff,
                t_bits: slice_in(mcx, &header.t_bits)?,
            },
        )?,
        data: slice_in(mcx, data)?,
    };
    minimal_tuple_to_flat(mcx, &mtup)
}

/// Deform a flat `MinimalTuple` blob into per-column `(value, isnull)` pairs —
/// the codec a slot-store provider (`ExecStoreMinimalTuple`) needs to turn the
/// boundary blob back into slot values.  Composes [`minimal_tuple_from_flat`]
/// with the crate's existing `heap_tuple_from_minimal_tuple` +
/// `heap_deform_tuple` (the same route C takes through
/// `tts_minimal_getsomeattrs`).
pub fn heap_deform_minimal_tuple_flat<'mcx>(
    mcx: Mcx<'mcx>,
    blob: &[u8],
    tuple_desc: &TupleDescData<'_>,
) -> Result<PgVec<'mcx, DeformedColumn<'mcx>>, MinimalTupleFlatError> {
    let mtup = minimal_tuple_from_flat(mcx, blob)?;
    let ft = heap_tuple_from_minimal_tuple(mcx, &mtup)?;
    Ok(crate::heap_deform_tuple(mcx, &ft.tuple, tuple_desc, &ft.data)?)
}
