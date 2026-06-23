//! Port of PostgreSQL 18.3 `src/backend/access/table/toast_helper.c` — the
//! per-tuple TOAST pass helpers a table AM's toaster driver (`heaptoast.c`)
//! calls to compress / push-out varlena attributes.
//!
//! Every function in the C source is ported here:
//!   - [`toast_tuple_init`] — classify each column, fetch back external values,
//!     record sizes and flags;
//!   - [`toast_tuple_find_biggest_attribute`] — the biggest still-shrinkable
//!     varlena column, or -1;
//!   - [`toast_tuple_try_compression`] — try compressing one column in place;
//!   - [`toast_tuple_externalize`] — push one column out to the TOAST table;
//!   - [`toast_tuple_cleanup`] — free temp values and delete obsolete old
//!     external values.
//!
//! ## The owned model vs. C's pointer / `Datum` model
//!
//! In C the context is a stack struct whose `Datum` arrays alias the caller's
//! deformed-value arrays; the relation crosses as a live `Relation *` and the
//! descriptor is reached by `ttc->ttc_rel->rd_att` (`RelationGetDescr`). Here
//! the context ([`types_tuple::toast_helper::ToastTupleContext`]) owns its
//! deformed values (the unified [`Datum`] enum) and the relation crosses as its
//! `Oid`; the descriptor is resolved back through the by-OID relcache seam
//! ([`relation_id_get_relation`]) and released with [`relation_close`], exactly
//! as the seam contract specifies (relations cross seams by OID; the relcache
//! resolves them to the live entry).
//!
//! A varlena value is its raw *encoded bytes* (`Datum::ByRef(PgVec<u8>)`, header
//! included — exactly what `DatumGetPointer` would dereference). Replacing a
//! value (detoast / compress / save) moves the new bytes into the slot and drops
//! the old buffer, covering both C's pointer overwrite and its matching
//! `pfree`; a `NEEDS_FREE` buffer lives in the caller's `mcx` and is freed when
//! that context resets (C's explicit `pfree` in cleanup). The `va_header`
//! bit-twiddling (`VARATT_IS_EXTERNAL`, `VARATT_IS_EXTERNAL_ONDISK`,
//! `VARATT_IS_COMPRESSED`, `VARSIZE`, `VARSIZE_EXTERNAL`, `VARSIZE_ANY`) is pure
//! `varatt.h` macro logic with no external dependency, ported in-crate over the
//! encoded bytes (native byte order, matching the build target).

extern crate alloc;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;
use types_tuple::toast_helper::{
    ToastTupleContext, TOASTCOL_IGNORE, TOASTCOL_INCOMPRESSIBLE,
    TOASTCOL_NEEDS_DELETE_OLD, TOASTCOL_NEEDS_FREE, TOAST_HAS_NULLS,
    TOAST_NEEDS_CHANGE, TOAST_NEEDS_DELETE_OLD, TOAST_NEEDS_FREE,
};

use detoast_seams as detoast_seams;
use toast_internals_seams as toast_internals_seams;
use toast_helper_seams as toast_helper_seams;
use relcache_seams as relcache_seams;

/// `TYPSTORAGE_PLAIN` (`catalog/pg_type.h`) — `'p'`: always store in line,
/// non-varlena.
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
/// `TYPSTORAGE_EXTENDED` (`catalog/pg_type.h`) — `'x'`: toast or compress.
const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
/// `TYPSTORAGE_EXTERNAL` (`catalog/pg_type.h`) — `'e'`: toast, never compress.
const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
/// `TYPSTORAGE_MAIN` (`catalog/pg_type.h`) — `'m'`: compress, push out of line
/// only if necessary.
const TYPSTORAGE_MAIN: i8 = b'm' as i8;

/// `VARHDRSZ_EXTERNAL` (varatt.h) == `offsetof(varattrib_1b_e, va_data)` == 2.
const VARHDRSZ_EXTERNAL: usize = 2;

/// `TOAST_POINTER_SIZE` (varatt.h) == `VARHDRSZ_EXTERNAL + sizeof(varatt_external)`
/// == `2 + 16`.
const TOAST_POINTER_SIZE: i32 = (VARHDRSZ_EXTERNAL as i32) + 16;

/// `VARTAG_ONDISK` (varatt.h) — the `va_tag` of an on-disk-external TOAST
/// pointer.
const VARTAG_ONDISK: u8 = 18;

/// `MAXALIGN(LEN)` (c.h) — align to `MAXIMUM_ALIGNOF` (8 on supported
/// platforms).
#[inline]
const fn maxalign(len: i32) -> i32 {
    const ALIGNOF: i32 = 8;
    (len + (ALIGNOF - 1)) & !(ALIGNOF - 1)
}

// ---------------------------------------------------------------------------
// varatt.h predicates over verbatim datum bytes (the value's on-disk image,
// exactly what `DatumGetPointer` would dereference; little-endian build).
// ---------------------------------------------------------------------------

/// `VARATT_IS_1B(PTR)`: low bit set.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARATT_IS_1B_E(PTR)`: `va_header == 0x01`. Equivalent to
/// `VARATT_IS_EXTERNAL`.
#[inline]
fn varatt_is_1b_e(b: &[u8]) -> bool {
    b[0] == 0x01
}

/// `VARATT_IS_EXTERNAL(PTR)` == `VARATT_IS_1B_E(PTR)`.
#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    varatt_is_1b_e(b)
}

/// `VARATT_IS_COMPRESSED(PTR)` == `VARATT_IS_4B_C(PTR)`: low two bits `0b10`.
#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

/// `VARTAG_EXTERNAL(PTR)` == `((varattrib_1b_e *) PTR)->va_tag`: the `va_tag`
/// byte at offset 1.
#[inline]
fn vartag_external(b: &[u8]) -> u8 {
    b[1]
}

/// `VARATT_IS_EXTERNAL_ONDISK(PTR)`:
/// `VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_ONDISK`.
#[inline]
fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    varatt_is_external(b) && b.len() >= 2 && vartag_external(b) == VARTAG_ONDISK
}

/// `VARTAG_SIZE(tag)` from varatt.h, restricted to the tags reachable here. The
/// only tag flowing into `VARSIZE_EXTERNAL` in this file is `VARTAG_ONDISK` (it
/// is gated by `VARATT_IS_EXTERNAL_ONDISK`), so this mirrors the
/// `(tag) == VARTAG_ONDISK ? sizeof(varatt_external) : ...` arm. The other arms
/// (`VARTAG_INDIRECT`, the expanded tags) are a single pointer (8 bytes) on
/// supported platforms.
#[inline]
fn vartag_size(tag: u8) -> usize {
    const SIZEOF_VARATT_EXTERNAL: usize = 16;
    const SIZEOF_POINTER: usize = 8;
    if tag == VARTAG_ONDISK {
        SIZEOF_VARATT_EXTERNAL
    } else {
        SIZEOF_POINTER
    }
}

/// `VARSIZE_EXTERNAL(PTR)` ==
/// `VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR))`.
#[inline]
fn varsize_external(b: &[u8]) -> usize {
    VARHDRSZ_EXTERNAL + vartag_size(vartag_external(b))
}

/// `VARSIZE_4B(PTR)` (native byte order): `(va_header >> 2) & 0x3FFFFFFF`.
/// Equivalent to `VARSIZE(PTR)`.
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let word = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    (word >> 2) & 0x3fff_ffff
}

/// `VARSIZE_1B(PTR)`: `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7f) as u32
}

/// `VARSIZE_ANY(PTR)`:
/// `VARATT_IS_1B_E ? VARSIZE_EXTERNAL : (VARATT_IS_1B ? VARSIZE_1B : VARSIZE_4B)`.
#[inline]
fn varsize_any(b: &[u8]) -> usize {
    if varatt_is_1b_e(b) {
        varsize_external(b)
    } else if varatt_is_1b(b) {
        varsize_1b(b) as usize
    } else {
        varsize_4b(b) as usize
    }
}

// ---------------------------------------------------------------------------
// toast_tuple_init
// ---------------------------------------------------------------------------

/// One attribute's descriptor fields read out of `ttc->ttc_rel->rd_att`.
struct AttDesc {
    attlen: i16,
    attstorage: i8,
    attcompression: i8,
}

/// `toast_tuple_init(ttc)` (toast_helper.c) — prepare to TOAST a tuple.
///
/// `ttc_values` / `ttc_isnull` are required; `ttc_oldvalues` / `ttc_oldisnull`
/// are `None` for a newly inserted tuple, or describe the existing tuple on an
/// UPDATE. Each array has length `natts` of the relation's descriptor, and the
/// caller provides a (blank) `ttc_attr` entry per attribute. On return,
/// `ttc_flags` and every `ttc_attr` entry have been (re)initialized: external
/// values still in the tuple have been fetched back into `ttc_values` (with the
/// original pointer image stashed in `tai_oldexternal`), `tai_size` is valid for
/// every non-`TOASTCOL_IGNORE` varlena column, and the UPDATE old-value
/// delete/reuse decisions have been made.
pub fn toast_tuple_init(ttc: &mut ToastTupleContext<'_>) -> PgResult<()> {
    // Recover the per-tuple memory context the caller built the value arrays
    // in; detoast fetches land there (C: palloc in CurrentMemoryContext).
    let mcx: Mcx<'_> = *ttc.ttc_values.allocator();

    // TupleDesc tupleDesc = ttc->ttc_rel->rd_att;
    // int numAttrs = tupleDesc->natts;
    let att = read_attrs(mcx, ttc.ttc_rel)?;
    let num_attrs = att.len() as i32;

    ttc.ttc_flags = 0;

    let mut i: i32 = 0;
    while i < num_attrs {
        let idx = i as usize;
        let att_attlen = att[idx].attlen;
        let att_attstorage = att[idx].attstorage;
        let att_attcompression = att[idx].attcompression;

        ttc.ttc_attr[idx].tai_colflags = 0;
        ttc.ttc_attr[idx].tai_oldexternal = None;
        ttc.ttc_attr[idx].tai_compression = att_attcompression;

        if ttc.ttc_oldvalues.is_some() {
            // For UPDATE get the old and new values of this attribute.
            let oldisnull_i = ttc
                .ttc_oldisnull
                .as_ref()
                .expect("ttc_oldisnull must be set when ttc_oldvalues is set")[idx];

            // If the old value is stored on disk, check if it has changed so we
            // have to delete it later. Both old_value and new_value are
            // dereferenced only inside this guard (att == -1, not null, external
            // on disk), matching C: a non-varlena or NULL old value never reads
            // the bytes.
            let old_is_external_ondisk = att_attlen == -1
                && !oldisnull_i
                && varatt_is_external_ondisk(
                    ttc.ttc_oldvalues.as_ref().unwrap()[idx].as_ref_bytes(),
                );

            if old_is_external_ondisk {
                let changed = {
                    let old_bytes =
                        ttc.ttc_oldvalues.as_ref().unwrap()[idx].as_ref_bytes();
                    if ttc.ttc_isnull[idx] {
                        true
                    } else {
                        let new_bytes = ttc.ttc_values[idx].as_ref_bytes();
                        if !varatt_is_external_ondisk(new_bytes) {
                            true
                        } else {
                            // memcmp(old, new, VARSIZE_EXTERNAL(old)) != 0
                            let n = varsize_external(old_bytes);
                            old_bytes.get(..n) != new_bytes.get(..n)
                        }
                    }
                };

                if changed {
                    // The old external stored value isn't needed any more after
                    // the update.
                    ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_NEEDS_DELETE_OLD;
                    ttc.ttc_flags |= TOAST_NEEDS_DELETE_OLD;
                } else {
                    // This attribute isn't changed by this update so we reuse
                    // the original reference to the old value in the new tuple.
                    ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_IGNORE;
                    i += 1;
                    continue;
                }
            }
        }
        // (For INSERT the new value is simply ttc_values[i]; no extra work.)

        // Handle NULL attributes.
        if ttc.ttc_isnull[idx] {
            ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_IGNORE;
            ttc.ttc_flags |= TOAST_HAS_NULLS;
            i += 1;
            continue;
        }

        // Now look at varlena attributes.
        if att_attlen == -1 {
            // If the table's attribute says PLAIN always, force it so.
            if att_attstorage == TYPSTORAGE_PLAIN {
                ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_IGNORE;
            }

            // We took care of UPDATE above, so any external value we find still
            // in the tuple must be someone else's that we cannot reuse (this
            // includes the case of an out-of-line in-memory datum). Fetch it
            // back (without decompression, unless we are forcing PLAIN storage).
            // If necessary, we'll push it out as a new external value below.
            if varatt_is_external(ttc.ttc_values[idx].as_ref_bytes()) {
                // tai_oldexternal = new_value (the original external pointer
                // image, retained for cleanup / comparison).
                let old_external: mcx::PgVec<'_, u8> =
                    mcx::slice_in(mcx, ttc.ttc_values[idx].as_ref_bytes())?;
                let fetched = {
                    let bytes = ttc.ttc_values[idx].as_ref_bytes();
                    if att_attstorage == TYPSTORAGE_PLAIN {
                        detoast_seams::detoast_attr::call(mcx, bytes)?
                    } else {
                        detoast_seams::detoast_external_attr::call(mcx, bytes)?
                    }
                };
                ttc.ttc_attr[idx].tai_oldexternal = Some(old_external);
                ttc.ttc_values[idx] = Datum::ByRef(fetched);
                ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_NEEDS_FREE;
                ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
            }

            // Remember the size of this attribute.
            ttc.ttc_attr[idx].tai_size =
                varsize_any(ttc.ttc_values[idx].as_ref_bytes()) as i32;
        } else {
            // Not a varlena attribute, plain storage always.
            ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_IGNORE;
        }

        i += 1;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// toast_tuple_find_biggest_attribute
// ---------------------------------------------------------------------------

/// `toast_tuple_find_biggest_attribute(ttc, for_compression, check_main)`
/// (toast_helper.c) — find the biggest varlena attribute that still needs to be
/// shrunk to make the tuple fit, or -1 if no suitable attribute remains.
///
/// `check_main` chooses among the `attstorage` classes considered: when set,
/// only `MAIN` columns; when clear, only `EXTENDED`/`EXTERNAL` columns.
/// `for_compression` additionally skips already-compressed and known-
/// incompressible columns.
pub fn toast_tuple_find_biggest_attribute(
    ttc: &ToastTupleContext<'_>,
    for_compression: bool,
    check_main: bool,
) -> PgResult<i32> {
    let mcx: Mcx<'_> = *ttc.ttc_values.allocator();
    let att = read_attrs(mcx, ttc.ttc_rel)?;
    let num_attrs = att.len() as i32;

    let mut biggest_attno: i32 = -1;
    let mut biggest_size: i32 = maxalign(TOAST_POINTER_SIZE);
    let mut skip_colflags: u8 = TOASTCOL_IGNORE;

    if for_compression {
        skip_colflags |= TOASTCOL_INCOMPRESSIBLE;
    }

    let mut i: i32 = 0;
    while i < num_attrs {
        let idx = i as usize;

        if (ttc.ttc_attr[idx].tai_colflags & skip_colflags) != 0 {
            i += 1;
            continue;
        }
        // can't happen, toast_action would be PLAIN
        if varatt_is_external(ttc.ttc_values[idx].as_ref_bytes()) {
            i += 1;
            continue;
        }
        if for_compression && varatt_is_compressed(ttc.ttc_values[idx].as_ref_bytes()) {
            i += 1;
            continue;
        }
        if check_main && att[idx].attstorage != TYPSTORAGE_MAIN {
            i += 1;
            continue;
        }
        if !check_main
            && att[idx].attstorage != TYPSTORAGE_EXTENDED
            && att[idx].attstorage != TYPSTORAGE_EXTERNAL
        {
            i += 1;
            continue;
        }

        if ttc.ttc_attr[idx].tai_size > biggest_size {
            biggest_attno = i;
            biggest_size = ttc.ttc_attr[idx].tai_size;
        }

        i += 1;
    }

    Ok(biggest_attno)
}

// ---------------------------------------------------------------------------
// toast_tuple_try_compression
// ---------------------------------------------------------------------------

/// `toast_tuple_try_compression(ttc, attribute)` (toast_helper.c) — try to
/// compress the given attribute in place. On success the value slot is replaced
/// with the compressed image and `tai_size` updated; on failure the column is
/// flagged `TOASTCOL_INCOMPRESSIBLE` so later compression passes skip it.
pub fn toast_tuple_try_compression(
    ttc: &mut ToastTupleContext<'_>,
    attribute: i32,
) -> PgResult<()> {
    let mcx: Mcx<'_> = *ttc.ttc_values.allocator();
    let idx = attribute as usize;
    let cmethod = ttc.ttc_attr[idx].tai_compression;

    // new_value = toast_compress_datum(*value, attr->tai_compression);
    let new_value = {
        let value = ttc.ttc_values[idx].as_ref_bytes();
        toast_internals_seams::toast_compress_datum::call(mcx, value, cmethod)?
    };

    match new_value {
        Some(compressed) => {
            // successful compression
            //
            // C: if NEEDS_FREE was already set, pfree the old value before
            // overwriting. In the owned model the old buffer is dropped when the
            // slot is reassigned, covering both the pointer overwrite and the
            // matching pfree.
            let size = varsize_4b(&compressed) as i32; // VARSIZE(*value)
            ttc.ttc_values[idx] = Datum::ByRef(compressed);
            ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_NEEDS_FREE;
            ttc.ttc_attr[idx].tai_size = size;
            ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
        }
        None => {
            // incompressible, ignore on subsequent compression passes
            ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// toast_tuple_externalize
// ---------------------------------------------------------------------------

/// `toast_tuple_externalize(ttc, attribute, options)` (toast_helper.c) — move
/// the given attribute out to the relation's TOAST table, replacing the value
/// slot with the resulting on-disk-external TOAST pointer and flagging the
/// column `TOASTCOL_IGNORE` so it is not considered again.
pub fn toast_tuple_externalize(
    ttc: &mut ToastTupleContext<'_>,
    attribute: i32,
    options: i32,
) -> PgResult<()> {
    let mcx: Mcx<'_> = *ttc.ttc_values.allocator();
    let idx = attribute as usize;

    ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_IGNORE;

    // *value = toast_save_datum(ttc->ttc_rel, old_value, attr->tai_oldexternal,
    //                           options);
    let saved = {
        let old_value = ttc.ttc_values[idx].as_ref_bytes();
        let oldexternal = ttc.ttc_attr[idx]
            .tai_oldexternal
            .as_deref()
            .map(|v| v as &[u8]);
        toast_internals_seams::toast_save_datum::call(
            mcx,
            ttc.ttc_rel,
            old_value,
            oldexternal,
            options,
        )?
    };

    // C: pfree the old in-line value if it was NEEDS_FREE. Reassigning the slot
    // drops the old buffer here.
    ttc.ttc_values[idx] = Datum::ByRef(saved);
    ttc.ttc_attr[idx].tai_colflags |= TOASTCOL_NEEDS_FREE;
    ttc.ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;

    Ok(())
}

// ---------------------------------------------------------------------------
// toast_tuple_cleanup
// ---------------------------------------------------------------------------

/// `toast_tuple_cleanup(ttc)` (toast_helper.c) — free temporary values built
/// during toasting and delete any old external values made obsolete by an
/// UPDATE.
///
/// The "free allocated temp values" pass is implicit in the owned model: the
/// `NEEDS_FREE` value buffers live in the caller's `mcx` and are dropped with it
/// (C `pfree`s each one). The "delete external values from the old tuple" pass
/// is real work — it deletes the obsolete TOAST chunks.
pub fn toast_tuple_cleanup(ttc: &mut ToastTupleContext<'_>) -> PgResult<()> {
    let mcx: Mcx<'_> = *ttc.ttc_values.allocator();
    let att = read_attrs(mcx, ttc.ttc_rel)?;
    let num_attrs = att.len() as i32;

    // Free allocated temp values.
    //
    // C pfree's each NEEDS_FREE value buffer here. In the owned model those
    // buffers are mcx-allocated and freed when the per-tuple context resets, so
    // there is no per-attribute pfree to perform; the structural test is kept.
    if (ttc.ttc_flags & TOAST_NEEDS_FREE) != 0 {
        // (no-op: owned buffers are dropped with the memory context)
    }

    // Delete external values from the old tuple.
    if (ttc.ttc_flags & TOAST_NEEDS_DELETE_OLD) != 0 {
        let oldvalues = ttc
            .ttc_oldvalues
            .as_ref()
            .expect("TOAST_NEEDS_DELETE_OLD is only set on an UPDATE with ttc_oldvalues");

        let mut i: i32 = 0;
        while i < num_attrs {
            let idx = i as usize;
            if (ttc.ttc_attr[idx].tai_colflags & TOASTCOL_NEEDS_DELETE_OLD) != 0 {
                toast_internals_seams::toast_delete_datum::call(
                    ttc.ttc_rel,
                    oldvalues[idx].as_ref_bytes(),
                    false,
                )?;
            }
            i += 1;
        }
    }

    Ok(())
}

/// Resolve `relid` to its relcache entry, read out the per-attribute descriptor
/// fields (`attlen`, `attstorage`, `attcompression`) the toast passes need, and
/// release the pin the by-OID open took. C reads these straight off
/// `ttc->ttc_rel->rd_att`; here the relation crosses by OID, so we round-trip
/// through the relcache.
fn read_attrs(mcx: Mcx<'_>, relid: Oid) -> PgResult<alloc::vec::Vec<AttDesc>> {
    let rel = relcache_seams::relation_id_get_relation::call(mcx, relid)?
        .expect("toast_helper: ttc_rel has no relcache entry (C would deref a live Relation *)");

    let descr = &rel.rd_att;
    let natts = descr.natts.max(0) as usize;
    let mut out = alloc::vec::Vec::with_capacity(natts);
    for i in 0..natts {
        let a = descr.attr(i);
        out.push(AttDesc {
            attlen: a.attlen,
            attstorage: a.attstorage,
            attcompression: a.attcompression,
        });
    }

    // Release the rd_refcnt pin the by-OID open took (C had a caller-owned open
    // `Relation *`; our resolve pinned a fresh handle just to read attributes).
    relcache_seams::relation_close::call(relid)?;

    Ok(out)
}

/// `toast_delete_external(rel, values, isnull, is_speculative)`
/// (toast_helper.c:317) — check for external stored attributes and delete them
/// from the secondary (TOAST) relation. `values`/`isnull` are the deformed
/// columns of the tuple being deleted; for each non-null varlena column whose
/// stored value is external-ondisk, delete its TOAST chunks.
pub fn toast_delete_external(
    rel: &rel::RelationData<'_>,
    values: &[Datum<'_>],
    isnull: &[bool],
    is_speculative: bool,
) -> PgResult<()> {
    let tuple_desc = &rel.rd_att;
    let num_attrs = tuple_desc.natts;

    for i in 0..num_attrs.max(0) as usize {
        // if (TupleDescCompactAttr(tupleDesc, i)->attlen == -1)
        if tuple_desc.attr(i).attlen == -1 {
            if isnull[i] {
                continue;
            }
            // else if (VARATT_IS_EXTERNAL_ONDISK(value)) toast_delete_datum(...)
            if let Datum::ByRef(bytes) = &values[i] {
                if varatt_is_external_ondisk(bytes) {
                    toast_internals_seams::toast_delete_datum::call(rel.rd_id, bytes, is_speculative)?;
                }
            }
        }
    }

    Ok(())
}

/// Install this unit's seams. Called from `seams-init`.
pub fn init_seams() {
    toast_helper_seams::toast_tuple_init::set(toast_tuple_init);
    toast_helper_seams::toast_tuple_find_biggest_attribute::set(
        toast_tuple_find_biggest_attribute,
    );
    toast_helper_seams::toast_tuple_try_compression::set(toast_tuple_try_compression);
    toast_helper_seams::toast_tuple_externalize::set(toast_tuple_externalize);
    toast_helper_seams::toast_tuple_cleanup::set(toast_tuple_cleanup);
    toast_internals_seams::toast_delete_external::set(toast_delete_external);
}
