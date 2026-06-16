//! KEYSTONE family — `src/backend/utils/adt/expandeddatum.c`.
//!
//! Support functions for "expanded" value representations: the
//! `ExpandedObjectHeader` ABI that every expanded container type (expanded
//! records in [`crate::expandedrecord`], expanded arrays in arrayfuncs, etc.)
//! embeds and dispatches through. This is the shared foundation the rest of
//! the unit — and external consumers such as
//! `backend-access-common-heaptuple`'s `heap_compute_data_size` / `fill_val`,
//! which flatten expanded datums into a tuple — compile against, so it is
//! ported in the scaffold phase and its two consumed seams
//! (`eoh_get_flat_size` / `eoh_flatten_into`) are installed here from
//! [`crate::init_seams`].
//!
//! In the owned model an expanded datum crosses as the typed
//! [`types_datum::ExpandedObjectRef`] handle (C's `ExpandedObjectHeader *`
//! reached through `DatumGetEOHP`), not raw `&[u8]`. The method dispatch
//! (`eoh_methods->get_flat_size` / `->flatten_into`) is owned by the concrete
//! expanded type; the keystone routes a flatten request to whichever expanded
//! type produced the datum.

use mcx::PgVec;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_datum::expandeddatum::{VARTAG_EXPANDED_RO, VARTAG_EXPANDED_RW};
use types_datum::ExpandedObjectRef;
use types_error::PgResult;

/// `EOH_HEADER_MAGIC` (`utils/expandeddatum.h`): the phony varlena length word
/// (`-1`) stamped into `ExpandedObjectHeader.vl_len_`. `VARATT_IS_EXPANDED_HEADER`
/// tests for it; no real 4-byte-header varlena can begin with `0xFFFFFFFF`.
pub const EOH_HEADER_MAGIC: i32 = -1;

/// `VARHDRSZ_EXTERNAL` == `offsetof(varattrib_1b_e, va_data)` (`varatt.h`): the
/// 1-byte `va_header` (`0x01`) plus the 1-byte `va_tag`.
const VARHDRSZ_EXTERNAL: usize = 2;

/// `sizeof(varatt_expanded)` (`varatt.h`): a single `ExpandedObjectHeader *`.
/// The owned datum image carries the pointer payload verbatim; its width is the
/// platform pointer width, exactly as C `memcpy(VARDATA_EXTERNAL(...), &ptr,
/// sizeof(ptr))` writes.
const SIZEOF_VARATT_EXPANDED: usize = core::mem::size_of::<usize>();

/// `EXPANDED_POINTER_SIZE` == `VARHDRSZ_EXTERNAL + sizeof(varatt_expanded)`
/// (`utils/expandeddatum.h`): the full size of an expanded-object TOAST pointer
/// datum image.
pub const EXPANDED_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + SIZEOF_VARATT_EXPANDED;

/// `EOH_get_flat_size(eohptr)` (expandeddatum.c):
///
/// ```c
/// Size
/// EOH_get_flat_size(ExpandedObjectHeader *eohptr)
/// {
///     return eohptr->eoh_methods->get_flat_size(eohptr);
/// }
/// ```
///
/// A one-line convenience wrapper that chases the object's method table and
/// invokes its `get_flat_size`. The method itself belongs to the concrete
/// expanded type (expanded record / expanded array), so in the owned model it
/// is the [`eom_get_flat_size`](backend_utils_adt_expanded_methods_seams::eom_get_flat_size)
/// seam — exactly mirroring the C indirection through `eoh_methods`.
///
/// This is the implementation installed into
/// `backend_utils_adt_misc2_seams::eoh_get_flat_size` so external flatteners
/// (heaptuple) can size an expanded datum without depending on this crate.
pub fn eoh_get_flat_size(eoh: ExpandedObjectRef<'_>) -> PgResult<usize> {
    backend_utils_adt_expanded_methods_seams::eom_get_flat_size::call(eoh)
}

/// `EOH_flatten_into(eohptr, result, allocated_size)` (expandeddatum.c):
///
/// ```c
/// void
/// EOH_flatten_into(ExpandedObjectHeader *eohptr,
///                  void *result, Size allocated_size)
/// {
///     eohptr->eoh_methods->flatten_into(eohptr, result, allocated_size);
/// }
/// ```
///
/// Flatten the expanded object into `dest`, which is exactly the preceding
/// `eoh_get_flat_size` bytes long (C passes `allocated_size` for the method to
/// cross-check; the slice length carries it here). Dispatches through the
/// object's method table — the concrete expanded type's `flatten_into`, modeled
/// as the [`eom_flatten_into`](backend_utils_adt_expanded_methods_seams::eom_flatten_into)
/// seam.
///
/// Installed into `backend_utils_adt_misc2_seams::eoh_flatten_into`.
pub fn eoh_flatten_into(eoh: ExpandedObjectRef<'_>, dest: &mut [u8]) -> PgResult<()> {
    backend_utils_adt_expanded_methods_seams::eom_flatten_into::call(eoh, dest)
}

/// `DatumGetEOHP(d)` (expandeddatum.c:28):
///
/// ```c
/// ExpandedObjectHeader *
/// DatumGetEOHP(Datum d)
/// {
///     varattrib_1b_e *datum = (varattrib_1b_e *) DatumGetPointer(d);
///     varatt_expanded ptr;
///     Assert(VARATT_IS_EXTERNAL_EXPANDED(datum));
///     memcpy(&ptr, VARDATA_EXTERNAL(datum), sizeof(ptr));
///     Assert(VARATT_IS_EXPANDED_HEADER(ptr.eohptr));
///     return ptr.eohptr;
/// }
/// ```
///
/// Given a Datum that is an expanded-object reference, extract the pointer to
/// the object header. In the owned model the "pointer" is the typed
/// [`ExpandedObjectRef`] handle reached through the datum's verbatim varlena
/// bytes; construction asserts the `VARATT_IS_EXTERNAL_EXPANDED` shape (the C
/// `Assert`), so a non-expanded datum stops loud at the boundary. The second C
/// `Assert(VARATT_IS_EXPANDED_HEADER(ptr.eohptr))` (the header's `vl_len_ ==
/// EOH_HEADER_MAGIC` crosscheck) belongs to the concrete header the handle
/// names; it is re-applied by the concrete type's `er_magic`/header-magic
/// asserts when it materializes the object behind this handle.
pub fn datum_get_eohp(datum: &[u8]) -> ExpandedObjectRef<'_> {
    ExpandedObjectRef::from_expanded_datum_bytes(datum)
}

/// `EOH_init_header(eohptr, methods, obj_context)` (expandeddatum.c:47):
///
/// ```c
/// void
/// EOH_init_header(ExpandedObjectHeader *eohptr,
///                 const ExpandedObjectMethods *methods,
///                 MemoryContext obj_context)
/// {
///     varatt_expanded ptr;
///     eohptr->vl_len_ = EOH_HEADER_MAGIC;
///     eohptr->eoh_methods = methods;
///     eohptr->eoh_context = obj_context;
///     ptr.eohptr = eohptr;
///     SET_VARTAG_EXTERNAL(eohptr->eoh_rw_ptr, VARTAG_EXPANDED_RW);
///     memcpy(VARDATA_EXTERNAL(eohptr->eoh_rw_ptr), &ptr, sizeof(ptr));
///     SET_VARTAG_EXTERNAL(eohptr->eoh_ro_ptr, VARTAG_EXPANDED_RO);
///     memcpy(VARDATA_EXTERNAL(eohptr->eoh_ro_ptr), &ptr, sizeof(ptr));
/// }
/// ```
///
/// Initialize the common header of an expanded object. The main thing this
/// encapsulates is building the two standard TOAST pointers (read-write and
/// read-only) that the object hands out via `EOHPGetRWDatum` / `EOHPGetRODatum`.
///
/// In the owned model the magic / method-table / `eoh_context` slots of
/// `ExpandedObjectHeader` are carried as the concrete header type's own fields
/// (e.g. [`crate::expandedrecord::ExpandedRecordHeader`]'s `er_magic` /
/// `obj_cxt`, dispatched through the free `er_*` methods), so what remains for
/// the keystone to materialize is exactly the two TOAST-pointer datum images.
/// This builds them given the object's identity payload `eohptr` (the
/// `varatt_expanded.eohptr` bytes — the owned stand-in for the C
/// `ExpandedObjectHeader *`). Returns `(rw_ptr, ro_ptr)` byte images,
/// `EXPANDED_POINTER_SIZE` long each, differing only in the `va_tag` byte
/// (`VARTAG_EXPANDED_RW` vs `_RO`), exactly as the two `memcpy`s do.
pub fn eoh_init_header<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    eohptr: &[u8],
) -> PgResult<(PgVec<'mcx, u8>, PgVec<'mcx, u8>)> {
    assert_eq!(
        eohptr.len(),
        SIZEOF_VARATT_EXPANDED,
        "eoh_init_header: eohptr payload must be sizeof(varatt_expanded)"
    );
    let rw = build_expanded_pointer(mcx, VARTAG_EXPANDED_RW, eohptr)?;
    let ro = build_expanded_pointer(mcx, VARTAG_EXPANDED_RO, eohptr)?;
    Ok((rw, ro))
}

/// `SET_VARTAG_EXTERNAL(ptr, tag)` + `memcpy(VARDATA_EXTERNAL(ptr), payload)`:
/// assemble one `varattrib_1b_e` external TOAST-pointer image — `va_header =
/// 0x01`, `va_tag = tag`, then the `varatt_expanded` payload.
fn build_expanded_pointer<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    tag: u8,
    payload: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut img = mcx::vec_with_capacity_in(mcx, VARHDRSZ_EXTERNAL + payload.len())?;
    img.push(0x01); // VARATT_IS_1B_E marker
    img.push(tag); // va_tag
    for &b in payload {
        img.push(b);
    }
    Ok(img)
}

/// `MakeExpandedObjectReadOnlyInternal(d)` (expandeddatum.c:94):
///
/// ```c
/// Datum
/// MakeExpandedObjectReadOnlyInternal(Datum d)
/// {
///     ExpandedObjectHeader *eohptr;
///     if (!VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d)))
///         return d;
///     eohptr = DatumGetEOHP(d);
///     return EOHPGetRODatum(eohptr);
/// }
/// ```
///
/// If the Datum represents a R/W expanded object, change it to R/O; otherwise
/// return the original Datum. The caller must ensure the datum is a non-null
/// varlena (the `MakeExpandedObjectReadOnly` macro checks `isnull`/`typlen`).
///
/// In the owned model both standard pointers carry the identical
/// `varatt_expanded` payload and differ only in the `va_tag` byte
/// (`EOHPGetRODatum` returns the object's `eoh_ro_ptr`, the same `eohptr` with
/// the R/O tag). So this is a pure datum-image transform: a non-RW input is
/// returned verbatim; a R/W input is copied with `va_tag` flipped to
/// `VARTAG_EXPANDED_RO`. `Ok(None)` signals "return the input unchanged" (the C
/// `return d` branch) so the caller need not reallocate.
pub fn make_expanded_object_read_only_internal<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    datum: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Nothing to do if not a read-write expanded-object pointer.
    let is_rw = datum.len() >= 2 && datum[0] == 0x01 && datum[1] == VARTAG_EXPANDED_RW;
    if !is_rw {
        return Ok(None);
    }
    // Return the built-in read-only pointer instead of the given pointer: same
    // eohptr payload, R/O tag.
    let mut ro = mcx::vec_with_capacity_in(mcx, datum.len())?;
    for &b in datum {
        ro.push(b);
    }
    ro[1] = VARTAG_EXPANDED_RO;
    Ok(Some(ro))
}

/// Value-typed wrapper installed as the
/// `make_expanded_object_read_only_internal_v` seam: marshals the unified
/// [`Datum`] in/out around [`make_expanded_object_read_only_internal`]. Reached
/// only on the non-null, `typlen == -1` branch of the
/// `MakeExpandedObjectReadOnly` macro (the caller does the short-circuit). A
/// by-value (`Datum::ByVal`) argument is never an expanded-object pointer, so it
/// passes through verbatim (the C macro reaches here only for varlena); a
/// by-reference argument is inspected by the internal helper, which returns the
/// R/O image (R/W input) or signals "unchanged" (`Ok(None)`), in which case the
/// original datum is returned.
pub fn make_expanded_object_read_only_internal_v<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    d: &Datum<'_>,
) -> PgResult<Datum<'mcx>> {
    let bytes: &[u8] = match d {
        Datum::ByRef(b) => b,
        // A by-value datum is never an expanded-object pointer; return it
        // unchanged (matching the internal helper's "not RW → return d").
        Datum::ByVal(v) => return Ok(Datum::ByVal(*v)),
        // A live `Datum::Expanded` would short-circuit here as already-RW, and
        // the other arms are never expanded varlenas — none has a producer that
        // reaches this seam yet.
        Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
            panic!("make_expanded_object_read_only_internal_v: non-varlena Datum arm (Cstring/Composite/Expanded/Internal) not yet produced — wave 2")
        }
    };
    match make_expanded_object_read_only_internal(mcx, bytes)? {
        Some(ro) => Ok(Datum::ByRef(ro)),
        // C `return d` — the input was not a read-write expanded pointer.
        None => Ok(d.clone_in(mcx)?),
    }
}

/// `TransferExpandedObject(d, new_parent)` (expandeddatum.c:117):
///
/// ```c
/// Datum
/// TransferExpandedObject(Datum d, MemoryContext new_parent)
/// {
///     ExpandedObjectHeader *eohptr = DatumGetEOHP(d);
///     Assert(VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d)));
///     MemoryContextSetParent(eohptr->eoh_context, new_parent);
///     return EOHPGetRWDatum(eohptr);
/// }
/// ```
///
/// Transfer ownership of an expanded object to a new parent memory context,
/// returning the object's standard R/W pointer. The crux is
/// `MemoryContextSetParent(eohptr->eoh_context, new_parent)` — an in-place
/// reparent of a live context.
///
/// The owned [`mcx::MemoryContext`] expresses context lifespan through Rust
/// ownership (a child context is *held* by its owner and reclaimed by `Drop`),
/// not through a callable in-place `MemoryContextSetParent` on a context reached
/// from a decoded datum-pointer handle. Faithfully reparenting an expanded
/// object's `eoh_context` therefore belongs to whoever owns the concrete header
/// value (it would move the owned `MemoryContext` into the new parent's
/// ownership), not to this bytes-handle keystone — so per mirror-PG-and-panic we
/// stop loud at this substrate boundary rather than fake a reparent that the
/// owned model cannot express here.
pub fn transfer_expanded_object(_datum: &[u8]) -> ! {
    panic!(
        "expandeddatum: TransferExpandedObject: MemoryContextSetParent on a live \
         eoh_context is expressed by ownership/move in the owned mcx model, not \
         callable on a decoded datum-pointer handle here (reparent belongs to the \
         concrete expanded-object owner)"
    )
}

/// `DeleteExpandedObject(d)` (expandeddatum.c:135):
///
/// ```c
/// void
/// DeleteExpandedObject(Datum d)
/// {
///     ExpandedObjectHeader *eohptr = DatumGetEOHP(d);
///     Assert(VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d)));
///     MemoryContextDelete(eohptr->eoh_context);
/// }
/// ```
///
/// Delete an expanded object (must be referenced by a R/W pointer) by deleting
/// its private `eoh_context`. In the owned [`mcx::MemoryContext`] model context
/// deletion is `Drop` (dropping the owned `MemoryContext` value cascades to its
/// children); there is no `MemoryContextDelete` callable on a context reached
/// from a decoded datum-pointer handle. Deleting an expanded object therefore
/// means dropping the concrete header value its owner holds, not an operation
/// this bytes-handle keystone can perform — mirror-PG-and-panic at the substrate
/// boundary.
pub fn delete_expanded_object(_datum: &[u8]) -> ! {
    panic!(
        "expandeddatum: DeleteExpandedObject: MemoryContextDelete of a live \
         eoh_context is expressed by dropping the owned MemoryContext value in the \
         owned mcx model, not callable on a decoded datum-pointer handle here \
         (deletion belongs to the concrete expanded-object owner)"
    )
}
