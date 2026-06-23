//! `backend/access/common/toast_internals.c` — TOAST-system internals.
//!
//! This crate owns the catalog-/index-side TOAST helpers consumers reach across
//! the cycle through `backend-access-common-toast-internals-seams`:
//!
//! * [`toast_open_indexes`] / `toast_close_indexes` — open the TOAST relation's
//!   indexes (`index_open` handles held by `ToastIndexesGuard`, closed on
//!   `Drop`) and find the valid one.
//! * [`toast_get_valid_index`] — the valid index's OID for a TOAST relation OID.
//! * [`get_toast_snapshot`] — the static `SnapshotToastData` sentinel, gated on
//!   the threaded `HaveRegisteredOrActiveSnapshot()` bit.
//! * [`toast_compress_datum`] — compress a varlena via the already-ported
//!   `backend-access-common-toast-compression` primitives, stamping the
//!   `va_tcinfo` (rawsize + 2-bit method) word.
//!
//! The `toast_save_datum` writer and `toast_delete_datum` chunk the value
//! across / reclaim it from the relation's TOAST table; `toast_fetch_datum`
//! reassembles a whole on-disk-external value from its chunks (the heap AM's
//! `heap_fetch_toast_slice` does the scan). The `toast_fetch_datum_slice` /
//! `indirect_pointer` detoast statics and `toast_delete_external`
//! (`toast_helper.c`) are still installed elsewhere / pending.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{vec_with_capacity_in, MemoryContext, Mcx, PgVec};
use ::types_core::primitive::{AttrNumber, OidIsValid};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use rel::{Relation, RelationData};
use snapshot::{SnapshotData, SnapshotType};
use ::types_storage::lock::{AccessShareLock, LOCKMODE, NoLock, RowExclusiveLock};

use heaptuple::{heap_form_tuple, Datum};
use ::scankey::ScanKeyInit;
use ::indexam::index_insert;
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_tableam::amapi::IndexUniqueCheck;
use ::types_tableam::index_info_carrier::IndexInfoCarrier;

use genam_seams as genam_seams;
use heapam_seams as heapam_seams;

/// `F_OIDEQ` (fmgroids.h): the `oideq` comparison proc OID for the toast
/// value-id scan key.
const F_OIDEQ: Oid = 184;

/// `TOAST_MAX_CHUNK_SIZE` (heaptoast.h): the maximum data payload of a single
/// TOAST chunk row.
const TOAST_MAX_CHUNK_SIZE: i32 = heaptoast::TOAST_MAX_CHUNK_SIZE;

/// `VARTAG_ONDISK` (varatt.h).
const VARTAG_ONDISK: u8 = 18;
/// `VARHDRSZ_EXTERNAL` (varatt.h): `offsetof(varattrib_1b_e, va_data)` — the
/// 1-byte `va_header` plus the 1-byte `va_tag`.
const VARHDRSZ_EXTERNAL: usize = 2;
/// `TOAST_POINTER_SIZE` (toast_internals.h): `VARHDRSZ_EXTERNAL +
/// sizeof(varatt_external)` (2 + 16).
const TOAST_POINTER_SIZE: usize = VARHDRSZ_EXTERNAL + 16;

use toast_compression::{
    lz4_compress_datum, pglz_compress_datum, TOAST_INVALID_COMPRESSION_ID,
    TOAST_LZ4_COMPRESSION, TOAST_LZ4_COMPRESSION_ID, TOAST_PGLZ_COMPRESSION,
    TOAST_PGLZ_COMPRESSION_ID, ToastCompressionId,
};

/// `VARHDRSZ` (varatt.h): the 4-byte varlena length word.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (varatt.h): a short (1-byte header) varlena's header.
const VARHDRSZ_SHORT: usize = 1;
/// `VARLENA_EXTSIZE_BITS` (varatt.h).
const VARLENA_EXTSIZE_BITS: u32 = 30;
/// `VARLENA_EXTSIZE_MASK` (varatt.h).
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;
/// `InvalidCompressionMethod` (toast_compression.h).
const INVALID_COMPRESSION_METHOD: i8 = 0;

// ---------------------------------------------------------------------------
// Local varatt.h header predicates (pure bit-twiddling over the encoded bytes).
// ---------------------------------------------------------------------------

#[inline]
fn varatt_is_external(b: &[u8]) -> bool {
    b[0] == 0x01
}

#[inline]
fn varatt_is_compressed(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x02
}

#[inline]
fn varatt_is_4b(b: &[u8]) -> bool {
    (b[0] & 0x03) == 0x00
}

#[inline]
fn varatt_is_short(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARSIZE(PTR)` (4-byte form, native order): the length word `>> 2`.
#[inline]
fn varsize_4b(b: &[u8]) -> u32 {
    let word = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
    (word >> 2) & 0x3fff_ffff
}

/// `VARSIZE_SHORT(PTR)`: `(va_header >> 1) & 0x7F`.
#[inline]
fn varsize_1b(b: &[u8]) -> u32 {
    ((b[0] >> 1) & 0x7f) as u32
}

/// `VARSIZE_ANY_EXHDR(PTR)`: the payload byte count of a non-external varlena.
fn varsize_any_exhdr(b: &[u8]) -> PgResult<i32> {
    if varatt_is_compressed(b) || varatt_is_4b(b) {
        Ok(varsize_4b(b) as i32 - VARHDRSZ as i32)
    } else if varatt_is_short(b) {
        Ok(varsize_1b(b) as i32 - VARHDRSZ_SHORT as i32)
    } else {
        Err(PgError::error("VARSIZE_ANY_EXHDR on external datum"))
    }
}

/// `TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method)`
/// (toast_internals.h): write `tcinfo = len | (cm_method << VARLENA_EXTSIZE_BITS)`
/// into the 4 bytes after the varlena length word.
fn toast_compress_set_size_and_compress_method(
    tmp: &mut [u8],
    len: i32,
    cm_method: ToastCompressionId,
) -> PgResult<()> {
    debug_assert!(len > 0 && (len as u32) <= VARLENA_EXTSIZE_MASK);
    debug_assert!(cm_method == TOAST_PGLZ_COMPRESSION_ID || cm_method == TOAST_LZ4_COMPRESSION_ID);
    let tcinfo = (len as u32) | ((cm_method as u32) << VARLENA_EXTSIZE_BITS);
    let word = tmp
        .get_mut(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(|| PgError::error("truncated compressed datum header"))?;
    word.copy_from_slice(&tcinfo.to_ne_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// toast_compress_datum
// ---------------------------------------------------------------------------

/// `toast_compress_datum(value, cmethod)` — create a compressed version of a
/// varlena datum, or `None` if the compressed result would not shrink it by
/// more than 2 bytes (C's `PointerGetDatum(NULL)`).
pub fn toast_compress_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    cmethod: i8,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    debug_assert!(!varatt_is_external(value));
    debug_assert!(!varatt_is_compressed(value));

    let valsize = varsize_any_exhdr(value)?;

    // If the compression method is not valid, use the current default.
    let mut cmethod = cmethod;
    if cmethod == INVALID_COMPRESSION_METHOD {
        cmethod = default_toast_compression();
    }
    let cmethod = cmethod as u8;

    let (tmp, cmid): (Option<PgVec<u8>>, ToastCompressionId) = if cmethod == TOAST_PGLZ_COMPRESSION {
        (pglz_compress_datum(mcx, value)?, TOAST_PGLZ_COMPRESSION_ID)
    } else if cmethod == TOAST_LZ4_COMPRESSION {
        (lz4_compress_datum(mcx, value)?, TOAST_LZ4_COMPRESSION_ID)
    } else {
        return Err(PgError::error(format!(
            "invalid compression method {}",
            format_byte_as_c_char(cmethod)
        )));
    };

    let Some(mut tmp) = tmp else {
        return Ok(None);
    };

    // Recheck the actual size: insist on a savings of more than 2 bytes. C
    // computes `VARSIZE(tmp) < valsize - 2` as an UNSIGNED comparison.
    let varsize_tmp = varsize_4b(&tmp);
    if varsize_tmp < (valsize - 2) as u32 {
        debug_assert!(cmid != TOAST_INVALID_COMPRESSION_ID);
        toast_compress_set_size_and_compress_method(&mut tmp, valsize, cmid)?;
        Ok(Some(tmp))
    } else {
        drop(tmp);
        Ok(None)
    }
}

/// `default_toast_compression` GUC (read through the installed accessors): the
/// enum value is the method char (`'p'`/`'l'`).
fn default_toast_compression() -> i8 {
    guc_tables::vars::default_toast_compression.read() as i8
}

/// Render a single byte the way C's `printf("%c")` would for the
/// `"invalid compression method %c"` message.
fn format_byte_as_c_char(b: u8) -> String {
    if b.is_ascii() {
        (b as char).to_string()
    } else {
        format!("\\x{b:02x}")
    }
}

// ---------------------------------------------------------------------------
// toast_open_indexes / toast_close_indexes / toast_get_valid_index
// ---------------------------------------------------------------------------

/// `toast_open_indexes(toastrel, lock)` — open all indexes of the TOAST
/// relation and return them held by a [`ToastIndexesGuard`], along with the
/// position of the (single) valid index.
pub fn toast_open_indexes<'mcx>(
    mcx: Mcx<'mcx>,
    toastrel: &RelationData<'_>,
    lock: LOCKMODE,
) -> PgResult<ToastIndexesGuard<'mcx>> {
    // RelationGetIndexList(toastrel).
    let indexlist =
        plancat_ext_seams::relation_get_index_list_oids::call(toastrel.rd_id)?;
    debug_assert!(!indexlist.is_empty());

    let num_indexes = indexlist.len();
    let mut toastidxs: PgVec<Relation<'mcx>> = vec_with_capacity_in(mcx, num_indexes)?;

    // index_open every index in the list.
    for &indexoid in indexlist.iter() {
        let idx = indexam_seams::index_open::call(mcx, indexoid, lock)?;
        toastidxs.push(idx);
    }

    // Fetch the first valid index in the list (rd_index->indisvalid).
    let mut found: Option<usize> = None;
    for (i, idx) in toastidxs.iter().enumerate() {
        let indisvalid = idx
            .rd_index
            .as_ref()
            .map(|ix| ix.indisvalid)
            .unwrap_or(false);
        if indisvalid {
            found = Some(i);
            break;
        }
    }

    let Some(valid_index) = found else {
        return Err(PgError::error(format!(
            "no valid index found for toast relation with Oid {}",
            toastrel.rd_id
        )));
    };

    Ok(ToastIndexesGuard::new(toastidxs, valid_index, lock))
}

/// `toast_close_indexes(toastidxs, num_indexes, lock)` — close the indexes
/// opened by [`toast_open_indexes`]. C: `for (i...) index_close(toastidxs[i],
/// lock);`. Each `index_close(rel, lock)` releases the relation lock when
/// `lock != NoLock`, so we must consume each handle via `close(lock)` rather
/// than drop it: the owned handle's `Drop` is the abort path, releasing the
/// relcache reference with `NoLock` and leaving the lock to transaction-end
/// cleanup. Closing with `lock` is what makes the toast-index AccessShareLock
/// taken during a detoast read drop immediately, matching C (otherwise a
/// `pg_toast_NNNN_index` AccessShareLock lingers for the whole transaction).
fn toast_close_indexes(toastidxs: PgVec<Relation<'_>>, lock: LOCKMODE) -> PgResult<()> {
    for idx in toastidxs.into_iter() {
        idx.close(lock)?;
    }
    Ok(())
}

/// `toast_get_valid_index(toastoid, lock)` — the OID of the valid index of a
/// TOAST relation.
pub fn toast_get_valid_index<'mcx>(
    mcx: Mcx<'mcx>,
    toastoid: Oid,
    lock: LOCKMODE,
) -> PgResult<Oid> {
    let toastrel = table_seams::table_open::call(mcx, toastoid, lock)?;
    let guard = toast_open_indexes(mcx, &toastrel, lock)?;
    let valid_index_oid = guard.valid_index().rd_id;
    // Close the toast relation and all its indexes (keep the lock until commit).
    guard.close()?;
    drop(toastrel);
    Ok(valid_index_oid)
}

// ---------------------------------------------------------------------------
// get_toast_snapshot
// ---------------------------------------------------------------------------

/// `get_toast_snapshot()` — the TOAST snapshot (`&SnapshotToastData`). Detoasting
/// must happen in the transaction that fetched the TOAST pointer; we can only
/// check the session has an active snapshot.
pub fn get_toast_snapshot(
    have_registered_or_active_snapshot: bool,
) -> PgResult<SnapshotData> {
    if !have_registered_or_active_snapshot {
        return Err(PgError::error(
            "cannot fetch toast data without an active snapshot",
        ));
    }
    Ok(SnapshotData::sentinel(SnapshotType::SNAPSHOT_TOAST))
}

// ---------------------------------------------------------------------------
// struct varatt_external (the on-disk TOAST pointer) — read/write.
// ---------------------------------------------------------------------------

/// `struct varatt_external` (varatt.h): the on-disk TOAST-pointer payload that
/// follows the 2-byte external header.
#[derive(Clone, Copy, Debug, Default)]
struct VarattExternal {
    /// `va_rawsize`: original datum size, header included.
    va_rawsize: i32,
    /// `va_extinfo`: external saved size (low 30 bits) + compression method
    /// (top 2 bits).
    va_extinfo: u32,
    /// `va_valueid`: unique ID of value within the toast table.
    va_valueid: Oid,
    /// `va_toastrelid`: RelID of the TOAST table containing it.
    va_toastrelid: Oid,
}

/// `VARATT_IS_EXTERNAL_ONDISK(PTR)`: an external 1-byte-header varlena whose
/// tag is `VARTAG_ONDISK`.
#[inline]
fn varatt_is_external_ondisk(b: &[u8]) -> bool {
    // VARATT_IS_EXTERNAL(b) && VARTAG_EXTERNAL(b) == VARTAG_ONDISK
    varatt_is_external(b) && b[1] == VARTAG_ONDISK
}

/// `VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr)`: decode the on-disk TOAST
/// pointer that begins just after the 2-byte external header.
fn varatt_external_get_pointer(b: &[u8]) -> PgResult<VarattExternal> {
    let p = b
        .get(VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 16)
        .ok_or_else(|| PgError::error("truncated external TOAST pointer"))?;
    Ok(VarattExternal {
        va_rawsize: i32::from_ne_bytes([p[0], p[1], p[2], p[3]]),
        va_extinfo: u32::from_ne_bytes([p[4], p[5], p[6], p[7]]),
        va_valueid: u32::from_ne_bytes([p[8], p[9], p[10], p[11]]),
        va_toastrelid: u32::from_ne_bytes([p[12], p[13], p[14], p[15]]),
    })
}

/// `memcpy(VARDATA_EXTERNAL(result), &toast_pointer, sizeof(toast_pointer))`:
/// the 16 payload bytes of a `varatt_external`, in native order.
fn varatt_external_bytes(p: &VarattExternal) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&p.va_rawsize.to_ne_bytes());
    out[4..8].copy_from_slice(&p.va_extinfo.to_ne_bytes());
    out[8..12].copy_from_slice(&p.va_valueid.to_ne_bytes());
    out[12..16].copy_from_slice(&p.va_toastrelid.to_ne_bytes());
    out
}

/// `VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm)`
/// (varatt.h): the external saved size in the low 30 bits, the compression
/// method in the top 2 bits of `va_extinfo`.
fn varatt_external_set_size_and_compress_method(p: &mut VarattExternal, len: i32, cm: u32) {
    debug_assert!(len >= 0 && (len as u32) <= VARLENA_EXTSIZE_MASK);
    debug_assert!(cm <= 2);
    p.va_extinfo = (len as u32) | (cm << VARLENA_EXTSIZE_BITS);
}

/// `VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)`: the external saved size is
/// strictly smaller than the raw size (header excluded).
fn varatt_external_is_compressed(p: &VarattExternal) -> bool {
    (p.va_extinfo & VARLENA_EXTSIZE_MASK) < (p.va_rawsize as u32).wrapping_sub(VARHDRSZ as u32)
}

/// `VARDATA_COMPRESSED_GET_EXTSIZE(PTR)` (varatt.h): the raw payload size stored
/// in the low 30 bits of the `va_tcinfo` word (the 4 bytes after the length
/// word) of a compressed in-line varlena.
fn vardata_compressed_get_extsize(b: &[u8]) -> PgResult<i32> {
    let w = b
        .get(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(|| PgError::error("truncated compressed datum header"))?;
    let tcinfo = u32::from_ne_bytes([w[0], w[1], w[2], w[3]]);
    Ok((tcinfo & VARLENA_EXTSIZE_MASK) as i32)
}

/// `VARDATA_COMPRESSED_GET_COMPRESS_METHOD(PTR)` (varatt.h): the compression
/// method in the top 2 bits of the `va_tcinfo` word.
fn vardata_compressed_get_compress_method(b: &[u8]) -> PgResult<u32> {
    let w = b
        .get(VARHDRSZ..VARHDRSZ + 4)
        .ok_or_else(|| PgError::error("truncated compressed datum header"))?;
    let tcinfo = u32::from_ne_bytes([w[0], w[1], w[2], w[3]]);
    Ok(tcinfo >> VARLENA_EXTSIZE_BITS)
}

/// `VARATT_EXTERNAL_GET_EXTSIZE(toast_pointer)` (varatt.h): the external saved
/// size (low 30 bits of `va_extinfo`).
#[inline]
fn varatt_external_get_extsize(p: &VarattExternal) -> i32 {
    (p.va_extinfo & VARLENA_EXTSIZE_MASK) as i32
}

// ---------------------------------------------------------------------------
// toast_fetch_datum (access/common/detoast.c, static)
// ---------------------------------------------------------------------------

/// `toast_fetch_datum(attr)` — reconstruct an in-memory datum from the chunks
/// saved in the TOAST relation. The reassembled varlena (a compressed-form
/// header if the saved value is compressed, else a plain 4-byte-header varlena)
/// is returned in `mcx`; decompression is left to the caller.
pub fn toast_fetch_datum<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    if !varatt_is_external_ondisk(attr) {
        return Err(PgError::error(
            "toast_fetch_datum shouldn't be called for non-ondisk datums",
        ));
    }

    // Must copy to access aligned fields.
    let toast_pointer = varatt_external_get_pointer(attr)?;
    let attrsize = varatt_external_get_extsize(&toast_pointer);

    // result = palloc(attrsize + VARHDRSZ); SET_VARSIZE[_COMPRESSED].
    let total = attrsize as usize + VARHDRSZ;
    let mut result: PgVec<u8> = vec_with_capacity_in(mcx, total)?;
    result.resize(total, 0);
    if varatt_external_is_compressed(&toast_pointer) {
        // SET_VARSIZE_COMPRESSED(result, attrsize + VARHDRSZ): the 4-byte length
        // word with the low two bits = 0b10 (compressed 4-byte form).
        let word = ((total as u32) << 2) | 0x02;
        result[0..4].copy_from_slice(&word.to_ne_bytes());
    } else {
        // SET_VARSIZE(result, attrsize + VARHDRSZ).
        let word = (total as u32) << 2;
        result[0..4].copy_from_slice(&word.to_ne_bytes());
    }

    if attrsize == 0 {
        return Ok(result); // Probably shouldn't happen, but just in case.
    }

    // Open the toast relation and fetch all chunks. C dispatches through
    // table_relation_fetch_toast_slice (the heap AM's heap_fetch_toast_slice);
    // the heap implementation is the only table AM and is reached directly.
    let toastrel = table_seams::table_open::call(
        mcx,
        toast_pointer.va_toastrelid,
        AccessShareLock,
    )?;
    let have_snapshot =
        snapmgr_seams::have_registered_or_active_snapshot::call();
    heaptoast::heap_fetch_toast_slice(
        mcx,
        &toastrel,
        toast_pointer.va_valueid,
        attrsize,
        0,
        attrsize,
        &mut result[VARHDRSZ..],
        have_snapshot,
    )?;
    toastrel.close(AccessShareLock)?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// toast_save_datum
// ---------------------------------------------------------------------------

/// `toast_save_datum(rel, value, oldexternal, options)` — move a varlena value
/// out to the relation's TOAST table, chunking it across rows, and return the
/// new on-disk-external TOAST pointer image.
pub fn toast_save_datum<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Oid,
    value: &[u8],
    oldexternal: Option<&[u8]>,
    options: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // Assert(!VARATT_IS_EXTERNAL(value));
    debug_assert!(!varatt_is_external(value));

    let mut toast_pointer = VarattExternal::default();

    // mycid = GetCurrentCommandId(true);
    let mycid = transam_xact_seams::get_current_command_id::call(true)?;

    // Open the toast relation and its indexes. reltoastrelid is read off the
    // heap relation's relcache entry (rel->rd_rel->reltoastrelid).
    let reltoastrelid = relcache_seams::rel_reltoastrelid::call(rel)?;
    let toastrel =
        table_seams::table_open::call(mcx, reltoastrelid, RowExclusiveLock)?;
    let toasttup_desc = &toastrel.rd_att;

    // Open all the toast indexes and look for the valid one.
    let guard = toast_open_indexes(mcx, &toastrel, RowExclusiveLock)?;

    // Get the data pointer and length, and compute va_rawsize and va_extinfo.
    let data: &[u8];
    if varatt_is_short(value) {
        // data_p = VARDATA_SHORT(dval); data_todo = VARSIZE_SHORT - VARHDRSZ_SHORT
        let end = varsize_1b(value) as usize;
        data = value
            .get(VARHDRSZ_SHORT..end)
            .ok_or_else(|| PgError::error("truncated short varlena"))?;
        let data_todo = data.len() as i32;
        toast_pointer.va_rawsize = data_todo + VARHDRSZ as i32; // as if not short
        toast_pointer.va_extinfo = data_todo as u32;
    } else if varatt_is_compressed(value) {
        // data_p = VARDATA(dval); data_todo = VARSIZE(dval) - VARHDRSZ
        let end = varsize_4b(value) as usize;
        data = value
            .get(VARHDRSZ..end)
            .ok_or_else(|| PgError::error("truncated compressed datum"))?;
        let data_todo = data.len() as i32;
        // rawsize in a compressed datum is just the size of the payload.
        toast_pointer.va_rawsize = vardata_compressed_get_extsize(value)? + VARHDRSZ as i32;
        // set external size and compression method
        varatt_external_set_size_and_compress_method(
            &mut toast_pointer,
            data_todo,
            vardata_compressed_get_compress_method(value)?,
        );
        // Assert that the numbers look like it's compressed.
        debug_assert!(varatt_external_is_compressed(&toast_pointer));
    } else {
        // data_p = VARDATA(dval); data_todo = VARSIZE(dval) - VARHDRSZ
        let end = varsize_4b(value) as usize;
        data = value
            .get(VARHDRSZ..end)
            .ok_or_else(|| PgError::error("truncated varlena"))?;
        let data_todo = data.len() as i32;
        toast_pointer.va_rawsize = varsize_4b(value) as i32; // VARSIZE(dval)
        toast_pointer.va_extinfo = data_todo as u32;
    }

    // Insert the correct table OID into the result TOAST pointer. During
    // table-rewriting operations rd_toastoid substitutes the permanent OID.
    let rd_toastoid = relcache_seams::rel_rd_toastoid::call(rel)?;
    if OidIsValid(rd_toastoid) {
        toast_pointer.va_toastrelid = rd_toastoid;
    } else {
        toast_pointer.va_toastrelid = toastrel.rd_id;
    }

    // Choose an OID to use as the value ID for this toast value.
    let mut data_todo = data.len() as i32;
    let valid_index_oid = guard.valid_index().rd_id;
    if !OidIsValid(rd_toastoid) {
        // normal case: just choose an unused OID.
        toast_pointer.va_valueid =
            catalog_catalog::GetNewOidWithIndex(&toastrel, valid_index_oid, 1 as AttrNumber)?;
    } else {
        // rewrite case: check to see if value was in old toast table.
        toast_pointer.va_valueid = InvalidOid;
        if let Some(oldexternal) = oldexternal {
            debug_assert!(varatt_is_external_ondisk(oldexternal));
            let old_toast_pointer = varatt_external_get_pointer(oldexternal)?;
            if old_toast_pointer.va_toastrelid == rd_toastoid {
                // This value came from the old toast table; reuse its OID.
                toast_pointer.va_valueid = old_toast_pointer.va_valueid;

                // Corner case: the reused OID may already exist in the new toast
                // table (the rewrite may copy live and recently-dead versions
                // referencing the same toast value). If so, fall through without
                // writing the data again.
                if toastrel_valueid_exists(mcx, &toastrel, toast_pointer.va_valueid)? {
                    data_todo = 0;
                }
            }
        }
        if !OidIsValid(toast_pointer.va_valueid) {
            // new value; must choose an OID that doesn't conflict in either old
            // or new toast table.
            loop {
                toast_pointer.va_valueid = catalog_catalog::GetNewOidWithIndex(
                    &toastrel,
                    valid_index_oid,
                    1 as AttrNumber,
                )?;
                if !toastid_valueid_exists(mcx, rd_toastoid, toast_pointer.va_valueid)? {
                    break;
                }
            }
        }
    }

    // Split up the item into chunks.
    let num_indexes = guard.indexes().len();
    let mut chunk_seq: i32 = 0;
    let mut offset: usize = 0;
    while data_todo > 0 {
        postgres_seams::check_for_interrupts::call()?;

        // Calculate the size of this chunk.
        let chunk_size = core::cmp::min(TOAST_MAX_CHUNK_SIZE, data_todo);

        // Build a tuple and store it.
        //   t_values[0] = ObjectIdGetDatum(va_valueid);
        //   t_values[1] = Int32GetDatum(chunk_seq++);
        //   t_values[2] = a bytea of (chunk_size + VARHDRSZ): SET_VARSIZE +
        //                 memcpy(VARDATA(&chunk_data), data_p, chunk_size).
        let mut chunk_data: PgVec<u8> =
            vec_with_capacity_in(mcx, VARHDRSZ + chunk_size as usize)?;
        // SET_VARSIZE(&chunk_data, chunk_size + VARHDRSZ): the 4-byte length word
        // ((len << 2) in native order, low 2 bits zero = 4-byte aligned form).
        let varsize = ((chunk_size as u32 + VARHDRSZ as u32) << 2).to_ne_bytes();
        for b in varsize {
            chunk_data.push(b);
        }
        for &b in &data[offset..offset + chunk_size as usize] {
            chunk_data.push(b);
        }

        let t_values = [
            Datum::from_oid(toast_pointer.va_valueid),
            Datum::from_i32(chunk_seq),
            Datum::ByRef(chunk_data),
        ];
        let t_isnull = [false, false, false];
        chunk_seq += 1;

        let mut toasttup = heap_form_tuple(mcx, toasttup_desc, &t_values, &t_isnull)?;

        heapam_seams::heap_insert::call(mcx, &toastrel, &mut toasttup, mycid, options, None)?;

        // Create the index entries. We cheat (as C does) by not using
        // FormIndexDatum: the index columns are the same as the initial columns
        // of the table for all toast indexes, and we pass a NULL IndexInfo
        // (`IndexInfoCarrier::empty()`); btree ignores it.
        for i in 0..num_indexes {
            let toastidx = &guard.indexes()[i];
            let indisready = toastidx
                .rd_index
                .as_ref()
                .map(|ix| ix.indisready)
                .unwrap_or(false);
            // Only index relations marked as ready can be updated.
            if indisready {
                let indisunique = toastidx
                    .rd_index
                    .as_ref()
                    .map(|ix| ix.indisunique)
                    .unwrap_or(false);
                let check_unique = if indisunique {
                    IndexUniqueCheck::UNIQUE_CHECK_YES
                } else {
                    IndexUniqueCheck::UNIQUE_CHECK_NO
                };
                index_insert(
                    mcx,
                    toastidx,
                    &t_values,
                    &t_isnull,
                    &toasttup.tuple.t_self,
                    &toastrel,
                    check_unique,
                    false, // indexUnchanged
                    &mut IndexInfoCarrier::empty(),
                )?;
            }
        }

        // Move on to next chunk.
        data_todo -= chunk_size;
        offset += chunk_size as usize;
    }

    // Done - close toast relation and its indexes but keep the lock until commit.
    guard.close()?;
    toastrel.close(NoLock)?;

    // Create the TOAST pointer value that we'll return.
    //   SET_VARTAG_EXTERNAL(result, VARTAG_ONDISK): a 1-byte external varlena
    //   whose first byte is the VARATT_IS_1B_E marker and second is the tag.
    let mut result: PgVec<u8> = vec_with_capacity_in(mcx, TOAST_POINTER_SIZE)?;
    result.push(0x01); // VARATT_IS_1B_E marker
    result.push(VARTAG_ONDISK);
    for b in varatt_external_bytes(&toast_pointer) {
        result.push(b);
    }
    debug_assert!(result.len() == TOAST_POINTER_SIZE);

    Ok(result)
}

// ---------------------------------------------------------------------------
// toast_delete_datum
// ---------------------------------------------------------------------------

/// `toast_delete_datum(rel, value, is_speculative)` — delete a single external
/// stored value's chunks from the relation's TOAST table.
pub fn toast_delete_datum(rel: Oid, value: &[u8], is_speculative: bool) -> PgResult<()> {
    let _ = rel; // C reads only the embedded va_toastrelid, not `rel`.
    if !varatt_is_external_ondisk(value) {
        return Ok(());
    }

    // Must copy to access aligned fields.
    let toast_pointer = varatt_external_get_pointer(value)?;

    // The scan tuples and scan-key argument are transient (the chunk rows are
    // only read for their t_self and immediately deleted), so they live in a
    // private context dropped on return — the faithful analogue of C palloc'ing
    // them in CurrentMemoryContext and letting the deletes / function exit free
    // them.
    let ctx = MemoryContext::new("toast_delete_datum");
    let mcx = ctx.mcx();

    // Open the toast relation and its indexes.
    let toastrel = table_seams::table_open::call(
        mcx,
        toast_pointer.va_toastrelid,
        RowExclusiveLock,
    )?;
    let guard = toast_open_indexes(mcx, &toastrel, RowExclusiveLock)?;

    // Setup a scan key to find chunks with matching va_valueid.
    let mut toastkey = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut toastkey[0],
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(toast_pointer.va_valueid),
    )?;

    // Find all the chunks (ordered) and delete each.
    let have_snapshot =
        snapmgr_seams::have_registered_or_active_snapshot::call();
    let snapshot = get_toast_snapshot(have_snapshot)?;
    let mut toastscan = genam_seams::systable_beginscan_ordered::call(
        &toastrel,
        guard.valid_index(),
        Some(&snapshot),
        &toastkey,
    )?;

    while let Some(toasttup) = genam_seams::systable_getnext_ordered::call(
        mcx,
        toastscan.desc_mut(),
        ForwardScanDirection,
    )? {
        // Have a chunk, delete it.
        let tid = toasttup.tuple.t_self;
        if is_speculative {
            heapam_seams::heap_abort_speculative::call(mcx, &toastrel, tid)?;
        } else {
            heapam_seams::simple_heap_delete::call(mcx, &toastrel, tid)?;
        }
    }

    // End scan and close relations but keep the lock until commit.
    toastscan.end()?;
    guard.close()?;
    toastrel.close(NoLock)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// toastrel_valueid_exists / toastid_valueid_exists
// ---------------------------------------------------------------------------

/// `toastrel_valueid_exists(toastrel, valueid)` — test whether a toast value
/// with the given ID exists in the toast relation (live or dead rows, under
/// `SnapshotAny`).
fn toastrel_valueid_exists<'mcx>(
    mcx: Mcx<'mcx>,
    toastrel: &RelationData<'_>,
    valueid: Oid,
) -> PgResult<bool> {
    // Fetch a valid index relation.
    let guard = toast_open_indexes(mcx, toastrel, RowExclusiveLock)?;

    // Setup a scan key to find chunks with matching va_valueid.
    let mut toastkey = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut toastkey[0],
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(valueid),
    )?;

    // Is there any such chunk? (SnapshotAny, index scan.)
    let snapshot_any = SnapshotData::sentinel(SnapshotType::SNAPSHOT_ANY);
    let mut toastscan = genam_seams::systable_beginscan::call(
        toastrel,
        guard.valid_index().rd_id,
        true,
        Some(&snapshot_any),
        &toastkey,
    )?;
    let result = genam_seams::systable_getnext::call(mcx, toastscan.desc_mut())?.is_some();
    toastscan.end()?;

    // Clean up.
    guard.close()?;

    Ok(result)
}

/// `toastid_valueid_exists(toastrelid, valueid)` — as above, but work from the
/// toast rel's OID not an open relation.
fn toastid_valueid_exists<'mcx>(mcx: Mcx<'mcx>, toastrelid: Oid, valueid: Oid) -> PgResult<bool> {
    let toastrel =
        table_seams::table_open::call(mcx, toastrelid, AccessShareLock)?;
    let result = toastrel_valueid_exists(mcx, &toastrel, valueid)?;
    toastrel.close(AccessShareLock)?;
    Ok(result)
}

pub use ::toast_internals_seams::ToastIndexesGuard;

/// Install this unit's owned seams. Wired into `seams-init::init_all()`.
///
/// The detoast-fetch seams (`toast_fetch_datum` / `toast_fetch_datum_slice` /
/// `indirect_pointer`) and `toast_delete_external` stay panic-stubbed pending
/// their keystones.
/// `VARATT_EXTERNAL_GET_POINTER(redirect, attr); redirect.pointer`
/// (access/common/detoast.c) — dereference a `VARATT_IS_EXTERNAL_INDIRECT`
/// datum to the in-memory varlena it points at.
///
/// In C the indirect datum's payload is a raw `struct varlena *`
/// (`varatt_indirect.pointer`) into `TopTransactionContext`; this port can't
/// embed a live address in the serialized composite image, so the producer
/// (`make_tuple_indirect`) stashes the target bytes in the per-backend
/// `INDIRECT_TARGETS` registry and embeds a stable `u64` token in the
/// payload slot instead. Here we read that token back and resolve it to the
/// registered target bytes, copied into `mcx` (C's verbatim follow of the
/// pointer). The token is stored native-endian, matching how C `memcpy`s the
/// 8-byte pointer word into the payload.
fn indirect_pointer<'mcx>(mcx: Mcx<'mcx>, attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let payload = attr
        .get(VARHDRSZ_EXTERNAL..VARHDRSZ_EXTERNAL + 8)
        .ok_or_else(|| PgError::error("truncated indirect TOAST pointer"))?;
    let token = u64::from_ne_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let bytes = ::toast_internals_seams::resolve_indirect_target(token)
        .ok_or_else(|| {
            PgError::error("indirect TOAST pointer references an unregistered target")
        })?;
    ::mcx::slice_in(mcx, &bytes)
}

pub fn init_seams() {
    use toast_internals_seams as ti;
    ti::toast_open_indexes::set(toast_open_indexes);
    ti::toast_close_indexes::set(toast_close_indexes);
    ti::toast_compress_datum::set(toast_compress_datum);
    ti::toast_save_datum::set(toast_save_datum);
    ti::toast_delete_datum::set(toast_delete_datum);
    ti::toast_fetch_datum::set(toast_fetch_datum);
    ti::indirect_pointer::set(indirect_pointer);
    ti::get_toast_snapshot::set(get_toast_snapshot);
    toastdesc_seams::toast_get_valid_index::set(toast_get_valid_index);
}

const _: LOCKMODE = NoLock;
