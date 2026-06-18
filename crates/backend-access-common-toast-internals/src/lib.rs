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
//! The `toast_save_datum` writer, `toast_delete_datum`, and the detoast-fetch
//! statics (`toast_fetch_datum` / `toast_fetch_datum_slice` / `indirect_pointer`)
//! are NOT installed here — see the lane notes (they need the
//! `table_relation_fetch_toast_slice` table-AM dispatch seam and a snapshot-bit
//! re-sign of the detoast seams, and `toast_delete_external` is the
//! `toast_helper.c` unit).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_rel::{Relation, RelationData};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::lock::{LOCKMODE, NoLock};

use backend_access_common_toast_compression::{
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
    backend_utils_misc_guc_tables::vars::default_toast_compression.read() as i8
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
        backend_optimizer_util_plancat_ext_seams::relation_get_index_list_oids::call(toastrel.rd_id)?;
    debug_assert!(!indexlist.is_empty());

    let num_indexes = indexlist.len();
    let mut toastidxs: PgVec<Relation<'mcx>> = vec_with_capacity_in(mcx, num_indexes)?;

    // index_open every index in the list.
    for &indexoid in indexlist.iter() {
        let idx = backend_access_index_indexam_seams::index_open::call(mcx, indexoid, lock)?;
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
/// opened by [`toast_open_indexes`]. In the owned model each `index_close` is
/// the `Relation` handle's `Drop`; this consumes (drops) the vector.
fn toast_close_indexes(toastidxs: PgVec<Relation<'_>>, _lock: LOCKMODE) -> PgResult<()> {
    drop(toastidxs);
    Ok(())
}

/// `toast_get_valid_index(toastoid, lock)` — the OID of the valid index of a
/// TOAST relation.
pub fn toast_get_valid_index<'mcx>(
    mcx: Mcx<'mcx>,
    toastoid: Oid,
    lock: LOCKMODE,
) -> PgResult<Oid> {
    let toastrel = backend_access_table_table_seams::table_open::call(mcx, toastoid, lock)?;
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

pub use backend_access_common_toast_internals_seams::ToastIndexesGuard;

/// Install this unit's owned seams. Wired into `seams-init::init_all()`.
///
/// Only the catalog-/index-side seams are installed here. The detoast-fetch
/// seams (`toast_fetch_datum` / `toast_fetch_datum_slice` / `indirect_pointer`)
/// and `toast_delete_external` stay panic-stubbed pending their keystones.
pub fn init_seams() {
    use backend_access_common_toast_internals_seams as ti;
    ti::toast_open_indexes::set(toast_open_indexes);
    ti::toast_close_indexes::set(toast_close_indexes);
    ti::toast_compress_datum::set(toast_compress_datum);
    ti::get_toast_snapshot::set(get_toast_snapshot);
    backend_access_common_toastdesc_seams::toast_get_valid_index::set(toast_get_valid_index);
}

const _: LOCKMODE = NoLock;
