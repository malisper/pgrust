use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult};
use ::types_rel::{Relation, RelationData};
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use ::types_tuple::varatt::{
    set_varsize_4b_c_word, varatt_is_1b, varsize_1b, varsize_4b, VARHDRSZ, VARHDRSZ_SHORT,
};
use toastdesc::{
    compression_method_is_valid, VarattExternal, TOAST_LZ4_COMPRESSION, TOAST_PGLZ_COMPRESSION,
    TOAST_PGLZ_COMPRESSION_ID, TOAST_POINTER_SIZE,
};

use crate::{check_for_interrupts, TOAST_MAX_CHUNK_SIZE};

const VARHDRSZ_COMPRESSED: usize = VARHDRSZ + 4;
pub(crate) const F_OIDEQ: Oid = 184;
pub(crate) const F_INT4EQ: Oid = 65;
pub(crate) const F_INT4LE: Oid = 149;
pub(crate) const F_INT4GE: Oid = 150;

#[track_caller]
#[cold]
#[inline(never)]
fn no_lz4_support() -> Box<PgError> {
    Box::new(
        PgError::error("compression method lz4 not supported")
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_compression_method(cmethod: i8) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "invalid compression method {}",
        cmethod as u8 as char
    )))
}

/// C `toast_compress_datum`: `None` is C's NULL (incompressible).
/// default_toast_compression GUC is unported; its boot default (pglz) is
/// the invalid-method fallback here.
pub fn toast_compress_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: &[u8],
    cmethod: i8,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    debug_assert!(value[0] != 0x01 && (value[0] & 0x03) != 0x02);

    let (data, valsize) = if (value[0] & 0x01) == 0x01 {
        (&value[VARHDRSZ_SHORT..], value.len() - VARHDRSZ_SHORT)
    } else {
        (&value[VARHDRSZ..], value.len() - VARHDRSZ)
    };

    let cmethod = if compression_method_is_valid(cmethod as u8) {
        cmethod as u8
    } else {
        TOAST_PGLZ_COMPRESSION
    };

    let tmp = match cmethod {
        TOAST_PGLZ_COMPRESSION => pglz_compress_datum(mcx, data)?,
        TOAST_LZ4_COMPRESSION => return Err(no_lz4_support()),
        _ => return Err(invalid_compression_method(cmethod as i8)),
    };

    let Some(mut tmp) = tmp else {
        return Ok(None);
    };

    // C insists on > 2 bytes of savings (header + padding worst case).
    if tmp.len() < valsize - 2 {
        toastdesc::toast_compress_set_size_and_compress_method(
            &mut tmp,
            valsize as i32,
            TOAST_PGLZ_COMPRESSION_ID,
        )?;
        Ok(Some(tmp))
    } else {
        Ok(None)
    }
}

// toast_compression.c pglz_compress_datum: header word patched by the caller.
fn pglz_compress_datum<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let valsize = data.len() as i32;
    if valsize < pglz::PGLZ_STRATEGY_DEFAULT.min_input_size
        || valsize > pglz::PGLZ_STRATEGY_DEFAULT.max_input_size
    {
        return Ok(None);
    }

    let max_out = pglz::pglz_max_output(data.len());
    let mut tmp = ::mcx::vec_with_capacity_in(mcx, VARHDRSZ_COMPRESSED + max_out)?;
    ::mcx::vec_append_bytes(&mut tmp, &[0u8; VARHDRSZ_COMPRESSED])?;
    let Some(len) = pglz::pglz_compress_into(
        data,
        &mut tmp.spare_capacity_mut()[..max_out],
        &pglz::PGLZ_STRATEGY_DEFAULT,
    ) else {
        return Ok(None);
    };
    // SAFETY: header appended + len compressed bytes initialized.
    unsafe { tmp.set_len(VARHDRSZ_COMPRESSED + len) };
    let word = set_varsize_4b_c_word((len + VARHDRSZ_COMPRESSED) as u32).to_ne_bytes();
    tmp[..VARHDRSZ].copy_from_slice(&word);
    Ok(Some(tmp))
}

/// C `toast_save_datum`: chunk `value` into the toast relation and return
/// the 18-byte on-disk toast pointer image. `rd_toastoid` is C's transient
/// NewHeap->rd_toastoid, threaded as a parameter (the rewrite owns it for
/// exactly the copy; no relcache field to reset).
pub fn toast_save_datum<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'_>,
    value: &[u8],
    oldexternal: Option<&[u8]>,
    rd_toastoid: Oid,
    options: i32,
) -> PgResult<[u8; TOAST_POINTER_SIZE]> {
    debug_assert!(value[0] != 0x01);

    let mycid = xact_seams::get_current_command_id::call(true)?;

    let toastrel = table::table_open(mcx, rel.rd_rel.reltoastrelid, RowExclusiveLock)?;
    let toasttup_desc = toastrel.rd_att.clone();

    let (toastidxs, valid_index) = toast_open_indexes(mcx, &toastrel, RowExclusiveLock)?;

    let mut toast_pointer = VarattExternal::default();
    let p = value.as_ptr();
    // SAFETY: `value` is the full varlena image (va_slice contract upstream).
    let (data, data_todo) = if unsafe { varatt_is_1b(p) } {
        let todo = unsafe { varsize_1b(p) } - VARHDRSZ_SHORT;
        toast_pointer.va_rawsize = (todo + VARHDRSZ) as i32; // as if not short
        toast_pointer.va_extinfo = todo as u32;
        (&value[VARHDRSZ_SHORT..], todo)
    } else if (value[0] & 0x03) == 0x02 {
        let todo = unsafe { varsize_4b(p) } - VARHDRSZ;
        toast_pointer.va_rawsize =
            (toastdesc::toast_compress_extsize(value)? as usize + VARHDRSZ) as i32;
        toast_pointer.set_size_and_compress_method(
            todo as u32,
            toastdesc::toast_compress_method(value)?,
        );
        debug_assert!(toast_pointer.is_compressed());
        (&value[VARHDRSZ..], todo)
    } else {
        let todo = unsafe { varsize_4b(p) } - VARHDRSZ;
        toast_pointer.va_rawsize = (todo + VARHDRSZ) as i32;
        toast_pointer.va_extinfo = todo as u32;
        (&value[VARHDRSZ..], todo)
    };

    toast_pointer.va_toastrelid =
        if rd_toastoid != InvalidOid { rd_toastoid } else { toastrel.rd_id };

    let mut data_todo = data_todo;
    if rd_toastoid == InvalidOid {
        toast_pointer.va_valueid = catalog_seams::get_new_oid_with_index::call(
            mcx,
            &toastrel,
            toastidxs[valid_index].rd_id,
            1,
        )?;
    } else {
        // Rewrite case: reuse the value OID of a prior external value from the
        // old toast table; a live + recently-dead pair sharing one value gets
        // stored once (data_todo = 0 short-circuits the chunk loop).
        toast_pointer.va_valueid = InvalidOid;
        if let Some(oldexternal) = oldexternal {
            debug_assert!(toastdesc::varatt_is_external_ondisk(oldexternal));
            let old_toast_pointer = VarattExternal::from_image(oldexternal)?;
            if old_toast_pointer.va_toastrelid == rd_toastoid {
                toast_pointer.va_valueid = old_toast_pointer.va_valueid;
                if toastrel_valueid_exists(mcx, &toastrel, toast_pointer.va_valueid)? {
                    data_todo = 0;
                }
            }
        }
        if toast_pointer.va_valueid == InvalidOid {
            // New value: the OID must not conflict in either toast table.
            loop {
                toast_pointer.va_valueid = catalog_seams::get_new_oid_with_index::call(
                    mcx,
                    &toastrel,
                    toastidxs[valid_index].rd_id,
                    1,
                )?;
                if !toastid_valueid_exists(mcx, rd_toastoid, toast_pointer.va_valueid)? {
                    break;
                }
            }
        }
    }

    let mut chunk_image: PgVec<'mcx, u8> =
        ::mcx::vec_with_capacity_in(mcx, TOAST_MAX_CHUNK_SIZE + VARHDRSZ)?;
    let mut t_values = [
        Datum::from_oid(toast_pointer.va_valueid),
        Datum::from_i32(0),
        Datum::null(),
    ];
    let t_isnull = [false, false, false];

    let mut chunk_seq: i32 = 0;
    let mut offset = 0usize;
    while offset < data_todo {
        check_for_interrupts()?;

        let chunk_size = TOAST_MAX_CHUNK_SIZE.min(data_todo - offset);

        t_values[1] = Datum::from_i32(chunk_seq);
        chunk_seq += 1;
        chunk_image.clear();
        ::mcx::vec_append_bytes(
            &mut chunk_image,
            &::types_tuple::varatt::set_varsize_4b_word((chunk_size + VARHDRSZ) as u32)
                .to_ne_bytes(),
        )?;
        ::mcx::vec_append_bytes(&mut chunk_image, &data[offset..offset + chunk_size])?;
        t_values[2] = Datum::from_usize(chunk_image.as_ptr() as usize);

        let mut toasttup = heaptuple::heap_form_tuple(mcx, &toasttup_desc, &t_values, &t_isnull)?;

        heapam::dml::heap_insert(&toastrel, toasttup.as_tuple_mut(), mycid, options, None)?;

        for idx in toastidxs.iter() {
            let index = idx.rd_index.as_ref().expect("toast index without rd_index");
            if index.indisready {
                let mut am_cache: Option<Box<dyn core::any::Any>> = None;
                // Loud today: nbtree's insert lane is phase 2 (btinsert panics).
                indexam::index_insert(
                    mcx,
                    idx,
                    &t_values,
                    &t_isnull,
                    &toasttup.as_tuple().t_self,
                    &toastrel,
                    if index.indisunique {
                        ::types_nbtree::IndexUniqueCheck::UNIQUE_CHECK_YES
                    } else {
                        ::types_nbtree::IndexUniqueCheck::UNIQUE_CHECK_NO
                    },
                    false,
                    &mut am_cache,
                )?;
            }
        }

        offset += chunk_size;
    }

    toast_close_indexes(toastidxs, NoLock)?;
    table::table_close(toastrel, NoLock)?;

    Ok(toast_pointer.to_ondisk_image())
}

/// C `toast_delete_datum`: delete all chunks of one external stored value.
pub fn toast_delete_datum<'mcx>(
    mcx: Mcx<'mcx>,
    _rel: &RelationData<'_>,
    value: &[u8],
    is_speculative: bool,
) -> PgResult<()> {
    if !toastdesc::varatt_is_external_ondisk(value) {
        return Ok(());
    }
    let toast_pointer = VarattExternal::from_image(value)?;

    let toastrel = table::table_open(mcx, toast_pointer.va_toastrelid, RowExclusiveLock)?;
    let (toastidxs, valid_index) = toast_open_indexes(mcx, &toastrel, RowExclusiveLock)?;

    let toastkey = [valueid_scan_key(toast_pointer.va_valueid)];

    let snapshot = std::rc::Rc::new(toastdesc::get_toast_snapshot(
        mcx,
        snapmgr::HaveRegisteredOrActiveSnapshot(),
    )?);
    let mut toastscan = genam::systable_beginscan_ordered(
        mcx,
        &toastrel,
        &toastidxs[valid_index],
        Some(snapshot),
        &toastkey,
    )?;
    loop {
        let Some(toasttup) =
            genam::systable_getnext_ordered(mcx, &mut toastscan, ForwardScanDirection)?
        else {
            break;
        };
        let tid = toasttup.t_self;
        if is_speculative {
            heapam::heap_abort_speculative(&toastrel, &tid)?;
        } else {
            heapam::dml::simple_heap_delete(&toastrel, &tid)?;
        }
    }
    genam::systable_endscan_ordered(mcx, toastscan)?;

    toast_close_indexes(toastidxs, NoLock)?;
    table::table_close(toastrel, NoLock)?;
    Ok(())
}

// C `toastrel_valueid_exists`: any chunk with this value OID, under SnapshotAny.
fn toastrel_valueid_exists<'mcx>(
    mcx: Mcx<'mcx>,
    toastrel: &Relation<'mcx>,
    valueid: Oid,
) -> PgResult<bool> {
    let (toastidxs, valid_index) = toast_open_indexes(mcx, toastrel, RowExclusiveLock)?;
    let toastkey = [valueid_scan_key(valueid)];
    let snapshot_any = std::rc::Rc::new(::types_snapshot::SnapshotData::sentinel(
        mcx,
        ::types_snapshot::SnapshotType::SNAPSHOT_ANY,
    ));
    let mut toastscan = genam::systable_beginscan(
        mcx,
        toastrel,
        toastidxs[valid_index].rd_id,
        true,
        Some(snapshot_any),
        &toastkey,
    )?;
    let result = genam::systable_getnext(mcx, &mut toastscan)?.is_some();
    genam::systable_endscan(mcx, toastscan)?;
    toast_close_indexes(toastidxs, RowExclusiveLock)?;
    Ok(result)
}

// C `toastid_valueid_exists`.
fn toastid_valueid_exists<'mcx>(mcx: Mcx<'mcx>, toastrelid: Oid, valueid: Oid) -> PgResult<bool> {
    let toastrel = table::table_open(mcx, toastrelid, AccessShareLock)?;
    let result = toastrel_valueid_exists(mcx, &toastrel, valueid)?;
    table::table_close(toastrel, AccessShareLock)?;
    Ok(result)
}

// ScanKeyInit + fmgr_info on a known builtin, resolved direct (the nbtree
// OrderProcFrame precedent); fmgroids.h OIDs asserted in adt tables.
pub(crate) fn valueid_scan_key(valueid: Oid) -> ScanKeyData {
    let mut k = ScanKeyData::empty();
    k.sk_attno = 1;
    k.sk_strategy = BTEqualStrategyNumber;
    k.sk_collation = ::types_core::C_COLLATION_OID;
    k.sk_func = ::types_fmgr::FmgrInfo::new(adt_scalar::builtins::fc_oideq, F_OIDEQ, 2, true, false);
    k.sk_argument = Datum::from_oid(valueid);
    k
}

/// C `toast_get_valid_index`.
pub fn toast_get_valid_index<'mcx>(mcx: Mcx<'mcx>, toastoid: Oid, lock: LOCKMODE) -> PgResult<Oid> {
    let toastrel = table::table_open(mcx, toastoid, lock)?;
    let (toastidxs, valid_index) = toast_open_indexes(mcx, &toastrel, lock)?;
    let valid_index_oid = toastidxs[valid_index].rd_id;
    toast_close_indexes(toastidxs, NoLock)?;
    table::table_close(toastrel, NoLock)?;
    Ok(valid_index_oid)
}

/// C `toast_open_indexes`: all indexes on the toast relation plus the
/// position of the (single) valid one.
pub fn toast_open_indexes<'mcx>(
    mcx: Mcx<'mcx>,
    toastrel: &RelationData<'_>,
    lock: LOCKMODE,
) -> PgResult<(PgVec<'mcx, Relation<'mcx>>, usize)> {
    let indexlist = relcache_seams::relation_get_index_list::call(mcx, toastrel.rd_id)?;
    debug_assert!(!indexlist.is_empty());

    let mut toastidxs: PgVec<'mcx, Relation<'mcx>> = PgVec::new_in(mcx);
    toastidxs
        .try_reserve_exact(indexlist.len())
        .map_err(|_| Box::new(mcx.oom(indexlist.len() * core::mem::size_of::<Relation<'_>>())))?;
    for oid in indexlist.iter() {
        toastidxs.push(indexam::index_open(mcx, *oid, lock)?);
    }

    for (i, idx) in toastidxs.iter().enumerate() {
        if idx.rd_index.as_ref().is_some_and(|f| f.indisvalid) {
            return Ok((toastidxs, i));
        }
    }
    Err(no_valid_index(toastrel.rd_id))
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_valid_index(relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "no valid index found for toast relation with Oid {relid}"
    )))
}

/// C `toast_close_indexes`.
pub fn toast_close_indexes(
    toastidxs: PgVec<'_, Relation<'_>>,
    lock: LOCKMODE,
) -> PgResult<()> {
    for idx in toastidxs {
        indexam::index_close(idx, lock)?;
    }
    Ok(())
}

pub(crate) fn reltoastrelid_valid(rel: &RelationData<'_>) -> bool {
    rel.rd_rel.reltoastrelid != InvalidOid
}
