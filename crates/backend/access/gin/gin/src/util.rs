//! ginutil.c: GinState init, page/buffer initialization, entry compare and
//! extraction, metapage stats.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_core::{Buffer, ForkNumber, InvalidBlockNumber, InvalidOid, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::PageMut;
use ::types_storage::ReadBufferMode;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::{
    meta_of, opclass, page_bytes_mut, page_mut, page_ref, relation_needs_wal, unported,
    write_meta_to, write_opaque_to, RM_GIN,
};

/// initGinState. Closed opclass set per column: jsonb_ops / jsonb_path_ops /
/// tsvector_ops / array_ops; anything else panics loudly.
pub fn initGinState(rel: &Relation<'_>) -> PgResult<GinState> {
    let natts = rel.rd_att.natts;
    if natts < 1 || natts as usize > GIN_MAX_KEY_COLS {
        unported(&format!("GIN index with {natts} key columns (initGinState)"));
    }
    let mut cols = [GinColState {
        opclass: GinOpclass::JsonbOps,
        elem_cmp: GinElemCmp::None,
        support_collation: InvalidOid,
        can_partial_match: false,
        key_byval: false,
        key_len: 0,
    }; GIN_MAX_KEY_COLS];
    for i in 0..natts as usize {
        cols[i] = init_gin_col(rel, i)?;
    }
    Ok(GinState {
        natts: natts as u16,
        one_col: natts == 1,
        cols,
    })
}

fn init_gin_col(rel: &Relation<'_>, i: usize) -> PgResult<GinColState> {
    let opcintype = rel.rd_opcintype[i];
    let opfamily = rel.rd_opfamily[i];

    let extract =
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, GIN_EXTRACTVALUE_PROC as i16)?;
    let opclass = match extract {
        opclass::F_GIN_EXTRACT_JSONB => GinOpclass::JsonbOps,
        opclass::F_GIN_EXTRACT_JSONB_PATH => GinOpclass::JsonbPathOps,
        opclass::F_GIN_EXTRACT_TSVECTOR => GinOpclass::TsvectorOps,
        // intarray's gin__int_ops also registers ginarrayextract as proc 2;
        // the extractQuery proc tells the two opclasses apart.
        opclass::F_GINARRAYEXTRACT => {
            let extract_query = lsyscache::get_opfamily_proc(
                opfamily,
                opcintype,
                opcintype,
                GIN_EXTRACTQUERY_PROC as i16,
            )?;
            if extract_query == opclass::F_GINQUERYARRAYEXTRACT {
                GinOpclass::ArrayOps
            } else {
                let cx = ::mcx::MemoryContext::new("gin ext opclass probe");
                let name = lsyscache::get_func_name(cx.mcx(), extract_query)?
                    .map(|n| n.as_str().to_string());
                match name.as_deref() {
                    Some("ginint4_queryextract") => GinOpclass::IntArrayOps,
                    _ => {
                        return Err(crate::unsupported(format!(
                            "GIN operator class with extractQuery support function {extract_query} is not supported"
                        )))
                    }
                }
            }
        }
        // Extension opclasses carry dynamic oids; match by proname.
        other => {
            let cx = ::mcx::MemoryContext::new("gin ext opclass probe");
            let name = lsyscache::get_func_name(cx.mcx(), other)?
                .map(|n| n.as_str().to_string());
            let btree_ty = name
                .as_deref()
                .and_then(|n| n.strip_prefix("gin_extract_value_"))
                .and_then(GinBtreeType::from_type_name);
            match (name.as_deref(), btree_ty) {
                (Some("gin_extract_value_trgm"), _) => GinOpclass::TrgmOps,
                (Some("gin_extract_hstore"), _) => GinOpclass::HstoreOps,
                (_, Some(ty)) => GinOpclass::BtreeOps(ty),
                // unported: opclasses beyond the closed set (user-reachable
                // via CREATE INDEX ... USING gin with a custom opclass).
                _ => {
                    return Err(crate::unsupported(format!(
                        "GIN operator class with extractValue support function {other} is not supported"
                    )))
                }
            }
        }
    };
    // array_ops has no GIN_COMPARE_PROC; C falls back to the index key
    // type's default btree comparator via typcache. The index tupdesc attr
    // is the element type (opckeytype anyelement, ConstructTupleDescriptor).
    let elem_cmp = if opclass == GinOpclass::ArrayOps {
        match rel.rd_att.attr(i).atttypid {
            ::types_core::INT2OID => GinElemCmp::Int2,
            ::types_core::INT4OID => GinElemCmp::Int4,
            ::types_core::INT8OID => GinElemCmp::Int8,
            ::types_core::OIDOID => GinElemCmp::Oid,
            ::types_core::TEXTOID | ::types_core::VARCHAROID => GinElemCmp::Text,
            // unported: typcache btree-comparator fallback (user-reachable
            // via CREATE INDEX USING gin on e.g. a numeric[] column).
            other => {
                return Err(crate::unsupported(format!(
                    "GIN array_ops over element type {other} is not supported (typcache comparator lane unported)"
                )))
            }
        }
    } else {
        GinElemCmp::None
    };
    debug_assert!(
        matches!(opclass, GinOpclass::BtreeOps(_))
            || lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, GIN_COMPARE_PROC as i16)?
                == match opclass {
                    GinOpclass::JsonbOps => opclass::F_GIN_COMPARE_JSONB,
                    GinOpclass::JsonbPathOps => opclass::F_BTINT4CMP,
                    GinOpclass::TsvectorOps => opclass::F_GIN_CMP_TSLEXEME,
                    GinOpclass::TrgmOps => opclass::F_BTINT4CMP,
                    GinOpclass::HstoreOps => opclass::F_BTTEXTCMP,
                    GinOpclass::IntArrayOps => opclass::F_BTINT4CMP,
                    GinOpclass::ArrayOps | GinOpclass::BtreeOps(_) => InvalidOid,
                }
    );
    let partial = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        GIN_COMPARE_PARTIAL_PROC as i16,
    )?;
    let can_partial_match = match partial {
        InvalidOid => false,
        opclass::F_GIN_CMP_PREFIX if opclass == GinOpclass::TsvectorOps => true,
        // btree_gin's gin_compare_prefix_<type> (extension oids; proname).
        other if matches!(opclass, GinOpclass::BtreeOps(_)) => {
            let cx = ::mcx::MemoryContext::new("gin ext opclass probe");
            let is_prefix = lsyscache::get_func_name(cx.mcx(), other)?
                .is_some_and(|n| n.as_str().starts_with("gin_compare_prefix_"));
            if !is_prefix {
                return Err(crate::unsupported(format!(
                    "GIN operator class with comparePartial support function {other} is not supported"
                )));
            }
            true
        }
        // unported: comparePartial support beyond tsvector's prefix compare
        // (user-reachable via a custom opclass registering proc 5).
        other => {
            return Err(crate::unsupported(format!(
                "GIN operator class with comparePartial support function {other} is not supported"
            )))
        }
    };

    let attr = rel.rd_att.compact_attr(i);
    Ok(GinColState {
        opclass,
        elem_cmp,
        support_collation: if rel.rd_indcollation[i] != InvalidOid {
            rel.rd_indcollation[i]
        } else {
            ::types_core::catalog::DEFAULT_COLLATION_OID
        },
        can_partial_match,
        key_byval: attr.attbyval,
        key_len: attr.attlen,
    })
}

// GinGetUseFastUpdate / GinGetPendingListCleanupSize
pub(crate) fn gin_use_fastupdate(rel: &Relation<'_>) -> bool {
    match rel.rd_options.as_ref().and_then(|o| o.gin()) {
        Some(o) => o.use_fast_update,
        None => GIN_DEFAULT_USE_FASTUPDATE,
    }
}

pub(crate) fn gin_pending_list_cleanup_size(rel: &Relation<'_>) -> i64 {
    match rel.rd_options.as_ref().and_then(|o| o.gin()) {
        Some(o) if o.pending_list_cleanup_size != -1 => o.pending_list_cleanup_size as i64,
        _ => guc_tables::vars::gin_pending_list_limit.read() as i64,
    }
}

/// GinPageIsRecyclable (ginvacuum.c).
pub(crate) fn gin_page_is_recyclable(buf: Buffer) -> PgResult<bool> {
    // SAFETY: caller holds at least a share lock (GinNewBuffer's conditional
    // lock, or ginvacuumcleanup's share lock).
    let page = unsafe { page_ref(buf) };
    if page.is_new() {
        return Ok(true);
    }
    let opaque = crate::page_opaque(&page);
    if crate::GinPageIsDeleted(&opaque) {
        // GinPageGetDeleteXid == pd_prune_xid; pending-list deletions leave
        // it invalid (always recyclable). A valid xid is a posting-tree page
        // deletion: recyclable once no scan can still see it. C passes
        // rel=NULL to GlobalVisCheckRemovableXid — the shared-rels horizon
        // (handle 1), the most conservative choice.
        let delete_xid = page.prune_xid();
        if delete_xid == 0 {
            return Ok(true);
        }
        return procarray_seams::global_vis_test_is_removable_xid::call(
            ::types_core::GlobalVisStateHandle::new(1),
            delete_xid,
        );
    }
    Ok(false)
}

/// GinNewBuffer: recycle via FSM or extend; returned pinned + exclusive.
pub fn GinNewBuffer(rel: &Relation<'_>) -> PgResult<Buffer> {
    loop {
        let blkno = freespace_seams::get_page_with_free_space::call(rel, (BLCKSZ / 2) as usize)?;
        if blkno == InvalidBlockNumber {
            break;
        }
        freespace_seams::record_page_with_free_space::call(rel, blkno, 0)?;

        let buffer = bm::read_buffer::call(rel, blkno)?;
        if bm::conditional_lock_buffer::call(buffer)? {
            if gin_page_is_recyclable(buffer)? {
                return Ok(buffer);
            }
            bm::lock_buffer::call(buffer, crate::GIN_UNLOCK)?;
        }
        bm::release_buffer::call(buffer)?;
    }

    let (buffer, extended_by) = bm::extend_buffered_rel_by::call(
        rel,
        ForkNumber::MAIN_FORKNUM,
        None,
        bm::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert!(extended_by == 1);
    Ok(buffer)
}

/// GinInitPage over a raw BLCKSZ image.
pub(crate) fn gin_init_page_bytes(bytes: &mut [u8], flags: u16) {
    // PageInit(page, BLCKSZ, sizeof(GinPageOpaqueData)).
    // SAFETY: BLCKSZ image with exclusive access.
    let mut page = unsafe { PageMut::from_raw(core::ptr::NonNull::new(bytes.as_mut_ptr()).unwrap()) };
    page.init(core::mem::size_of::<GinPageOpaqueData>());
    write_opaque_to(
        bytes,
        &GinPageOpaqueData {
            rightlink: InvalidBlockNumber,
            maxoff: 0,
            flags,
        },
    );
}

/// GinInitBuffer.
pub fn GinInitBuffer(buf: Buffer, flags: u16) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    // SAFETY: borrow confined to this call.
    gin_init_page_bytes(unsafe { page_bytes_mut(&mut page) }, flags);
}

/// GinInitMetabuffer.
pub fn GinInitMetabuffer(buf: Buffer) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    // SAFETY: borrow confined to this call.
    let bytes = unsafe { page_bytes_mut(&mut page) };
    gin_init_metapage_bytes(bytes);
}

pub(crate) fn gin_init_metapage_bytes(bytes: &mut [u8]) {
    gin_init_page_bytes(bytes, GIN_META);
    write_meta_to(
        bytes,
        &GinMetaPageData {
            head: InvalidBlockNumber,
            tail: InvalidBlockNumber,
            tailFreeSize: 0,
            nPendingPages: 0,
            nPendingHeapTuples: 0,
            nTotalPages: 0,
            nEntryPages: 0,
            nDataPages: 0,
            nEntries: 0,
            ginVersion: GIN_CURRENT_VERSION,
        },
    );
    set_meta_pd_lower(bytes);
}

/// pd_lower just past the metadata: required so xlog page compression keeps it.
pub(crate) fn set_meta_pd_lower(bytes: &mut [u8]) {
    let lower = (crate::META_OFF + core::mem::size_of::<GinMetaPageData>()) as u16;
    bytes[12..14].copy_from_slice(&lower.to_ne_bytes());
}

/// ginCompareEntries.
pub fn ginCompareEntries(
    state: &GinState,
    attnum: OffsetNumber,
    a: Datum,
    category_a: GinNullCategory,
    b: Datum,
    category_b: GinNullCategory,
) -> i32 {
    if category_a != category_b {
        return if category_a < category_b { -1 } else { 1 };
    }
    if category_a != GIN_CAT_NORM_KEY {
        return 0;
    }
    opclass::compare(state.col(attnum), a, b)
}

/// ginCompareAttEntries: attribute number dominates.
pub fn ginCompareAttEntries(
    state: &GinState,
    attnum_a: OffsetNumber,
    a: Datum,
    category_a: GinNullCategory,
    attnum_b: OffsetNumber,
    b: Datum,
    category_b: GinNullCategory,
) -> i32 {
    if attnum_a != attnum_b {
        return if attnum_a < attnum_b { -1 } else { 1 };
    }
    ginCompareEntries(state, attnum_a, a, category_a, b, category_b)
}

/// ginExtractEntries: keys sorted + de-duplicated, with null categories.
/// Null keys (array_ops elements) sort after normal keys (cmpEntries) and
/// dedup into one GIN_CAT_NULL_KEY entry.
pub fn ginExtractEntries<'mcx>(
    mcx: Mcx<'mcx>,
    state: &GinState,
    attnum: OffsetNumber,
    value: Datum,
    is_null: bool,
) -> PgResult<(PgVec<'mcx, Datum>, PgVec<'mcx, GinNullCategory>)> {
    let mut categories: PgVec<'mcx, GinNullCategory>;
    if is_null {
        let mut entries = mcx::vec_with_capacity_in(mcx, 1)?;
        entries.push(Datum::null());
        categories = mcx::vec_with_capacity_in(mcx, 1)?;
        categories.push(GIN_CAT_NULL_ITEM);
        return Ok((entries, categories));
    }

    let (mut entries, null_flags) = opclass::extract_value(mcx, state.col(attnum), value)?;

    if entries.is_empty() {
        entries.try_reserve(1).map_err(|_| crate::oom(8))?;
        entries.push(Datum::null());
        categories = mcx::vec_with_capacity_in(mcx, 1)?;
        categories.push(GIN_CAT_EMPTY_ITEM);
        return Ok((entries, categories));
    }

    categories = mcx::vec_with_capacity_in(mcx, entries.len())?;
    if null_flags.is_empty() {
        for _ in 0..entries.len() {
            categories.push(GIN_CAT_NORM_KEY);
        }
    } else {
        debug_assert!(null_flags.len() == entries.len());
        for &f in null_flags.iter() {
            categories.push(if f { GIN_CAT_NULL_KEY } else { GIN_CAT_NORM_KEY });
        }
    }

    if entries.len() > 1 {
        // cmpEntries + qsort_arg + dedup over (datum, category) pairs.
        let mut keydata: PgVec<'mcx, (Datum, GinNullCategory)> =
            mcx::vec_with_capacity_in(mcx, entries.len())?;
        for i in 0..entries.len() {
            keydata.push((entries[i], categories[i]));
        }
        let mut have_dups = false;
        keydata.sort_by(|a, b| {
            let r = ginCompareEntries(state, attnum, a.0, a.1, b.0, b.1);
            if r == 0 {
                have_dups = true;
            }
            r.cmp(&0)
        });
        if have_dups {
            let mut j = 0usize;
            for i in 1..keydata.len() {
                if ginCompareEntries(state, attnum, keydata[j].0, keydata[j].1, keydata[i].0, keydata[i].1)
                    != 0
                {
                    j += 1;
                    keydata[j] = keydata[i];
                }
            }
            keydata.truncate(j + 1);
        }
        entries.truncate(keydata.len());
        categories.truncate(keydata.len());
        for (i, (d, c)) in keydata.iter().enumerate() {
            entries[i] = *d;
            categories[i] = *c;
        }
    }

    Ok((entries, categories))
}

/// ginGetStats.
pub fn ginGetStats(rel: &Relation<'_>) -> PgResult<GinStatsData> {
    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    bm::lock_buffer::call(metabuffer, crate::GIN_SHARE)?;
    // SAFETY: pin + share lock held.
    let metadata = meta_of(crate::page_bytes(&unsafe { page_ref(metabuffer) }));
    bm::lock_buffer::call(metabuffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;
    Ok(GinStatsData {
        nPendingPages: metadata.nPendingPages,
        nTotalPages: metadata.nTotalPages,
        nEntryPages: metadata.nEntryPages,
        nDataPages: metadata.nDataPages,
        nEntries: metadata.nEntries,
        ginVersion: metadata.ginVersion,
    })
}

/// ginUpdateStats.
pub fn ginUpdateStats(rel: &Relation<'_>, stats: &GinStatsData, is_build: bool) -> PgResult<()> {
    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    bm::lock_buffer::call(metabuffer, crate::GIN_EXCLUSIVE)?;

    let metadata = {
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(metabuffer) };
        // SAFETY: borrow confined to this block.
        let bytes = unsafe { page_bytes_mut(&mut page) };
        let mut metadata = meta_of(bytes);
        metadata.nTotalPages = stats.nTotalPages;
        metadata.nEntryPages = stats.nEntryPages;
        metadata.nDataPages = stats.nDataPages;
        metadata.nEntries = stats.nEntries;
        write_meta_to(bytes, &metadata);
        set_meta_pd_lower(bytes);
        metadata
    };
    bm::mark_buffer_dirty::call(metabuffer)?;

    if relation_needs_wal(rel) && !is_build {
        let data = crate::wal::ginxlog_update_meta(
            rel,
            &metadata,
            InvalidBlockNumber,
            InvalidBlockNumber,
            0,
        );
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_UPDATE_META_PAGE,
            0,
            &[&data],
            &[XLogRegBuf {
                block_id: 0,
                buffer: metabuffer,
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(metabuffer) }.set_lsn(recptr);
    }

    bm::lock_buffer::call(metabuffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;
    Ok(())
}
