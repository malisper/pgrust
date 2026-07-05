//! ginutil.c: GinState init, page/buffer initialization, entry compare and
//! extraction, metapage stats.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_core::{Buffer, ForkNumber, InvalidBlockNumber, InvalidOid, BLCKSZ};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::PageMut;
use ::types_storage::ReadBufferMode;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::{
    meta_of, opclass, page_bytes_mut, page_mut, page_ref, relation_needs_wal, unported,
    write_meta_to, write_opaque_to, RM_GIN,
};

/// initGinState. Closed set: single key column, jsonb_ops / jsonb_path_ops /
/// tsvector_ops. Anything else panics loudly (multicol / array_ops / unknown).
pub fn initGinState(rel: &Relation<'_>) -> PgResult<GinState> {
    let natts = rel.rd_att.natts;
    if natts != 1 {
        unported("multicolumn GIN index (initGinState)");
    }
    let opcintype = rel.rd_opcintype[0];
    let opfamily = rel.rd_opfamily[0];

    let extract =
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, GIN_EXTRACTVALUE_PROC as i16)?;
    let opclass = match extract {
        opclass::F_GIN_EXTRACT_JSONB => GinOpclass::JsonbOps,
        opclass::F_GIN_EXTRACT_JSONB_PATH => GinOpclass::JsonbPathOps,
        opclass::F_GIN_EXTRACT_TSVECTOR => GinOpclass::TsvectorOps,
        2743 => unported("array_ops GIN opclass (arrays lane)"),
        other => unported(&format!("GIN opclass with extractValue proc {other}")),
    };
    debug_assert!(
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, GIN_COMPARE_PROC as i16)?
            == match opclass {
                GinOpclass::JsonbOps => opclass::F_GIN_COMPARE_JSONB,
                GinOpclass::JsonbPathOps => opclass::F_BTINT4CMP,
                GinOpclass::TsvectorOps => opclass::F_GIN_CMP_TSLEXEME,
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
        other => unported(&format!("GIN comparePartialFn {other}")),
    };

    let attr = rel.rd_att.compact_attr(0);
    Ok(GinState {
        opclass,
        support_collation: if rel.rd_indcollation[0] != InvalidOid {
            rel.rd_indcollation[0]
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

/// GinPageIsRecyclable (ginvacuum.c): only vacuum produces deleted pages.
pub(crate) fn gin_page_is_recyclable(buf: Buffer) -> bool {
    // SAFETY: caller holds the conditional lock taken in GinNewBuffer.
    let page = unsafe { page_ref(buf) };
    if page.is_new() {
        return true;
    }
    let opaque = crate::page_opaque(&page);
    if crate::GinPageIsDeleted(&opaque) {
        // GinPageGetDeleteXid == pd_prune_xid; pending-list deletions leave
        // it invalid (always recyclable). A valid xid is the posting-tree
        // page-deletion lane (GlobalVisCheckRemovableXid; vacuum).
        let delete_xid = page.prune_xid();
        if delete_xid == 0 {
            return true;
        }
        unported("recycling a deleted posting-tree page (vacuum lane)");
    }
    false
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
            if gin_page_is_recyclable(buffer) {
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
pub(crate) fn ginCompareEntries(
    state: &GinState,
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
    opclass::compare(state, a, b)
}

/// ginExtractEntries: keys sorted + de-duplicated, with null categories. The
/// closed opclass set produces no null keys, so nullFlags handling collapses
/// to the placeholder arms.
pub fn ginExtractEntries<'mcx>(
    mcx: Mcx<'mcx>,
    state: &GinState,
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

    let mut entries = opclass::extract_value(mcx, state, value)?;

    if entries.is_empty() {
        entries.try_reserve(1).map_err(|_| crate::oom(8))?;
        entries.push(Datum::null());
        categories = mcx::vec_with_capacity_in(mcx, 1)?;
        categories.push(GIN_CAT_EMPTY_ITEM);
        return Ok((entries, categories));
    }

    if entries.len() > 1 {
        // cmpEntries + qsort_arg + dedup. Keys are non-null here.
        let mut have_dups = false;
        entries.sort_by(|a, b| {
            let r = opclass::compare(state, *a, *b);
            if r == 0 {
                have_dups = true;
            }
            r.cmp(&0)
        });
        if have_dups {
            let mut j = 0usize;
            for i in 1..entries.len() {
                if opclass::compare(state, entries[j], entries[i]) != 0 {
                    j += 1;
                    entries[j] = entries[i];
                }
            }
            entries.truncate(j + 1);
        }
    }

    categories = mcx::vec_with_capacity_in(mcx, entries.len())?;
    for _ in 0..entries.len() {
        categories.push(GIN_CAT_NORM_KEY);
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
