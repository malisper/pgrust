//! spgutils.c: state/cache init, page management, tuple builders/deformers,
//! SpGistPageAddNewItem.

use ::bufmgr_seams::{self as bufmgr};
use ::datum::Datum;
use ::mcx::Mcx;
use ::nbtree::itup::ItupBuf;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, OffsetNumber, Oid, BLCKSZ,
};
use ::types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_rel::Relation;
use ::types_spgist::*;
pub use ::types_spgist::{spgFormDeadTuple, SpGistInitPage};
use ::types_spgist::state::SpGistState;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::TupleDescData;

pub(crate) const FirstOffsetNumber: OffsetNumber = 1;
pub(crate) const InvalidOffsetNumber: OffsetNumber = 0;
pub(crate) const InvalidBuffer: Buffer = 0;
const InvalidOid: Oid = 0;

#[inline]
pub fn buf_page_mut(buffer: Buffer) -> PageMut<'static> {
    // SAFETY: caller holds the content lock required for its access mode.
    unsafe { PageMut::from_raw(bufmgr::buffer_get_page::call(buffer)) }
}

pub fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == ::types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get()
                    == ::types_core::InvalidSubTransactionId))
}

pub fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_UNLOCK)?;
    bufmgr::release_buffer::call(buffer)?;
    Ok(())
}

/// Item bytes of `offnum` on the page, immutable.
#[inline]
pub fn item_slice<'a>(page: &PageRef<'a>, offnum: OffsetNumber) -> &'a [u8] {
    let id = page.item_id(offnum);
    let (p, len) = page.item_raw(id);
    // SAFETY: item_raw bounds the item within the page (lock held by caller).
    unsafe { core::slice::from_raw_parts(p, len as usize) }
}

/// Item bytes of `offnum`, mutable (exclusive lock held by caller).
#[inline]
pub fn item_slice_mut<'a>(pm: &'a mut PageMut<'_>, offnum: OffsetNumber) -> &'a mut [u8] {
    let r = pm.as_ref();
    let id = r.item_id(offnum);
    let (p, len) = r.item_raw(id);
    let off = p as usize - r.as_ptr() as usize;
    // SAFETY: same in-page span as item_raw; exclusive content lock held.
    unsafe { core::slice::from_raw_parts_mut(pm.as_mut_ptr().add(off), len as usize) }
}

/// C: elog(ERROR, "unexpected SPGiST tuple state: %d", tupstate) — the
/// corrupted-page checks of spgdoinsert.c/spgscan.c/spgvacuum.c are catchable
/// XX000 errors (a plpgsql EXCEPTION block catches them), never a panic.
#[cold]
#[inline(never)]
#[track_caller]
pub fn tuple_state_error(tupstate: u8) -> Box<PgError> {
    Box::new(PgError::error(format!("unexpected SPGiST tuple state: {tupstate}")))
}

/// C: elog(ERROR, "failed to add item of size %u to SPGiST index page", size)
/// (spgdoinsert.c:170 et al., spgutils.c:1285) — catchable XX000, not a panic.
#[cold]
#[inline(never)]
#[track_caller]
pub fn add_item_failed(size: usize) -> Box<PgError> {
    Box::new(PgError::error(format!("failed to add item of size {size} to SPGiST index page")))
}

#[cold]
#[inline(never)]
pub fn corrupt_leaf_chain(index: &Relation<'_>, blkno: BlockNumber) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "inconsistent tuple chain links in page {blkno} of index \"{}\"",
            index.name()
        ))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

/// Bounds a walk over a leaf-tuple `nextOffset` chain against crafted pages.
///
/// SP-GiST leaf tuples form singly linked chains via a 14-bit `nextOffset`
/// field stored on-disk, which an attacker can craft (backup restore, hostile
/// replica, direct file write with a recomputable checksum) to contain a cycle
/// (e.g. two live tuples pointing at each other) or an out-of-range link. The
/// insert (checkSplitConditions/moveLeafs/doPickSplit) and scan (spgWalk) chain
/// walks otherwise follow these links unconditionally while holding a buffer
/// content lock, so a cycle spins the backend forever without ever servicing an
/// interrupt — an uninterruptible DoS that also grows per-step arena vectors
/// toward OOM. A well-formed chain visits each of a page's `max` live offsets at
/// most once, so more than `max` steps — or any link outside [First, max] —
/// proves corruption. Mirrors the unique-predecessor/bounds validation the
/// vacuum path (`vacuum_leaf_page`) already performs.
pub struct LeafChainGuard {
    max: OffsetNumber,
    steps: u32,
}

impl LeafChainGuard {
    #[inline]
    pub fn new(max: OffsetNumber) -> Self {
        LeafChainGuard { max, steps: 0 }
    }

    /// Validate the offset `off` about to be dereferenced as the next chain link.
    /// Returns Err(ERRCODE_DATA_CORRUPTED) if the step count exceeds the page's
    /// max offset (a cycle) or the offset lies outside [First, max].
    #[inline]
    pub fn visit(
        &mut self,
        off: OffsetNumber,
        index: &Relation<'_>,
        blkno: BlockNumber,
    ) -> PgResult<()> {
        if self.advance(off) {
            Ok(())
        } else {
            Err(corrupt_leaf_chain(index, blkno))
        }
    }

    /// Pure step check: counts the visit and returns false on overrun (cycle) or
    /// an out-of-range link. Split out from `visit` so the bound is unit-testable
    /// without a live `Relation`.
    #[inline]
    fn advance(&mut self, off: OffsetNumber) -> bool {
        self.steps += 1;
        self.steps <= self.max as u32 && off >= FirstOffsetNumber && off <= self.max
    }
}

#[cfg(test)]
mod leaf_chain_guard_tests {
    use super::*;

    // A well-formed chain of `max` distinct in-range offsets walks to completion.
    #[test]
    fn valid_chain_completes() {
        let max: OffsetNumber = 5;
        let mut guard = LeafChainGuard::new(max);
        for off in FirstOffsetNumber..=max {
            assert!(guard.advance(off), "valid offset {off} rejected");
        }
    }

    // A 2-tuple cycle (A -> B -> A -> ...) is bounded rather than looping forever.
    #[test]
    fn two_cycle_is_bounded() {
        let max: OffsetNumber = 4;
        let mut guard = LeafChainGuard::new(max);
        let cycle = [1u16, 2];
        let mut caught = false;
        for step in 0..1000 {
            if !guard.advance(cycle[step % 2]) {
                caught = true;
                break;
            }
        }
        assert!(caught, "cyclic chain was not bounded");
    }

    // An out-of-range link is rejected immediately.
    #[test]
    fn out_of_range_link_rejected() {
        let mut guard = LeafChainGuard::new(3);
        assert!(!guard.advance(0)); // below FirstOffsetNumber
        let mut guard = LeafChainGuard::new(3);
        assert!(!guard.advance(4)); // above max
    }
}

// ---------------------------------------------------------------------------
// Metapage codec
// ---------------------------------------------------------------------------

const META_OFFSET: usize = MAXALIGN(SizeOfPageHeaderData);
const SIZEOF_META: usize = 4 + SPGIST_CACHED_PAGES * 8;

pub(crate) fn read_meta(page: &PageRef<'_>) -> SpGistMetaPageData {
    // SAFETY: metapage content area holds SpGistMetaPageData (init contract).
    let b = unsafe {
        core::slice::from_raw_parts(page.as_ptr().add(META_OFFSET), SIZEOF_META)
    };
    let mut m = SpGistMetaPageData {
        magicNumber: u32::from_ne_bytes([b[0], b[1], b[2], b[3]]),
        ..Default::default()
    };
    let mut off = 4;
    for i in 0..SPGIST_CACHED_PAGES {
        m.lastUsedPages.cachedPage[i] = SpGistLastUsedPage {
            blkno: BlockNumber::from_ne_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]),
            freeSpace: i32::from_ne_bytes([b[off + 4], b[off + 5], b[off + 6], b[off + 7]]),
        };
        off += 8;
    }
    m
}

pub(crate) fn write_meta(pm: &mut PageMut<'_>, m: &SpGistMetaPageData) {
    {
        // SAFETY: in-bounds metapage content area, exclusive lock held.
        let b = unsafe {
            core::slice::from_raw_parts_mut(pm.as_mut_ptr().add(META_OFFSET), SIZEOF_META)
        };
        b[0..4].copy_from_slice(&m.magicNumber.to_ne_bytes());
        let mut off = 4;
        for i in 0..SPGIST_CACHED_PAGES {
            let s = &m.lastUsedPages.cachedPage[i];
            b[off..off + 4].copy_from_slice(&s.blkno.to_ne_bytes());
            b[off + 4..off + 8].copy_from_slice(&s.freeSpace.to_ne_bytes());
            off += 8;
        }
    }
    // pd_lower past the metadata so xlog page compression keeps it.
    pm.set_pd_lower((META_OFFSET + SIZEOF_META) as u16);
}

// ---------------------------------------------------------------------------
// Cache / state init
// ---------------------------------------------------------------------------

fn fillTypeDesc(type_oid: Oid) -> PgResult<SpGistTypeDesc> {
    let shape = syscache_shape(type_oid)?;
    Ok(SpGistTypeDesc {
        type_: type_oid,
        attlen: shape.0,
        attbyval: shape.1,
        attalign: shape.2,
        attstorage: shape.3,
    })
}

fn syscache_shape(type_oid: Oid) -> PgResult<(i16, bool, i8, i8)> {
    let (typlen, typbyval, typalign) = lsyscache::typ::get_typlenbyvalalign(type_oid)?;
    let typstorage = lsyscache::typ::get_typstorage(type_oid)?;
    Ok((typlen, typbyval, typalign, typstorage))
}

pub fn index_getprocid(index: &Relation<'_>, attno_0based: usize, procnum: u16) -> Oid {
    let base = attno_0based * SPGISTNProc;
    index
        .rd_support
        .get(base + (procnum as usize - 1))
        .copied()
        .unwrap_or(InvalidOid)
}

// GetIndexInputType (spgutils.c); single-key AM.
fn get_index_input_type(index: &Relation<'_>) -> PgResult<Oid> {
    let opcintype = index.rd_opcintype[spgKeyColumn];
    const ANYOID_LOW: Oid = 2276; // "any"
    let polymorphic = matches!(
        opcintype,
        2277 | 2283 | 2776 | 3500 | 3831 | 5077 | 5078 | 5079 | 5080 | 4537 | 4538
    ) || opcintype == ANYOID_LOW;
    if !polymorphic {
        return Ok(opcintype);
    }
    let ind = index.rd_index.as_ref().expect("spgist index without rd_index");
    let heapcol = ind.indkey.first().copied().unwrap_or(0);
    if heapcol != 0 {
        return lsyscache::typ::getBaseType(lsyscache::attribute::get_atttype(
            ind.indrelid,
            heapcol,
        )?);
    }
    indexam_seams::index_expression_input_type::call(index, spgKeyColumn)
}

/// spgGetCache. Reads/installs the rd_amcache_spgist slot on the relcache
/// entry (rule-5 cache); callers get a snapshot and write mutations back.
pub fn spgGetCache(index: &Relation<'_>) -> PgResult<SpGistCache> {
    if let Some(cache) = index.rd_amcache_spgist.borrow().as_deref() {
        return Ok(*cache);
    }

    let mut cache = SpGistCache::default();

    debug_assert_eq!(index.indnkeyatts(), 1);

    let atttype = get_index_input_type(index)?;

    let config_oid = index_getprocid(index, spgKeyColumn, SPGIST_CONFIG_PROC);
    if config_oid == InvalidOid {
        // C: index_getprocinfo's elog (indexam.c) — user-reachable via a
        // defective opclass, so ereport rather than panic.
        return Err(Box::new(PgError::error(format!(
            "missing support function {SPGIST_CONFIG_PROC} for attribute 1 of index \"{}\"",
            index.name()
        ))));
    }
    let mut config_fn = fmgr_seams::fmgr_info::call(config_oid)?;
    let cfgin = spgConfigIn { attType: atttype };
    {
        // C: FunctionCall2Coll(procinfo, index->rd_indcollation[spgKeyColumn], ...)
        let mut frame = ::types_fmgr::LocalFcinfo::<2>::new(
            index.rd_indcollation.first().copied().unwrap_or(InvalidOid),
        );
        frame.set_arg(0, Datum::from_usize(&cfgin as *const spgConfigIn as usize));
        frame.set_arg(
            1,
            Datum::from_usize(&mut cache.config as *mut spgConfigOut as usize),
        );
        config_fn.invoke(&mut frame)?;
    }

    if cache.config.leafType == InvalidOid {
        cache.config.leafType = index.rd_att.attr(spgKeyColumn).atttypid;
        // A column type binary-coercible to atttype (e.g. a domain over it)
        // is treated as plain atttype so no compress method is required.
        if cache.config.leafType != atttype
            && coerce::IsBinaryCoercible(cache.config.leafType, atttype)?
        {
            cache.config.leafType = atttype;
        }
    }

    cache.attType = fillTypeDesc(atttype)?;

    if cache.config.leafType != atttype {
        if index_getprocid(index, spgKeyColumn, SPGIST_COMPRESS_PROC) == InvalidOid {
            return Err(Box::new(
                PgError::error(
                    "compress method must be defined when leaf type is different from input type"
                        .to_string(),
                )
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        cache.attLeafType = fillTypeDesc(cache.config.leafType)?;
    } else {
        cache.attLeafType = cache.attType;
    }

    cache.attPrefixType = fillTypeDesc(cache.config.prefixType)?;
    cache.attLabelType = fillTypeDesc(cache.config.labelType)?;

    if index.rd_rel.relkind != ::types_rel::RELKIND_PARTITIONED_INDEX {
        let metabuffer = bufmgr::read_buffer::call(index, SPGIST_METAPAGE_BLKNO)?;
        bufmgr::lock_buffer::call(metabuffer, bufmgr::BUFFER_LOCK_SHARE)?;
        let metadata = read_meta(&buf_page_mut(metabuffer).as_ref());
        if metadata.magicNumber != SPGIST_MAGIC_NUMBER {
            unlock_release(metabuffer)?;
            return Err(not_spgist_index(index));
        }
        cache.lastUsedPages = metadata.lastUsedPages;
        unlock_release(metabuffer)?;
    }

    *index.rd_amcache_spgist.borrow_mut() = Some(Box::new(cache));
    Ok(cache)
}

#[inline]
pub(crate) fn set_cache(index: &Relation<'_>, cache: SpGistCache) {
    *index.rd_amcache_spgist.borrow_mut() = Some(Box::new(cache));
}

/// getSpGistTupleDesc; the copy arm serves compress opclasses (leaf type !=
/// column type, e.g. poly_ops storing bounding boxes). The copy allocates in
/// `mcx`, which must outlive the consuming state/scan.
pub fn getSpGistTupleDesc<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    keyType: &SpGistTypeDesc,
) -> PgResult<std::rc::Rc<TupleDescData<'mcx>>> {
    if keyType.type_ == index.rd_att.attr(spgKeyColumn).atttypid {
        Ok(index.rd_att.clone())
    } else {
        let mut desc = ::tupdesc::CreateTupleDescCopy(mcx, &index.rd_att)?;
        let att = desc.attr_mut(spgKeyColumn);
        att.atttypid = keyType.type_;
        att.atttypmod = -1;
        att.attlen = keyType.attlen;
        att.attbyval = keyType.attbyval;
        att.attalign = keyType.attalign;
        att.attstorage = keyType.attstorage;
        att.attcompression = 0;
        att.attcollation = InvalidOid;
        desc.populate_compact_attribute(spgKeyColumn);
        Ok(std::rc::Rc::new(desc))
    }
}

/// initSpGistState; support procs resolved once onto the carrier.
pub fn initSpGistState<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
) -> PgResult<SpGistState<'mcx>> {
    let cache = spgGetCache(index)?;
    let leaf_tup_desc = getSpGistTupleDesc(mcx, index, &cache.attLeafType)?;
    let redirect_xid = xact::GetTopTransactionIdIfAny();

    let resolve = |procnum: u16| -> PgResult<::types_fmgr::FmgrInfo> {
        let oid = index_getprocid(index, spgKeyColumn, procnum);
        if oid == InvalidOid {
            // C: index_getprocinfo's elog (indexam.c).
            return Err(Box::new(PgError::error(format!(
                "missing support function {procnum} for attribute 1 of index \"{}\"",
                index.name()
            ))));
        }
        fmgr_seams::fmgr_info::call(oid)
    };
    let compress_oid = index_getprocid(index, spgKeyColumn, SPGIST_COMPRESS_PROC);
    let compress = if compress_oid != InvalidOid {
        fmgr_seams::fmgr_info::call(compress_oid)?
    } else {
        ::types_fmgr::FmgrInfo::unresolved()
    };

    Ok(SpGistState {
        config: cache.config,
        attType: cache.attType,
        attLeafType: cache.attLeafType,
        attPrefixType: cache.attPrefixType,
        attLabelType: cache.attLabelType,
        leafTupDesc: leaf_tup_desc,
        redirectXid: redirect_xid,
        isBuild: false,
        indexCollation: index.rd_indcollation.first().copied().unwrap_or(InvalidOid),
        chooseFn: resolve(SPGIST_CHOOSE_PROC)?,
        picksplitFn: resolve(SPGIST_PICKSPLIT_PROC)?,
        compressFn: compress,
        frame1: ::types_fmgr::LocalFcinfo::<1>::new(0),
        frame2: ::types_fmgr::LocalFcinfo::<2>::new(0),
    })
}

// ---------------------------------------------------------------------------
// Buffer / page management
// ---------------------------------------------------------------------------

/// SpGistNewBuffer: pinned + exclusive-locked; caller initializes the page.
pub fn SpGistNewBuffer(index: &Relation<'_>) -> PgResult<Buffer> {
    loop {
        let blkno = freespace::GetFreeIndexPage(index)?;
        if blkno == InvalidBlockNumber {
            break;
        }
        if SpGistBlockIsFixed(blkno) {
            continue;
        }
        let buffer = bufmgr::read_buffer::call(index, blkno)?;
        if bufmgr::conditional_lock_buffer::call(buffer)? {
            let pm = buf_page_mut(buffer);
            let page = pm.as_ref();
            if page.is_new() || SpGistPageIsDeleted(&page) || page_is_empty(&page) {
                return Ok(buffer);
            }
            bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_UNLOCK)?;
        }
        bufmgr::release_buffer::call(buffer)?;
    }

    let (buf, _extended_by) = bufmgr::extend_buffered_rel_by::call(
        index,
        ForkNumber::MAIN_FORKNUM,
        None,
        bufmgr::EB_LOCK_FIRST,
        1,
    )?;
    Ok(buf)
}

#[inline]
fn page_is_empty(page: &PageRef<'_>) -> bool {
    page.pd_lower() as usize <= SizeOfPageHeaderData
}

/// SpGistUpdateMetaPage: push lastUsedPages back if the conditional lock wins.
pub fn SpGistUpdateMetaPage(index: &Relation<'_>) -> PgResult<()> {
    let Some(cache) = index.rd_amcache_spgist.borrow().as_deref().copied() else {
        return Ok(());
    };
    let metabuffer = bufmgr::read_buffer::call(index, SPGIST_METAPAGE_BLKNO)?;
    if bufmgr::conditional_lock_buffer::call(metabuffer)? {
        {
            let mut pm = buf_page_mut(metabuffer);
            let mut metadata = read_meta(&pm.as_ref());
            metadata.lastUsedPages = cache.lastUsedPages;
            write_meta(&mut pm, &metadata);
        }
        bufmgr::mark_buffer_dirty::call(metabuffer)?;
        unlock_release(metabuffer)?;
    } else {
        bufmgr::release_buffer::call(metabuffer)?;
    }
    Ok(())
}

#[inline]
fn get_lup_index(flags: i32) -> usize {
    (flags as u32 as usize) % SPGIST_CACHED_PAGES
}

fn allocNewBuffer(
    index: &Relation<'_>,
    flags: i32,
    cache: &mut SpGistCache,
) -> PgResult<Buffer> {
    let mut pageflags: u16 = 0;
    if GBUF_REQ_LEAF(flags) {
        pageflags |= SPGIST_LEAF;
    }
    if GBUF_REQ_NULLS(flags) {
        pageflags |= SPGIST_NULLS;
    }

    loop {
        let buffer = SpGistNewBuffer(index)?;
        SpGistInitBuffer(buffer, pageflags);

        if pageflags & SPGIST_LEAF != 0 {
            return Ok(buffer);
        }

        let blkno = bufmgr::buffer_get_block_number::call(buffer);
        let mut blk_flags = GBUF_INNER_PARITY(blkno);
        if (flags & GBUF_PARITY_MASK) == blk_flags {
            return Ok(buffer);
        }

        if pageflags & SPGIST_NULLS != 0 {
            blk_flags |= GBUF_NULLS;
        }
        let free = buf_page_mut(buffer).as_ref().exact_free_space() as i32;
        cache.lastUsedPages.cachedPage[blk_flags as usize] = SpGistLastUsedPage {
            blkno,
            freeSpace: free,
        };
        unlock_release(buffer)?;
    }
}

#[inline]
pub(crate) fn SpGistGetTargetPageFreeSpace(index: &Relation<'_>) -> usize {
    BLCKSZ * (100 - index.get_fillfactor(SPGIST_DEFAULT_FILLFACTOR) as usize) / 100
}

/// SpGistGetBuffer; rd_amcache mutations are written back through set_cache.
pub fn SpGistGetBuffer(
    index: &Relation<'_>,
    flags: i32,
    mut need_space: i32,
    is_new: &mut bool,
) -> PgResult<Buffer> {
    let mut cache = spgGetCache(index)?;

    // C (spgutils.c:576): int comparison, elog(ERROR) — catchable XX000.
    if need_space > SPGIST_PAGE_CAPACITY as i32 {
        return Err(Box::new(PgError::error("desired SPGiST tuple size is too big")));
    }

    need_space += SpGistGetTargetPageFreeSpace(index) as i32;
    need_space = need_space.min(SPGIST_PAGE_CAPACITY as i32);

    let lup_idx = get_lup_index(flags);

    if cache.lastUsedPages.cachedPage[lup_idx].blkno == InvalidBlockNumber {
        *is_new = true;
        let buffer = allocNewBuffer(index, flags, &mut cache)?;
        set_cache(index, cache);
        return Ok(buffer);
    }

    debug_assert!(!SpGistBlockIsFixed(cache.lastUsedPages.cachedPage[lup_idx].blkno));

    if cache.lastUsedPages.cachedPage[lup_idx].freeSpace >= need_space {
        let blkno = cache.lastUsedPages.cachedPage[lup_idx].blkno;
        let buffer = bufmgr::read_buffer::call(index, blkno)?;

        if !bufmgr::conditional_lock_buffer::call(buffer)? {
            bufmgr::release_buffer::call(buffer)?;
            *is_new = true;
            let nb = allocNewBuffer(index, flags, &mut cache)?;
            set_cache(index, cache);
            return Ok(nb);
        }

        let pm = buf_page_mut(buffer);
        let page = pm.as_ref();

        if page.is_new() || SpGistPageIsDeleted(&page) || page_is_empty(&page) {
            let mut pageflags: u16 = 0;
            if GBUF_REQ_LEAF(flags) {
                pageflags |= SPGIST_LEAF;
            }
            if GBUF_REQ_NULLS(flags) {
                pageflags |= SPGIST_NULLS;
            }
            SpGistInitBuffer(buffer, pageflags);
            let free = buf_page_mut(buffer).as_ref().exact_free_space() as i32;
            cache.lastUsedPages.cachedPage[lup_idx].freeSpace = free - need_space;
            *is_new = true;
            set_cache(index, cache);
            return Ok(buffer);
        }

        let type_ok = if GBUF_REQ_LEAF(flags) {
            SpGistPageIsLeaf(&page)
        } else {
            !SpGistPageIsLeaf(&page)
        };
        let nulls_ok = if GBUF_REQ_NULLS(flags) {
            SpGistPageStoresNulls(&page)
        } else {
            !SpGistPageStoresNulls(&page)
        };
        if type_ok && nulls_ok {
            let free_space = page.exact_free_space() as i32;
            if free_space >= need_space {
                cache.lastUsedPages.cachedPage[lup_idx].freeSpace = free_space - need_space;
                *is_new = false;
                set_cache(index, cache);
                return Ok(buffer);
            }
        }

        unlock_release(buffer)?;
    }

    *is_new = true;
    let buffer = allocNewBuffer(index, flags, &mut cache)?;
    set_cache(index, cache);
    Ok(buffer)
}

/// SpGistSetLastUsedPage.
pub fn SpGistSetLastUsedPage(index: &Relation<'_>, buffer: Buffer) -> PgResult<()> {
    let mut cache = spgGetCache(index)?;
    let blkno = bufmgr::buffer_get_block_number::call(buffer);
    if SpGistBlockIsFixed(blkno) {
        return Ok(());
    }

    let pm = buf_page_mut(buffer);
    let page = pm.as_ref();
    let mut flags = if SpGistPageIsLeaf(&page) {
        GBUF_LEAF
    } else {
        GBUF_INNER_PARITY(blkno)
    };
    if SpGistPageStoresNulls(&page) {
        flags |= GBUF_NULLS;
    }
    let free_space = page.exact_free_space() as i32;

    let lup = &mut cache.lastUsedPages.cachedPage[get_lup_index(flags)];
    if lup.blkno == InvalidBlockNumber || lup.blkno == blkno || lup.freeSpace < free_space {
        lup.blkno = blkno;
        lup.freeSpace = free_space;
        set_cache(index, cache);
    }
    Ok(())
}

/// SpGistInitBuffer.
pub fn SpGistInitBuffer(b: Buffer, f: u16) {
    let mut pm = buf_page_mut(b);
    SpGistInitPage(&mut pm, f);
}

/// SpGistInitMetapage.
pub fn SpGistInitMetapage(pm: &mut PageMut<'_>) {
    SpGistInitPage(pm, SPGIST_META);
    let metadata = SpGistMetaPageData {
        magicNumber: SPGIST_MAGIC_NUMBER,
        ..Default::default()
    };
    write_meta(pm, &metadata);
}

// ---------------------------------------------------------------------------
// Inner-datum helpers + tuple builders
// ---------------------------------------------------------------------------

#[cold]
#[inline(never)]
fn corrupt_inline_datum() -> Box<PgError> {
    Box::new(
        PgError::error(
            "SP-GiST index tuple contains a datum whose declared length exceeds the tuple",
        )
        .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

/// Validate that a by-reference datum stored inline at `off` within the bounded
/// tuple image `tup` declares a length that lies entirely within the image.
///
/// SP-GiST stores key/prefix/label datums inline in index tuples, so a varlena
/// datum's length word (or a fixed-length by-ref extent) lives on a page an
/// attacker can craft (backup restore, hostile replica, direct file write with
/// a recomputable checksum). `leaf_datum`/`inner_prefix_datum`/
/// `node_label_datum` hand a raw in-tuple pointer to the opclass, which then
/// materializes the value using that on-page length; without this check a
/// declared length of up to ~1 GB would drive a read far past the 8 KB page
/// (information disclosure of neighbouring buffers, or SIGSEGV). We surface the
/// mismatch as a catchable data-corruption error instead of an OOB read.
fn validate_inline_datum(tup: &[u8], off: usize, att: &SpGistTypeDesc) -> PgResult<()> {
    use ::types_tuple::varatt;
    if att.attbyval {
        // by-value datums are read by fixed width, never dereferenced.
        return Ok(());
    }
    if off > tup.len() {
        return Err(corrupt_inline_datum());
    }
    let avail = tup.len() - off;
    let need = match att.attlen {
        -1 => {
            // varlena: decode the on-page header, guarding every header read
            // against the tuple bound before touching it.
            if avail < 1 {
                return Err(corrupt_inline_datum());
            }
            let p = tup[off..].as_ptr();
            // SAFETY: `p` is in-bounds of `tup`; each branch below only reads
            // header bytes it has first confirmed are available.
            unsafe {
                if varatt::varatt_is_1b_e(p) {
                    // External/expanded TOAST pointer: a small, fixed on-page
                    // extent. Validate the tag ourselves so a crafted tag byte
                    // surfaces as a corruption error rather than a panic in
                    // vartag_size.
                    if avail < varatt::VARHDRSZ_EXTERNAL {
                        return Err(corrupt_inline_datum());
                    }
                    match varatt::vartag_external(p) {
                        varatt::VARTAG_INDIRECT
                        | varatt::VARTAG_EXPANDED_RO
                        | varatt::VARTAG_EXPANDED_RW
                        | varatt::VARTAG_ONDISK => varatt::varsize_external(p),
                        _ => return Err(corrupt_inline_datum()),
                    }
                } else if varatt::varatt_is_1b(p) {
                    varatt::varsize_1b(p)
                } else {
                    if avail < varatt::VARHDRSZ {
                        return Err(corrupt_inline_datum());
                    }
                    varatt::varsize_4b(p)
                }
            }
        }
        -2 => {
            // cstring: needs a NUL terminator within the image.
            match tup[off..].iter().position(|&b| b == 0) {
                Some(n) => n + 1,
                None => return Err(corrupt_inline_datum()),
            }
        }
        n if n > 0 => n as usize,
        _ => return Err(corrupt_inline_datum()),
    };
    if need > avail {
        return Err(corrupt_inline_datum());
    }
    Ok(())
}

/// fetch_att over a leaf datum image (SGLTDATUM).
#[inline]
pub(crate) fn fetch_att(p: *const u8, attbyval: bool, attlen: i16) -> Datum {
    if attbyval {
        // SAFETY: caller points p at a live in-tuple value of `attlen` bytes.
        unsafe {
            match attlen {
                1 => Datum::from_i8(p.cast::<i8>().read()),
                2 => Datum::from_i16(p.cast::<i16>().read_unaligned()),
                4 => Datum::from_i32(p.cast::<i32>().read_unaligned()),
                8 => Datum::from_i64(p.cast::<i64>().read_unaligned()),
                other => panic!("unsupported byval length: {other}"),
            }
        }
    } else {
        Datum::from_usize(p as usize)
    }
}

/// SpGistGetInnerTypeSize.
pub fn SpGistGetInnerTypeSize(att: &SpGistTypeDesc, datum: Datum) -> usize {
    let size = if att.attbyval {
        SIZEOF_DATUM
    } else if att.attlen > 0 {
        att.attlen as usize
    } else {
        // SAFETY: by-ref varlena datum carries a live pointer (caller contract).
        unsafe {
            ::types_tuple::varatt::varsize_any(datum.as_usize() as *const u8)
        }
    };
    MAXALIGN(size)
}

/// memcpyInnerDatum.
pub(crate) fn memcpyInnerDatum(target: &mut [u8], att: &SpGistTypeDesc, datum: Datum) {
    if att.attbyval {
        target[..SIZEOF_DATUM].copy_from_slice(&datum.as_u64().to_ne_bytes());
    } else {
        let size = if att.attlen > 0 {
            att.attlen as usize
        } else {
            // SAFETY: by-ref varlena datum (att.attbyval == false, attlen == -1).
            unsafe { ::types_tuple::varatt::varsize_any(datum.as_usize() as *const u8) }
        };
        // SAFETY: source live for `size` bytes per the datum's shape.
        unsafe {
            core::ptr::copy_nonoverlapping(
                datum.as_usize() as *const u8,
                target.as_mut_ptr(),
                size,
            );
        }
    }
}

/// SpGistGetLeafTupleSize.
pub fn SpGistGetLeafTupleSize(
    tuple_descriptor: &TupleDescData<'_>,
    datums: &[Datum],
    isnulls: &[bool],
) -> usize {
    let natts = tuple_descriptor.natts as usize;
    let needs_null_mask = natts > 1 && isnulls[..natts].contains(&true);
    let data_size = ::heaptuple::heap_compute_data_size(tuple_descriptor, datums, isnulls);
    let mut size = SGLTHDRSZ(needs_null_mask) + data_size;
    size = MAXALIGN(size);
    size.max(SGDTSIZE)
}

/// spgFormLeafTuple: owned 8-aligned on-disk image.
pub fn spgFormLeafTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'_>,
    heap_ptr: &ItemPointerData,
    datums: &[Datum],
    isnulls: &[bool],
) -> PgResult<ItupBuf<'mcx>> {
    let tuple_descriptor = &state.leafTupDesc;
    let natts = tuple_descriptor.natts as usize;
    let needs_null_mask = natts > 1 && isnulls[..natts].contains(&true);

    let data_size = ::heaptuple::heap_compute_data_size(tuple_descriptor, datums, isnulls);
    let hoff = SGLTHDRSZ(needs_null_mask);
    let size = MAXALIGN(hoff + data_size).max(SGDTSIZE);

    let mut tup = ItupBuf::with_size(mcx, size)?;
    // SAFETY: fresh zeroed image of `size` bytes.
    let img = unsafe { core::slice::from_raw_parts_mut(tup.as_mut_ptr(), size) };

    let mut header = SpGistLeafTupleHeader {
        tupstate: SPGIST_LIVE,
        size: size as u32,
        t_info: 0,
        heapPtr: *heap_ptr,
    };
    header.set_nextOffset(InvalidOffsetNumber);
    header.set_hasNullMask(needs_null_mask);

    if needs_null_mask {
        let mut infomask = 0u16;
        // SAFETY: data area hoff..hoff+data_size zeroed; bitmap area at
        // offset 12 zeroed; datums live per caller.
        unsafe {
            ::heaptuple::heap_fill_tuple(
                tuple_descriptor,
                datums,
                isnulls,
                img.as_mut_ptr().add(hoff),
                data_size,
                &mut infomask,
                Some(img.as_mut_ptr().add(SIZEOF_SPGIST_LEAF_TUPLE_DATA)),
            );
        }
    } else if natts > 1 || !isnulls[spgKeyColumn] {
        let mut infomask = 0u16;
        // SAFETY: as above, no bitmap.
        unsafe {
            ::heaptuple::heap_fill_tuple(
                tuple_descriptor,
                datums,
                isnulls,
                img.as_mut_ptr().add(hoff),
                data_size,
                &mut infomask,
                None,
            );
        }
    }

    header.encode(img);
    Ok(tup)
}

/// spgFormNodeTuple.
pub fn spgFormNodeTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'_>,
    label: Datum,
    isnull: bool,
) -> PgResult<ItupBuf<'mcx>> {
    let mut size = SGNTHDRSZ;
    if !isnull {
        size += SpGistGetInnerTypeSize(&state.attLabelType, label);
    }

    if (size as u16 & INDEX_SIZE_MASK) as usize != size {
        return Err(index_row_too_big(size, INDEX_SIZE_MASK as usize));
    }

    let mut tup = ItupBuf::with_size(mcx, size)?;
    // SAFETY: fresh zeroed image of `size` bytes.
    let img = unsafe { core::slice::from_raw_parts_mut(tup.as_mut_ptr(), size) };

    let mut infomask: u16 = 0;
    if isnull {
        infomask |= INDEX_NULL_MASK;
    }
    infomask |= size as u16;

    node_tuple_set_tid(img, &ItemPointerData::invalid());
    img[6..8].copy_from_slice(&infomask.to_ne_bytes());

    if !isnull {
        memcpyInnerDatum(&mut img[SGNTHDRSZ..], &state.attLabelType, label);
    }
    Ok(tup)
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_row_too_big(size: usize, max: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index row requires {size} bytes, maximum size is {max}"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_spgist_index(index: &Relation<'_>) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "index \"{}\" is not an SP-GiST index",
        index.name()
    )))
}

#[cold]
#[inline(never)]
pub(crate) fn inner_tuple_too_big(size: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "SP-GiST inner tuple size {size} exceeds maximum {}",
            SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_hint("Values larger than a buffer page cannot be indexed."),
    )
}

/// spgFormInnerTuple; `nodes` are owned node-tuple images.
pub fn spgFormInnerTuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'_>,
    has_prefix: bool,
    prefix: Datum,
    nodes: &[&[u8]],
) -> PgResult<ItupBuf<'mcx>> {
    let prefix_size = if has_prefix {
        SpGistGetInnerTypeSize(&state.attPrefixType, prefix)
    } else {
        0
    };

    let mut size = SGITHDRSZ + prefix_size;
    for node in nodes {
        size += node_tuple_size(node);
    }
    size = size.max(SGDTSIZE);

    if size > SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA {
        return Err(inner_tuple_too_big(size));
    }
    if size > SGITMAXSIZE as usize
        || prefix_size > SGITMAXPREFIXSIZE as usize
        || nodes.len() > SGITMAXNNODES as usize
    {
        panic!("SPGiST inner tuple header field is too small");
    }

    let mut tup = ItupBuf::with_size(mcx, MAXALIGN(size))?;
    // SAFETY: fresh zeroed image (size <= allocated MAXALIGN(size)).
    let img = unsafe { core::slice::from_raw_parts_mut(tup.as_mut_ptr(), MAXALIGN(size)) };

    SpGistInnerTupleHeader {
        tupstate: SPGIST_LIVE,
        allTheSame: false,
        nNodes: nodes.len() as u16,
        prefixSize: prefix_size as u16,
        size: size as u16,
    }
    .encode(img);

    if has_prefix {
        memcpyInnerDatum(&mut img[SGITHDRSZ..], &state.attPrefixType, prefix);
    }

    let mut off = SGITHDRSZ + prefix_size;
    for node in nodes {
        let n = node_tuple_size(node);
        img[off..off + n].copy_from_slice(&node[..n]);
        off += n;
    }

    Ok(tup)
}

/// SGLTDATUM over a raw leaf-tuple image.
#[inline]
pub(crate) fn leaf_datum(tup: &[u8], state: &SpGistState<'_>) -> PgResult<Datum> {
    let hdr = SpGistLeafTupleHeader::decode(tup);
    let off = SGLTHDRSZ(hdr.hasNullMask());
    // Reject an on-page varlena whose declared length would read past the
    // tuple image before the opclass ever dereferences it.
    validate_inline_datum(tup, off, &state.attLeafType)?;
    // SAFETY: leaf tuple image extends past its header per its size field.
    Ok(fetch_att(
        tup[off..].as_ptr(),
        state.attLeafType.attbyval,
        state.attLeafType.attlen,
    ))
}

/// spgDeformLeafTuple.
pub fn spgDeformLeafTuple(
    tup: &[u8],
    tuple_descriptor: &TupleDescData<'_>,
    datums: &mut [Datum],
    isnulls: &mut [bool],
    keyColumnIsNull: bool,
) {
    let hdr = SpGistLeafTupleHeader::decode(tup);
    let has_nulls_mask = hdr.hasNullMask();

    if keyColumnIsNull && tuple_descriptor.natts == 1 {
        debug_assert!(!has_nulls_mask);
        datums[spgKeyColumn] = Datum::null();
        isnulls[spgKeyColumn] = true;
        return;
    }

    let tp = &tup[SGLTHDRSZ(has_nulls_mask)..];
    let bp = &tup[SIZEOF_SPGIST_LEAF_TUPLE_DATA..];
    index_deform_tuple_internal(tuple_descriptor, datums, isnulls, tp, bp, has_nulls_mask);

    debug_assert_eq!(keyColumnIsNull, isnulls[spgKeyColumn]);
}

// index_deform_tuple_internal (indextuple.c) over an external data
// pointer + bitmap; attcacheoff is not consulted (images are transient).
fn index_deform_tuple_internal(
    tuple_descriptor: &TupleDescData<'_>,
    datums: &mut [Datum],
    isnulls: &mut [bool],
    tp: &[u8],
    bp: &[u8],
    hasnulls: bool,
) {
    use ::types_tuple::tupmacs::att_addlength_pointer;
    let natts = tuple_descriptor.natts as usize;
    let mut off = 0usize;

    for i in 0..natts {
        if hasnulls && (bp[i >> 3] & (1 << (i & 7))) == 0 {
            datums[i] = Datum::null();
            isnulls[i] = true;
            continue;
        }
        isnulls[i] = false;
        let att = tuple_descriptor.compact_attr(i);
        let attlen = att.attlen as i32;
        if attlen == -1 {
            // SAFETY: in-bounds varlena start within the tuple image.
            off = unsafe { att_align_pointer_var(tp.as_ptr(), att.attalignby, off) };
        } else {
            off = att_align_nominal_by(off, att.attalignby);
        }
        datums[i] = fetch_att(tp[off..].as_ptr(), att.attbyval, att.attlen);
        // SAFETY: value at off is live within the image.
        off = unsafe { att_addlength_pointer(off, attlen, tp[off..].as_ptr()) };
    }
}

#[inline]
fn att_align_nominal_by(off: usize, alignby: u8) -> usize {
    let a = alignby as usize;
    (off + a - 1) & !(a - 1)
}

// att_align_pointer for varlena: no alignment if the byte at `off` starts a
// short varlena header (nonzero first byte means 1B header).
#[inline]
unsafe fn att_align_pointer_var(tp: *const u8, alignby: u8, off: usize) -> usize {
    if *tp.add(off) != 0 {
        off
    } else {
        att_align_nominal_by(off, alignby)
    }
}

/// spgExtractNodeLabels: labels into `out` (temp mcx scratch); None if all
/// labels are NULL.
pub fn spgExtractNodeLabels(
    state: &SpGistState<'_>,
    inner: &[u8],
    out: &mut Vec<Datum>,
) -> PgResult<bool> {
    out.clear();
    let hdr = SpGistInnerTupleHeader::decode(inner);
    if hdr.nNodes == 0 {
        return Ok(false);
    }
    let first_off = SGITHDRSZ + hdr.prefixSize as usize;
    if node_tuple_has_nulls(&inner[first_off..]) {
        for (_, off) in inner_tuple_nodes(inner) {
            if !node_tuple_has_nulls(&inner[off..]) {
                return Err(mixed_null_labels());
            }
        }
        Ok(false)
    } else {
        for (_, off) in inner_tuple_nodes(inner) {
            let node = &inner[off..];
            if node_tuple_has_nulls(node) {
                return Err(mixed_null_labels());
            }
            out.push(node_label_datum(node, state)?);
        }
        Ok(true)
    }
}

// C (spgutils.c:1173/1184): elog(ERROR) — catchable XX000.
#[cold]
#[inline(never)]
fn mixed_null_labels() -> Box<PgError> {
    Box::new(PgError::error("some but not all node labels are null in SPGiST inner tuple"))
}

/// SGNTDATUM.
#[inline]
pub(crate) fn node_label_datum(node: &[u8], state: &SpGistState<'_>) -> PgResult<Datum> {
    if state.attLabelType.attbyval {
        Ok(Datum::from_u64(u64::from_ne_bytes(
            node[SGNTHDRSZ..SGNTHDRSZ + 8].try_into().expect("8 bytes"),
        )))
    } else {
        validate_inline_datum(node, SGNTHDRSZ, &state.attLabelType)?;
        Ok(Datum::from_usize(node[SGNTHDRSZ..].as_ptr() as usize))
    }
}

/// SGITDATUM.
#[inline]
pub(crate) fn inner_prefix_datum(inner: &[u8], state: &SpGistState<'_>) -> PgResult<Datum> {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    if hdr.prefixSize == 0 {
        return Ok(Datum::null());
    }
    if state.attPrefixType.attbyval {
        Ok(Datum::from_u64(u64::from_ne_bytes(
            inner[SGITHDRSZ..SGITHDRSZ + 8].try_into().expect("8 bytes"),
        )))
    } else {
        validate_inline_datum(inner, SGITHDRSZ, &state.attPrefixType)?;
        Ok(Datum::from_usize(inner[SGITHDRSZ..].as_ptr() as usize))
    }
}

/// SpGistPageAddNewItem: add, replacing a PLACEHOLDER if possible.
pub fn SpGistPageAddNewItem(
    pm: &mut PageMut<'_>,
    item: &[u8],
    start_offset: Option<&mut OffsetNumber>,
    error_ok: bool,
) -> PgResult<OffsetNumber> {
    let size = item.len();
    let opaque = page_opaque(&pm.as_ref());

    if opaque.nPlaceholder > 0
        && pm.as_ref().exact_free_space() + SGDTSIZE >= MAXALIGN(size)
    {
        let maxoff = pm.as_ref().max_offset_number();
        let mut offnum = InvalidOffsetNumber;
        let mut hint = start_offset.as_ref().map_or(InvalidOffsetNumber, |s| **s);

        loop {
            let start = if hint != InvalidOffsetNumber {
                hint
            } else {
                FirstOffsetNumber
            };
            for i in start..=maxoff {
                let it = item_slice(&pm.as_ref(), i);
                if tuple_state(it) == SPGIST_PLACEHOLDER {
                    offnum = i;
                    break;
                }
            }
            if offnum != InvalidOffsetNumber {
                break;
            }
            if hint != InvalidOffsetNumber {
                hint = InvalidOffsetNumber;
                continue;
            }
            page_opaque_update(pm, |op| op.nPlaceholder = 0);
            break;
        }

        if offnum != InvalidOffsetNumber {
            pm.index_tuple_delete(offnum);
            match pm.add_item(item, offnum, 0) {
                Some(o) if o == offnum => {
                    page_opaque_update(pm, |op| {
                        debug_assert!(op.nPlaceholder > 0);
                        op.nPlaceholder -= 1;
                    });
                    if let Some(s) = start_offset {
                        *s = offnum + 1;
                    }
                }
                _ => panic!("failed to add item of size {size} to SPGiST index page"),
            }
            return Ok(offnum);
        }
    }

    match pm.add_item(item, InvalidOffsetNumber, 0) {
        Some(o) => Ok(o),
        None => {
            if !error_ok {
                return Err(add_item_failed(size));
            }
            Ok(InvalidOffsetNumber)
        }
    }
}

pub(crate) trait ItupExt {
    fn as_slice(&self) -> &[u8];
    fn as_mut_slice(&mut self) -> &mut [u8];
}

impl ItupExt for ::nbtree::itup::ItupBuf<'_> {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        // SAFETY: ItupBuf owns size() initialized bytes.
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.size()) }
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let n = self.size();
        // SAFETY: as as_slice, exclusive borrow.
        unsafe { core::slice::from_raw_parts_mut(self.as_mut_ptr(), n) }
    }
}

#[cfg(test)]
mod inline_datum_tests {
    use super::*;
    use ::types_tuple::varatt::set_varsize_4b_word;

    fn text_desc() -> SpGistTypeDesc {
        // text: variable-length, by-reference.
        SpGistTypeDesc {
            type_: 25,
            attlen: -1,
            attbyval: false,
            attalign: b'i' as i8,
            attstorage: b'x' as i8,
        }
    }

    // A 4-byte-header varlena image declaring `total` bytes, backed by
    // `image_len` bytes of actual storage (image_len may be smaller than the
    // declared length to simulate a crafted on-page header).
    fn image(total: u32, image_len: usize) -> Vec<u8> {
        let mut v = vec![0u8; image_len.max(4)];
        v[..4].copy_from_slice(&set_varsize_4b_word(total).to_ne_bytes());
        v
    }

    #[test]
    fn accepts_datum_within_tuple() {
        // 6-byte varlena (2 data bytes) fully contained in an 8-byte image.
        let img = image(6, 8);
        assert!(validate_inline_datum(&img, 0, &text_desc()).is_ok());
    }

    #[test]
    fn rejects_declared_length_past_tuple() {
        // Header claims ~1 GiB but only 8 bytes are backed by the tuple image.
        let img = image(0x3FFF_FFFF, 8);
        let err = validate_inline_datum(&img, 0, &text_desc()).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn rejects_truncated_header() {
        // Fewer than VARHDRSZ bytes available for a 4B header.
        let img = vec![0u8; 2];
        let err = validate_inline_datum(&img, 0, &text_desc()).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    #[test]
    fn byval_datum_is_never_dereferenced() {
        let desc = SpGistTypeDesc {
            type_: 21,
            attlen: 2,
            attbyval: true,
            attalign: b's' as i8,
            attstorage: b'p' as i8,
        };
        // Empty image is fine: by-value datums are read by width, not pointer.
        assert!(validate_inline_datum(&[], 0, &desc).is_ok());
    }
}

#[cfg(test)]
mod node_label_tests {
    use super::*;
    use std::rc::Rc;

    fn mock_proc(
        _f: Option<&mut ::types_fmgr::FmgrInfo>,
        _fc: &mut ::types_fmgr::FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        Ok(Datum::from_usize(0))
    }

    fn desc(type_: ::types_core::Oid, attlen: i16, attbyval: bool) -> SpGistTypeDesc {
        SpGistTypeDesc { type_, attlen, attbyval, attalign: b's' as i8, attstorage: b'p' as i8 }
    }

    fn state<'m>(mcx: Mcx<'m>) -> SpGistState<'m> {
        let text = desc(25, -1, false);
        let int2 = desc(21, 2, true);
        let fi = || ::types_fmgr::FmgrInfo::new(mock_proc, 0, 2, true, false);
        SpGistState {
            config: spgConfigOut {
                prefixType: 25,
                labelType: 21,
                leafType: 25,
                canReturnData: true,
                longValuesOK: true,
            },
            attType: text,
            attLeafType: text,
            attPrefixType: text,
            attLabelType: int2,
            leafTupDesc: Rc::new(TupleDescData {
                natts: 1,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: 1,
                constr: None,
                compact_attrs: ::mcx::PgVec::new_in(mcx),
                attrs: ::mcx::PgVec::new_in(mcx),
            }),
            redirectXid: 0,
            isBuild: false,
            indexCollation: 0,
            chooseFn: fi(),
            picksplitFn: fi(),
            compressFn: fi(),
            frame1: ::types_fmgr::LocalFcinfo::new(0),
            frame2: ::types_fmgr::LocalFcinfo::new(0),
        }
    }

    // An inner tuple image with two label-less (8-byte) node tuples; node 1
    // carries INDEX_NULL_MASK, node 2 does not.
    fn mixed_null_inner() -> Vec<u8> {
        let mut v = vec![0u8; SGITHDRSZ + 16];
        SpGistInnerTupleHeader {
            tupstate: SPGIST_LIVE,
            allTheSame: false,
            nNodes: 2,
            prefixSize: 0,
            size: (SGITHDRSZ + 16) as u16,
        }
        .encode(&mut v);
        v[SGITHDRSZ + 6..SGITHDRSZ + 8].copy_from_slice(&(8u16 | INDEX_NULL_MASK).to_ne_bytes());
        v[SGITHDRSZ + 14..SGITHDRSZ + 16].copy_from_slice(&8u16.to_ne_bytes());
        v
    }

    // spgutils.c:1173 spgExtractNodeLabels: a mixture of NULL and non-NULL
    // node labels is elog(ERROR, "some but not all node labels are null in
    // SPGiST inner tuple") — a catchable XX000, never a panic (audit row
    // spgutils-18230dc8).
    #[test]
    fn mixed_null_labels_error_not_panic() {
        let ctx = ::mcx::MemoryContext::new("spgExtractNodeLabels test");
        let st = state(ctx.mcx());
        let inner = mixed_null_inner();
        assert!(node_tuple_has_nulls(&inner[SGITHDRSZ..]));
        assert!(!node_tuple_has_nulls(&inner[SGITHDRSZ + 8..]));
        let mut out = Vec::new();
        let err = spgExtractNodeLabels(&st, &inner, &mut out)
            .expect_err("mixed null/non-null labels must be rejected");
        assert_eq!(err.message(), "some but not all node labels are null in SPGiST inner tuple");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    }
}
