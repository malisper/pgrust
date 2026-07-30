//! spgutils.c: state/cache init, page management, tuple builders/deformers,
//! SpGistPageAddNewItem.

use ::bufmgr_seams::{self as bufmgr};
use ::datum::Datum;
use ::mcx::Mcx;
use ::nbtree::itup::ItupBuf;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, OffsetNumber, Oid, BLCKSZ,
};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
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
    bufmgr::release_buffer::call(buffer);
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

#[cold]
#[inline(never)]
pub fn tuple_state_error(tupstate: u8) -> ! {
    panic!("unexpected SPGiST tuple state: {tupstate}")
}

#[cold]
#[inline(never)]
pub fn add_item_failed(size: usize) -> ! {
    panic!("failed to add item of size {size} to SPGiST index page")
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
    if let Some(cache) = index.rd_amcache_spgist.get() {
        return Ok(cache);
    }

    let mut cache = SpGistCache::default();

    debug_assert_eq!(index.indnkeyatts(), 1);

    let atttype = get_index_input_type(index)?;

    let config_oid = index_getprocid(index, spgKeyColumn, SPGIST_CONFIG_PROC);
    if config_oid == InvalidOid {
        panic!(
            "missing support function {SPGIST_CONFIG_PROC} for attribute 1 of index \"{}\"",
            index.name()
        );
    }
    let mut config_fn = fmgr_seams::fmgr_info::call(config_oid)?;
    let cfgin = spgConfigIn { attType: atttype };
    {
        let mut frame = ::types_fmgr::LocalFcinfo::<2>::new(0);
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
            panic!("index \"{}\" is not an SP-GiST index", index.name());
        }
        cache.lastUsedPages = metadata.lastUsedPages;
        unlock_release(metabuffer)?;
    }

    index.rd_amcache_spgist.set(Some(cache));
    Ok(cache)
}

#[inline]
pub(crate) fn set_cache(index: &Relation<'_>, cache: SpGistCache) {
    index.rd_amcache_spgist.set(Some(cache));
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
            panic!(
                "missing support function {procnum} for attribute 1 of index \"{}\"",
                index.name()
            );
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
        bufmgr::release_buffer::call(buffer);
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
    let Some(cache) = index.rd_amcache_spgist.get() else {
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
        bufmgr::release_buffer::call(metabuffer);
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

    if need_space as usize > SPGIST_PAGE_CAPACITY {
        panic!("desired SPGiST tuple size is too big");
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
            bufmgr::release_buffer::call(buffer);
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
pub(crate) fn leaf_datum(tup: &[u8], state: &SpGistState<'_>) -> Datum {
    let hdr = SpGistLeafTupleHeader::decode(tup);
    let off = SGLTHDRSZ(hdr.hasNullMask());
    // SAFETY: leaf tuple image extends past its header per its size field.
    fetch_att(
        tup[off..].as_ptr(),
        state.attLeafType.attbyval,
        state.attLeafType.attlen,
    )
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
) -> bool {
    out.clear();
    let hdr = SpGistInnerTupleHeader::decode(inner);
    if hdr.nNodes == 0 {
        return false;
    }
    let first_off = SGITHDRSZ + hdr.prefixSize as usize;
    if node_tuple_has_nulls(&inner[first_off..]) {
        for (_, off) in inner_tuple_nodes(inner) {
            if !node_tuple_has_nulls(&inner[off..]) {
                panic!("some but not all node labels are null in SPGiST inner tuple");
            }
        }
        false
    } else {
        for (_, off) in inner_tuple_nodes(inner) {
            let node = &inner[off..];
            if node_tuple_has_nulls(node) {
                panic!("some but not all node labels are null in SPGiST inner tuple");
            }
            out.push(node_label_datum(node, state));
        }
        true
    }
}

/// SGNTDATUM.
#[inline]
pub(crate) fn node_label_datum(node: &[u8], state: &SpGistState<'_>) -> Datum {
    if state.attLabelType.attbyval {
        Datum::from_u64(u64::from_ne_bytes(
            node[SGNTHDRSZ..SGNTHDRSZ + 8].try_into().expect("8 bytes"),
        ))
    } else {
        Datum::from_usize(node[SGNTHDRSZ..].as_ptr() as usize)
    }
}

/// SGITDATUM.
#[inline]
pub(crate) fn inner_prefix_datum(inner: &[u8], state: &SpGistState<'_>) -> Datum {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    if hdr.prefixSize == 0 {
        return Datum::null();
    }
    if state.attPrefixType.attbyval {
        Datum::from_u64(u64::from_ne_bytes(
            inner[SGITHDRSZ..SGITHDRSZ + 8].try_into().expect("8 bytes"),
        ))
    } else {
        Datum::from_usize(inner[SGITHDRSZ..].as_ptr() as usize)
    }
}

/// SpGistPageAddNewItem: add, replacing a PLACEHOLDER if possible.
pub fn SpGistPageAddNewItem(
    pm: &mut PageMut<'_>,
    item: &[u8],
    start_offset: Option<&mut OffsetNumber>,
    error_ok: bool,
) -> OffsetNumber {
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
            return offnum;
        }
    }

    match pm.add_item(item, InvalidOffsetNumber, 0) {
        Some(o) => o,
        None => {
            if !error_ok {
                add_item_failed(size);
            }
            InvalidOffsetNumber
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
