//! nbtpage.c: metapage decode + rd_amcache, _bt_getroot (read + root-creation
//! arms), buffer traffic helpers, _bt_allocbuf incl. FSM recycling, deleted-
//! page helpers. Page-deletion write arms live in pagedel.rs.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::types_core::xact::{FirstNormalFullTransactionId, FullTransactionId};
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_nbtree::{
    BTDeletedPageData, BTMetaPageData, BTPageOpaqueData, BTP_DELETED, BTP_HALF_DEAD,
    BTP_HAS_FULLXID, BTP_LEAF, BTP_META, BTP_ROOT, P_HAS_FULLXID, P_IGNORE, P_ISDELETED, P_ISMETA,
    P_LEFTMOST, P_RIGHTMOST, BTREE_MAGIC, BTREE_METAPAGE, BTREE_MIN_VERSION, BTREE_NOVAC_VERSION,
    BTREE_VERSION, BT_READ, BT_WRITE, P_NONE, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{ItemIdData, PageMut, PageRef, SizeOfPageHeaderData};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::unported_phase2;

const PD_SPECIAL_OFF: usize = 16;
const _: () = assert!(core::mem::size_of::<BTPageOpaqueData>() == 16);

#[inline]
pub(crate) fn page_special_off(page: &PageRef<'_>) -> usize {
    // SAFETY: pd_special lives at a 2-aligned in-page offset (PageRef contract).
    let off = unsafe { page.as_ptr().add(PD_SPECIAL_OFF).cast::<u16>().read() } as usize;
    // hard-validated once per acquisition (bt_checkpage, C's model)
    debug_assert!(off >= SizeOfPageHeaderData && off <= BLCKSZ, "corrupt pd_special");
    off.min(BLCKSZ - core::mem::size_of::<BTPageOpaqueData>())
}

/// BTPageGetOpaque, by value (read-only users; killitems writes raw).
#[inline]
pub fn page_opaque(page: &PageRef<'_>) -> BTPageOpaqueData {
    let off = page_special_off(page);
    // SAFETY: in-bounds (page_special_off clamps), 4-aligned (MAXALIGNed).
    unsafe { page.as_ptr().add(off).cast::<BTPageOpaqueData>().read() }
}


#[inline]
pub fn page_item(page: &PageRef<'_>, id: ItemIdData) -> crate::itup::ITup {
    // SAFETY: ids come from this page's line-pointer array on a pinned+locked
    // page that passed bt_checkpage — the invariant C reads unchecked.
    unsafe { page.item_raw_unchecked(id).0 }
}

// BTPageGetMeta: contents start at MAXALIGN(SizeOfPageHeaderData).
pub fn page_meta(page: &PageRef<'_>) -> BTMetaPageData {
    // SAFETY: metapage contents at +24, 8-aligned, 48B in-bounds.
    unsafe {
        page.as_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<BTMetaPageData>()
            .read()
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_corrupted(msg: std::string::String) -> Box<PgError> {
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
    )
}

/// _bt_getmeta.
fn bt_getmeta(rel: &Relation<'_>, metapin: &BufferPin) -> PgResult<BTMetaPageData> {
    let page = metapin.page();
    let metaopaque = page_opaque(&page);
    let metad = page_meta(&page);

    if !P_ISMETA(&metaopaque) || metad.btm_magic != BTREE_MAGIC {
        return Err(index_corrupted(format!(
            "index \"{}\" is not a btree",
            rel.name()
        )));
    }
    if metad.btm_version < BTREE_MIN_VERSION || metad.btm_version > BTREE_VERSION {
        return Err(index_corrupted(format!(
            "version mismatch in index \"{}\": file version {}, current version {}, minimal supported version {}",
            rel.name(), metad.btm_version, BTREE_VERSION, BTREE_MIN_VERSION
        )));
    }
    Ok(metad)
}

/// _bt_checkpage.
pub(crate) fn bt_checkpage(rel: &Relation<'_>, pin: &BufferPin) -> PgResult<()> {
    bt_checkpage_ref(rel, &pin.page(), pin.block_number())
}

/// _bt_checkpage over a page image (amcheck reads local page copies).
pub fn bt_checkpage_ref(
    rel: &Relation<'_>,
    page: &PageRef<'_>,
    blkno: BlockNumber,
) -> PgResult<()> {
    if page.is_new() {
        return Err(index_corrupted(format!(
            "index \"{}\" contains unexpected zero page at block {}",
            rel.name(),
            blkno
        )));
    }
    // Raw pd_special, NOT page_special_off: its corruption clamp would mask
    // exactly the damage this check exists to report.
    // SAFETY: pd_special lives at a 2-aligned in-page offset (PageRef contract).
    let raw_special = unsafe { page.as_ptr().add(PD_SPECIAL_OFF).cast::<u16>().read() } as usize;
    let special_size = BLCKSZ.wrapping_sub(raw_special);
    if special_size != core::mem::size_of::<BTPageOpaqueData>() {
        return Err(index_corrupted(format!(
            "index \"{}\" contains corrupted page at block {}",
            rel.name(),
            blkno
        )));
    }
    Ok(())
}

/// _bt_lockbuf (no Valgrind client requests in this build).
#[inline]
pub(crate) fn bt_lockbuf(_rel: &Relation<'_>, pin: &BufferPin, access: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), access)
}

/// _bt_unlockbuf.
#[inline]
pub(crate) fn bt_unlockbuf(_rel: &Relation<'_>, pin: &BufferPin) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)
}

/// _bt_upgradelockbufcleanup.
pub(crate) fn bt_upgradelockbufcleanup(rel: &Relation<'_>, pin: &BufferPin) -> PgResult<()> {
    bt_unlockbuf(rel, pin)?;
    bufmgr::lock_buffer_for_cleanup::call(pin.buffer())
}

/// BTPageSetDeleted.
pub(crate) fn bt_page_set_deleted(pm: &mut PageMut<'_>, safexid: FullTransactionId) {
    let mut opaque = page_opaque(&pm.as_ref());
    opaque.btpo_flags &= !BTP_HALF_DEAD;
    opaque.btpo_flags |= BTP_DELETED | BTP_HAS_FULLXID;
    write_opaque(pm, &opaque);
    pm.set_pd_lower(
        (maxalign_hdr() + core::mem::size_of::<BTDeletedPageData>()) as u16,
    );
    pm.set_pd_upper(pm.as_ref().pd_special());
    // SAFETY: PageGetContents at MAXALIGN(SizeOfPageHeaderData), 8-aligned,
    // 8B in-bounds; caller holds the exclusive lock.
    unsafe {
        pm.as_ref()
            .as_ptr()
            .cast_mut()
            .add(maxalign_hdr())
            .cast::<FullTransactionId>()
            .write(safexid)
    };
}

const fn maxalign_hdr() -> usize {
    (SizeOfPageHeaderData + 7) & !7
}

/// BTPageGetDeleteXid.
pub(crate) fn bt_page_get_delete_xid(page: &PageRef<'_>) -> FullTransactionId {
    debug_assert!(!page.is_new());
    let opaque = page_opaque(page);
    debug_assert!(P_ISDELETED(&opaque));
    if !P_HAS_FULLXID(&opaque) {
        return FirstNormalFullTransactionId;
    }
    // SAFETY: deleted pages store BTDeletedPageData at PageGetContents.
    unsafe {
        page.as_ptr()
            .add(maxalign_hdr())
            .cast::<FullTransactionId>()
            .read()
    }
}

/// BTPageIsRecyclable.
pub(crate) fn bt_page_is_recyclable(
    page: &PageRef<'_>,
    heaprel: &::types_rel::RelationData<'_>,
) -> PgResult<bool> {
    debug_assert!(!page.is_new());
    let opaque = page_opaque(page);
    if P_ISDELETED(&opaque) {
        let safexid = bt_page_get_delete_xid(page);
        return procarray_seams::global_vis_check_removable_full_xid::call(heaprel, safexid);
    }
    Ok(false)
}

/// _bt_getbuf: pin + lock + checkpage.
pub(crate) fn bt_getbuf(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    access: i32,
) -> PgResult<BufferPin> {
    let pin = BufferPin::adopt(bufmgr::read_buffer::call(rel, blkno)?)
        .expect("ReadBuffer returned InvalidBuffer");
    bt_lockbuf(rel, &pin, access)?;
    bt_checkpage(rel, &pin)?;
    Ok(pin)
}

/// _bt_relandgetbuf: the lock-coupling step.
pub(crate) fn bt_relandgetbuf(
    rel: &Relation<'_>,
    obuf: Option<BufferPin>,
    blkno: BlockNumber,
    access: i32,
) -> PgResult<BufferPin> {
    let old = match obuf {
        Some(pin) => {
            bt_unlockbuf(rel, &pin)?;
            pin.into_buffer()
        }
        None => ::types_core::InvalidBuffer,
    };
    let pin = BufferPin::adopt(bufmgr::release_and_read_buffer::call(old, rel, blkno)?)
        .expect("ReleaseAndReadBuffer returned InvalidBuffer");
    bt_lockbuf(rel, &pin, access)?;
    bt_checkpage(rel, &pin)?;
    Ok(pin)
}

/// _bt_relbuf: drop lock and pin.
pub fn bt_relbuf(rel: &Relation<'_>, pin: BufferPin) -> PgResult<()> {
    bt_unlockbuf(rel, &pin)?;
    pin.release();
    Ok(())
}

#[inline]
fn amcache_sane(metad: &BTMetaPageData) -> bool {
    metad.btm_magic == BTREE_MAGIC
        && metad.btm_version >= BTREE_MIN_VERSION
        && metad.btm_version <= BTREE_VERSION
        && (!metad.btm_allequalimage || metad.btm_version > BTREE_NOVAC_VERSION)
        && metad.btm_root != P_NONE
}

/// _bt_getroot: the pinned+read-locked (fast) root. `None` for an empty index
/// under BT_READ; BT_WRITE creates the first root page (heaprel required, as C).
pub(crate) fn bt_getroot<'mcx>(
    rel: &Relation<'mcx>,
    heaprel: Option<&::types_rel::RelationData<'mcx>>,
    access: i32,
) -> PgResult<Option<BufferPin>> {
    if let Some(metad) = rel.rd_amcache.get() {
        debug_assert!(amcache_sane(&metad));
        let rootblkno = metad.btm_fastroot;
        debug_assert!(rootblkno != P_NONE);
        let rootlevel = metad.btm_fastlevel;

        let rootpin = bt_getbuf(rel, rootblkno, BT_READ)?;
        let rootopaque = page_opaque(&rootpin.page());

        // its level (no P_ISROOT check — fast roots don't set it).
        if !P_IGNORE(&rootopaque)
            && rootopaque.btpo_level == rootlevel
            && P_LEFTMOST(&rootopaque)
            && P_RIGHTMOST(&rootopaque)
        {
            return Ok(Some(rootpin));
        }
        bt_relbuf(rel, rootpin)?;
        rel.rd_amcache.set(None);
    }

    let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
    let metad = bt_getmeta(rel, &metapin)?;

    if metad.btm_root == P_NONE {
        if access == BT_READ {
            bt_relbuf(rel, metapin)?;
            return Ok(None);
        }

        bt_unlockbuf(rel, &metapin)?;
        bt_lockbuf(rel, &metapin, BT_WRITE)?;

        // C's create-root race recheck; single lock trade keeps the shape.
        if page_meta(&metapin.page()).btm_root != P_NONE {
            bt_relbuf(rel, metapin)?;
            return bt_getroot(rel, heaprel, access);
        }

        let rootpin = bt_allocbuf(rel, heaprel.expect("BT_WRITE getroot requires heaprel"))?;
        let rootblkno = rootpin.block_number();
        {
            let mut rootpage = page_of_mut(&rootpin);
            write_opaque(
                &mut rootpage,
                &BTPageOpaqueData {
                    btpo_prev: P_NONE,
                    btpo_next: P_NONE,
                    btpo_level: 0,
                    btpo_flags: BTP_LEAF | BTP_ROOT,
                    btpo_cycleid: 0,
                },
            );
        }

        if page_meta(&metapin.page()).btm_version < BTREE_NOVAC_VERSION {
            unported_phase2("_bt_upgrademetapage (v2/v3 pg_upgrade metapages)");
        }
        let mut metad = page_meta(&metapin.page());
        metad.btm_root = rootblkno;
        metad.btm_level = 0;
        metad.btm_fastroot = rootblkno;
        metad.btm_fastlevel = 0;
        metad.btm_last_cleanup_num_delpages = 0;
        metad.btm_last_cleanup_num_heap_tuples = -1.0;
        write_meta(&metapin, &metad);

        bufmgr::mark_buffer_dirty::call(rootpin.buffer())?;
        bufmgr::mark_buffer_dirty::call(metapin.buffer())?;

        if crate::relation_needs_wal(rel) {
            debug_assert!(metad.btm_version >= BTREE_NOVAC_VERSION);
            let md = crate::wal::xl_btree_metadata(&metad);
            let xlrec = crate::wal::xl_btree_newroot(rootblkno, 0);
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                ::rmgr::RM_BTREE_ID as u8,
                XLOG_BTREE_NEWROOT,
                0,
                &[&xlrec],
                &[
                    XLogRegBuf {
                        block_id: 0,
                        buffer: rootpin.buffer(),
                        flags: REGBUF_WILL_INIT,
                        bufdata: &[],
                    },
                    XLogRegBuf {
                        block_id: 2,
                        buffer: metapin.buffer(),
                        flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                        bufdata: &[&md],
                    },
                ],
            )?;
            page_of_mut(&rootpin).set_lsn(recptr);
            page_of_mut(&metapin).set_lsn(recptr);
        }

        // swap the new root's write lock for the read lock callers expect.
        bt_unlockbuf(rel, &rootpin)?;
        bt_lockbuf(rel, &rootpin, BT_READ)?;

        bt_relbuf(rel, metapin)?;
        return Ok(Some(rootpin));
    }

    let rootblkno = metad.btm_fastroot;
    debug_assert!(rootblkno != P_NONE);
    let rootlevel = metad.btm_fastlevel;

    rel.rd_amcache.set(Some(metad));

    let mut rootpin = metapin;
    let mut rootblkno = rootblkno;
    let rootopaque = loop {
        rootpin = bt_relandgetbuf(rel, Some(rootpin), rootblkno, BT_READ)?;
        let opaque = page_opaque(&rootpin.page());
        if !P_IGNORE(&opaque) {
            break opaque;
        }
        if P_RIGHTMOST(&opaque) {
            return Err(Box::new(PgError::error(format!(
                "no live root page found in index \"{}\"",
                rel.name()
            ))));
        }
        rootblkno = opaque.btpo_next;
    };

    if rootopaque.btpo_level != rootlevel {
        return Err(Box::new(PgError::error(format!(
            "root page {} of index \"{}\" has level {}, expected {}",
            rootblkno,
            rel.name(),
            rootopaque.btpo_level,
            rootlevel
        ))));
    }

    Ok(Some(rootpin))
}

/// _bt_gettrueroot: BT_READ root via the true-root link. `None` for an empty
/// index. Flushes the amcache — this path implies it's stale.
pub(crate) fn bt_gettrueroot(rel: &Relation<'_>) -> PgResult<Option<BufferPin>> {
    rel.rd_amcache.set(None);

    let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
    let metad = bt_getmeta(rel, &metapin)?;

    if metad.btm_root == P_NONE {
        bt_relbuf(rel, metapin)?;
        return Ok(None);
    }

    let rootlevel = metad.btm_level;
    let mut rootblkno = metad.btm_root;
    let mut rootpin = metapin;
    let rootopaque = loop {
        rootpin = bt_relandgetbuf(rel, Some(rootpin), rootblkno, BT_READ)?;
        let opaque = page_opaque(&rootpin.page());
        if !P_IGNORE(&opaque) {
            break opaque;
        }
        if P_RIGHTMOST(&opaque) {
            return Err(Box::new(PgError::error(format!(
                "no live root page found in index \"{}\"",
                rel.name()
            ))));
        }
        rootblkno = opaque.btpo_next;
    };

    if rootopaque.btpo_level != rootlevel {
        return Err(Box::new(PgError::error(format!(
            "root page {} of index \"{}\" has level {}, expected {}",
            rootblkno,
            rel.name(),
            rootopaque.btpo_level,
            rootlevel
        ))));
    }

    Ok(Some(rootpin))
}

fn prime_amcache(rel: &Relation<'_>) -> PgResult<Option<BTMetaPageData>> {
    if rel.rd_amcache.get().is_none() {
        let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
        let metad = bt_getmeta(rel, &metapin)?;
        if metad.btm_root == P_NONE {
            bt_relbuf(rel, metapin)?;
            return Ok(Some(metad));
        }
        rel.rd_amcache.set(Some(metad));
        bt_relbuf(rel, metapin)?;
    }
    Ok(None)
}

/// _bt_getrootheight.
pub fn bt_getrootheight(rel: &Relation<'_>) -> PgResult<i32> {
    if let Some(uncached) = prime_amcache(rel)? {
        if uncached.btm_root == P_NONE {
            return Ok(0);
        }
    }
    let metad = rel.rd_amcache.get().expect("amcache primed above");
    debug_assert!(amcache_sane(&metad) && metad.btm_fastroot != P_NONE);
    Ok(metad.btm_fastlevel as i32)
}

/// _bt_metaversion -> (heapkeyspace, allequalimage).
pub fn bt_metaversion(rel: &Relation<'_>) -> PgResult<(bool, bool)> {
    if let Some(uncached) = prime_amcache(rel)? {
        return Ok((
            uncached.btm_version > BTREE_NOVAC_VERSION,
            uncached.btm_allequalimage,
        ));
    }
    let metad = rel.rd_amcache.get().expect("amcache primed above");
    debug_assert!(amcache_sane(&metad) && metad.btm_fastroot != P_NONE);
    Ok((
        metad.btm_version > BTREE_NOVAC_VERSION,
        metad.btm_allequalimage,
    ))
}

/// Mutable view of a pinned, exclusively locked buffer's page.
pub(crate) fn page_of_mut<'p>(pin: &'p BufferPin) -> PageMut<'p> {
    // SAFETY: pin held; callers hold the exclusive content lock (BT_WRITE).
    unsafe { PageMut::from_raw(bufmgr::buffer_get_page::call(pin.buffer())) }
}

/// # Safety
/// `buf` pinned + write-locked for the borrow.
pub(crate) unsafe fn buf_page_mut<'p>(buf: Buffer) -> PageMut<'p> {
    PageMut::from_raw(bufmgr::buffer_get_page::call(buf))
}

pub(crate) fn write_opaque(page: &mut PageMut<'_>, opaque: &BTPageOpaqueData) {
    let off = page_special_off(&page.as_ref());
    // SAFETY: in-bounds special area, 4-aligned, exclusive page access.
    unsafe {
        page.as_ref()
            .as_ptr()
            .cast_mut()
            .add(off)
            .cast::<BTPageOpaqueData>()
            .write(*opaque)
    }
}

// BTPageGetMeta store + the pd_lower bump _bt_initmetapage documents (metadata
// survives xlog page-hole compression).
pub(crate) fn write_meta(pin: &BufferPin, metad: &BTMetaPageData) {
    let mut page = page_of_mut(pin);
    let img = metad.page_image();
    // SAFETY: metapage contents at +24, 48B in-bounds; exclusive lock.
    unsafe {
        core::ptr::copy_nonoverlapping(
            img.as_ptr(),
            page.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            img.len(),
        )
    };
    page.set_pd_lower((SizeOfPageHeaderData + core::mem::size_of::<BTMetaPageData>()) as u16);
}

/// _bt_pageinit.
pub fn bt_pageinit(page: &mut PageMut<'_>) {
    page.init(core::mem::size_of::<BTPageOpaqueData>());
}

/// _bt_initmetapage: fill `page` with a fresh metapage image (index build /
/// empty-index fixtures).
pub fn bt_initmetapage(page: &mut PageMut<'_>, rootbknum: BlockNumber, level: u32, allequalimage: bool) {
    bt_pageinit(page);
    let img = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: rootbknum,
        btm_level: level,
        btm_fastroot: rootbknum,
        btm_fastlevel: level,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: allequalimage,
    }
    .page_image();
    // SAFETY: metapage contents at +24, 48B in-bounds; caller owns page.
    unsafe {
        core::ptr::copy_nonoverlapping(
            img.as_ptr(),
            page.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            img.len(),
        )
    };
    let off = page_special_off(&page.as_ref());
    // SAFETY: special area write, 4-aligned, in-bounds.
    unsafe {
        page.as_ref()
            .as_ptr()
            .cast_mut()
            .add(off)
            .cast::<BTPageOpaqueData>()
            .write(BTPageOpaqueData {
                btpo_prev: 0,
                btpo_next: 0,
                btpo_level: 0,
                btpo_flags: BTP_META,
                btpo_cycleid: 0,
            })
    };
    page.set_pd_lower((SizeOfPageHeaderData + core::mem::size_of::<BTMetaPageData>()) as u16);
}

/// _bt_conditionallockbuf.
pub(crate) fn bt_conditionallockbuf(_rel: &Relation<'_>, pin: &BufferPin) -> PgResult<bool> {
    bufmgr::conditional_lock_buffer::call(pin.buffer())
}

/// _bt_allocbuf: a write-locked, freshly initialized page.
pub(crate) fn bt_allocbuf(rel: &Relation<'_>, heaprel: &::types_rel::RelationData<'_>) -> PgResult<BufferPin> {
    loop {
        let blkno = ::freespace::GetFreeIndexPage(rel)?;
        if blkno == InvalidBlockNumber {
            break;
        }
        let pin = BufferPin::adopt(bufmgr::read_buffer::call(rel, blkno)?)
            .expect("ReadBuffer returned InvalidBuffer");
        if bt_conditionallockbuf(rel, &pin)? {
            if pin.page().is_new() {
                bt_pageinit(&mut page_of_mut(&pin));
                return Ok(pin);
            }

            if bt_page_is_recyclable(&pin.page(), heaprel)? {
                if crate::relation_needs_wal(rel)
                    && transam_xlog_seams::xlog_standby_info_active::call()
                {
                    // RelationIsAccessibleInLogicalDecoding const-false
                    // (pruneheap precedent): isCatalogRel = false.
                    let xlrec = crate::wal::xl_btree_reuse_page(
                        rel.rd_locator.get(),
                        blkno,
                        bt_page_get_delete_xid(&pin.page()),
                    );
                    ::xloginsert_seams::xlog_insert_record::call(
                        ::rmgr::RM_BTREE_ID as u8,
                        XLOG_BTREE_REUSE_PAGE,
                        0,
                        &[&xlrec],
                        &[],
                    )?;
                }
                bt_pageinit(&mut page_of_mut(&pin));
                return Ok(pin);
            }
            bt_unlockbuf(rel, &pin)?;
        }
        pin.release();
    }

    let (buf, extended_by) = bufmgr::extend_buffered_rel_by::call(
        rel,
        ForkNumber::MAIN_FORKNUM,
        None,
        bufmgr::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert!(extended_by == 1);
    let pin = BufferPin::adopt(buf).expect("ExtendBufferedRelBy returned InvalidBuffer");
    debug_assert!(pin.page().is_new());
    bt_pageinit(&mut page_of_mut(&pin));
    Ok(pin)
}

const _: () = assert!(BT_READ != BT_WRITE);
