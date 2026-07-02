//! nbtpage.c, READ side: metapage decode + rd_amcache, root descent entry,
//! and the pinned-buffer traffic helpers. Write side (root creation,
//! _bt_allocbuf, page deletion, delitems) is phase 2.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::types_core::{BlockNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, P_IGNORE, P_ISMETA, P_LEFTMOST, P_RIGHTMOST,
    BTREE_MAGIC, BTREE_METAPAGE, BTREE_MIN_VERSION, BTREE_NOVAC_VERSION, BTREE_VERSION, BT_READ,
    BT_WRITE, P_NONE,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{ItemIdData, PageRef, SizeOfPageHeaderData};

use crate::unported_phase2;

const PD_SPECIAL_OFF: usize = 16;
const _: () = assert!(core::mem::size_of::<BTPageOpaqueData>() == 16);

#[inline]
pub(crate) fn page_special_off(page: &PageRef<'_>) -> usize {
    // SAFETY: pd_special lives at a 2-aligned in-page offset (PageRef contract).
    let off = unsafe { page.as_ptr().add(PD_SPECIAL_OFF).cast::<u16>().read() } as usize;
    assert!(off >= SizeOfPageHeaderData && off <= BLCKSZ, "corrupt pd_special");
    off
}

/// BTPageGetOpaque, by value (16B; read-only users only — killitems writes raw).
#[inline]
pub(crate) fn page_opaque(page: &PageRef<'_>) -> BTPageOpaqueData {
    let off = page_special_off(page);
    assert!(off + core::mem::size_of::<BTPageOpaqueData>() <= BLCKSZ);
    // SAFETY: in-bounds (asserted), 4-aligned (special areas are MAXALIGNed).
    unsafe { page.as_ptr().add(off).cast::<BTPageOpaqueData>().read() }
}

/// PageGetItem for an index tuple: raw pointer + length, page-bounded.
#[inline]
pub(crate) fn page_item(page: &PageRef<'_>, id: ItemIdData) -> crate::itup::ITup {
    page.item_raw(id).0
}

// BTPageGetMeta: contents start at MAXALIGN(SizeOfPageHeaderData).
fn page_meta(page: &PageRef<'_>) -> BTMetaPageData {
    // SAFETY: metapage contents at +24, 8-aligned, 48B in-bounds.
    unsafe {
        page.as_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<BTMetaPageData>()
            .read()
    }
}

#[cold]
#[inline(never)]
fn index_corrupted(msg: std::string::String) -> Box<PgError> {
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
    )
}

/// _bt_getmeta: decode + sanity-check the metapage held in `metapin`.
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
    let page = pin.page();
    if page.is_new() {
        return Err(index_corrupted(format!(
            "index \"{}\" contains unexpected zero page at block {}",
            rel.name(),
            pin.block_number()
        )));
    }
    let special_size = BLCKSZ - page_special_off(&page);
    if special_size != core::mem::size_of::<BTPageOpaqueData>() {
        return Err(index_corrupted(format!(
            "index \"{}\" contains corrupted page at block {}",
            rel.name(),
            pin.block_number()
        )));
    }
    Ok(())
}

/// _bt_lockbuf (Valgrind client requests are cfg'd out of this build).
#[inline]
pub(crate) fn bt_lockbuf(_rel: &Relation<'_>, pin: &BufferPin, access: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), access)
}

/// _bt_unlockbuf.
#[inline]
pub(crate) fn bt_unlockbuf(_rel: &Relation<'_>, pin: &BufferPin) -> PgResult<()> {
    bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)
}

/// _bt_getbuf: pin + lock + checkpage. Returned pin is locked per `access`.
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

/// _bt_relandgetbuf: lock-coupling step — unlock+unpin `obuf`, pin+lock blkno.
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
pub(crate) fn bt_relbuf(rel: &Relation<'_>, pin: BufferPin) -> PgResult<()> {
    bt_unlockbuf(rel, &pin)?;
    pin.release();
    Ok(())
}

// Fields of the cached metapage that _bt_getroot trusts (Assert set in C).
#[inline]
fn amcache_sane(metad: &BTMetaPageData) -> bool {
    metad.btm_magic == BTREE_MAGIC
        && metad.btm_version >= BTREE_MIN_VERSION
        && metad.btm_version <= BTREE_VERSION
        && (!metad.btm_allequalimage || metad.btm_version > BTREE_NOVAC_VERSION)
        && metad.btm_root != P_NONE
}

/// _bt_getroot, BT_READ arm: locate + read-lock the (fast) root. Returns None
/// for an empty index. BT_WRITE root creation is nbtinsert's phase 2.
///
/// Batched-descent lever (docs/graviton.md §1.5): point lookups descend one
/// level per lock-couple; a future change could prefetch the child while the
/// parent binary search finishes. Noted, not forced — needs bufmgr support.
pub(crate) fn bt_getroot(rel: &Relation<'_>, access: i32) -> PgResult<Option<BufferPin>> {
    if access != BT_READ {
        unported_phase2("_bt_getroot(BT_WRITE) root creation (nbtinsert lane)");
    }

    // rd_amcache fast path: skip the metapage read (rule-5 cache).
    if let Some(metad) = rel.rd_amcache.get() {
        debug_assert!(amcache_sane(&metad));
        let rootblkno = metad.btm_fastroot;
        debug_assert!(rootblkno != P_NONE);
        let rootlevel = metad.btm_fastlevel;

        let rootpin = bt_getbuf(rel, rootblkno, BT_READ)?;
        let rootopaque = page_opaque(&rootpin.page());

        // Stale-cache re-validation: must not be deleted, must be alone on
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
        // Empty index; BT_READ callers get no root.
        bt_relbuf(rel, metapin)?;
        return Ok(None);
    }

    let rootblkno = metad.btm_fastroot;
    debug_assert!(rootblkno != P_NONE);
    let rootlevel = metad.btm_fastlevel;

    rel.rd_amcache.set(Some(metad));

    // Metapage pin trades in via the first relandgetbuf, as in C.
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

fn prime_amcache(rel: &Relation<'_>) -> PgResult<Option<BTMetaPageData>> {
    if rel.rd_amcache.get().is_none() {
        let metapin = bt_getbuf(rel, BTREE_METAPAGE, BT_READ)?;
        let metad = bt_getmeta(rel, &metapin)?;
        if metad.btm_root == P_NONE {
            // No root yet: _bt_getroot doesn't expect a cache to exist.
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

/// _bt_metaversion: (heapkeyspace, allequalimage) for the insertion scankey.
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

const _: () = assert!(BT_READ != BT_WRITE);
