//! gist.c insert spine: gistdoinsert, gistplacetopage, gistSplit, the
//! downlink machinery (gistinserttuple(s)/gistfinishsplit/gistformdownlink/
//! gistFindPath/gistFindCorrectParent/gistfixsplit), gistprunepage.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{Mcx, PgVec};
use ::nbtree::itup::ItupBuf;
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber, XLogRecPtr};
use ::types_error::PgResult;
use ::types_gist::{
    page_opaque, page_opaque_update, GistBuildLSN, GistFollowRight, GistPageGetNSN,
    GistPageIsDeleted, GistPageIsLeaf, GistNSN, F_FOLLOW_RIGHT, F_LEAF, GIST_MAX_SPLIT_PAGES,
    GIST_ROOT_BLKNO,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageTemp, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;

use crate::split::{gistSplitByKey, GistSplitVector};
use crate::state::GistState;
use crate::util::{
    copy_itup, gist_init_buffer, gist_tuple_is_invalid, gist_tuple_set_valid, gistGetFakeLSN,
    gistcheckpage, gistchoose, gistextractpage, gistfillbuffer, gistfillitupvec, gistfitpage,
    gistgetadjusted, gistnospace, gistNewBuffer, index_tuple_size, itup_block_number,
    itup_get_tid, itup_set_block_number, itup_slice, page_item, FirstOffsetNumber,
    InvalidOffsetNumber, ITup,
};
use crate::{buf_page_mut, relation_needs_wal};

const GIST_UNLOCK: i32 = bufmgr::BUFFER_LOCK_UNLOCK;
const GIST_SHARE: i32 = bufmgr::BUFFER_LOCK_SHARE;
const GIST_EXCLUSIVE: i32 = bufmgr::BUFFER_LOCK_EXCLUSIVE;

pub struct SplitPageLayout<'m> {
    pub blkno: BlockNumber,
    pub block_num: i32,
    pub list: PgVec<'m, u8>,
    pub itup: Option<ItupBuf<'m>>,
    pub buf: Buffer,
    pub pin: Option<BufferPin>,
    pub temp_page: Option<PageTemp>,
}

pub struct GISTPageSplitInfo<'m> {
    pub buf: Buffer,
    pub pin: Option<BufferPin>,
    pub downlink: ItupBuf<'m>,
}

struct Frame {
    blkno: BlockNumber,
    pin: Option<BufferPin>,
    lsn: XLogRecPtr,
    retry_from_parent: bool,
    downlinkoffnum: OffsetNumber,
    parent: Option<usize>,
}

pub struct InsertState<'a, 'mcx> {
    pub r: &'a Relation<'mcx>,
    pub heap_rel: &'a Relation<'mcx>,
    pub freespace: usize,
    pub is_build: bool,
    frames: Vec<Frame>,
    current: usize,
}

fn unlock(buffer: Buffer) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, GIST_UNLOCK)
}

fn lock(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}

// PageGetTempPageCopySpecial.
fn page_get_temp_page_copy_special(src: &::types_storage::bufpage::PageRef<'_>) -> PgResult<PageTemp> {
    let mut temp = PageTemp::new(::types_core::BLCKSZ)
        .map_err(|e| -> Box<::types_error::PgError> { Box::new(e) })?;
    let special = src.pd_special() as usize;
    {
        // SAFETY: PageTemp owns a full BLCKSZ buffer.
        let mut pm = unsafe {
            PageMut::from_raw(core::ptr::NonNull::new_unchecked(
                temp.as_mut_bytes().as_mut_ptr(),
            ))
        };
        pm.init(::types_core::BLCKSZ - special);
    }
    // copy the special area verbatim
    let bytes = temp.as_mut_bytes();
    // SAFETY: src page is BLCKSZ readable (PageRef contract).
    let src_special =
        unsafe { core::slice::from_raw_parts(src.as_ptr().add(special), ::types_core::BLCKSZ - special) };
    bytes[special..].copy_from_slice(src_special);
    Ok(temp)
}

fn temp_page_mut(temp: &mut PageTemp) -> PageMut<'_> {
    // SAFETY: PageTemp owns a full, exclusively borrowed BLCKSZ buffer.
    unsafe {
        PageMut::from_raw(core::ptr::NonNull::new_unchecked(
            temp.as_mut_bytes().as_mut_ptr(),
        ))
    }
}

/// gistplacetopage. `buffer` must be pinned + exclusively locked by caller
/// and stays so on return. Returns (is_split, splitinfo); `newblkno` receives
/// the block the first new/updated tuple landed on.
#[allow(clippy::too_many_arguments)]
pub fn gistplacetopage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    freespace: usize,
    giststate: &mut GistState<'_>,
    buffer: &BufferPin,
    itup: &[&[u8]],
    oldoffnum: OffsetNumber,
    leftchildbuf: Option<&BufferPin>,
    markfollowright: bool,
    heap_rel: &Relation<'_>,
    is_build: bool,
    mut newblkno: Option<&mut BlockNumber>,
) -> PgResult<(bool, Vec<GISTPageSplitInfo<'mcx>>)> {
    let blkno = buffer.block_number();
    let page = buffer.page();
    let is_leaf = GistPageIsLeaf(&page);

    if GistFollowRight(&page) {
        panic!("concurrent GiST page split was incomplete");
    }
    debug_assert!(!GistPageIsDeleted(&page));

    let mut splitinfo: Vec<GISTPageSplitInfo<'mcx>> = Vec::new();

    let mut is_split = gistnospace(&page, itup, oldoffnum, freespace);

    if is_split && is_leaf && ::types_gist::GistPageHasGarbage(&page) {
        gistprunepage(rel, buffer, heap_rel)?;
        let page = buffer.page();
        is_split = gistnospace(&page, itup, oldoffnum, freespace);
    }

    let recptr: XLogRecPtr;
    if is_split {
        let is_rootsplit = blkno == GIST_ROOT_BLKNO;

        // form the tuple vector, dropping the old version when replacing
        let extracted = gistextractpage(mcx, &buffer.page())?;
        let mut itvec: Vec<ITup> = Vec::with_capacity(extracted.len() + itup.len());
        for (i, t) in extracted.iter().enumerate() {
            if oldoffnum != InvalidOffsetNumber && (i as OffsetNumber) + 1 == oldoffnum {
                continue;
            }
            itvec.push(t.as_ptr());
        }
        for t in itup {
            itvec.push(t.as_ptr());
        }

        let mut dist = gistSplit(mcx, rel, &itvec, giststate)?;

        let mut npage = dist.len();
        if is_rootsplit {
            npage += 1;
        }
        if npage > GIST_MAX_SPLIT_PAGES {
            panic!(
                "GiST page split into too many halves ({npage}, maximum {GIST_MAX_SPLIT_PAGES})"
            );
        }

        let mut oldrlink = InvalidBlockNumber;
        let mut oldnsn: GistNSN = 0;

        // assign pages/buffers
        let mut start = 0usize;
        if !is_rootsplit {
            let page = buffer.page();
            let op = page_opaque(&page);
            oldrlink = op.rightlink;
            oldnsn = op.nsn;

            dist[0].buf = buffer.buffer();
            dist[0].blkno = blkno;
            let mut temp = page_get_temp_page_copy_special(&page)?;
            {
                let mut pm = temp_page_mut(&mut temp);
                page_opaque_update(&mut pm, |op| {
                    op.flags = if is_leaf { F_LEAF } else { 0 };
                });
            }
            dist[0].temp_page = Some(temp);
            start = 1;
        }
        for d in dist[start..].iter_mut() {
            let pin = gistNewBuffer(rel, heap_rel)?;
            gist_init_buffer(&pin, if is_leaf { F_LEAF } else { 0 });
            d.blkno = pin.block_number();
            d.buf = pin.buffer();
            predicate_seams::predicate_lock_page_split::call(rel, blkno, d.blkno)?;
            d.pin = Some(pin);
        }

        // point downlinks at their pages
        for d in dist.iter_mut() {
            let it = d.itup.as_mut().expect("split half has a downlink");
            // SAFETY: owned image; writes within the first 6 bytes.
            let sz = unsafe { index_tuple_size(it.as_ptr()) };
            let img = unsafe { core::slice::from_raw_parts_mut(it.as_mut_ptr(), sz) };
            itup_set_block_number(img, d.blkno);
            gist_tuple_set_valid(img);
        }

        if is_rootsplit {
            // build the new root page directly
            let mut root_temp = page_get_temp_page_copy_special(&buffer.page())?;
            {
                let mut pm = temp_page_mut(&mut root_temp);
                page_opaque_update(&mut pm, |op| op.flags = 0);
            }
            let downlinks: Vec<&[u8]> = dist
                .iter()
                .map(|d| {
                    let it = d.itup.as_ref().expect("downlink");
                    // SAFETY: owned live image.
                    unsafe { itup_slice(it.as_ptr()) }
                })
                .collect();
            let ndownlinks = downlinks.len() as i32;
            let list = gistfillitupvec(mcx, &downlinks)?;
            let rootpg = SplitPageLayout {
                blkno: GIST_ROOT_BLKNO,
                block_num: ndownlinks,
                list,
                itup: None,
                buf: buffer.buffer(),
                pin: None,
                temp_page: Some(root_temp),
            };
            dist.insert(0, rootpg);
        } else {
            for d in dist.iter() {
                let it = d.itup.as_ref().expect("downlink");
                splitinfo.push(GISTPageSplitInfo {
                    buf: d.buf,
                    pin: d.pin.as_ref().map(|p| p.incr_clone()),
                    downlink: copy_itup(mcx, it.as_ptr())?,
                });
            }
        }

        // fill all pages (freshly inited pages or temp copies)
        let ndist = dist.len();
        for i in 0..ndist {
            let next_blkno = if i + 1 < ndist { dist[i + 1].blkno } else { 0 };
            let has_next = i + 1 < ndist;
            let d = &mut dist[i];

            let tuples: Vec<&[u8]> = {
                let mut v = Vec::with_capacity(d.block_num as usize);
                let mut off = 0usize;
                for _ in 0..d.block_num {
                    let p = d.list[off..].as_ptr();
                    // SAFETY: list is the concatenated tuple images.
                    let sz = unsafe { index_tuple_size(p) };
                    v.push(&d.list[off..off + sz]);
                    off += sz;
                }
                v
            };

            if let Some(nb) = newblkno.as_deref_mut() {
                for t in &tuples {
                    if t[..6] == itup[0][..6] {
                        *nb = d.blkno;
                    }
                }
            }

            let fill = |pm: &mut PageMut<'_>| -> PgResult<()> {
                gistfillbuffer(rel.name(), pm, &tuples, FirstOffsetNumber)?;
                page_opaque_update(pm, |op| {
                    op.rightlink = if has_next && d.blkno != GIST_ROOT_BLKNO {
                        next_blkno
                    } else {
                        oldrlink
                    };
                    if has_next && !is_rootsplit && markfollowright {
                        op.flags |= F_FOLLOW_RIGHT;
                    } else {
                        op.flags &= !F_FOLLOW_RIGHT;
                    }
                    op.nsn = oldnsn;
                });
                Ok(())
            };

            if let Some(temp) = d.temp_page.as_mut() {
                let mut pm = temp_page_mut(temp);
                fill(&mut pm)?;
            } else {
                let mut pm = buf_page_mut(d.buf);
                fill(&mut pm)?;
            }
        }

        // "critical section": dirty buffers, restore temp copies, WAL
        for d in dist.iter() {
            bufmgr::mark_buffer_dirty::call(d.buf)?;
        }
        if let Some(lc) = leftchildbuf {
            bufmgr::mark_buffer_dirty::call(lc.buffer())?;
        }

        {
            // PageRestoreTempPage over the head (the original page's copy)
            let d = &mut dist[0];
            let temp = d.temp_page.take().expect("head is a temp copy");
            bufmgr::overwrite_buffer_page::call(d.buf, temp.as_bytes());
        }

        recptr = if is_build {
            GistBuildLSN
        } else if relation_needs_wal(rel) {
            crate::wal::gistXLogSplit(
                is_leaf,
                &dist,
                oldrlink,
                oldnsn,
                leftchildbuf,
                markfollowright,
            )?
        } else {
            gistGetFakeLSN(rel)?
        };

        for d in dist.iter() {
            buf_page_mut(d.buf).set_lsn(recptr);
        }

        if is_rootsplit {
            // downlinks are already in the new root; release the children
            for d in dist.drain(..).skip(1) {
                unlock(d.buf)?;
                drop(d.pin);
            }
        } else {
            // keep dist pins alive through splitinfo (incr_clone'd); drop ours
            // for the new pages so splitinfo holds the single owned pin.
            for d in dist.drain(..) {
                drop(d.pin);
            }
        }
    } else {
        // enough space
        let mut pm = buf_page_mut(buffer.buffer());
        if oldoffnum != InvalidOffsetNumber {
            if itup.len() == 1 {
                if !pm.index_tuple_overwrite(oldoffnum, itup[0]) {
                    panic!(
                        "failed to add item to index page in \"{}\"",
                        rel.name()
                    );
                }
            } else {
                pm.index_tuple_delete(oldoffnum);
                gistfillbuffer(rel.name(), &mut pm, itup, InvalidOffsetNumber)?;
            }
        } else {
            gistfillbuffer(rel.name(), &mut pm, itup, InvalidOffsetNumber)?;
        }

        bufmgr::mark_buffer_dirty::call(buffer.buffer())?;
        if let Some(lc) = leftchildbuf {
            bufmgr::mark_buffer_dirty::call(lc.buffer())?;
        }

        recptr = if is_build {
            GistBuildLSN
        } else if relation_needs_wal(rel) {
            let mut deloffs: [OffsetNumber; 1] = [0];
            let ndeloffs = if oldoffnum != InvalidOffsetNumber {
                deloffs[0] = oldoffnum;
                1
            } else {
                0
            };
            crate::wal::gistXLogUpdate(buffer, &deloffs[..ndeloffs], itup, leftchildbuf)?
        } else {
            gistGetFakeLSN(rel)?
        };
        buf_page_mut(buffer.buffer()).set_lsn(recptr);

        if let Some(nb) = newblkno {
            *nb = blkno;
        }
    }

    if let Some(lc) = leftchildbuf {
        let mut pm = buf_page_mut(lc.buffer());
        page_opaque_update(&mut pm, |op| {
            op.nsn = recptr;
            op.flags &= !F_FOLLOW_RIGHT;
        });
        pm.set_lsn(recptr);
    }

    Ok((is_split, splitinfo))
}

/// gistdoinsert. Assumes a short-lived `mcx` (reset by the caller per tuple).
pub fn gistdoinsert<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'_>,
    itup: ITup,
    freespace: usize,
    giststate: &mut GistState<'_>,
    heap_rel: &Relation<'_>,
    is_build: bool,
) -> PgResult<()> {
    let mut state = InsertState {
        r,
        heap_rel,
        freespace,
        is_build,
        frames: vec![Frame {
            blkno: GIST_ROOT_BLKNO,
            pin: None,
            lsn: 0,
            retry_from_parent: false,
            downlinkoffnum: InvalidOffsetNumber,
            parent: None,
        }],
        current: 0,
    };
    let mut xlocked = false;

    loop {
        while state.frames[state.current].retry_from_parent {
            let cur = state.current;
            if xlocked {
                unlock(state.frames[cur].pin.as_ref().expect("pinned").buffer())?;
            }
            xlocked = false;
            state.frames[cur].pin = None;
            state.current = state.frames[cur].parent.expect("retry has a parent");
        }

        let cur = state.current;
        if state.frames[cur].lsn == 0 && state.frames[cur].pin.is_none() {
            let pin = BufferPin::adopt(bufmgr::read_buffer::call(state.r, state.frames[cur].blkno)?)
                .expect("ReadBuffer");
            state.frames[cur].pin = Some(pin);
        }
        let buffer = state.frames[cur].pin.as_ref().expect("pinned").buffer();

        if !xlocked {
            lock(buffer, GIST_SHARE)?;
            gistcheckpage(state.r, state.frames[cur].pin.as_ref().expect("pinned"))?;
        }

        let lsn_now = if xlocked {
            state.frames[cur].pin.as_ref().expect("pinned").page().lsn()
        } else {
            bufmgr::buffer_get_lsn_atomic::call(buffer)
        };
        state.frames[cur].lsn = lsn_now;
        debug_assert!(!relation_needs_wal(state.r) || lsn_now != 0);

        let follow_right = {
            let page = state.frames[cur].pin.as_ref().expect("pinned").page();
            GistFollowRight(&page)
        };
        if follow_right {
            if !xlocked {
                unlock(buffer)?;
                lock(buffer, GIST_EXCLUSIVE)?;
                xlocked = true;
                let page = state.frames[cur].pin.as_ref().expect("pinned").page();
                if !GistFollowRight(&page) {
                    continue;
                }
            }
            gistfixsplit(mcx, &mut state, giststate)?;

            let cur = state.current;
            unlock(state.frames[cur].pin.as_ref().expect("pinned").buffer())?;
            state.frames[cur].pin = None;
            xlocked = false;
            state.current = state.frames[cur].parent.expect("followright frame has parent");
            continue;
        }

        let page = state.frames[cur].pin.as_ref().expect("pinned").page();
        let parent_lsn = state.frames[cur]
            .parent
            .map(|p| state.frames[p].lsn)
            .unwrap_or(0);
        if (state.frames[cur].blkno != GIST_ROOT_BLKNO && parent_lsn < GistPageGetNSN(&page))
            || GistPageIsDeleted(&page)
        {
            // concurrent split or page deletion: back to parent
            unlock(buffer)?;
            state.frames[cur].pin = None;
            xlocked = false;
            state.current = state.frames[cur].parent.expect("non-root frame has parent");
            continue;
        }

        if !GistPageIsLeaf(&page) {
            // internal page: descend along minimum-penalty child
            let downlinkoffnum = {
                let pin = state.frames[cur].pin.as_ref().expect("pinned");
                gistchoose(mcx, state.r, &pin.page(), itup, giststate)?
            };
            let (childblkno, invalid) = {
                let pin = state.frames[cur].pin.as_ref().expect("pinned");
                let idxtuple = page_item(&pin.page(), downlinkoffnum);
                // SAFETY: page item under our content lock.
                unsafe { (itup_block_number(idxtuple), gist_tuple_is_invalid(idxtuple)) }
            };

            if invalid {
                return Err(Box::new(
                    ::types_error::PgError::error(format!(
                        "index \"{}\" contains an inner tuple marked as invalid",
                        r.name()
                    ))
                    .with_detail(
                        "This is caused by an incomplete page split at crash recovery \
                         before upgrading to PostgreSQL 9.1.",
                    )
                    .with_hint("Please REINDEX it."),
                ));
            }

            let newtup = {
                let pin = state.frames[cur].pin.as_ref().expect("pinned");
                let idxtuple = page_item(&pin.page(), downlinkoffnum);
                gistgetadjusted(mcx, state.r, idxtuple, itup, giststate)?
            };
            if let Some(newtup) = newtup {
                if !xlocked {
                    unlock(buffer)?;
                    lock(buffer, GIST_EXCLUSIVE)?;
                    xlocked = true;
                    let page = state.frames[cur].pin.as_ref().expect("pinned").page();
                    if page.lsn() != state.frames[cur].lsn {
                        continue;
                    }
                }

                // SAFETY: owned newtup image live for the call.
                let newtup_slice = unsafe { itup_slice(newtup.as_ptr()) };
                if gistinserttuple(mcx, &mut state, cur, giststate, newtup_slice, downlinkoffnum)? {
                    // page split: retry from parent unless we're the root
                    if state.frames[cur].blkno != GIST_ROOT_BLKNO {
                        unlock(state.frames[cur].pin.as_ref().expect("pinned").buffer())?;
                        state.frames[cur].pin = None;
                        xlocked = false;
                        state.current = state.frames[cur].parent.expect("non-root");
                    }
                    continue;
                }
            }
            unlock(state.frames[cur].pin.as_ref().expect("pinned").buffer())?;
            xlocked = false;

            state.frames.push(Frame {
                blkno: childblkno,
                pin: None,
                lsn: 0,
                retry_from_parent: false,
                downlinkoffnum,
                parent: Some(cur),
            });
            state.current = state.frames.len() - 1;
        } else {
            // leaf page
            if !xlocked {
                unlock(buffer)?;
                lock(buffer, GIST_EXCLUSIVE)?;
                xlocked = true;
                let page_lsn = state.frames[cur].pin.as_ref().expect("pinned").page().lsn();
                state.frames[cur].lsn = page_lsn;
                let page = state.frames[cur].pin.as_ref().expect("pinned").page();

                if state.frames[cur].blkno == GIST_ROOT_BLKNO {
                    if !GistPageIsLeaf(&page) {
                        unlock(buffer)?;
                        xlocked = false;
                        continue;
                    }
                } else {
                    let parent_lsn = state.frames[cur]
                        .parent
                        .map(|p| state.frames[p].lsn)
                        .unwrap_or(0);
                    if GistFollowRight(&page)
                        || parent_lsn < GistPageGetNSN(&page)
                        || GistPageIsDeleted(&page)
                    {
                        unlock(buffer)?;
                        state.frames[cur].pin = None;
                        xlocked = false;
                        state.current = state.frames[cur].parent.expect("non-root");
                        continue;
                    }
                }
            }

            // SAFETY: caller-owned itup image, live for the call.
            let itup_s = unsafe { itup_slice(itup) };
            gistinserttuple(mcx, &mut state, cur, giststate, itup_s, InvalidOffsetNumber)?;
            unlock(state.frames[cur].pin.as_ref().expect("pinned").buffer())?;

            // release any pins still held
            let mut f = Some(cur);
            while let Some(i) = f {
                state.frames[i].pin = None;
                f = state.frames[i].parent;
            }
            break;
        }
    }
    Ok(())
}

// gistFindPath: build the parent chain from root to `child`'s parent, in
// state.frames; returns the parent frame index. Locks one page at a time.
fn gistFindPath(
    state: &mut InsertState<'_, '_>,
    child: BlockNumber,
) -> PgResult<(usize, OffsetNumber)> {
    state.frames.push(Frame {
        blkno: GIST_ROOT_BLKNO,
        pin: None,
        lsn: 0,
        retry_from_parent: false,
        downlinkoffnum: InvalidOffsetNumber,
        parent: None,
    });
    let top0 = state.frames.len() - 1;
    let mut fifo: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    fifo.push_back(top0);

    while let Some(top) = fifo.pop_front() {
        let pin = BufferPin::adopt(bufmgr::read_buffer::call(state.r, state.frames[top].blkno)?)
            .expect("ReadBuffer");
        lock(pin.buffer(), GIST_SHARE)?;
        gistcheckpage(state.r, &pin)?;
        let page = pin.page();

        if GistPageIsLeaf(&page) {
            unlock(pin.buffer())?;
            drop(pin);
            break;
        }
        debug_assert!(!GistPageIsDeleted(&page));

        state.frames[top].lsn = bufmgr::buffer_get_lsn_atomic::call(pin.buffer());

        if GistFollowRight(&page) {
            panic!("concurrent GiST page split was incomplete");
        }

        let parent_lsn = state.frames[top]
            .parent
            .map(|p| state.frames[p].lsn)
            .unwrap_or(0);
        let op = page_opaque(&page);
        if state.frames[top].parent.is_some()
            && parent_lsn < op.nsn
            && op.rightlink != InvalidBlockNumber
        {
            // split while we looked elsewhere; visit the right page next
            state.frames.push(Frame {
                blkno: op.rightlink,
                pin: None,
                lsn: 0,
                retry_from_parent: false,
                downlinkoffnum: InvalidOffsetNumber,
                parent: state.frames[top].parent,
            });
            fifo.push_front(state.frames.len() - 1);
        }

        let maxoff = page.max_offset_number();
        for i in FirstOffsetNumber..=maxoff {
            let idxtuple = page_item(&page, i);
            // SAFETY: page item under our content lock.
            let blkno = unsafe { itup_block_number(idxtuple) };
            if blkno == child {
                unlock(pin.buffer())?;
                drop(pin);
                return Ok((top, i));
            }
            state.frames.push(Frame {
                blkno,
                pin: None,
                lsn: 0,
                retry_from_parent: false,
                downlinkoffnum: i,
                parent: Some(top),
            });
            fifo.push_back(state.frames.len() - 1);
        }

        unlock(pin.buffer())?;
        drop(pin);
    }

    panic!(
        "failed to re-find parent of a page in index \"{}\", block {}",
        state.r.name(),
        child
    );
}

// gistFindCorrectParent: child's parent must be exclusively locked on entry
// and stays so on exit (possibly a different page).
fn gistFindCorrectParent(
    state: &mut InsertState<'_, '_>,
    child: usize,
) -> PgResult<()> {
    let parent = state.frames[child].parent.expect("child has a parent");
    gistcheckpage(state.r, state.frames[parent].pin.as_ref().expect("pinned"))?;

    {
        let pin = state.frames[parent].pin.as_ref().expect("pinned");
        let page = pin.page();
        let maxoff = page.max_offset_number();
        let dl = state.frames[child].downlinkoffnum;
        if dl != InvalidOffsetNumber && dl <= maxoff {
            let idxtuple = page_item(&page, dl);
            // SAFETY: page item under our exclusive lock.
            if unsafe { itup_block_number(idxtuple) } == state.frames[child].blkno {
                return Ok(());
            }
        }
    }

    // re-scan; follow rightlinks
    loop {
        let found = {
            let pin = state.frames[parent].pin.as_ref().expect("pinned");
            let page = pin.page();
            let maxoff = page.max_offset_number();
            let mut found = None;
            for i in FirstOffsetNumber..=maxoff {
                let idxtuple = page_item(&page, i);
                // SAFETY: page item under our exclusive lock.
                if unsafe { itup_block_number(idxtuple) } == state.frames[child].blkno {
                    found = Some(i);
                    break;
                }
            }
            found
        };
        if let Some(i) = found {
            state.frames[child].downlinkoffnum = i;
            return Ok(());
        }

        let rightlink = {
            let pin = state.frames[parent].pin.as_ref().expect("pinned");
            page_opaque(&pin.page()).rightlink
        };
        {
            let pin = state.frames[parent].pin.take().expect("pinned");
            unlock(pin.buffer())?;
            drop(pin);
        }
        state.frames[parent].blkno = rightlink;
        state.frames[parent].downlinkoffnum = InvalidOffsetNumber;
        if rightlink == InvalidBlockNumber {
            break;
        }
        let pin = BufferPin::adopt(bufmgr::read_buffer::call(state.r, rightlink)?)
            .expect("ReadBuffer");
        lock(pin.buffer(), GIST_EXCLUSIVE)?;
        gistcheckpage(state.r, &pin)?;
        state.frames[parent].pin = Some(pin);
    }

    // rightlink chain exhausted (root split race): release old parents and
    // re-find the path from the root
    let mut ptr = state.frames[parent].parent;
    while let Some(i) = ptr {
        state.frames[i].pin = None;
        ptr = state.frames[i].parent;
    }

    let (new_parent, downlinkoffnum) = gistFindPath(state, state.frames[child].blkno)?;
    state.frames[child].downlinkoffnum = downlinkoffnum;

    // read all buffers as expected by caller (no locks, no checks — C parity)
    let mut ptr = Some(new_parent);
    while let Some(i) = ptr {
        let pin = BufferPin::adopt(bufmgr::read_buffer::call(state.r, state.frames[i].blkno)?)
            .expect("ReadBuffer");
        state.frames[i].pin = Some(pin);
        ptr = state.frames[i].parent;
    }
    state.frames[child].parent = Some(new_parent);

    lock(
        state.frames[new_parent].pin.as_ref().expect("pinned").buffer(),
        GIST_EXCLUSIVE,
    )?;
    gistFindCorrectParent(state, child)
}

// gistformdownlink.
fn gistformdownlink<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InsertState<'_, '_>,
    buf: &BufferPin,
    giststate: &mut GistState<'_>,
    stack: usize,
) -> PgResult<ItupBuf<'mcx>> {
    let mut downlink: Option<ItupBuf<'mcx>> = None;
    {
        let page = buf.page();
        let maxoff = page.max_offset_number();
        for offset in FirstOffsetNumber..=maxoff {
            let ituple = page_item(&page, offset);
            downlink = Some(match downlink {
                None => copy_itup(mcx, ituple)?,
                Some(dl) => {
                    match gistgetadjusted(mcx, state.r, dl.as_ptr(), ituple, giststate)? {
                        Some(new_dl) => new_dl,
                        None => dl,
                    }
                }
            });
        }
    }

    let mut downlink = match downlink {
        Some(d) => d,
        None => {
            // empty page: borrow the original downlink from the parent
            let parent = state.frames[stack].parent.expect("stack has parent");
            lock(
                state.frames[parent].pin.as_ref().expect("pinned").buffer(),
                GIST_EXCLUSIVE,
            )?;
            gistFindCorrectParent(state, stack)?;
            let parent = state.frames[stack].parent.expect("stack has parent");
            let d = {
                let pin = state.frames[parent].pin.as_ref().expect("pinned");
                let page = pin.page();
                let iid = state.frames[stack].downlinkoffnum;
                copy_itup(mcx, page_item(&page, iid))?
            };
            unlock(state.frames[parent].pin.as_ref().expect("pinned").buffer())?;
            d
        }
    };

    // SAFETY: owned image.
    let sz = unsafe { index_tuple_size(downlink.as_ptr()) };
    let img = unsafe { core::slice::from_raw_parts_mut(downlink.as_mut_ptr(), sz) };
    itup_set_block_number(img, buf.block_number());
    gist_tuple_set_valid(img);
    Ok(downlink)
}

// gistfixsplit: complete the incomplete split of the current frame's page.
fn gistfixsplit<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InsertState<'_, '_>,
    giststate: &mut GistState<'_>,
) -> PgResult<()> {
    let stack = state.current;
    debug_assert!(state.frames[stack].downlinkoffnum != InvalidOffsetNumber);

    let mut splitinfo: Vec<GISTPageSplitInfo<'mcx>> = Vec::new();

    // walk the chain of split pages following rightlinks
    let mut cur: Option<BufferPin> = None;
    loop {
        let (buf_id, pin_for_call): (Buffer, Option<&BufferPin>) = match cur.as_ref() {
            Some(p) => (p.buffer(), Some(p)),
            None => (
                state.frames[stack].pin.as_ref().expect("pinned").buffer(),
                None,
            ),
        };
        let _ = buf_id;

        let (downlink, follow_right, rightlink) = {
            let pin: &BufferPin = match pin_for_call {
                Some(p) => p,
                None => state.frames[stack].pin.as_ref().expect("pinned"),
            };
            // borrow checker: form the downlink first (may relock parents)
            let pin_clone = pin.incr_clone();
            let downlink = gistformdownlink(mcx, state, &pin_clone, giststate, stack)?;
            let page = pin_clone.page();
            let fr = GistFollowRight(&page);
            let rl = page_opaque(&page).rightlink;
            pin_clone.release();
            (downlink, fr, rl)
        };

        {
            let (buf, pin_owned) = match cur.take() {
                Some(p) => (p.buffer(), Some(p)),
                None => (
                    state.frames[stack].pin.as_ref().expect("pinned").buffer(),
                    None,
                ),
            };
            splitinfo.push(GISTPageSplitInfo {
                buf,
                pin: pin_owned,
                downlink,
            });
        }

        if follow_right {
            let pin = BufferPin::adopt(bufmgr::read_buffer::call(state.r, rightlink)?)
                .expect("ReadBuffer");
            lock(pin.buffer(), GIST_EXCLUSIVE)?;
            cur = Some(pin);
        } else {
            break;
        }
    }

    gistfinishsplit(mcx, state, stack, giststate, splitinfo, false)
}

// gistinserttuple.
fn gistinserttuple<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InsertState<'_, '_>,
    stack: usize,
    giststate: &mut GistState<'_>,
    tuple: &[u8],
    oldoffnum: OffsetNumber,
) -> PgResult<bool> {
    gistinserttuples(
        mcx,
        state,
        stack,
        giststate,
        &[tuple],
        oldoffnum,
        None,
        None,
        false,
        false,
    )
}

// gistinserttuples; see gist.c for the locking contract.
#[allow(clippy::too_many_arguments)]
fn gistinserttuples<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InsertState<'_, '_>,
    stack: usize,
    giststate: &mut GistState<'_>,
    tuples: &[&[u8]],
    oldoffnum: OffsetNumber,
    leftchild: Option<&BufferPin>,
    rightchild: Option<BufferPin>,
    unlockbuf: bool,
    unlockleftchild: bool,
) -> PgResult<bool> {
    predicate_seams::check_for_serializable_conflict_in::call(
        state.r,
        None,
        state.frames[stack].pin.as_ref().expect("pinned").block_number(),
    )?;

    let (is_split, splitinfo) = {
        let pin = state.frames[stack].pin.as_ref().expect("pinned").incr_clone();
        let res = gistplacetopage(
            mcx,
            state.r,
            state.freespace,
            giststate,
            &pin,
            tuples,
            oldoffnum,
            leftchild,
            true,
            state.heap_rel,
            state.is_build,
            None,
        );
        pin.release();
        res?
    };

    if let Some(rc) = rightchild {
        unlock(rc.buffer())?;
        drop(rc);
    }
    if let Some(lc) = leftchild {
        if unlockleftchild {
            unlock(lc.buffer())?;
        }
    }

    if !splitinfo.is_empty() {
        gistfinishsplit(mcx, state, stack, giststate, splitinfo, unlockbuf)?;
    } else if unlockbuf {
        unlock(state.frames[stack].pin.as_ref().expect("pinned").buffer())?;
    }

    Ok(is_split)
}

// gistfinishsplit.
fn gistfinishsplit<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InsertState<'_, '_>,
    stack: usize,
    giststate: &mut GistState<'_>,
    mut splitinfo: Vec<GISTPageSplitInfo<'mcx>>,
    unlockbuf: bool,
) -> PgResult<()> {
    debug_assert!(splitinfo.len() >= 2);

    let parent = state.frames[stack].parent.expect("split page has a parent");
    lock(
        state.frames[parent].pin.as_ref().expect("pinned").buffer(),
        GIST_EXCLUSIVE,
    )?;

    // insert downlinks right-to-left until two remain
    while splitinfo.len() > 2 {
        let right = splitinfo.pop().expect("len > 2");
        let left_buf_pin = {
            let left = splitinfo.last().expect("len >= 2");
            match left.pin.as_ref() {
                Some(p) => p.incr_clone(),
                None => state.frames[stack]
                    .pin
                    .as_ref()
                    .expect("pinned")
                    .incr_clone(),
            }
        };

        gistFindCorrectParent(state, stack)?;
        let parent = state.frames[stack].parent.expect("has parent");
        // SAFETY: owned downlink image.
        let dl = unsafe { itup_slice(right.downlink.as_ptr()) };
        if gistinserttuples(
            mcx,
            state,
            parent,
            giststate,
            &[dl],
            InvalidOffsetNumber,
            Some(&left_buf_pin),
            right.pin,
            false,
            false,
        )? {
            state.frames[stack].downlinkoffnum = InvalidOffsetNumber;
        }
        left_buf_pin.release();
    }

    let right = splitinfo.pop().expect("two remain");
    let left = splitinfo.pop().expect("one remains");
    debug_assert!(splitinfo.is_empty());
    debug_assert!(
        left.buf == state.frames[stack].pin.as_ref().expect("pinned").buffer()
    );

    let left_pin_for_child = state.frames[stack].pin.as_ref().expect("pinned").incr_clone();
    gistFindCorrectParent(state, stack)?;
    let parent = state.frames[stack].parent.expect("has parent");
    // SAFETY: owned downlink images.
    let dl_left = unsafe { itup_slice(left.downlink.as_ptr()) };
    let dl_right = unsafe { itup_slice(right.downlink.as_ptr()) };
    let downlinkoffnum = state.frames[stack].downlinkoffnum;
    gistinserttuples(
        mcx,
        state,
        parent,
        giststate,
        &[dl_left, dl_right],
        downlinkoffnum,
        Some(&left_pin_for_child),
        right.pin,
        true,
        unlockbuf,
    )?;
    left_pin_for_child.release();
    drop(left.pin);

    state.frames[stack].downlinkoffnum = InvalidOffsetNumber;
    state.frames[stack].retry_from_parent = true;
    Ok(())
}

/// gistSplit: recursive multi-page split of the tuple vector.
pub fn gistSplit<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'_>,
    itup: &[ITup],
    giststate: &mut GistState<'_>,
) -> PgResult<Vec<SplitPageLayout<'mcx>>> {
    let len = itup.len();
    debug_assert!(len > 0);

    if len == 1 {
        // SAFETY: caller-owned live image.
        let sz = unsafe { index_tuple_size(itup[0]) };
        return Err(Box::new(
            ::types_error::PgError::error(format!(
                "index row size {sz} exceeds maximum {} for index \"{}\"",
                ::types_gist::GiSTPageSize,
                r.name()
            ))
            .with_sqlstate(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }

    let mut v = GistSplitVector::new();
    gistSplitByKey(mcx, r, itup, giststate, &mut v, 0)?;

    let lvectup: Vec<ITup> = v
        .splitVector
        .spl_left
        .iter()
        .map(|&i| itup[i as usize - 1])
        .collect();
    let rvectup: Vec<ITup> = v
        .splitVector
        .spl_right
        .iter()
        .map(|&i| itup[i as usize - 1])
        .collect();

    let rvec_slices: Vec<&[u8]> = rvectup
        .iter()
        // SAFETY: caller-owned live images.
        .map(|&p| unsafe { itup_slice(p) })
        .collect();
    let lvec_slices: Vec<&[u8]> = lvectup
        .iter()
        // SAFETY: caller-owned live images.
        .map(|&p| unsafe { itup_slice(p) })
        .collect();

    let mut res: Vec<SplitPageLayout<'mcx>>;
    if !gistfitpage(&rvec_slices) {
        res = gistSplit(mcx, r, &rvectup, giststate)?;
    } else {
        let list = gistfillitupvec(mcx, &rvec_slices)?;
        let itup_dl = gistFormTupleFromSplit(mcx, giststate, r, &v, false)?;
        res = vec![SplitPageLayout {
            blkno: InvalidBlockNumber,
            block_num: rvec_slices.len() as i32,
            list,
            itup: Some(itup_dl),
            buf: 0,
            pin: None,
            temp_page: None,
        }];
    }

    if !gistfitpage(&lvec_slices) {
        let mut subres = gistSplit(mcx, r, &lvectup, giststate)?;
        subres.append(&mut res);
        res = subres;
    } else {
        let list = gistfillitupvec(mcx, &lvec_slices)?;
        let itup_dl = gistFormTupleFromSplit(mcx, giststate, r, &v, true)?;
        res.insert(
            0,
            SplitPageLayout {
                blkno: InvalidBlockNumber,
                block_num: lvec_slices.len() as i32,
                list,
                itup: Some(itup_dl),
                buf: 0,
                pin: None,
                temp_page: None,
            },
        );
    }

    Ok(res)
}

fn gistFormTupleFromSplit<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    v: &GistSplitVector,
    left: bool,
) -> PgResult<ItupBuf<'mcx>> {
    let n = giststate.nonLeafTupdesc.natts as usize;
    if left {
        crate::util::gistFormTuple(mcx, giststate, r, &v.spl_lattr[..n], &v.spl_lisnull[..n], false)
    } else {
        crate::util::gistFormTuple(mcx, giststate, r, &v.spl_rattr[..n], &v.spl_risnull[..n], false)
    }
}

// gistprunepage: remove LP_DEAD items; buffer exclusively locked.
fn gistprunepage(
    rel: &Relation<'_>,
    buffer: &BufferPin,
    heap_rel: &Relation<'_>,
) -> PgResult<()> {
    let mut deletable: Vec<OffsetNumber> = Vec::new();
    {
        let page = buffer.page();
        debug_assert!(GistPageIsLeaf(&page));
        let maxoff = page.max_offset_number();
        for offnum in FirstOffsetNumber..=maxoff {
            let item_id = page.item_id(offnum);
            if item_id.is_dead() {
                deletable.push(offnum);
            }
        }
    }

    if !deletable.is_empty() {
        // unported: index_compute_xid_horizon_for_tuples (standby conflict
        // horizon; C gistprunepage computes it iff XLogStandbyInfoActive() &&
        // RelationNeedsWAL). Clean 0A000 in place of the old panic: nothing
        // has been mutated yet (page edit + WAL follow below), so failing the
        // triggering DML here is unwind-safe. A fabricated horizon would
        // silently break hot-standby conflict detection for C replayers.
        let snapshot_conflict_horizon: ::types_core::TransactionId =
            if transam_xlog_seams::xlog_standby_info_active::call() && relation_needs_wal(rel) {
                unported_xid_horizon()?
            } else {
                0
            };

        {
            let mut pm = buf_page_mut(buffer.buffer());
            pm.index_multi_delete(&deletable);
            page_opaque_update(&mut pm, |op| op.flags &= !::types_gist::F_HAS_GARBAGE);
        }
        bufmgr::mark_buffer_dirty::call(buffer.buffer())?;

        let recptr = if relation_needs_wal(rel) {
            crate::wal::gistXLogDelete(buffer, &deletable, snapshot_conflict_horizon, heap_rel)?
        } else {
            gistGetFakeLSN(rel)?
        };
        buf_page_mut(buffer.buffer()).set_lsn(recptr);
    }
    Ok(())
}

const _: () = {
    // itup_get_tid currently unused but kept for the scan layer.
    let _ = itup_get_tid;
    let _ = ItemPointerData::invalid;
};

// unported: shared 0A000 for the missing xid-horizon callee (see
// gistprunepage); mirrors genam::index_compute_xid_horizon_for_tuples.
#[cold]
#[inline(never)]
fn unported_xid_horizon() -> ::types_error::PgResult<::types_core::TransactionId> {
    Err(Box::new(
        ::types_error::PgError::error(
            "reuse of dead index entries is not supported (index_compute_xid_horizon_for_tuples unported)",
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}
