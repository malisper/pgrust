//! ginbtree.c: search/insert machinery shared by the entry and posting
//! trees. C's fn-pointer GinBtreeData becomes the monomorphized `GinBt`
//! trait (rule 4); the parent chain is arena frames + u32 handles.

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageRef, PageTemp};
use ::types_tuple::itemptr::InvalidOffsetNumber;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_FORCE_IMAGE, REGBUF_STANDARD};

use crate::{
    check_for_interrupts, page_bytes, page_mut, page_opaque, page_ref, relation_needs_wal,
    unported, util::gin_init_page_bytes, util::GinNewBuffer, write_opaque, GinPageIsData,
    GinPageIsIncompleteSplit, GinPageIsLeaf, GinPageRightMost, GIN_EXCLUSIVE, GIN_SHARE,
    GIN_UNLOCK, RM_GIN,
};

pub(crate) const NO_PARENT: u32 = u32::MAX;

#[derive(Clone, Copy)]
pub(crate) struct Frame {
    pub blkno: BlockNumber,
    pub buffer: Buffer,
    pub off: OffsetNumber,
    pub predictNumber: u32,
    pub parent: u32,
}

pub(crate) struct GinStack<'s> {
    pub frames: PgVec<'s, Frame>,
    pub top: u32,
}

impl<'s> GinStack<'s> {
    #[inline]
    pub fn top(&self) -> &Frame {
        &self.frames[self.top as usize]
    }
    #[inline]
    pub fn top_mut(&mut self) -> &mut Frame {
        let t = self.top as usize;
        &mut self.frames[t]
    }
    #[inline]
    pub fn frame(&self, id: u32) -> &Frame {
        &self.frames[id as usize]
    }
    #[inline]
    pub fn frame_mut(&mut self, id: u32) -> &mut Frame {
        &mut self.frames[id as usize]
    }
    fn push(&mut self, f: Frame) -> u32 {
        let id = self.frames.len() as u32;
        self.frames.push(f);
        id
    }
}

pub(crate) enum GinPlace {
    NoWork,
    Insert,
    Split(PageTemp, PageTemp),
}

/// The tree-specific half of ginbtree.c (C's GinBtreeData callbacks). The
/// insert payload and begin→exec workspace live in the implementor.
pub(crate) trait GinBt<'r> {
    // The index relation is threaded as an explicit parameter (borrowck:
    // trait-returned borrows would pin &self across &mut self calls).
    const IS_DATA: bool;

    fn root_blkno(&self) -> BlockNumber;
    fn is_build(&self) -> bool;
    fn full_scan(&self) -> bool;

    fn find_child_page(&self, page: &PageRef<'_>, frame: &mut Frame) -> BlockNumber;
    fn get_leftmost_child(&self, page: &PageRef<'_>) -> BlockNumber;
    fn is_move_right(&self, page: &PageRef<'_>) -> bool;
    fn find_child_ptr(
        &self,
        page: &PageRef<'_>,
        blkno: BlockNumber,
        stored_off: OffsetNumber,
    ) -> OffsetNumber;

    fn begin_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
        is_rightmost_insert_hint: bool,
    ) -> PgResult<GinPlace>;
    /// Page update inside the (conceptual) critical section; returns extra
    /// per-buffer WAL data for block 0 when WAL is needed.
    fn exec_place_to_page(
        &mut self,
        buf: Buffer,
        off: OffsetNumber,
        update_blkno: BlockNumber,
    ) -> PgResult<Vec<Vec<u8>>>;
    fn prepare_downlink(&mut self, lbuf: Buffer) -> PgResult<()>;
    fn fill_root(&self, root: &mut [u8], lblkno: BlockNumber, lpage: &[u8], rblkno: BlockNumber, rpage: &[u8]) -> PgResult<()>;
}

/// ginTraverseLock.
pub(crate) fn ginTraverseLock(buffer: Buffer, search_mode: bool) -> PgResult<i32> {
    let mut access = GIN_SHARE;
    bm::lock_buffer::call(buffer, GIN_SHARE)?;
    // SAFETY: pin + share lock.
    let is_leaf = { GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) };
    if is_leaf && !search_mode {
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
        // SAFETY: pin + exclusive lock.
        if !GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) {
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            bm::lock_buffer::call(buffer, GIN_SHARE)?;
        } else {
            access = GIN_EXCLUSIVE;
        }
    }
    Ok(access)
}

/// ginStepRight: lock-couple to the right sibling.
pub(crate) fn ginStepRight(buffer: Buffer, rel: &Relation<'_>, lockmode: i32) -> PgResult<Buffer> {
    // SAFETY: pin + lock held on buffer.
    let (is_leaf, is_data, blkno) = {
        let page = unsafe { page_ref(buffer) };
        let o = page_opaque(&page);
        (GinPageIsLeaf(&o), GinPageIsData(&o), o.rightlink)
    };
    let nextbuffer = bm::read_buffer::call(rel, blkno)?;
    bm::lock_buffer::call(nextbuffer, lockmode)?;
    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(buffer)?;

    // SAFETY: pin + lock held on nextbuffer.
    let o = page_opaque(&unsafe { page_ref(nextbuffer) });
    if is_leaf != GinPageIsLeaf(&o) || is_data != GinPageIsData(&o) {
        panic!("right sibling of GIN page is of different type");
    }
    Ok(nextbuffer)
}

/// freeGinBtreeStack: release every pinned buffer on the chain from `top`.
pub(crate) fn free_stack(stack: &GinStack<'_>, mut id: u32) -> PgResult<()> {
    while id != NO_PARENT {
        let f = stack.frame(id);
        if f.buffer != InvalidBuffer {
            bm::release_buffer::call(f.buffer)?;
        }
        id = f.parent;
    }
    Ok(())
}

/// ginFindLeafPage. On return stack.top is the leaf frame: exclusively
/// locked with the full root path when !search_mode, share-locked with no
/// parent chain otherwise.
pub(crate) fn ginFindLeafPage<'s, 'r, T: GinBt<'r>>(
    mcx: Mcx<'s>,
    rel: &Relation<'r>,
    btree: &mut T,
    search_mode: bool,
    root_conflict_check: bool,
) -> PgResult<GinStack<'s>> {
    let mut stack = GinStack {
        frames: mcx::vec_new_in(mcx),
        top: 0,
    };
    let root = btree.root_blkno();
    stack.push(Frame {
        blkno: root,
        buffer: bm::read_buffer::call(rel, root)?,
        off: InvalidOffsetNumber,
        predictNumber: 1,
        parent: NO_PARENT,
    });

    if root_conflict_check {
        predicate_seams::check_for_serializable_conflict_in::call(rel, None, root)?;
    }

    loop {
        stack.top_mut().off = InvalidOffsetNumber;
        let buffer = stack.top().buffer;

        let access = ginTraverseLock(buffer, search_mode)?;

        // SAFETY: pin + lock held.
        if !search_mode
            && GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(buffer) }))
        {
            let top = stack.top;
            ginFinishOldSplitAt(mcx, rel, btree, &mut stack, top, None, access)?;
        }

        loop {
            let buffer = stack.top().buffer;
            // SAFETY: pin + lock held.
            let page = unsafe { page_ref(buffer) };
            if btree.full_scan()
                || stack.top().blkno == btree.root_blkno()
                || !btree.is_move_right(&page)
            {
                break;
            }
            let rightlink = page_opaque(&page).rightlink;
            if rightlink == InvalidBlockNumber {
                break;
            }
            let next = ginStepRight(buffer, rel, access)?;
            {
                let top = stack.top_mut();
                top.buffer = next;
                top.blkno = rightlink;
            }
            // SAFETY: pin + lock held.
            if !search_mode
                && GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(next) }))
            {
                let top = stack.top;
                ginFinishOldSplitAt(mcx, rel, btree, &mut stack, top, None, access)?;
            }
        }

        let buffer = stack.top().buffer;
        // SAFETY: pin + lock held.
        let opaque = page_opaque(&unsafe { page_ref(buffer) });
        if GinPageIsLeaf(&opaque) {
            return Ok(stack);
        }

        // SAFETY: pin + lock held for the child lookup.
        let child = {
            let page = unsafe { page_ref(buffer) };
            let top = stack.top as usize;
            btree.find_child_page(&page, &mut stack.frames[top])
        };
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        debug_assert!(child != InvalidBlockNumber && stack.top().blkno != child);

        if search_mode {
            let top = stack.top_mut();
            top.blkno = child;
            top.buffer = bm::release_and_read_buffer::call(top.buffer, rel, child)?;
        } else {
            let parent = stack.top;
            let id = stack.push(Frame {
                blkno: child,
                buffer: bm::read_buffer::call(rel, child)?,
                off: InvalidOffsetNumber,
                predictNumber: 1,
                parent,
            });
            stack.top = id;
        }
    }
}

/// ginFindParents: rebuild the parent chain of frame `at` from the retained
/// root frame.
fn ginFindParents<'r, T: GinBt<'r>>(
    mcx: Mcx<'_>,
    rel: &Relation<'r>,
    btree: &mut T,
    stack: &mut GinStack<'_>,
    at: u32,
) -> PgResult<()> {

    // Unwind to the root, releasing intermediate pins (never the root's).
    let mut root_id = stack.frame(at).parent;
    while stack.frame(root_id).parent != NO_PARENT {
        bm::release_buffer::call(stack.frame(root_id).buffer)?;
        root_id = stack.frame(root_id).parent;
    }
    debug_assert!(stack.frame(root_id).blkno == btree.root_blkno());
    stack.frame_mut(root_id).off = InvalidOffsetNumber;

    let mut blkno = stack.frame(root_id).blkno;
    let mut buffer = stack.frame(root_id).buffer;

    loop {
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
        // SAFETY: pin + exclusive lock held.
        if GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) {
            panic!("Lost path");
        }

        // SAFETY: pin + exclusive lock held.
        if GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(buffer) })) {
            debug_assert!(blkno != btree.root_blkno());
            let ptr = stack.push(Frame {
                blkno,
                buffer,
                off: InvalidOffsetNumber,
                predictNumber: 1,
                parent: root_id,
            });
            ginFinishSplitAt(mcx, rel, btree, stack, ptr, false, None)?;
        }

        // SAFETY: pin + exclusive lock held.
        let leftmost = { btree.get_leftmost_child(&unsafe { page_ref(buffer) }) };

        let mut offset;
        loop {
            let target = stack.frame(at).blkno;
            // SAFETY: pin + exclusive lock held.
            offset = {
                let page = unsafe { page_ref(buffer) };
                btree.find_child_ptr(&page, target, InvalidOffsetNumber)
            };
            if offset != InvalidOffsetNumber {
                break;
            }
            // SAFETY: pin + exclusive lock held.
            let rightlink = page_opaque(&unsafe { page_ref(buffer) }).rightlink;
            if rightlink == InvalidBlockNumber {
                bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
                if buffer != stack.frame(root_id).buffer {
                    bm::release_buffer::call(buffer)?;
                }
                blkno = InvalidBlockNumber;
                break;
            }
            blkno = rightlink;
            buffer = ginStepRight(buffer, rel, GIN_EXCLUSIVE)?;
            // SAFETY: pin + exclusive lock held.
            if GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(buffer) })) {
                debug_assert!(blkno != btree.root_blkno());
                let ptr = stack.push(Frame {
                    blkno,
                    buffer,
                    off: InvalidOffsetNumber,
                    predictNumber: 1,
                    parent: root_id,
                });
                ginFinishSplitAt(mcx, rel, btree, stack, ptr, false, None)?;
            }
        }

        if blkno != InvalidBlockNumber {
            let ptr = stack.push(Frame {
                blkno,
                buffer,
                off: offset,
                predictNumber: 1,
                parent: root_id,
            });
            stack.frame_mut(at).parent = ptr;
            return Ok(());
        }

        blkno = leftmost;
        buffer = bm::read_buffer::call(rel, blkno)?;
    }
}

/// ginPlaceToPage. stack.frame(at) is locked on entry and kept locked.
fn ginPlaceToPage<'r, T: GinBt<'r>>(
    mcx: Mcx<'_>,
    rel: &Relation<'r>,
    btree: &mut T,
    stack: &mut GinStack<'_>,
    at: u32,
    update_blkno: BlockNumber,
    childbuf: Buffer,
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<bool> {
    let buffer = stack.frame(at).buffer;
    let off = stack.frame(at).off;

    // SAFETY: pin + exclusive lock held on buffer.
    let opaque = page_opaque(&unsafe { page_ref(buffer) });
    let mut xlflags: u16 = 0;
    if GinPageIsData(&opaque) {
        xlflags |= GIN_INSERT_ISDATA;
    }
    if GinPageIsLeaf(&opaque) {
        xlflags |= GIN_INSERT_ISLEAF;
        debug_assert!(childbuf == InvalidBuffer && update_blkno == InvalidBlockNumber);
    } else {
        debug_assert!(childbuf != InvalidBuffer && update_blkno != InvalidBlockNumber);
    }

    let is_root = stack.frame(at).parent == NO_PARENT;
    let rc = btree.begin_place_to_page(buffer, off, update_blkno, is_root)?;

    match rc {
        GinPlace::NoWork => Ok(true),
        GinPlace::Insert => {
            let wal = relation_needs_wal(rel) && !btree.is_build();
            let bufdata0 = btree.exec_place_to_page(buffer, off, update_blkno)?;

            if childbuf != InvalidBuffer {
                // SAFETY: pin + exclusive lock held on childbuf.
                let mut childpage = unsafe { page_mut(childbuf) };
                let mut o = page_opaque(&childpage.as_ref());
                o.flags &= !GIN_INCOMPLETE_SPLIT;
                write_opaque(&mut childpage, &o);
                bm::mark_buffer_dirty::call(childbuf)?;
            }

            if wal {
                let xlrec = crate::wal::ginxlog_insert(xlflags);
                let frags: Vec<&[u8]> = bufdata0.iter().map(|v| v.as_slice()).collect();
                let recptr = if childbuf != InvalidBuffer {
                    let cb = bm::buffer_get_block_number::call(childbuf);
                    // SAFETY: pin + lock held on childbuf.
                    let crl = page_opaque(&unsafe { page_ref(childbuf) }).rightlink;
                    // BlockIdData[2]: {bi_hi, bi_lo} pairs.
                    let mut both = [0u8; 8];
                    both[0..2].copy_from_slice(&((cb >> 16) as u16).to_ne_bytes());
                    both[2..4].copy_from_slice(&((cb & 0xffff) as u16).to_ne_bytes());
                    both[4..6].copy_from_slice(&((crl >> 16) as u16).to_ne_bytes());
                    both[6..8].copy_from_slice(&((crl & 0xffff) as u16).to_ne_bytes());
                    ::xloginsert_seams::xlog_insert_record::call(
                        RM_GIN,
                        XLOG_GIN_INSERT,
                        0,
                        &[&xlrec, &both],
                        &[
                            XLogRegBuf {
                                block_id: 0,
                                buffer,
                                flags: REGBUF_STANDARD,
                                bufdata: &frags,
                            },
                            XLogRegBuf {
                                block_id: 1,
                                buffer: childbuf,
                                flags: REGBUF_STANDARD,
                                bufdata: &[],
                            },
                        ],
                    )?
                } else {
                    ::xloginsert_seams::xlog_insert_record::call(
                        RM_GIN,
                        XLOG_GIN_INSERT,
                        0,
                        &[&xlrec],
                        &[XLogRegBuf {
                            block_id: 0,
                            buffer,
                            flags: REGBUF_STANDARD,
                            bufdata: &frags,
                        }],
                    )?
                };
                // SAFETY: pin + exclusive lock held.
                unsafe { page_mut(buffer) }.set_lsn(recptr);
                if childbuf != InvalidBuffer {
                    // SAFETY: pin + exclusive lock held.
                    unsafe { page_mut(childbuf) }.set_lsn(recptr);
                }
            }
            Ok(true)
        }
        GinPlace::Split(mut newlpage, mut newrpage) => {
            let rbuffer = GinNewBuffer(rel)?;
            if let Some(stats) = buildStats.as_deref_mut() {
                if T::IS_DATA {
                    stats.nDataPages += 1;
                } else {
                    stats.nEntryPages += 1;
                }
            }

            // SAFETY: pin + exclusive lock held.
            let saved_right_link = page_opaque(&unsafe { page_ref(buffer) }).rightlink;

            let (left_child, right_child) = if childbuf != InvalidBuffer {
                // SAFETY: pin + lock held on childbuf.
                let crl = page_opaque(&unsafe { page_ref(childbuf) }).rightlink;
                (bm::buffer_get_block_number::call(childbuf), crl)
            } else {
                (InvalidBlockNumber, InvalidBlockNumber)
            };

            let rblkno = bm::buffer_get_block_number::call(rbuffer);
            let mut lbuffer = InvalidBuffer;
            let mut newrootpg: Option<PageTemp> = None;
            let mut flags = xlflags;
            let rrlink;

            if is_root {
                lbuffer = GinNewBuffer(rel)?;
                if let Some(stats) = buildStats.as_deref_mut() {
                    if T::IS_DATA {
                        stats.nDataPages += 1;
                    } else {
                        stats.nEntryPages += 1;
                    }
                }
                rrlink = InvalidBlockNumber;
                flags |= GIN_SPLIT_ROOT;

                let lblkno = bm::buffer_get_block_number::call(lbuffer);
                {
                    let mut o = crate::opaque_of(newrpage.as_bytes());
                    o.rightlink = InvalidBlockNumber;
                    crate::write_opaque_to(newrpage.as_mut_bytes(), &o);
                    let mut o = crate::opaque_of(newlpage.as_bytes());
                    o.rightlink = rblkno;
                    crate::write_opaque_to(newlpage.as_mut_bytes(), &o);
                }

                let mut rootpg = PageTemp::new(BLCKSZ)?;
                let rootflags =
                    crate::opaque_of(newlpage.as_bytes()).flags & !(GIN_LEAF | GIN_COMPRESSED);
                gin_init_page_bytes(rootpg.as_mut_bytes(), rootflags);
                btree.fill_root(
                    rootpg.as_mut_bytes(),
                    lblkno,
                    newlpage.as_bytes(),
                    rblkno,
                    newrpage.as_bytes(),
                )?;
                newrootpg = Some(rootpg);

                // SAFETY: pin + exclusive lock held.
                if GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) {
                    let this = bm::buffer_get_block_number::call(buffer);
                    predicate_seams::predicate_lock_page_split::call(rel, this, lblkno)?;
                    predicate_seams::predicate_lock_page_split::call(rel, this, rblkno)?;
                }
            } else {
                rrlink = saved_right_link;
                let mut o = crate::opaque_of(newrpage.as_bytes());
                o.rightlink = saved_right_link;
                crate::write_opaque_to(newrpage.as_mut_bytes(), &o);
                let mut o = crate::opaque_of(newlpage.as_bytes());
                o.flags |= GIN_INCOMPLETE_SPLIT;
                o.rightlink = rblkno;
                crate::write_opaque_to(newlpage.as_mut_bytes(), &o);

                // SAFETY: pin + exclusive lock held.
                if GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) {
                    let this = bm::buffer_get_block_number::call(buffer);
                    predicate_seams::predicate_lock_page_split::call(rel, this, rblkno)?;
                }
            }

            bm::mark_buffer_dirty::call(rbuffer)?;
            bm::mark_buffer_dirty::call(buffer)?;

            if is_root {
                bm::mark_buffer_dirty::call(lbuffer)?;
                bm::overwrite_buffer_page::call(buffer, newrootpg.as_ref().unwrap().as_bytes());
                bm::overwrite_buffer_page::call(lbuffer, newlpage.as_bytes());
                bm::overwrite_buffer_page::call(rbuffer, newrpage.as_bytes());
            } else {
                bm::overwrite_buffer_page::call(buffer, newlpage.as_bytes());
                bm::overwrite_buffer_page::call(rbuffer, newrpage.as_bytes());
            }

            if childbuf != InvalidBuffer {
                // SAFETY: pin + exclusive lock held on childbuf.
                let mut childpage = unsafe { page_mut(childbuf) };
                let mut o = page_opaque(&childpage.as_ref());
                o.flags &= !GIN_INCOMPLETE_SPLIT;
                write_opaque(&mut childpage, &o);
                bm::mark_buffer_dirty::call(childbuf)?;
            }

            if relation_needs_wal(rel) && !btree.is_build() {
                let data = crate::wal::ginxlog_split(rel, rrlink, left_child, right_child, flags);
                let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(4);
                if is_root {
                    bufs.push(regbuf_image(0, lbuffer));
                    bufs.push(regbuf_image(1, rbuffer));
                    bufs.push(regbuf_image(2, buffer));
                } else {
                    bufs.push(regbuf_image(0, buffer));
                    bufs.push(regbuf_image(1, rbuffer));
                }
                if childbuf != InvalidBuffer {
                    bufs.push(XLogRegBuf {
                        block_id: 3,
                        buffer: childbuf,
                        flags: REGBUF_STANDARD,
                        bufdata: &[],
                    });
                }
                let recptr = ::xloginsert_seams::xlog_insert_record::call(
                    RM_GIN,
                    XLOG_GIN_SPLIT,
                    0,
                    &[&data],
                    &bufs,
                )?;
                // SAFETY: pins + exclusive locks held on all registered pages.
                unsafe {
                    page_mut(buffer).set_lsn(recptr);
                    page_mut(rbuffer).set_lsn(recptr);
                    if is_root {
                        page_mut(lbuffer).set_lsn(recptr);
                    }
                    if childbuf != InvalidBuffer {
                        page_mut(childbuf).set_lsn(recptr);
                    }
                }
            }

            bm::lock_buffer::call(rbuffer, GIN_UNLOCK)?;
            bm::release_buffer::call(rbuffer)?;
            if is_root {
                bm::lock_buffer::call(lbuffer, GIN_UNLOCK)?;
                bm::release_buffer::call(lbuffer)?;
            }

            let _ = mcx;
            Ok(is_root)
        }
    }
}

fn regbuf_image(block_id: u8, buffer: Buffer) -> XLogRegBuf<'static> {
    XLogRegBuf {
        block_id,
        buffer,
        flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
        bufdata: &[],
    }
}

/// ginFinishSplit starting at frame `at`.
fn ginFinishSplitAt<'r, T: GinBt<'r>>(
    mcx: Mcx<'_>,
    rel: &Relation<'r>,
    btree: &mut T,
    stack: &mut GinStack<'_>,
    mut at: u32,
    freestack: bool,
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut first = true;

    loop {
        let mut parent = stack.frame(at).parent;
        debug_assert!(parent != NO_PARENT, "split root without parent frame");

        bm::lock_buffer::call(stack.frame(parent).buffer, GIN_EXCLUSIVE)?;

        // SAFETY: pin + exclusive lock held.
        if GinPageIsIncompleteSplit(&page_opaque(&unsafe {
            page_ref(stack.frame(parent).buffer)
        })) {
            ginFinishOldSplitAt(mcx, rel, btree, stack, parent, buildStats.as_deref_mut(), GIN_EXCLUSIVE)?;
        }

        loop {
            let target = stack.frame(at).blkno;
            let pbuf = stack.frame(parent).buffer;
            // SAFETY: pin + exclusive lock held.
            let off = {
                let page = unsafe { page_ref(pbuf) };
                btree.find_child_ptr(&page, target, stack.frame(parent).off)
            };
            stack.frame_mut(parent).off = off;
            if off != InvalidOffsetNumber {
                break;
            }
            // SAFETY: pin + exclusive lock held.
            let o = page_opaque(&unsafe { page_ref(pbuf) });
            if GinPageRightMost(&o) {
                bm::lock_buffer::call(pbuf, GIN_UNLOCK)?;
                // Rebuild the parent chain with a plain search.
                ginFindParents(mcx, rel, btree, stack, at)?;
                parent = stack.frame(at).parent;
                break;
            }
            let next = ginStepRight(pbuf, rel, GIN_EXCLUSIVE)?;
            {
                let pf = stack.frame_mut(parent);
                pf.buffer = next;
                pf.blkno = bm::buffer_get_block_number::call(next);
            }
            // SAFETY: pin + exclusive lock held.
            if GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(next) })) {
                ginFinishOldSplitAt(mcx, rel, btree, stack, parent, buildStats.as_deref_mut(), GIN_EXCLUSIVE)?;
            }
        }

        let childbuf = stack.frame(at).buffer;
        btree.prepare_downlink(childbuf)?;
        // SAFETY: pin + exclusive lock held on childbuf.
        let update_blkno = page_opaque(&unsafe { page_ref(childbuf) }).rightlink;
        let done = ginPlaceToPage(
            mcx,
            rel,
            btree,
            stack,
            parent,
            update_blkno,
            childbuf,
            buildStats.as_deref_mut(),
        )?;

        if !first || freestack {
            bm::lock_buffer::call(stack.frame(at).buffer, GIN_UNLOCK)?;
        }
        if freestack {
            bm::release_buffer::call(stack.frame(at).buffer)?;
            stack.frame_mut(at).buffer = InvalidBuffer;
        }
        at = parent;
        first = false;

        if done {
            break;
        }
    }

    bm::lock_buffer::call(stack.frame(at).buffer, GIN_UNLOCK)?;

    if freestack {
        free_stack(stack, at)?;
    }
    Ok(())
}

/// ginFinishOldSplit at frame `at`: may be share-locked; upgraded to
/// exclusive. (Upgrading momentarily releases the lock — safe for inserts
/// only, as C notes; scans never reach this.)
pub(crate) fn ginFinishOldSplitAt<'r, T: GinBt<'r>>(
    mcx: Mcx<'_>,
    rel: &Relation<'r>,
    btree: &mut T,
    stack: &mut GinStack<'_>,
    at: u32,
    buildStats: Option<&mut GinStatsData>,
    access: i32,
) -> PgResult<()> {
    let buffer = stack.frame(at).buffer;
    if access == GIN_SHARE {
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
        // SAFETY: pin + exclusive lock held.
        if !GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(buffer) })) {
            return Ok(());
        }
    }
    ginFinishSplitAt(mcx, rel, btree, stack, at, false, buildStats)
}

/// ginInsertValue: consumes the stack (buffers released on completion).
pub(crate) fn ginInsertValue<'r, T: GinBt<'r>>(
    mcx: Mcx<'_>,
    rel: &Relation<'r>,
    btree: &mut T,
    stack: &mut GinStack<'_>,
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    check_for_interrupts()?;
    let buffer = stack.top().buffer;
    // SAFETY: pin + exclusive lock held (ginFindLeafPage !searchMode).
    if GinPageIsIncompleteSplit(&page_opaque(&unsafe { page_ref(buffer) })) {
        let top = stack.top;
        ginFinishOldSplitAt(mcx, rel, btree, stack, top, buildStats.as_deref_mut(), GIN_EXCLUSIVE)?;
    }

    let top = stack.top;
    let done = ginPlaceToPage(
        mcx,
        rel,
        btree,
        stack,
        top,
        InvalidBlockNumber,
        InvalidBuffer,
        buildStats.as_deref_mut(),
    )?;
    if done {
        bm::lock_buffer::call(stack.top().buffer, GIN_UNLOCK)?;
        free_stack(stack, top)?;
    } else {
        ginFinishSplitAt(mcx, rel, btree, stack, top, true, buildStats)?;
    }
    Ok(())
}


