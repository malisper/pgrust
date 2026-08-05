use core::ptr::NonNull;

use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use ::types_core::BLCKSZ;
use ::types_error::PgResult;
use ::types_storage::bufpage::SizeOfPageHeaderData;

const CONTENTS_OFF: usize = (SizeOfPageHeaderData + 7) & !7;
const FP_NODES_OFF: usize = CONTENTS_OFF + 4;

pub const NON_LEAF_NODES_PER_PAGE: i32 = (BLCKSZ / 2 - 1) as i32;
pub const NODES_PER_PAGE: i32 = (BLCKSZ - CONTENTS_OFF - 4) as i32;
pub const LEAF_NODES_PER_PAGE: i32 = NODES_PER_PAGE - NON_LEAF_NODES_PER_PAGE;
pub const SLOTS_PER_FSM_PAGE: i32 = LEAF_NODES_PER_PAGE;

/// FSMPageData view over a pinned page (fsm_internals.h); node indices are
/// C's signed ints (navigation computes transient out-of-range values).
#[derive(Clone, Copy)]
pub struct FsmPage(NonNull<u8>);

impl FsmPage {
    /// SAFETY: `page` is a live BLCKSZ buffer page pinned for this value's
    /// lifetime; writes follow C's lock discipline.
    #[inline]
    pub unsafe fn from_raw(page: NonNull<u8>) -> FsmPage {
        FsmPage(page)
    }

    #[inline(always)]
    pub(crate) fn node(self, nodeno: i32) -> u8 {
        debug_assert!((0..NODES_PER_PAGE).contains(&nodeno));
        // SAFETY: FP_NODES_OFF + nodeno < BLCKSZ by the assert; live per from_raw.
        unsafe { *self.0.as_ptr().add(FP_NODES_OFF + nodeno as usize) }
    }

    #[inline(always)]
    pub(crate) fn set_node(self, nodeno: i32, value: u8) {
        debug_assert!((0..NODES_PER_PAGE).contains(&nodeno));
        // SAFETY: in-page by the assert; write discipline per from_raw.
        unsafe { *self.0.as_ptr().add(FP_NODES_OFF + nodeno as usize) = value }
    }

    #[inline(always)]
    pub(crate) fn next_slot(self) -> i32 {
        // SAFETY: CONTENTS_OFF is 4-aligned and in-page.
        unsafe { self.0.as_ptr().add(CONTENTS_OFF).cast::<i32>().read() }
    }

    #[inline(always)]
    pub(crate) fn set_next_slot(self, value: i32) {
        // SAFETY: as next_slot; racy-hint write discipline per from_raw.
        unsafe { self.0.as_ptr().add(CONTENTS_OFF).cast::<i32>().write(value) }
    }
}

#[inline(always)]
fn leftchild(x: i32) -> i32 {
    2 * x + 1
}

#[inline(always)]
fn parentof(x: i32) -> i32 {
    (x - 1) / 2
}

#[inline(always)]
fn rightneighbor(mut x: i32) -> i32 {
    x += 1;
    // (x+1) a power of two = stepped past the level's right edge; wrap.
    if ((x + 1) & x) == 0 {
        x = parentof(x);
    }
    x
}

pub fn fsm_set_avail(page: FsmPage, slot: i32, value: u8) -> bool {
    debug_assert!(slot < LEAF_NODES_PER_PAGE);
    let mut nodeno = NON_LEAF_NODES_PER_PAGE + slot;

    let oldvalue = page.node(nodeno);
    if oldvalue == value && value <= page.node(0) {
        return false;
    }
    page.set_node(nodeno, value);

    loop {
        nodeno = parentof(nodeno);
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;

        let mut newvalue = page.node(lchild);
        if rchild < NODES_PER_PAGE {
            newvalue = newvalue.max(page.node(rchild));
        }
        if page.node(nodeno) == newvalue {
            break;
        }
        page.set_node(nodeno, newvalue);
        if nodeno == 0 {
            break;
        }
    }

    // Value still above the root: the tree was already corrupt — rebuild.
    if value > page.node(0) {
        fsm_rebuild_page(page);
    }
    true
}

pub fn fsm_get_avail(page: FsmPage, slot: i32) -> u8 {
    debug_assert!(slot < LEAF_NODES_PER_PAGE);
    page.node(NON_LEAF_NODES_PER_PAGE + slot)
}

pub fn fsm_get_max_avail(page: FsmPage) -> u8 {
    page.node(0)
}

/// Caller holds the pin and at least a shared content lock, as C.
pub fn fsm_search_avail(
    pin: &BufferPin,
    minvalue: u8,
    advancenext: bool,
    mut exclusive_lock_held: bool,
) -> PgResult<i32> {
    // SAFETY: pinned + content-locked by the caller (C contract).
    let page = unsafe { FsmPage::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };

    'restart: loop {
        if page.node(0) < minvalue {
            return Ok(-1);
        }

        // fp_next_slot is only a hint — sanitize (this is also the wraparound).
        let mut target = page.next_slot();
        if !(0..LEAF_NODES_PER_PAGE).contains(&target) {
            target = 0;
        }
        target += NON_LEAF_NODES_PER_PAGE;

        // Expanding search triangle: right (wrapping) then climb; log2(N).
        let mut nodeno = target;
        while nodeno > 0 {
            if page.node(nodeno) >= minvalue {
                break;
            }
            nodeno = parentof(rightneighbor(nodeno));
        }

        while nodeno < NON_LEAF_NODES_PER_PAGE {
            let mut childnodeno = leftchild(nodeno);
            if childnodeno < NODES_PER_PAGE && page.node(childnodeno) >= minvalue {
                nodeno = childnodeno;
                continue;
            }
            childnodeno += 1;
            if childnodeno < NODES_PER_PAGE && page.node(childnodeno) >= minvalue {
                nodeno = childnodeno;
            } else {
                // Torn page: repair under an exclusive lock and restart
                // (C's DEBUG1 elog here is log-only, skipped).
                if !exclusive_lock_held {
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                    exclusive_lock_held = true;
                }
                fsm_rebuild_page(page);
                bufmgr_seams::mark_buffer_dirty_hint::call(pin.buffer(), false)?;
                continue 'restart;
            }
        }

        let slot = nodeno - NON_LEAF_NODES_PER_PAGE;
        // Hint write even under a shared lock — C's deliberate trade-off.
        page.set_next_slot(slot + advancenext as i32);
        return Ok(slot);
    }
}

pub fn fsm_truncate_avail(page: FsmPage, nslots: i32) -> bool {
    debug_assert!((0..LEAF_NODES_PER_PAGE).contains(&nslots));
    let mut changed = false;
    for nodeno in NON_LEAF_NODES_PER_PAGE + nslots..NODES_PER_PAGE {
        if page.node(nodeno) != 0 {
            changed = true;
        }
        page.set_node(nodeno, 0);
    }
    if changed {
        fsm_rebuild_page(page);
    }
    changed
}

pub fn fsm_rebuild_page(page: FsmPage) -> bool {
    let mut changed = false;
    for nodeno in (0..NON_LEAF_NODES_PER_PAGE).rev() {
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;
        let mut newvalue: u8 = 0;
        if lchild < NODES_PER_PAGE {
            newvalue = page.node(lchild);
        }
        if rchild < NODES_PER_PAGE {
            newvalue = newvalue.max(page.node(rchild));
        }
        if page.node(nodeno) != newvalue {
            page.set_node(nodeno, newvalue);
            changed = true;
        }
    }
    changed
}
