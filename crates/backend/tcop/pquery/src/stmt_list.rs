// The Rust marshal for C's `portal->stmts` raw pointer: the planned-statement
// list lives in MessageContext (or plancache) and the PortalData<'static> can
// only carry an opaque StmtListHandle (types_portal). Entries are (ptr, len,
// generation); a stale handle is a loud panic, never a dangling read.
use core::cell::RefCell;

use types_nodes::plannodes::PlannedStmt;
use types_portal::StmtListHandle;

#[derive(Clone, Copy)]
struct Entry {
    ptr: *const PlannedStmt<'static>,
    len: usize,
    generation: u32,
}

thread_local! {
    static ENTRIES: RefCell<Vec<Option<Entry>>> = const { RefCell::new(Vec::new()) };
    static FREE: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static GENERATION: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

fn encode(idx: u32, generation: u32) -> StmtListHandle {
    StmtListHandle((u64::from(generation) << 32) | u64::from(idx + 1))
}

fn decode(h: StmtListHandle) -> (u32, u32) {
    ((h.0 as u32) - 1, (h.0 >> 32) as u32)
}

/// # Safety
/// `stmts` must stay alive and unmoved until [`free`]/[`reset_all`] — the C
/// contract of storing a `List *` into the portal. Callers (exec_simple_query,
/// later plancache) free the handle before resetting the backing arena.
pub unsafe fn register(stmts: &[PlannedStmt<'_>]) -> StmtListHandle {
    let generation = GENERATION.with(|g| {
        let v = g.get().wrapping_add(1);
        g.set(v);
        v
    });
    let entry = Entry {
        ptr: stmts.as_ptr().cast::<PlannedStmt<'static>>(),
        len: stmts.len(),
        generation,
    };
    let idx = match FREE.with(|f| f.borrow_mut().pop()) {
        Some(i) => {
            ENTRIES.with(|e| e.borrow_mut()[i as usize] = Some(entry));
            i
        }
        None => ENTRIES.with(|e| {
            let mut e = e.borrow_mut();
            e.push(Some(entry));
            (e.len() - 1) as u32
        }),
    };
    encode(idx, generation)
}

fn lookup(h: StmtListHandle) -> Entry {
    assert!(!h.is_null(), "stmt_list: NULL handle dereferenced");
    let (idx, generation) = decode(h);
    let entry = ENTRIES.with(|e| e.borrow().get(idx as usize).copied().flatten());
    match entry {
        Some(e) if e.generation == generation => e,
        _ => panic!("stmt_list: stale StmtListHandle {h:?} (freed or reset)"),
    }
}

pub fn with<R>(h: StmtListHandle, f: impl FnOnce(&[PlannedStmt<'_>]) -> R) -> R {
    f(resolve(h))
}

/// One validated lookup for a whole entry point (C dereferences
/// portal->stmts). The slice is live under register()'s contract; callers
/// must not cache it past [`free`]/[`reset_all`].
pub fn resolve(h: StmtListHandle) -> &'static [PlannedStmt<'static>] {
    let e = lookup(h);
    // SAFETY: register()'s liveness contract; no RefCell borrow is held here,
    // so re-entrant access (PortalRunMulti -> ProcessQuery) is fine.
    unsafe { core::slice::from_raw_parts(e.ptr, e.len) }
}

pub fn free(h: StmtListHandle) {
    if h.is_null() {
        return;
    }
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        if let Some(slot) = e.get_mut(idx as usize) {
            if slot.map(|en| en.generation) == Some(generation) {
                *slot = None;
                FREE.with(|f| f.borrow_mut().push(idx));
            }
        }
    });
}

pub fn reset_all() {
    ENTRIES.with(|e| e.borrow_mut().clear());
    FREE.with(|f| f.borrow_mut().clear());
}

pub fn is_live(h: StmtListHandle) -> bool {
    if h.is_null() {
        return false;
    }
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        e.borrow()
            .get(idx as usize)
            .copied()
            .flatten()
            .map(|en| en.generation)
            == Some(generation)
    })
}
