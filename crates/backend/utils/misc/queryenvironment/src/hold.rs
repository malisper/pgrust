// The Rust marshal for C's `QueryEnvironment *` threaded through portals and
// parse/exec entry points (tuplestore::hold shape): the registry owns the
// environment; a stale handle is a loud panic. Entries are boxed so resolve's
// borrows survive registry growth.
use core::cell::{Cell, RefCell};

use ::types_portal::QueryEnvHandle;

use crate::QueryEnvironment;

struct Entry {
    generation: u32,
    env: Box<QueryEnvironment<'static>>,
}

thread_local! {
    static ENTRIES: RefCell<Vec<Option<Entry>>> = const { RefCell::new(Vec::new()) };
    static FREE: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static GENERATION: Cell<u32> = const { Cell::new(0) };
}

fn encode(idx: u32, generation: u32) -> QueryEnvHandle {
    QueryEnvHandle((u64::from(generation) << 32) | u64::from(idx + 1))
}

fn decode(h: QueryEnvHandle) -> (u32, u32) {
    ((h.0 as u32) - 1, (h.0 >> 32) as u32)
}

pub fn register(env: QueryEnvironment<'static>) -> QueryEnvHandle {
    let generation = GENERATION.with(|g| {
        let v = g.get().wrapping_add(1);
        g.set(v);
        v
    });
    let entry = Entry { generation, env: Box::new(env) };
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

pub fn with_env<R>(h: QueryEnvHandle, f: impl FnOnce(&mut QueryEnvironment<'static>) -> R) -> R {
    assert!(!h.is_null(), "queryenvironment: NULL handle dereferenced");
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize).and_then(Option::as_mut) {
            Some(entry) if entry.generation == generation => f(&mut entry.env),
            _ => panic!("queryenvironment: stale QueryEnvHandle {h:?} (unregistered)"),
        }
    })
}

/// The C bare-pointer read path (pstate->p_queryEnv, estate->es_queryEnv).
///
/// # Safety
/// The environment must stay registered — and unmutated via [`with_env`] —
/// while the returned borrow is live; the boxed entry makes the address
/// stable across registry growth.
pub unsafe fn resolve<'e, 'mcx>(h: QueryEnvHandle) -> &'e QueryEnvironment<'mcx> {
    assert!(!h.is_null(), "queryenvironment: NULL handle dereferenced");
    let (idx, generation) = decode(h);
    let ptr = ENTRIES.with(|e| {
        let e = e.borrow();
        match e.get(idx as usize).and_then(Option::as_ref) {
            Some(entry) if entry.generation == generation => {
                core::ptr::from_ref::<QueryEnvironment<'static>>(&*entry.env)
            }
            _ => panic!("queryenvironment: stale QueryEnvHandle {h:?} (unregistered)"),
        }
    });
    // SAFETY: boxed entry, live per the function contract; 'static shortens
    // covariantly to 'mcx and the shared borrow outlives no registration.
    unsafe { &*ptr.cast::<QueryEnvironment<'mcx>>() }
}

pub fn unregister(h: QueryEnvHandle) -> Option<QueryEnvironment<'static>> {
    if h.is_null() {
        return None;
    }
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        let mut e = e.borrow_mut();
        match e.get_mut(idx as usize) {
            Some(slot) if slot.as_ref().map(|en| en.generation) == Some(generation) => {
                FREE.with(|f| f.borrow_mut().push(idx));
                slot.take().map(|en| *en.env)
            }
            _ => None,
        }
    })
}
