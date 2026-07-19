// params.c ParamListInfo as a registry (stmt_list precedent); stale handle = loud panic.
use core::cell::RefCell;

use ::datum::Datum;
use ::types_core::Oid;

use crate::ParamListHandle;

pub const PARAM_FLAG_CONST: u16 = 0x0001;

#[derive(Clone, Copy, Debug)]
pub struct ParamExternData {
    pub value: Datum,
    pub isnull: bool,
    pub pflags: u16,
    pub ptype: Oid,
}

// C params.h ParamExecData; exec_plan = `void *execPlan` presence bit (write side: nodeSubplan.c lane).
#[derive(Clone, Copy, Debug)]
pub struct ParamExecData {
    pub value: Datum,
    pub isnull: bool,
    pub exec_plan: bool,
}

impl ParamExecData {
    pub const EMPTY: ParamExecData =
        ParamExecData { value: Datum::null(), isnull: true, exec_plan: false };
}

// Resolve-once compile binding (execexpr's AggBind precedent); both arrays are address-stable.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParamBind<'a> {
    pub extern_params: Option<&'a [ParamExternData]>,
    pub exec_vals: Option<core::ptr::NonNull<ParamExecData>>,
    pub n_exec: u32,
}

impl ParamBind<'_> {
    pub const NONE: ParamBind<'static> =
        ParamBind { extern_params: None, exec_vals: None, n_exec: 0 };
}

#[derive(Clone, Copy)]
struct Entry {
    ptr: *const ParamExternData,
    len: usize,
    generation: u32,
    // C ParamListInfo.paramFetch != NULL: the list is PL-owned and lazily
    // fetched there (plpgsql setup_param_list, functions.c
    // sql_fn_param_fetch). This port materializes such lists, but consumers
    // that mirror C's paramFetch bail-outs (params.c BuildParamLogString ->
    // auto_explain's Query Parameters line) need the provenance bit.
    hooked: bool,
}

thread_local! {
    static ENTRIES: RefCell<Vec<Option<Entry>>> = const { RefCell::new(Vec::new()) };
    static FREE: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static GENERATION: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

fn encode(idx: u32, generation: u32) -> ParamListHandle {
    ParamListHandle((u64::from(generation) << 32) | u64::from(idx + 1))
}

fn decode(h: ParamListHandle) -> (u32, u32) {
    ((h.0 as u32) - 1, (h.0 >> 32) as u32)
}

/// # Safety
/// `params` (with its by-ref datums) must outlive [`free`] (PortalDrop's job).
pub unsafe fn register(params: &[ParamExternData]) -> ParamListHandle {
    // SAFETY: forwarded caller contract.
    unsafe { register_with_hooked(params, false) }
}

/// [`register`] for a list that C would back with a paramFetch hook (PL-owned
/// variables). See `Entry::hooked`.
///
/// # Safety
/// Same liveness contract as [`register`].
pub unsafe fn register_hooked(params: &[ParamExternData]) -> ParamListHandle {
    // SAFETY: forwarded caller contract.
    unsafe { register_with_hooked(params, true) }
}

unsafe fn register_with_hooked(params: &[ParamExternData], hooked: bool) -> ParamListHandle {
    let generation = GENERATION.with(|g| {
        let v = g.get().wrapping_add(1);
        g.set(v);
        v
    });
    let entry = Entry { ptr: params.as_ptr(), len: params.len(), generation, hooked };
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

fn lookup(h: ParamListHandle) -> Entry {
    assert!(!h.is_null(), "params: NULL handle dereferenced");
    let (idx, generation) = decode(h);
    let entry = ENTRIES.with(|e| e.borrow().get(idx as usize).copied().flatten());
    match entry {
        Some(e) if e.generation == generation => e,
        _ => panic!("params: stale ParamListHandle {h:?} (freed)"),
    }
}

/// # Safety
/// The borrow must not outlive the register/[`free`] window (the portal outlives one execution).
pub unsafe fn resolve<'a>(h: ParamListHandle) -> &'a [ParamExternData] {
    let e = lookup(h);
    // SAFETY: register()'s liveness contract, narrowed by the caller's bound.
    unsafe { core::slice::from_raw_parts(e.ptr, e.len) }
}

pub fn with<R>(h: ParamListHandle, f: impl FnOnce(&[ParamExternData]) -> R) -> R {
    let e = lookup(h);
    // SAFETY: register()'s liveness contract; no RefCell borrow held here.
    let params = unsafe { core::slice::from_raw_parts(e.ptr, e.len) };
    f(params)
}

pub fn num_params(h: ParamListHandle) -> usize {
    if h.is_null() {
        return 0;
    }
    lookup(h).len
}

/// C's `params->paramFetch != NULL` test (see `Entry::hooked`).
pub fn is_fetch_hooked(h: ParamListHandle) -> bool {
    if h.is_null() {
        return false;
    }
    lookup(h).hooked
}

pub fn free(h: ParamListHandle) {
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

pub fn is_live(h: ParamListHandle) -> bool {
    if h.is_null() {
        return false;
    }
    let (idx, generation) = decode(h);
    ENTRIES.with(|e| {
        e.borrow().get(idx as usize).copied().flatten().map(|en| en.generation)
            == Some(generation)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_with_free_roundtrip() {
        let params = vec![ParamExternData {
            value: Datum::from_i32(42),
            isnull: false,
            pflags: PARAM_FLAG_CONST,
            ptype: 23,
        }];
        let h = unsafe { register(&params) };
        assert!(is_live(h));
        assert_eq!(num_params(h), 1);
        with(h, |p| {
            assert_eq!(p[0].value.as_i32(), 42);
            assert_eq!(p[0].ptype, 23);
        });
        free(h);
        assert!(!is_live(h));
        free(h); /* idempotent */
    }

    #[test]
    #[should_panic(expected = "stale ParamListHandle")]
    fn stale_handle_is_loud() {
        let params = [ParamExternData {
            value: Datum::null(),
            isnull: true,
            pflags: 0,
            ptype: 23,
        }];
        let h = unsafe { register(&params) };
        free(h);
        with(h, |_| ());
    }
}
