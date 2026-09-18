// portalmem.c — portal lifecycle + the per-backend portal hash. The table is
// PRIVATE and per-statement-hot (CreatePortal/PortalDrop per simple query), so
// it is a monomorphized PgHashMap in TopPortalContext, not dynahash.
// stmts/cplan are opaque handles (plancache unported): sharing
// cplan->stmt_list into portal->stmts is a handle copy (an internal issue), and the
// refcount touchpoint crosses plancache_portal_seams.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::RefCell;
use core::fmt::Write as _;
use core::mem::ManuallyDrop;

use ::elog::{elog, ereport};
use ::mcx::{Mcx, MemoryContext, PgBox, PgHashMap, PgString, PgVec};
use ::types_core::{InvalidSubTransactionId, SubTransactionId, TimestampTz};
use ::types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_CURSOR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_CURSOR_STATE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, DEBUG1, ERROR, WARNING,
};
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, PlanSourceHandle, Portal, PortalCleanupHook, PortalData,
    QueryCompletion, QueryDescHandle, QueryEnvHandle, StmtListHandle, TuplestoreHandle,
    CMDTAG_UNKNOWN,
    CURSOR_OPT_BINARY, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
    MAX_PORTALNAME_LEN, PORTAL_ACTIVE, PORTAL_DEFINED, PORTAL_DONE, PORTAL_FAILED,
    PORTAL_MULTI_QUERY, PORTAL_NEW, PORTAL_ONE_SELECT, PORTAL_READY,
};
use ::types_resowner::{
    ResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, RESOURCE_RELEASE_BEFORE_LOCKS,
    RESOURCE_RELEASE_LOCKS,
};

pub use ::types_core::CommandTag;

mod funcs;
pub use funcs::{fc_pg_cursor, PORTALMEM_BUILTINS};

#[cfg(test)]
mod tests;

const PORTALS_PER_USER: usize = 16;

// dynahash HASH_STRINGS key: strlcpy to MAX_PORTALNAME_LEN-1 bytes, even
// mid-character — over-long names collide exactly as in C. Hash/Eq run over
// the used prefix only, as C's string_hash runs over strlen(name) bytes (the
// per-statement unnamed portal hashes 0 bytes, not 64).
#[derive(Clone, Copy)]
struct PortalName {
    len: u8,
    buf: [u8; MAX_PORTALNAME_LEN],
}

impl PortalName {
    fn new(name: &str) -> PortalName {
        let mut buf = [0u8; MAX_PORTALNAME_LEN];
        let end = name.len().min(MAX_PORTALNAME_LEN - 1);
        buf[..end].copy_from_slice(&name.as_bytes()[..end]);
        PortalName { len: end as u8, buf }
    }

    fn bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    // The portal's own name string: the key minus a split trailing character.
    fn as_str(&self) -> &str {
        let bytes = self.bytes();
        core::str::from_utf8(bytes)
            .unwrap_or_else(|e| core::str::from_utf8(&bytes[..e.valid_up_to()]).unwrap())
    }
}

impl PartialEq for PortalName {
    fn eq(&self, other: &PortalName) -> bool {
        self.bytes() == other.bytes()
    }
}

impl Eq for PortalName {}

impl core::hash::Hash for PortalName {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        state.write(self.bytes());
    }
}

// The C PortalHashTable's dynahash iteration order (hash_create with
// PORTALS_PER_USER=16 buckets, HASH_STRINGS): every hash_seq_search walk in
// portalmem.c visits portals bucket by bucket, each chain in insertion order,
// with linear-hashing splits once the entry count passes the bucket count.
// User-visible through PreCommit_Portals (which WITH HOLD cursor is
// materialized first) and pg_cursors, so the order is tracked beside the
// PgHashMap. Splits are never deferred by an in-progress walk here (C's
// has_seq_scans): a walk that creates a 17th+ portal is not a shape we hit.
struct DynaOrder {
    buckets: Vec<Vec<PortalName>>,
    max_bucket: u32,
    low_mask: u32,
    high_mask: u32,
    nentries: usize,
}

impl DynaOrder {
    fn new() -> DynaOrder {
        let nbuckets = PORTALS_PER_USER as u32;
        DynaOrder {
            buckets: (0..nbuckets).map(|_| Vec::new()).collect(),
            max_bucket: nbuckets - 1,
            low_mask: nbuckets - 1,
            high_mask: (nbuckets << 1) - 1,
            nentries: 0,
        }
    }

    fn hash(name: &PortalName) -> u32 {
        hashfn::string_hash(name.bytes(), MAX_PORTALNAME_LEN)
    }

    // dynahash.c calc_bucket.
    fn bucket(&self, hash: u32) -> usize {
        let mut b = hash & self.high_mask;
        if b > self.max_bucket {
            b &= self.low_mask;
        }
        b as usize
    }

    // dynahash.c expand_table: one new bucket, the split source re-linked in
    // chain order.
    fn expand(&mut self) {
        let new_bucket = self.max_bucket + 1;
        self.buckets.push(Vec::new());
        self.max_bucket = new_bucket;
        let old_bucket = (new_bucket & self.low_mask) as usize;
        if new_bucket > self.high_mask {
            self.low_mask = self.high_mask;
            self.high_mask = new_bucket | self.low_mask;
        }
        let chain = core::mem::take(&mut self.buckets[old_bucket]);
        for name in chain {
            let b = self.bucket(Self::hash(&name));
            self.buckets[b].push(name);
        }
    }

    // HASH_ENTER: the split check precedes the insert; new entries chain last.
    fn insert(&mut self, name: PortalName) {
        if self.nentries > self.max_bucket as usize {
            self.expand();
        }
        let b = self.bucket(Self::hash(&name));
        self.buckets[b].push(name);
        self.nentries += 1;
    }

    fn remove(&mut self, name: &PortalName) {
        let b = self.bucket(Self::hash(name));
        if let Some(pos) = self.buckets[b].iter().position(|n| n == name) {
            self.buckets[b].remove(pos);
            self.nentries -= 1;
        }
    }

    fn iter(&self) -> impl Iterator<Item = &PortalName> {
        self.buckets.iter().flatten()
    }
}

// Pool caps: C parks up to 100 small contexts (aset.c context_freelists) and
// pfrees PortalData into TopPortalContext's freelists; a backend rarely has
// more than a few portals alive, so a small cap bounds parked keeper blocks.
const PORTAL_POOL_MAX: usize = 16;

struct PortalManager {
    top: &'static MemoryContext,
    entries: PgVec<'static, Portal<'static>>,
    // entries[i]'s hash key (the name string cannot carry a split character).
    keys: PgVec<'static, PortalName>,
    index: PgHashMap<'static, PortalName, u32>,
    order: DynaOrder,
    unnamed_counter: u32,
    // Per-statement recycling, C's shape: dropped PortalContexts park whole
    // (keeper block intact — aset.c context_freelists) and dropped portal
    // slots park for overwrite (C's pfree into the TopPortalContext freelist).
    free_contexts: Vec<PgBox<'static, MemoryContext>>,
    free_portals: Vec<Portal<'static>>,
    // Parked portal shells, keyed by the CachedPlanSource they executed (the
    // retained execution rebinds only against the same still-valid generic
    // plan). Each shell pins its cplan with a plancache refcount.
    parked: Vec<(PlanSourceHandle, Portal<'static>)>,
}

thread_local! {
    // ManuallyDrop keeps the TLS payload !needs_drop (backend-lifetime in C too).
    static PORTAL_MGR: RefCell<Option<ManuallyDrop<PortalManager>>> =
        const { RefCell::new(None) };
}

fn with_mgr<R>(f: impl FnOnce(&mut PortalManager) -> R) -> Option<R> {
    PORTAL_MGR.with(|m| m.borrow_mut().as_mut().map(|mgr| f(mgr)))
}

fn mgr<R>(func: &str, f: impl FnOnce(&mut PortalManager) -> R) -> PgResult<R> {
    with_mgr(f).ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("{func}: EnablePortalManager has not run"))
            .into_error()
            .into()
    })
}

fn table_len() -> usize {
    with_mgr(|m| m.entries.len()).unwrap_or(0)
}

// Portals phase (C's AtCleanup_Portals slot, see the phase doc in mcx),
// BEFORE any State clear or Roots free, so every context a portal's drop
// glue deallocates into is still alive. Held cursors survive the exit
// ceremony (AtCleanup_Portals skips createSubid == Invalid, as C does; C
// then lets them die with the process): PortalDrop them here so their
// tuplestores are ended — the registry is ManuallyDrop TLS, so a store no
// one ends outlives the session. Then the manager itself — parked shells
// (releasing their plancache pins) and pooled PortalContext values.
fn session_teardown_portals() {
    if PORTAL_MGR.with(|m| m.borrow().is_none()) {
        return;
    }
    let _ = PortalHashTableDeleteAll();
    PORTAL_MGR.with(|m| {
        let Some(mgr) = m.borrow_mut().take() else { return };
        let mut mgr = ManuallyDrop::into_inner(mgr);
        for (_, shell) in core::mem::take(&mut mgr.parked) {
            discard_shell(&shell);
        }
        if !mgr.entries.is_empty() {
            let _ = elog(
                DEBUG1,
                format!("session teardown found {} live portal(s)", mgr.entries.len()),
            );
        }
        drop(mgr);
    });
}

impl PortalManager {
    // The i-th portal of a hash_seq_search walk.
    fn nth_in_scan_order(&self, i: usize) -> Option<Portal<'static>> {
        let key = self.order.iter().nth(i)?;
        self.index.get(key).map(|&idx| self.entries[idx as usize].clone())
    }

    fn scan_order(&self) -> impl Iterator<Item = &Portal<'static>> {
        self.order
            .iter()
            .filter_map(|key| self.index.get(key).map(|&idx| &self.entries[idx as usize]))
    }
}

fn portal_at(i: usize) -> Option<Portal<'static>> {
    with_mgr(|m| m.nth_in_scan_order(i)).flatten()
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub fn EnablePortalManager() {
    PORTAL_MGR.with(|m| {
        let mut slot = m.borrow_mut();
        debug_assert!(slot.is_none(), "portal manager already enabled");
        // Backend-lifetime context; freed at clean task end (session_root).
        let top: &'static MemoryContext =
            ::mcx::session_root("TopPortalContext");
        ::mcx::register_session_cleanup_phase(
            ::mcx::SessionCleanupPhase::Portals,
            Box::new(session_teardown_portals),
        );
        let mut entries: PgVec<'static, Portal<'static>> = PgVec::new_in(top.mcx());
        entries.reserve(PORTALS_PER_USER);
        // hash_create's own context (dynahash.c:387), a TopMemoryContext child.
        let hash_cx: &'static MemoryContext = ::mcx::session_root("Portal hash");
        let mut keys: PgVec<'static, PortalName> = PgVec::new_in(hash_cx.mcx());
        keys.reserve(PORTALS_PER_USER);
        *slot = Some(ManuallyDrop::new(PortalManager {
            top,
            entries,
            keys,
            index: PgHashMap::with_capacity_in(PORTALS_PER_USER, hash_cx.mcx()),
            order: DynaOrder::new(),
            unnamed_counter: 0,
            free_contexts: Vec::new(),
            free_portals: Vec::new(),
            parked: Vec::new(),
        }));
    });
}

pub fn GetPortalByName(name: Option<&str>) -> Option<Portal<'static>> {
    let name = name?;
    let key = PortalName::new(name);
    with_mgr(|m| m.index.get(&key).map(|&i| m.entries[i as usize].clone())).flatten()
}

pub fn CreatePortal(name: &str, allowDup: bool, dupSilent: bool) -> PgResult<Portal<'static>> {
    // One key build + one probe for the whole call (C: one HASH_ENTER); the
    // dup lookup, dup re-check, and insert used to cost 3 probes per portal.
    let key = PortalName::new(name);
    let existing =
        with_mgr(|m| m.index.get(&key).map(|&i| m.entries[i as usize].clone())).flatten();
    if let Some(existing) = existing {
        if !allowDup {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_CURSOR)
                .errmsg(format!("cursor \"{name}\" already exists"))
                .into_error()
                .into());
        }
        if !dupSilent {
            ereport(WARNING)
                .errcode(ERRCODE_DUPLICATE_CURSOR)
                .errmsg(format!("closing existing cursor \"{name}\""))
                .finish(loc("CreatePortal"))?;
        }
        PortalDrop(&existing, false)?;
    }

    let resowner = resowner_portal_seams::resource_owner_create_portal::call();
    let create_subid = xact_seams::get_current_sub_transaction_id::call();
    let create_level = xact_seams::get_current_transaction_nest_level::call();
    let creation_time = xact_portal_seams::get_current_statement_start_timestamp::call();

    mgr("CreatePortal", |m| -> PgResult<Portal<'static>> {
        let mcx = m.top.mcx();
        debug_assert!(!m.index.contains_key(&key), "duplicate portal name");
        let name_copy = PgString::from_str_in(key.as_str(), mcx)?;
        // Parked contexts are already reset (PortalDrop): reuse is a pop, as
        // C's context_freelists hit in AllocSetContextCreate (which also
        // overwrites the name on reuse).
        let portal_context = match m.free_contexts.pop() {
            Some(ctx) => {
                ctx.set_name("PortalContext");
                ctx.reattach_to_parent();
                ctx
            }
            None => PgBox::new_in(m.top.new_child("PortalContext"), mcx),
        };
        // portalmem.c:225: MemoryContextSetIdentifier(portalContext,
        // portal->name[0] ? portal->name : "<unnamed>") — the unnamed portal
        // is identified too (pg_backend_memory_contexts.ident, the
        // memory-context dump line).
        portal_context.set_ident(Some(if name_copy.is_empty() { "<unnamed>" } else { name_copy.as_str() }));
        let mut data = PortalData {
            name: name_copy,
            prepStmtName: None,
            portalContext: Some(portal_context),
            resowner,
            cleanup: PortalCleanupHook::PortalCleanup,
            createSubid: create_subid,
            activeSubid: create_subid,
            createLevel: create_level,
            sourceText: None,
            commandTag: CMDTAG_UNKNOWN,
            qc: QueryCompletion::default(),
            stmts: StmtListHandle::NULL,
            cplan: CachedPlanHandle::NULL,
            plansource: PlanSourceHandle::NULL,
            planContext: core::ptr::null_mut(),
            portalParams: ParamListHandle::NULL,
            queryEnv: QueryEnvHandle::NULL,
            strategy: PORTAL_MULTI_QUERY,
            cursorOptions: CURSOR_OPT_NO_SCROLL,
            status: PORTAL_NEW,
            portalPinned: false,
            autoHeld: false,
            queryDesc: QueryDescHandle::NULL,
            tupDesc: None,
            formats: PgVec::new_in(mcx),
            portalSnapshot: None,
            holdStore: TuplestoreHandle::NULL,
            holdContext: None,
            holdSnapshot: None,
            atStart: true,
            atEnd: true, // disallow fetches until the query is set
            portalPos: 0,
            creation_time,
            visible: true,
            // WS-CA wave-10 (cursors inc-2): store fields idle until
            // PortalStart's arming decision.
            cursorStoreArmed: false,
            cursorStore: TuplestoreHandle::NULL,
            cursorFillExhausted: false,
            currentOfEligible: None,
            cursorCaptureBatch: false,
            cursorTidStore: TuplestoreHandle::NULL,
        };
        // Portal-slot reuse: overwrite a parked slot no clone can still see
        // (is_unique); otherwise a fresh allocation. The overwrite drops the
        // previous portal's strings here, where C pfree'd them at drop time.
        let portal = match m.free_portals.pop() {
            Some(slot) if slot.is_unique() => {
                let mut b = slot.borrow_mut();
                // Retain the formats capacity across slot reuse: the EXECUTE
                // path rebuilt it per portal (an alloc+free pair per query).
                let mut formats = core::mem::replace(&mut b.formats, PgVec::new_in(mcx));
                formats.clear();
                data.formats = formats;
                *b = data;
                drop(b);
                slot
            }
            _ => Portal::new(data),
        };
        let i = m.entries.len() as u32;
        m.entries.push(portal.clone());
        m.keys.push(key);
        m.index.insert(key, i);
        m.order.insert(key);
        Ok(portal)
    })?
}

struct NameBuf {
    buf: [u8; MAX_PORTALNAME_LEN],
    len: usize,
}

impl core::fmt::Write for NameBuf {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let n = s.len().min(self.buf.len() - self.len);
        self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
        self.len += n;
        Ok(())
    }
}

pub fn CreateNewPortal() -> PgResult<Portal<'static>> {
    loop {
        let count = mgr("CreateNewPortal", |m| {
            m.unnamed_counter = m.unnamed_counter.wrapping_add(1);
            m.unnamed_counter
        })?;
        let mut name = NameBuf { buf: [0; MAX_PORTALNAME_LEN], len: 0 };
        write!(name, "<unnamed portal {count}>").expect("NameBuf never errors");
        let name = core::str::from_utf8(&name.buf[..name.len]).expect("ASCII");
        if GetPortalByName(Some(name)).is_none() {
            return CreatePortal(name, false, false);
        }
    }
}

// Stores the passed values; the stmts/cplan handles are Copy stores written
// before anything fallible, so a failed pstrdup cannot leak the plancache
// refcount the caller handed off (C's no-elog-before-storing-cplan rule). The
// source-text/prep-name copies are the single pstrdup analog; C shares the
// caller's pointer.
pub fn PortalDefineQuery(
    portal: &Portal<'static>,
    prepStmtName: Option<&str>,
    sourceText: &str,
    commandTag: CommandTag,
    stmts: StmtListHandle,
    cplan: CachedPlanHandle,
) -> PgResult<()> {
    let mcx: Mcx<'static> = mgr("PortalDefineQuery", |m| m.top.mcx())?;
    let mut p = portal.borrow_mut();
    debug_assert_eq!(p.status, PORTAL_NEW);
    debug_assert!(commandTag != CMDTAG_UNKNOWN || stmts.is_null());

    p.stmts = stmts;
    p.cplan = cplan;
    p.qc = QueryCompletion { commandTag, nprocessed: 0 };
    p.commandTag = commandTag;
    p.prepStmtName = match prepStmtName {
        Some(s) => Some(PgString::from_str_in(s, mcx)?),
        None => None,
    };
    // SAFETY: bytes copied from a `&str` are valid UTF-8; the copy lives in
    // TopPortalContext for the portal's lifetime.
    let copy: &'static [u8] = mcx::slice_in(mcx, sourceText.as_bytes())?.leak();
    p.sourceText = Some(unsafe { core::str::from_utf8_unchecked(copy) });
    p.status = PORTAL_DEFINED;
    Ok(())
}

/// `PortalDefineQuery` for a caller whose source text outlives the portal
/// (exec_simple_query: the text is the MessageContext message, and the
/// unnamed portal is dropped before the message is reset). This is C's own
/// contract — `portal->sourceText = sourceText` shares the caller's pointer —
/// and avoids copying the whole multi-statement message once per statement.
///
/// # Safety
/// `sourceText` must stay valid and unmoved until the portal is dropped.
pub unsafe fn PortalDefineQuerySharedText(
    portal: &Portal<'static>,
    sourceText: &'static str,
    commandTag: CommandTag,
    stmts: StmtListHandle,
    cplan: CachedPlanHandle,
) {
    let mut p = portal.borrow_mut();
    debug_assert_eq!(p.status, PORTAL_NEW);
    p.stmts = stmts;
    p.cplan = cplan;
    p.qc = QueryCompletion { commandTag, nprocessed: 0 };
    p.commandTag = commandTag;
    p.prepStmtName = None;
    p.sourceText = Some(sourceText);
    p.status = PORTAL_DEFINED;
}

fn PortalReleaseCachedPlan(portal: &Portal<'static>) {
    let (cplan, stmts) = {
        let mut p = portal.borrow_mut();
        let cplan = p.cplan;
        if cplan.is_null() {
            return;
        }
        p.cplan = CachedPlanHandle::NULL;
        // C nulls the borrowed portal->stmts pointer; the registry handle is
        // owned by the portal and must be freed with it.
        (cplan, core::mem::replace(&mut p.stmts, StmtListHandle::NULL))
    };
    if !stmts.is_null() {
        pquery_seams::stmt_list_free::call(stmts);
    }
    plancache_portal_seams::release_cached_plan::call(cplan);
}

pub fn PortalCreateHoldStore(portal: &Portal<'static>) -> PgResult<()> {
    let (top, pooled) = mgr("PortalCreateHoldStore", |m| (m.top, m.free_contexts.pop()))?;
    let random_access = {
        let mut p = portal.borrow_mut();
        debug_assert!(p.holdContext.is_none());
        debug_assert!(p.holdStore.is_null());
        debug_assert!(p.holdSnapshot.is_none());
        // NOT a child of portalContext: the store must survive the source
        // transaction. Pool reuse = C's context_freelists hit.
        let hold = match pooled {
            Some(ctx) => {
                ctx.set_name("PortalHoldContext");
                ctx.reattach_to_parent();
                ctx
            }
            None => PgBox::new_in(top.new_child("PortalHoldContext"), top.mcx()),
        };
        p.holdContext = Some(hold);
        (p.cursorOptions & CURSOR_OPT_SCROLL) != 0
    };
    let store = tuplestore_hold_seams::tuplestore_begin_heap_hold::call(random_access)?;
    portal.borrow_mut().holdStore = store;
    Ok(())
}

// --- WS-CA wave-10 (cursors inc-2, contract §7.3) --------------------------
//
// RETIRED (SEAM-WIRING, SE10-GATES item 1 — CB review F1(a)): the portal
// layer's duplicate `PGRUST_LANE_V2_CURSORS` memo cell (`CURSOR_STORE` +
// `cursor_store_enabled` + `cursor_store_set_for_tests`) lived here between
// the CA landing and the seam wiring. THE single knob cell is
// lanev2/push.rs `CURSORS`; the portal layer reads it through the
// `execmain_seams::cursor_store_fill_enabled` seam (pquery PortalStart),
// and unit batteries flip it through the execmain crate-root re-export
// `cursor_store_fill_set_for_tests`. Two cells parsing one env var could
// skew under the two independent test levers — closed by construction.
// --- end WS-CA wave-10 ------------------------------------------------------

pub fn PinPortal(portal: &Portal<'static>) -> PgResult<()> {
    let mut p = portal.borrow_mut();
    if p.portalPinned {
        return Err(ereport(ERROR).errmsg_internal("portal already pinned").into_error().into());
    }
    p.portalPinned = true;
    Ok(())
}

pub fn UnpinPortal(portal: &Portal<'static>) -> PgResult<()> {
    let mut p = portal.borrow_mut();
    if !p.portalPinned {
        return Err(ereport(ERROR).errmsg_internal("portal not pinned").into_error().into());
    }
    p.portalPinned = false;
    Ok(())
}

pub fn MarkPortalActive(portal: &Portal<'static>) -> PgResult<()> {
    if portal.borrow().status != PORTAL_READY {
        let name = portal.borrow().name.as_str().to_owned();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("portal \"{name}\" cannot be run"))
            .into_error()
            .into());
    }
    let subid = xact_seams::get_current_sub_transaction_id::call();
    let mut p = portal.borrow_mut();
    p.status = PORTAL_ACTIVE;
    p.activeSubid = subid;
    Ok(())
}

fn run_cleanup_hook(portal: &Portal<'static>) -> PgResult<()> {
    if portal.borrow().cleanup == PortalCleanupHook::PortalCleanup {
        portalcmds_seams::portal_cleanup::call(portal)?;
        portal.borrow_mut().cleanup = PortalCleanupHook::None;
    }
    Ok(())
}

pub fn MarkPortalDone(portal: &Portal<'static>) -> PgResult<()> {
    {
        let mut p = portal.borrow_mut();
        debug_assert_eq!(p.status, PORTAL_ACTIVE);
        p.status = PORTAL_DONE;
    }
    run_cleanup_hook(portal)
}

pub fn MarkPortalFailed(portal: &Portal<'static>) -> PgResult<()> {
    {
        let mut p = portal.borrow_mut();
        debug_assert!(p.status != PORTAL_DONE);
        p.status = PORTAL_FAILED;
    }
    run_cleanup_hook(portal)
}

pub fn PortalDrop(portal: &Portal<'static>, isTopCommit: bool) -> PgResult<()> {
    // One borrow for the checks + one extraction borrow + one field-clear
    // borrow on the happy path (was ~18 borrow round trips per drop —
    // select1-gate prepared attribution). Seam callouts stay borrow-free.
    let park_candidate = {
        let p = portal.borrow();
        if p.portalPinned {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_CURSOR_STATE)
                .errmsg(format!("cannot drop pinned portal \"{}\"", p.name.as_str()))
                .into_error()
                .into());
        }
        if p.status == PORTAL_ACTIVE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_CURSOR_STATE)
                .errmsg(format!("cannot drop active portal \"{}\"", p.name.as_str()))
                .into_error()
                .into());
        }
        !p.plansource.is_null()
    };

    if park_candidate && try_park(portal, isTopCommit)? {
        return Ok(());
    }

    run_cleanup_hook(portal)?;

    let (query_desc, stmts, params, cplan, plan_ctx, resowner, hold_snapshot, hold_store, status) = {
        let mut p = portal.borrow_mut();
        debug_assert!(p.portalSnapshot.is_none() || !isTopCommit);
        (
            core::mem::replace(&mut p.queryDesc, QueryDescHandle::NULL),
            core::mem::replace(&mut p.stmts, StmtListHandle::NULL),
            core::mem::replace(&mut p.portalParams, ParamListHandle::NULL),
            core::mem::replace(&mut p.cplan, CachedPlanHandle::NULL),
            core::mem::replace(&mut p.planContext, core::ptr::null_mut()),
            core::mem::replace(&mut p.resowner, ResourceOwner::NULL),
            p.holdSnapshot.take(),
            core::mem::replace(&mut p.holdStore, TuplestoreHandle::NULL),
            p.status,
        )
    };
    // WS-CA wave-10: the cursor store + tid sidecar die with the portal
    // (contract §1.1 "dies at PortalDrop"; §2.5 "the store is discarded at
    // portal cleanup"). Freed ahead of holdStore below; tuplestore_end closes
    // any spill file.
    let (cursor_store, cursor_tid_store) = {
        let mut p = portal.borrow_mut();
        p.cursorStoreArmed = false;
        p.cursorFillExhausted = false;
        p.currentOfEligible = None;
        p.cursorCaptureBatch = false;
        (
            core::mem::replace(&mut p.cursorStore, TuplestoreHandle::NULL),
            core::mem::replace(&mut p.cursorTidStore, TuplestoreHandle::NULL),
        )
    };
    if !cursor_store.is_null() {
        tuplestore_hold_seams::tuplestore_end::call(cursor_store);
    }
    if !cursor_tid_store.is_null() {
        tuplestore_hold_seams::tuplestore_end::call(cursor_tid_store);
    }

    // C frees a leftover QueryDesc with the portal context (failed portals
    // skip ExecutorEnd); the owning registry entry must drop explicitly.
    if !query_desc.is_null() {
        execmain_seams::release_query_desc::call(query_desc);
    }

    let removed = remove_from_table(portal);
    if removed.is_none() {
        elog(WARNING, "trying to delete portal name that does not exist")?;
    }

    if !stmts.is_null() {
        pquery_seams::stmt_list_free::call(stmts);
    }
    types_portal::params::free(params);
    if !cplan.is_null() {
        plancache_portal_seams::release_cached_plan::call(cplan);
    }
    if !plan_ctx.is_null() {
        // SAFETY: PortalAttachPlanContext's Box::into_raw, nulled by the take above.
        drop(unsafe { Box::from_raw(plan_ctx) });
    }

    if let Some(snap) = hold_snapshot {
        if !resowner.is_null() {
            snapmgr_portal_seams::unregister_snapshot_from_owner::call(snap, resowner);
        }
    }

    if !resowner.is_null() && (!isTopCommit || status == PORTAL_FAILED) {
        let is_commit = status != PORTAL_FAILED;
        for phase in [
            RESOURCE_RELEASE_BEFORE_LOCKS,
            RESOURCE_RELEASE_LOCKS,
            RESOURCE_RELEASE_AFTER_LOCKS,
        ] {
            resowner_portal_seams::resource_owner_release::call(resowner, phase, is_commit, false);
        }
        resowner_portal_seams::resource_owner_delete::call(resowner);
    }

    if !hold_store.is_null() {
        tuplestore_hold_seams::tuplestore_end::call(hold_store);
    }

    let (ctx, hold_ctx) = {
        let mut p = portal.borrow_mut();
        p.tupDesc = None; // may live in portalContext/holdContext: free before the arenas
        (p.portalContext.take(), p.holdContext.take())
    };
    // Park empty contexts whole (C's AllocSetDelete -> context_freelists):
    // reset runs outside the manager borrow (reset callbacks are user code).
    // A context with live (leaked-in) allocations takes the full destroy path.
    // Retention caps measure the SUBTREE — children of a parked skeleton are
    // retained memory: a live child still parented here rides into the pool's
    // next life, so "empty" means the whole subtree, not self_used alone.
    let park = |cb: Option<PgBox<'static, MemoryContext>>| {
        cb.and_then(|mut cb| {
            if cb.subtree_used() == 0 {
                cb.reset();
                cb.set_ident(None);
                Some(cb)
            } else {
                None
            }
        })
    };
    let parked_ctx = park(ctx);
    let parked_hold = park(hold_ctx);
    with_mgr(|m| {
        for cb in [parked_ctx, parked_hold].into_iter().flatten() {
            if m.free_contexts.len() < PORTAL_POOL_MAX {
                cb.detach_from_parent();
                m.free_contexts.push(cb);
            }
        }
        // Park the slot for reuse; CreatePortal only overwrites it once every
        // outside clone is gone (Portal::is_unique).
        if m.free_portals.len() < PORTAL_POOL_MAX {
            m.free_portals.push(portal.clone());
        }
    });
    Ok(())
}


// One parked shell per plansource; small cap bounds the plancache refcount
// pins dead plans can hold (DropCachedPlan discards its shell eagerly).
const PARKED_PORTAL_MAX: usize = 8;

fn remove_from_table(portal: &Portal<'static>) -> Option<Portal<'static>> {
    with_mgr(|m| {
        let i = m.entries.iter().position(|e| e.ptr_eq(portal))?;
        let key = m.keys.swap_remove(i);
        m.index.remove(&key);
        m.order.remove(&key);
        let removed = m.entries.swap_remove(i);
        if i < m.entries.len() {
            m.index.insert(m.keys[i], i as u32);
        }
        Some(removed)
    })
    .flatten()
}

// Portal retention (no C counterpart): a completed unnamed cached-plan SELECT
// portal parks whole — context, resowner-free shell, QueryDesc with its
// initialized executor — instead of dropping. Reuse (TakeParkedPortal)
// happens only after GetCachedPlan returned the SAME still-valid generic
// plan, so RevalidateCachedQuery's contract decides retention; everything
// per-execution (locks, snapshot, params, scans, permissions) is re-derived
// at rearm.
// inline(never): keeps PortalDrop's frame at its pre-retention size (the
// select1/simple lanes never enter here; layout-recovery respin).
#[inline(never)]
fn try_park(portal: &Portal<'static>, isTopCommit: bool) -> PgResult<bool> {
    {
        let p = portal.borrow();
        if p.plansource.is_null()
            || !p.name.is_empty()
            || p.cplan.is_null()
            || p.queryDesc.is_null()
            || p.strategy != PORTAL_ONE_SELECT
            || p.status != PORTAL_READY
            || p.cleanup != PortalCleanupHook::PortalCleanup
            || !p.holdStore.is_null()
            || p.holdContext.is_some()
            // WS-CA wave-10: a store-armed cursor portal never parks (its
            // store/sidecar are position-carrying per-portal state).
            || p.cursorStoreArmed
            || !p.cursorStore.is_null()
            || !p.cursorTidStore.is_null()
            || p.autoHeld
            || !p.queryEnv.is_null()
            || !p.planContext.is_null()
            || p.portalSnapshot.is_some()
            || p.tupDesc.is_none()
        {
            return Ok(false);
        }
    }
    // Test fixtures shim only the seams they use.
    if !execmain_seams::executor_finish_and_park::is_installed()
        || !plancache_portal_seams::is_source_generic_plan::is_installed()
    {
        return Ok(false);
    }
    // A one-shot custom plan never comes back from GetCachedPlan, so the
    // parked execution could never be retained — take the plain drop path.
    if !plancache_portal_seams::is_source_generic_plan::call(portal.borrow().cplan) {
        return Ok(false);
    }

    let query_desc = portal.borrow().queryDesc;
    // PortalCleanup's resowner discipline around the executor shutdown.
    let save_owner = resowner_seams::current_resource_owner::call();
    let portal_owner = portal.borrow().resowner;
    if !portal_owner.is_null() {
        resowner_seams::set_current_resource_owner::call(portal_owner);
    }
    let parked = execmain_seams::executor_finish_and_park::call(query_desc);
    resowner_seams::set_current_resource_owner::call(save_owner);
    // Success or not, the executor shutdown ran: the cleanup hook is consumed.
    portal.borrow_mut().cleanup = PortalCleanupHook::None;
    let parked = match parked {
        Ok(parked) => parked,
        Err(e) => {
            // Shutdown error mid-hook: the QueryDesc may already be gone;
            // drop our reference and let the normal error path unwind
            // (PortalCleanup's error contract).
            portal.borrow_mut().queryDesc = QueryDescHandle::NULL;
            return Err(e);
        }
    };
    if !parked {
        // executor_finish_and_park ran ExecutorEnd + FreeQueryDesc.
        portal.borrow_mut().queryDesc = QueryDescHandle::NULL;
        return Ok(false);
    }

    let (params, resowner, psrc) = {
        let mut p = portal.borrow_mut();
        (
            core::mem::replace(&mut p.portalParams, ParamListHandle::NULL),
            core::mem::replace(&mut p.resowner, ResourceOwner::NULL),
            p.plansource,
        )
    };
    if remove_from_table(portal).is_none() {
        elog(WARNING, "trying to park portal name that does not exist")?;
    }
    types_portal::params::free(params);
    // Top-commit drops leave the resowner to the transaction machinery
    // (PortalDrop's isTopCommit arm); displacement drops release it here.
    if !resowner.is_null() && !isTopCommit {
        for phase in [
            RESOURCE_RELEASE_BEFORE_LOCKS,
            RESOURCE_RELEASE_LOCKS,
            RESOURCE_RELEASE_AFTER_LOCKS,
        ] {
            resowner_portal_seams::resource_owner_release::call(resowner, phase, true, false);
        }
        resowner_portal_seams::resource_owner_delete::call(resowner);
    }
    // The per-execution allocations (bind params) die with the context,
    // exactly as PortalDrop: park it empty or drop it whole; TakeParkedPortal
    // re-attaches a pooled one (CreatePortal parity).
    let ctx = portal.borrow_mut().portalContext.take();
    let parked_ctx = ctx.and_then(|mut cb| {
        // Subtree, not self: see the park-empty law above (retention caps
        // measure the SUBTREE).
        if cb.subtree_used() == 0 {
            cb.reset();
            cb.set_ident(None);
            Some(cb)
        } else {
            None
        }
    });
    {
        let mut p = portal.borrow_mut();
        p.status = PORTAL_NEW;
        p.qc = QueryCompletion { commandTag: p.commandTag, nprocessed: 0 };
        p.atStart = true;
        p.atEnd = true;
        p.portalPos = 0;
        p.createSubid = InvalidSubTransactionId;
        p.activeSubid = InvalidSubTransactionId;
        p.createLevel = 0;
    }
    let displaced = with_mgr(|m| {
        if let Some(cb) = parked_ctx {
            if m.free_contexts.len() < PORTAL_POOL_MAX {
                cb.detach_from_parent();
                m.free_contexts.push(cb);
            }
        }
        let mut displaced = Vec::new();
        if let Some(i) = m.parked.iter().position(|(k, _)| *k == psrc) {
            displaced.push(m.parked.remove(i).1);
        }
        while m.parked.len() >= PARKED_PORTAL_MAX {
            displaced.push(m.parked.remove(0).1);
        }
        m.parked.push((psrc, portal.clone()));
        displaced
    })
    .unwrap_or_default();
    for shell in displaced {
        discard_shell(&shell);
    }
    Ok(true)
}

// Frees a parked shell: the retained execution drops on its normal path, the
// plan pin releases, and the empty context/slot park in the reuse pools
// (PortalDrop's tail).
#[inline(never)]
fn discard_shell(shell: &Portal<'static>) {
    let (query_desc, stmts, cplan, ctx) = {
        let mut p = shell.borrow_mut();
        p.tupDesc = None;
        (
            core::mem::replace(&mut p.queryDesc, QueryDescHandle::NULL),
            core::mem::replace(&mut p.stmts, StmtListHandle::NULL),
            core::mem::replace(&mut p.cplan, CachedPlanHandle::NULL),
            p.portalContext.take(),
        )
    };
    if !query_desc.is_null() {
        execmain_seams::release_query_desc::call(query_desc);
    }
    if !stmts.is_null() {
        pquery_seams::stmt_list_free::call(stmts);
    }
    if !cplan.is_null() {
        plancache_portal_seams::release_cached_plan::call(cplan);
    }
    let parked_ctx = ctx.and_then(|mut cb| {
        // Subtree, not self: see the park-empty law above (retention caps
        // measure the SUBTREE).
        if cb.subtree_used() == 0 {
            cb.reset();
            cb.set_ident(None);
            Some(cb)
        } else {
            None
        }
    });
    with_mgr(|m| {
        if let Some(cb) = parked_ctx {
            if m.free_contexts.len() < PORTAL_POOL_MAX {
                cb.detach_from_parent();
                m.free_contexts.push(cb);
            }
        }
        if m.free_portals.len() < PORTAL_POOL_MAX {
            m.free_portals.push(shell.clone());
        }
    });
}

/// Take the parked shell for `plansource`, re-initialized as this cycle's
/// unnamed portal (CreatePortal parity) and re-entered in the portal table.
/// Caller contract: no live unnamed portal exists (drop it first), and the
/// caller verifies the shell's cplan against this bind's GetCachedPlan result
/// before reusing the retained execution.
#[inline(never)]
pub fn TakeParkedPortal(plansource: PlanSourceHandle) -> PgResult<Option<Portal<'static>>> {
    let Some(shell) = with_mgr(|m| {
        let i = m.parked.iter().position(|(k, _)| *k == plansource)?;
        Some(m.parked.remove(i).1)
    })
    .flatten() else {
        return Ok(None);
    };
    let resowner = resowner_portal_seams::resource_owner_create_portal::call();
    let create_subid = xact_seams::get_current_sub_transaction_id::call();
    let create_level = xact_seams::get_current_transaction_nest_level::call();
    let creation_time = xact_portal_seams::get_current_statement_start_timestamp::call();
    let portal_context = mgr("TakeParkedPortal", |m| match m.free_contexts.pop() {
        Some(ctx) => {
            ctx.set_name("PortalContext");
            ctx.reattach_to_parent();
            ctx
        }
        None => PgBox::new_in(m.top.new_child("PortalContext"), m.top.mcx()),
    })?;
    {
        let mut p = shell.borrow_mut();
        debug_assert!(p.name.is_empty() && p.status == PORTAL_NEW);
        debug_assert!(p.portalContext.is_none());
        p.portalContext = Some(portal_context);
        p.resowner = resowner;
        p.createSubid = create_subid;
        p.activeSubid = create_subid;
        p.createLevel = create_level;
        p.creation_time = creation_time;
        // The hook stays disarmed until PortalStartParked rearms the retained
        // executor (es_finished is still set from the park-time finish); an
        // error before then drops the shell via release_query_desc.
        p.cleanup = PortalCleanupHook::None;
        p.status = PORTAL_DEFINED;
    }
    mgr("TakeParkedPortal", |m| {
        let key = PortalName::new("");
        debug_assert!(!m.index.contains_key(&key), "unnamed portal already exists");
        let i = m.entries.len() as u32;
        m.entries.push(shell.clone());
        m.keys.push(key);
        m.index.insert(key, i);
        m.order.insert(key);
    })?;
    Ok(Some(shell))
}

/// The taken shell's plan no longer matches this bind's GetCachedPlan result
/// (revalidation replanned): shed the retained execution and hand the shell
/// back as a plain just-created portal (status PORTAL_NEW) for the normal
/// PortalDefineQuery + PortalStart path.
#[cold]
#[inline(never)]
pub fn ShedRetainedExecution(portal: &Portal<'static>) {
    let (query_desc, stmts, cplan) = {
        let mut p = portal.borrow_mut();
        p.tupDesc = None;
        p.status = PORTAL_NEW;
        p.cleanup = PortalCleanupHook::PortalCleanup;
        (
            core::mem::replace(&mut p.queryDesc, QueryDescHandle::NULL),
            core::mem::replace(&mut p.stmts, StmtListHandle::NULL),
            core::mem::replace(&mut p.cplan, CachedPlanHandle::NULL),
        )
    };
    if !query_desc.is_null() {
        execmain_seams::release_query_desc::call(query_desc);
    }
    if !stmts.is_null() {
        pquery_seams::stmt_list_free::call(stmts);
    }
    if !cplan.is_null() {
        plancache_portal_seams::release_cached_plan::call(cplan);
    }
}

// DropCachedPlan's parked-shell discard (DEALLOCATE / DISCARD ALL / re-PREPARE
// of the unnamed statement): eager, so dead plans do not stay pinned.
fn discard_parked_portal_seam(plansource: PlanSourceHandle) {
    let shell = with_mgr(|m| {
        let i = m.parked.iter().position(|(k, _)| *k == plansource)?;
        Some(m.parked.remove(i).1)
    })
    .flatten();
    if let Some(shell) = shell {
        discard_shell(&shell);
    }
}

pub fn init_seams() {
    plancache_portal_seams::discard_parked_portal::set(discard_parked_portal_seam);
}

/// portalcmds.c:109's copy-into-portalContext analog: the portal owns the plan's arena.
pub fn PortalAttachPlanContext(portal: &Portal<'static>, ctx: Box<MemoryContext>) {
    let mut p = portal.borrow_mut();
    assert!(p.planContext.is_null(), "portal already owns a plan context");
    p.planContext = Box::into_raw(ctx);
}

pub fn PortalHashTableDeleteAll() -> PgResult<()> {
    loop {
        let next = with_mgr(|m| {
            m.scan_order()
                .find(|p| p.borrow().status != PORTAL_ACTIVE)
                .cloned()
        });
        match next {
            Some(Some(portal)) => PortalDrop(&portal, false)?,
            _ => return Ok(()),
        }
    }
}

fn HoldPortal(portal: &Portal<'static>) -> PgResult<()> {
    // WS-CA wave-10 (contract §1.1): a store-armed SCROLL+HOLD portal created
    // its holdStore at first fill demand; commit persists THAT store
    // (fill_to(EOF) inside PersistHoldablePortal) instead of minting a new
    // one. Everything else (incl. the knob-OFF world) keeps C's shape.
    if portal.borrow().holdStore.is_null() {
        PortalCreateHoldStore(portal)?;
    } else {
        debug_assert!(portal.borrow().cursorStoreArmed);
    }
    portalcmds_seams::persist_holdable_portal::call(portal)?;
    PortalReleaseCachedPlan(portal);
    let mut p = portal.borrow_mut();
    p.resowner = ResourceOwner::NULL;
    p.createSubid = InvalidSubTransactionId;
    p.activeSubid = InvalidSubTransactionId;
    p.createLevel = 0;
    Ok(())
}

pub fn PreCommit_Portals(isPrepare: bool) -> PgResult<bool> {
    let mut result = false;
    'restart: loop {
        for i in 0..table_len() {
            let Some(portal) = portal_at(i) else { break };
            let (pinned, auto_held, status, cursor_options, create_subid) = {
                let p = portal.borrow();
                (p.portalPinned, p.autoHeld, p.status, p.cursorOptions, p.createSubid)
            };

            if pinned && !auto_held {
                return Err(ereport(ERROR)
                    .errmsg_internal("cannot commit while a portal is pinned")
                    .into_error()
                    .into());
            }

            // Active portals (multi-transaction utility command / commit in a
            // procedure): only detach their resources.
            if status == PORTAL_ACTIVE {
                let resowner = portal.borrow().resowner;
                let snap = portal.borrow_mut().holdSnapshot.take();
                if let Some(snap) = snap {
                    if !resowner.is_null() {
                        snapmgr_portal_seams::unregister_snapshot_from_owner::call(snap, resowner);
                    }
                }
                let mut p = portal.borrow_mut();
                p.resowner = ResourceOwner::NULL;
                p.portalSnapshot = None;
                continue;
            }

            if (cursor_options & CURSOR_OPT_HOLD) != 0
                && create_subid != InvalidSubTransactionId
                && status == PORTAL_READY
            {
                if isPrepare {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot PREPARE a transaction that has created a cursor WITH HOLD")
                        .into_error()
                        .into());
                }
                HoldPortal(&portal)?;
                result = true;
            } else if create_subid == InvalidSubTransactionId {
                continue;
            } else {
                PortalDrop(&portal, true)?;
                result = true;
            }

            // Holding or dropping may have run user code that dropped other
            // portals: restart, as C restarts its hash_seq.
            continue 'restart;
        }
        return Ok(result);
    }
}

pub fn AtAbort_Portals() -> PgResult<()> {
    for i in 0..table_len() {
        let Some(portal) = portal_at(i) else { break };

        if portal.borrow().status == PORTAL_ACTIVE
            && ipc_portal_seams::shmem_exit_inprogress::call()
        {
            MarkPortalFailed(&portal)?;
        }

        let (create_subid, auto_held) = {
            let p = portal.borrow();
            (p.createSubid, p.autoHeld)
        };
        if create_subid == InvalidSubTransactionId || auto_held {
            continue;
        }

        // Created in this transaction: a READY portal might refer to objects
        // created in the failed transaction.
        if portal.borrow().status == PORTAL_READY {
            MarkPortalFailed(&portal)?;
        }

        run_cleanup_hook(&portal)?;
        PortalReleaseCachedPlan(&portal);
        // Resources are released in the upcoming transaction-wide cleanup.
        portal.borrow_mut().resowner = ResourceOwner::NULL;
        // MemoryContextDeleteChildren(portalContext): child contexts here are
        // RAII-owned by their creators and already dropped; portalContext's own
        // allocations are preserved, as in C.
    }
    Ok(())
}

pub fn AtCleanup_Portals() -> PgResult<()> {
    let mut i = 0;
    while let Some(portal) = portal_at(i) {
        let (status, create_subid, auto_held) = {
            let p = portal.borrow();
            (p.status, p.createSubid, p.autoHeld)
        };
        if status == PORTAL_ACTIVE {
            i += 1;
            continue;
        }
        if create_subid == InvalidSubTransactionId || auto_held {
            debug_assert!(portal.borrow().resowner.is_null());
            i += 1;
            continue;
        }

        // PortalDrop refuses pinned portals; whoever pinned it was aborted too.
        portal.borrow_mut().portalPinned = false;

        // No user-defined code during cleanup: skip an unrun cleanup hook.
        if portal.borrow().cleanup == PortalCleanupHook::PortalCleanup {
            let name = portal.borrow().name.as_str().to_owned();
            elog(WARNING, format!("skipping cleanup for portal \"{name}\""))?;
            portal.borrow_mut().cleanup = PortalCleanupHook::None;
        }

        // Removes slot i (swap_remove backfills it): do not advance.
        PortalDrop(&portal, false)?;
    }
    Ok(())
}

pub fn PortalErrorCleanup() -> PgResult<()> {
    let mut i = 0;
    while let Some(portal) = portal_at(i) {
        if !portal.borrow().autoHeld {
            i += 1;
            continue;
        }
        portal.borrow_mut().portalPinned = false;
        PortalDrop(&portal, false)?;
    }
    Ok(())
}

pub fn AtSubCommit_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    parentLevel: i32,
    parentXactOwner: ResourceOwner,
) {
    at_subcommit_inner(mySubid, parentSubid, parentLevel, parentXactOwner);
}

fn at_subcommit_inner(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    parentLevel: i32,
    parent_owner: ResourceOwner,
) {
    for i in 0..table_len() {
        let Some(portal) = portal_at(i) else { break };
        // A portal mid-ProcessQuery holds its RefCell borrow across the whole
        // execution (pquery with_source_text); it was created and activated
        // before this subxact existed, so it never matches mySubid. Read-check
        // before mutating (same shape as at_subabort_inner) or a plpgsql
        // exception-block subcommit under a running DML double-borrows.
        let needs_update = {
            let p = portal.borrow();
            p.createSubid == mySubid || p.activeSubid == mySubid
        };
        if !needs_update {
            continue;
        }
        let reparent = {
            let mut p = portal.borrow_mut();
            let mine = p.createSubid == mySubid;
            if mine {
                p.createSubid = parentSubid;
                p.createLevel = parentLevel;
            }
            if p.activeSubid == mySubid {
                p.activeSubid = parentSubid;
            }
            (mine && !p.resowner.is_null()).then_some(p.resowner)
        };
        if let Some(owner) = reparent {
            resowner_portal_seams::resource_owner_new_parent::call(owner, parent_owner);
        }
    }
}

pub fn AtSubAbort_Portals(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    myXactOwner: ResourceOwner,
    _parentXactOwner: ResourceOwner,
) -> PgResult<()> {
    at_subabort_inner(mySubid, parentSubid, myXactOwner)
}

fn at_subabort_inner(
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
    my_owner: ResourceOwner,
) -> PgResult<()> {
    for i in 0..table_len() {
        let Some(portal) = portal_at(i) else { break };

        if portal.borrow().createSubid != mySubid {
            // Not created here — but was it used in this subtransaction?
            if portal.borrow().activeSubid == mySubid {
                portal.borrow_mut().activeSubid = parentSubid;

                // An upper-level portal left ACTIVE can't happen, but fail it.
                if portal.borrow().status == PORTAL_ACTIVE {
                    MarkPortalFailed(&portal)?;
                }

                // If failed during this subtransaction, reattach its resources
                // to this subtransaction's owner so they release with it.
                let reparent = {
                    let mut p = portal.borrow_mut();
                    if p.status == PORTAL_FAILED && !p.resowner.is_null() {
                        let owner = p.resowner;
                        p.resowner = ResourceOwner::NULL;
                        Some(owner)
                    } else {
                        None
                    }
                };
                if let Some(owner) = reparent {
                    resowner_portal_seams::resource_owner_new_parent::call(owner, my_owner);
                }
            }
            continue;
        }

        let status = portal.borrow().status;
        if status == PORTAL_READY || status == PORTAL_ACTIVE {
            MarkPortalFailed(&portal)?;
        }

        run_cleanup_hook(&portal)?;
        PortalReleaseCachedPlan(&portal);
        portal.borrow_mut().resowner = ResourceOwner::NULL;
        // MemoryContextDeleteChildren: no-op here, as in AtAbort_Portals.
    }
    Ok(())
}

pub fn AtSubCleanup_Portals(mySubid: SubTransactionId) -> PgResult<()> {
    let mut i = 0;
    while let Some(portal) = portal_at(i) {
        if portal.borrow().createSubid != mySubid {
            i += 1;
            continue;
        }

        portal.borrow_mut().portalPinned = false;

        if portal.borrow().cleanup == PortalCleanupHook::PortalCleanup {
            let name = portal.borrow().name.as_str().to_owned();
            elog(WARNING, format!("skipping cleanup for portal \"{name}\""))?;
            portal.borrow_mut().cleanup = PortalCleanupHook::None;
        }

        PortalDrop(&portal, false)?;
    }
    Ok(())
}

pub struct PgCursorRow<'a> {
    pub name: PgString<'a>,
    pub statement: PgString<'a>,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
    pub creation_time: TimestampTz,
}

// pg_cursor() minus the SRF plumbing: the visible, defined portals in table
// scan order; the funcapi owner materializes these into its tuplestore.
pub fn pg_cursor_rows<'a>(mcx: Mcx<'a>) -> PgResult<PgVec<'a, PgCursorRow<'a>>> {
    let mut rows: PgVec<'a, PgCursorRow<'a>> = PgVec::new_in(mcx);
    with_mgr(|m| -> PgResult<()> {
        for portal in m.scan_order() {
            let p = portal.borrow();
            if !p.visible {
                continue;
            }
            let Some(source_text) = &p.sourceText else { continue };
            rows.push(PgCursorRow {
                name: p.name.clone_in(mcx)?,
                statement: PgString::from_str_in(source_text, mcx)?,
                is_holdable: (p.cursorOptions & CURSOR_OPT_HOLD) != 0,
                is_binary: (p.cursorOptions & CURSOR_OPT_BINARY) != 0,
                is_scrollable: (p.cursorOptions & CURSOR_OPT_SCROLL) != 0,
                creation_time: p.creation_time,
            });
        }
        Ok(())
    })
    .transpose()?;
    Ok(rows)
}

pub fn ThereAreNoReadyPortals() -> bool {
    with_mgr(|m| m.entries.iter().all(|p| p.borrow().status != PORTAL_READY)).unwrap_or(true)
}

pub fn HoldPinnedPortals() -> PgResult<()> {
    for i in 0..table_len() {
        let Some(portal) = portal_at(i) else { break };
        let (pinned, auto_held, strategy, status) = {
            let p = portal.borrow();
            (p.portalPinned, p.autoHeld, p.strategy, p.status)
        };
        if pinned && !auto_held {
            if strategy != PORTAL_ONE_SELECT {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(
                        "cannot perform transaction commands inside a cursor loop that is not read-only",
                    )
                    .into_error()
                    .into());
            }
            if status != PORTAL_READY {
                return Err(ereport(ERROR)
                    .errmsg_internal("pinned portal is not ready to be auto-held")
                    .into_error()
                    .into());
            }
            HoldPortal(&portal)?;
            portal.borrow_mut().autoHeld = true;
        }
    }
    Ok(())
}

pub fn ForgetPortalSnapshots() -> PgResult<()> {
    let mut num_portal_snaps: i32 = 0;
    with_mgr(|m| {
        for portal in m.scan_order() {
            let mut p = portal.borrow_mut();
            if p.portalSnapshot.take().is_some() {
                num_portal_snaps += 1;
            }
            // portal->holdSnapshot is cleaned up in PreCommit_Portals.
        }
    });

    let mut num_active_snaps: i32 = 0;
    while snapmgr_portal_seams::active_snapshot_set::call() {
        snapmgr_portal_seams::pop_active_snapshot::call()?;
        num_active_snaps += 1;
    }

    if num_portal_snaps != num_active_snaps {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "portal snapshots ({num_portal_snaps}) did not account for all active snapshots ({num_active_snaps})"
            ))
            .into_error()
            .into());
    }
    Ok(())
}
