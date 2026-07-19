#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::RefCell;
use std::rc::Rc;

use ::mcx::{MemoryContext, PgBox, PgString, PgVec};
use ::types_core::{CommandTag, SubTransactionId, TimestampTz};
use ::types_resowner::ResourceOwner;
use ::types_snapshot::SnapshotData;
use ::types_tuple::TupleDescData;

pub mod params;

pub const CMDTAG_UNKNOWN: CommandTag = CommandTag(0);
pub const CMDTAG_DELETE: CommandTag = CommandTag(103);
pub const CMDTAG_FETCH: CommandTag = CommandTag(154);
pub const CMDTAG_INSERT: CommandTag = CommandTag(158);
pub const CMDTAG_MERGE: CommandTag = CommandTag(163);
pub const CMDTAG_MOVE: CommandTag = CommandTag(164);
pub const CMDTAG_REFRESH_MATERIALIZED_VIEW: CommandTag = CommandTag(169);
pub const CMDTAG_SELECT: CommandTag = CommandTag(179);
pub const CMDTAG_UPDATE: CommandTag = CommandTag(191);

pub const COMPLETION_TAG_BUFSIZE: usize = 64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryCompletion {
    pub commandTag: CommandTag,
    pub nprocessed: u64,
}

pub use ::types_nodes::parsenodes::FetchDirection::{self, *};
pub use ::types_nodes::parsenodes::FETCH_ALL;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum PortalStrategy {
    #[default]
    PORTAL_ONE_SELECT = 0,
    PORTAL_ONE_RETURNING,
    PORTAL_ONE_MOD_WITH,
    PORTAL_UTIL_SELECT,
    PORTAL_MULTI_QUERY,
}

pub use PortalStrategy::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u32)]
pub enum PortalStatus {
    #[default]
    PORTAL_NEW = 0,
    PORTAL_DEFINED,
    PORTAL_READY,
    PORTAL_ACTIVE,
    PORTAL_DONE,
    PORTAL_FAILED,
}

pub use PortalStatus::*;

pub use ::types_nodes::parsenodes::{
    CURSOR_OPT_ASENSITIVE, CURSOR_OPT_BINARY, CURSOR_OPT_CUSTOM_PLAN, CURSOR_OPT_FAST_PLAN,
    CURSOR_OPT_GENERIC_PLAN, CURSOR_OPT_HOLD, CURSOR_OPT_INSENSITIVE, CURSOR_OPT_NO_SCROLL,
    CURSOR_OPT_PARALLEL_OK, CURSOR_OPT_SCROLL,
};

// MAX_PORTALNAME_LEN (portalmem.c) == NAMEDATALEN.
pub const MAX_PORTALNAME_LEN: usize = 64;

// Identity tokens for portal payloads whose owners are unported (C bare
// pointers the portal only stores and threads back); 0 is the C NULL.
macro_rules! extern_handle {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
        pub struct $name(pub u64);

        impl $name {
            pub const NULL: $name = $name(0);
            pub const fn is_null(self) -> bool {
                self.0 == 0
            }
        }
    )+};
}

extern_handle!(
    StmtListHandle,
    CachedPlanHandle,
    PlanSourceHandle,
    ParamListHandle,
    QueryEnvHandle,
    QueryDescHandle,
    TuplestoreHandle,
);

mcx::forget_safe_nodrop!(ParamListHandle, QueryEnvHandle, TuplestoreHandle);

// C's `void (*cleanup)(Portal)` is only ever NULL or portalcmds.c's
// PortalCleanup: a closed set, so an enum rather than a pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PortalCleanupHook {
    #[default]
    None,
    PortalCleanup,
}

// PortalData (utils/portal.h); 'mcx is the manager's context (TopPortalContext).
// portalContext/holdContext are PgBox'd for address stability across moves.
pub struct PortalData<'mcx> {
    pub name: PgString<'mcx>,
    pub prepStmtName: Option<PgString<'mcx>>,
    pub portalContext: Option<PgBox<'mcx, MemoryContext>>,
    pub resowner: ResourceOwner,
    pub cleanup: PortalCleanupHook,

    pub createSubid: SubTransactionId,
    pub activeSubid: SubTransactionId,
    pub createLevel: i32,

    pub sourceText: Option<PgString<'mcx>>,
    pub commandTag: CommandTag,
    pub qc: QueryCompletion,
    pub stmts: StmtListHandle,
    pub cplan: CachedPlanHandle,
    // CachedPlanSource backing cplan (parked-portal retention key; no C field).
    pub plansource: PlanSourceHandle,
    // DECLARE's plan arena, C's copy-into-portalContext analog (leaked Box; PortalDrop reclaims).
    pub planContext: *mut MemoryContext,

    pub portalParams: ParamListHandle,
    pub queryEnv: QueryEnvHandle,

    pub strategy: PortalStrategy,
    pub cursorOptions: i32,

    pub status: PortalStatus,
    pub portalPinned: bool,
    pub autoHeld: bool,

    pub queryDesc: QueryDescHandle,

    pub tupDesc: Option<Rc<TupleDescData<'mcx>>>,
    pub formats: PgVec<'mcx, i16>,

    pub portalSnapshot: Option<Rc<SnapshotData<'mcx>>>,

    pub holdStore: TuplestoreHandle,
    pub holdContext: Option<PgBox<'mcx, MemoryContext>>,
    pub holdSnapshot: Option<Rc<SnapshotData<'mcx>>>,

    pub atStart: bool,
    pub atEnd: bool,
    pub portalPos: u64,

    pub creation_time: TimestampTz,
    pub visible: bool,

    // --- WS-CA wave-10 (cursors inc-2, contract §1/§4; fields granted by
    // name in the contract §8 WS-CA row). The portal-boundary cursor store:
    // SCROLL cursors are served from a lazy-materialized spill-armed
    // tuplestore; operators only ever run forward.
    /// Decided once at PortalStart: knob-ON && CURSOR_OPT_SCROLL &&
    /// PORTAL_ONE_SELECT. Armed portals never set
    /// EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD on the child (contract §3.1).
    pub cursorStoreArmed: bool,
    /// The §1.1 store for SCROLL-without-HOLD (inter_xact=false; dies at
    /// PortalDrop). SCROLL+HOLD portals use `holdStore` instead (created at
    /// first fill demand, PortalCreateHoldStore shape — contract §1.1).
    pub cursorStore: TuplestoreHandle,
    /// §2.2: the executor returned short (or a count-0 drain ran); fill_to
    /// never touches the executor again once set.
    pub cursorFillExhausted: bool,
    /// §4 CURRENT-OF eligibility, probed once at first fill (None = not yet
    /// probed; Some(false) = scan-state resolution can never find a row).
    pub currentOfEligible: Option<bool>,
    /// SE-R41 (notes/se-r41-retire.md §3.1/§3.2): the eligible plan is the
    /// batch-fill shape (bare T_SeqScan top over a tid-capable heap AM), so
    /// the fill captures §4.2 identity INSIDE the run (batch sink / capture
    /// row loop) and the portal takes the PLAIN store-armed eflags instead
    /// of the D-CA-2 fence. PortalStart-fixed like `currentOfEligible`;
    /// meaningful only when `currentOfEligible == Some(true)`.
    pub cursorCaptureBatch: bool,
    /// §4.2 hidden (tableoid, ctid) row-identity sidecar, eligible plans
    /// only; row index == store row index; spill-armed like the store.
    pub cursorTidStore: TuplestoreHandle,
    // --- end WS-CA wave-10 ---
}

// C Portal alias: shared interior-mutable handle (cf. types_rel Relation);
// per-statement paths only, so RefCell stays off the per-row spine.
#[derive(Clone)]
pub struct Portal<'mcx>(Rc<RefCell<PortalData<'mcx>>>);

impl<'mcx> Portal<'mcx> {
    pub fn new(data: PortalData<'mcx>) -> Self {
        Portal(Rc::new(RefCell::new(data)))
    }

    pub fn borrow(&self) -> core::cell::Ref<'_, PortalData<'mcx>> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> core::cell::RefMut<'_, PortalData<'mcx>> {
        self.0.borrow_mut()
    }

    pub fn ptr_eq(&self, other: &Portal<'mcx>) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }

    /// True iff this is the only handle: portalmem's slot-recycling gate —
    /// a parked portal is overwritten only when every outstanding clone is
    /// gone (C's pfree-into-aset-freelist reuse, made alias-safe).
    pub fn is_unique(&self) -> bool {
        Rc::strong_count(&self.0) == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_match_portal_h_order() {
        assert_eq!(PORTAL_ONE_SELECT as u32, 0);
        assert_eq!(PORTAL_ONE_RETURNING as u32, 1);
        assert_eq!(PORTAL_ONE_MOD_WITH as u32, 2);
        assert_eq!(PORTAL_UTIL_SELECT as u32, 3);
        assert_eq!(PORTAL_MULTI_QUERY as u32, 4);
        assert_eq!(PORTAL_NEW as u32, 0);
        assert_eq!(PORTAL_DEFINED as u32, 1);
        assert_eq!(PORTAL_READY as u32, 2);
        assert_eq!(PORTAL_ACTIVE as u32, 3);
        assert_eq!(PORTAL_DONE as u32, 4);
        assert_eq!(PORTAL_FAILED as u32, 5);
        assert_eq!(FETCH_FORWARD as u32, 0);
        assert_eq!(FETCH_BACKWARD as u32, 1);
        assert_eq!(FETCH_ABSOLUTE as u32, 2);
        assert_eq!(FETCH_RELATIVE as u32, 3);
    }

    #[test]
    fn cursor_bits_match_parsenodes_h() {
        assert_eq!(CURSOR_OPT_BINARY, 0x0001);
        assert_eq!(CURSOR_OPT_SCROLL, 0x0002);
        assert_eq!(CURSOR_OPT_NO_SCROLL, 0x0004);
        assert_eq!(CURSOR_OPT_INSENSITIVE, 0x0008);
        assert_eq!(CURSOR_OPT_ASENSITIVE, 0x0010);
        assert_eq!(CURSOR_OPT_HOLD, 0x0020);
        assert_eq!(CURSOR_OPT_FAST_PLAN, 0x0100);
        assert_eq!(CURSOR_OPT_GENERIC_PLAN, 0x0200);
        assert_eq!(CURSOR_OPT_CUSTOM_PLAN, 0x0400);
        assert_eq!(CURSOR_OPT_PARALLEL_OK, 0x0800);
    }

    #[test]
    fn cmdtags_match_cmdtaglist_positions() {
        assert_eq!(CMDTAG_UNKNOWN.0, 0);
        assert_eq!(CMDTAG_DELETE.0, 103);
        assert_eq!(CMDTAG_FETCH.0, 154);
        assert_eq!(CMDTAG_INSERT.0, 158);
        assert_eq!(CMDTAG_MERGE.0, 163);
        assert_eq!(CMDTAG_MOVE.0, 164);
        assert_eq!(CMDTAG_SELECT.0, 179);
        assert_eq!(CMDTAG_UPDATE.0, 191);
        assert_eq!(CMDTAG_SELECT, CommandTag::SELECT);
        assert_eq!(COMPLETION_TAG_BUFSIZE, 64);
        assert_eq!(MAX_PORTALNAME_LEN, 64);
        assert_eq!(FETCH_ALL, i64::MAX);
    }

    #[test]
    fn handles_null_matches_c_null() {
        assert!(StmtListHandle::NULL.is_null());
        assert!(CachedPlanHandle::default().is_null());
        assert!(!QueryDescHandle(7).is_null());
        assert_eq!(PortalCleanupHook::default(), PortalCleanupHook::None);
    }
}
