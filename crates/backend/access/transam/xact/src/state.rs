// xact.c's file-scope statics, one owned value per backend, plus const-init
// Cell mirrors of the hottest scalar reads (fabled #347 endpoint): an
// initialized read compiles to the one load C pays for
// `CurrentTransactionState->blockState`. Coherence is structural, not
// audited: the stack and mirrored scalars are private; every `&mut` node
// access goes through `NodeMut`, whose Drop refreshes the mirrors, and
// scalar writes go through `set_*` that write both sides.

use std::cell::{Cell, UnsafeCell};
use std::mem::ManuallyDrop;

use mcx::MemoryContext;
use types_core::xact::*;
use types_core::{LocalTransactionId, Oid, TimestampTz, TransactionId};

use crate::{SubXactCallbackItem, XactCallbackItem};

// `TransactionStateData`. C's parent-linked list is the top node inline plus
// a Vec of subtransaction nodes; `priorContext` and `curTransactionOwner`
// dissolve (no ambient context; owner value lives in the resowner unit,
// `has_resource_owner` keeps the `if (s->curTransactionOwner)` arms).
//
// Transaction-lifetime collections here are std Vec/String, a ledgered
// divergence from AGENTS.md rule 3: this state owns TopTransactionContext,
// so it cannot also borrow from it (self-referential); every allocating
// touch is a fallible reserve carrying C's OOM surface. Subxact push and
// savepoint naming are per-command cold paths.
#[derive(Debug)]
pub(crate) struct TransactionNode {
    pub full_transaction_id: FullTransactionId,
    pub sub_transaction_id: SubTransactionId,
    pub name: Option<String>,
    pub savepoint_level: i32,
    pub state: TransState,
    pub block_state: TBlockState,
    pub nesting_level: i32,
    pub guc_nest_level: i32,
    /// `childXids`: subcommitted child XIDs, in `TransactionIdPrecedes` order.
    pub child_xids: Vec<TransactionId>,
    pub prev_user: Oid,
    pub prev_sec_context: i32,
    pub prev_xact_read_only: bool,
    pub started_in_recovery: bool,
    pub did_log_xid: bool,
    pub parallel_mode_level: i32,
    pub parallel_child_xact: bool,
    pub chain: bool,
    pub top_xid_logged: bool,
    pub has_resource_owner: bool,
    /// Subxact `curTransactionContext` (child of the parent's); `None` on the
    /// top node, whose CurTransactionContext IS TopTransactionContext.
    pub cur_transaction_context: Option<MemoryContext>,
    /// Non-empty subxact contexts kept alive at subcommit (C keeps them as
    /// children of the parent context until top-level end).
    pub retained_child_contexts: Vec<MemoryContext>,
}

impl TransactionNode {
    pub(crate) const fn top() -> Self {
        Self {
            full_transaction_id: InvalidFullTransactionId,
            sub_transaction_id: InvalidSubTransactionId,
            name: None,
            savepoint_level: 0,
            state: TRANS_DEFAULT,
            block_state: TBLOCK_DEFAULT,
            nesting_level: 0,
            guc_nest_level: 0,
            child_xids: Vec::new(),
            prev_user: 0,
            prev_sec_context: 0,
            prev_xact_read_only: false,
            started_in_recovery: false,
            did_log_xid: false,
            parallel_mode_level: 0,
            parallel_child_xact: false,
            chain: false,
            top_xid_logged: false,
            has_resource_owner: false,
            cur_transaction_context: None,
            retained_child_contexts: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct XactStack {
    top: TransactionNode,
    subs: Vec<TransactionNode>,
}

#[derive(Debug)]
pub(crate) struct XactState {
    pub DefaultXactIsoLevel: i32,
    pub XactIsoLevel: i32,
    pub DefaultXactReadOnly: bool,
    pub XactReadOnly: bool,
    pub DefaultXactDeferrable: bool,
    pub XactDeferrable: bool,
    pub synchronous_commit: i32,
    pub CheckXidAlive: TransactionId,
    pub bsysscan: bool,
    pub MyXactFlags: i32,
    pub xact_is_sampled: bool,
    xact_top_full_transaction_id: FullTransactionId,
    /// `ParallelCurrentXids`, sorted numerically; empty in a non-worker.
    pub parallel_current_xids: Vec<TransactionId>,
    pub current_sub_transaction_id: SubTransactionId,
    current_command_id: CommandId,
    current_command_id_used: bool,
    pub xact_start_timestamp: TimestampTz,
    pub stmt_start_timestamp: TimestampTz,
    pub xact_stop_timestamp: TimestampTz,
    pub force_sync_commit: bool,
    pub prepare_gid: Option<String>,
    pub unreported_xids: Vec<TransactionId>,
    /// `GetStableLatestTransactionId`'s function-local static latch.
    pub stable_latest: (LocalTransactionId, TransactionId),
    stack: XactStack,
    pub xact_callbacks: Vec<XactCallbackItem>,
    pub subxact_callbacks: Vec<SubXactCallbackItem>,
    pub top_transaction_context: Option<MemoryContext>,
    pub transaction_abort_context: Option<MemoryContext>,
}

thread_local! {
    static CUR_TRANS_STATE: Cell<TransState> = const { Cell::new(TRANS_DEFAULT) };
    static CUR_BLOCK_STATE: Cell<TBlockState> = const { Cell::new(TBLOCK_DEFAULT) };
    static CUR_NEST_LEVEL: Cell<i32> = const { Cell::new(0) };
    static CUR_FULL_XID: Cell<FullTransactionId> = const { Cell::new(InvalidFullTransactionId) };
    static CUR_COMMAND_ID: Cell<CommandId> = const { Cell::new(FirstCommandId) };
    static CUR_COMMAND_ID_USED: Cell<bool> = const { Cell::new(false) };
    static TOP_FULL_XID: Cell<FullTransactionId> = const { Cell::new(InvalidFullTransactionId) };
}

pub(crate) fn mirror_trans_state() -> TransState {
    CUR_TRANS_STATE.get()
}
pub(crate) fn mirror_block_state() -> TBlockState {
    CUR_BLOCK_STATE.get()
}
pub(crate) fn mirror_nest_level() -> i32 {
    CUR_NEST_LEVEL.get()
}
pub(crate) fn mirror_cur_full_xid() -> FullTransactionId {
    CUR_FULL_XID.get()
}
pub(crate) fn mirror_command_id() -> CommandId {
    CUR_COMMAND_ID.get()
}
pub(crate) fn mirror_command_id_used() -> bool {
    CUR_COMMAND_ID_USED.get()
}
pub(crate) fn mirror_top_full_xid() -> FullTransactionId {
    TOP_FULL_XID.get()
}

fn refresh_node_mirrors(node: &TransactionNode) {
    CUR_TRANS_STATE.set(node.state);
    CUR_BLOCK_STATE.set(node.block_state);
    CUR_NEST_LEVEL.set(node.nesting_level);
    CUR_FULL_XID.set(node.full_transaction_id);
}

/// Mutable access to one stack node; refreshes the current-node mirrors on
/// drop when the node is the current one.
pub(crate) struct NodeMut<'a> {
    node: &'a mut TransactionNode,
    is_current: bool,
}

impl std::ops::Deref for NodeMut<'_> {
    type Target = TransactionNode;
    fn deref(&self) -> &TransactionNode {
        self.node
    }
}

impl std::ops::DerefMut for NodeMut<'_> {
    fn deref_mut(&mut self) -> &mut TransactionNode {
        self.node
    }
}

impl Drop for NodeMut<'_> {
    fn drop(&mut self) {
        if self.is_current {
            refresh_node_mirrors(self.node);
        }
    }
}

impl XactState {
    pub(crate) const fn new() -> Self {
        Self {
            DefaultXactIsoLevel: XACT_READ_COMMITTED,
            XactIsoLevel: XACT_READ_COMMITTED,
            DefaultXactReadOnly: false,
            XactReadOnly: false,
            DefaultXactDeferrable: false,
            XactDeferrable: false,
            synchronous_commit: SYNCHRONOUS_COMMIT_ON,
            CheckXidAlive: InvalidTransactionId,
            bsysscan: false,
            MyXactFlags: 0,
            xact_is_sampled: false,
            xact_top_full_transaction_id: InvalidFullTransactionId,
            parallel_current_xids: Vec::new(),
            current_sub_transaction_id: InvalidSubTransactionId,
            current_command_id: FirstCommandId,
            current_command_id_used: false,
            xact_start_timestamp: 0,
            stmt_start_timestamp: 0,
            xact_stop_timestamp: 0,
            force_sync_commit: false,
            prepare_gid: None,
            unreported_xids: Vec::new(),
            stable_latest: (0, InvalidTransactionId),
            stack: XactStack { top: TransactionNode::top(), subs: Vec::new() },
            xact_callbacks: Vec::new(),
            subxact_callbacks: Vec::new(),
            top_transaction_context: None,
            transaction_abort_context: None,
        }
    }

    pub(crate) fn current(&self) -> &TransactionNode {
        self.stack.subs.last().unwrap_or(&self.stack.top)
    }

    /// Stack depth; `[0]` is `TopTransactionStateData`, never 0.
    pub(crate) fn stack_len(&self) -> usize {
        1 + self.stack.subs.len()
    }

    pub(crate) fn node(&self, i: usize) -> &TransactionNode {
        if i == 0 {
            &self.stack.top
        } else {
            &self.stack.subs[i - 1]
        }
    }

    /// Front-to-back (top-level first): C's parent-first print order.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = &TransactionNode> {
        std::iter::once(&self.stack.top).chain(self.stack.subs.iter())
    }

    /// Back-to-front (current first): C's `s->parent` walk.
    pub(crate) fn nodes_rev(&self) -> impl Iterator<Item = &TransactionNode> {
        self.stack.subs.iter().rev().chain(std::iter::once(&self.stack.top))
    }

    /// Index of the innermost node matching `f`.
    pub(crate) fn rposition_node(
        &self,
        mut f: impl FnMut(&TransactionNode) -> bool,
    ) -> Option<usize> {
        if let Some(k) = self.stack.subs.iter().rposition(&mut f) {
            return Some(k + 1);
        }
        if f(&self.stack.top) {
            Some(0)
        } else {
            None
        }
    }

    pub(crate) fn is_subxact(&self) -> bool {
        !self.stack.subs.is_empty()
    }

    pub(crate) fn current_mut(&mut self) -> NodeMut<'_> {
        let node = if self.stack.subs.is_empty() {
            &mut self.stack.top
        } else {
            self.stack.subs.last_mut().expect("non-empty subs")
        };
        NodeMut { node, is_current: true }
    }

    pub(crate) fn node_mut(&mut self, i: usize) -> NodeMut<'_> {
        let is_current = i + 1 == self.stack_len();
        let node = if i == 0 {
            &mut self.stack.top
        } else {
            &mut self.stack.subs[i - 1]
        };
        NodeMut { node, is_current }
    }

    pub(crate) fn try_push_node(
        &mut self,
        node: TransactionNode,
    ) -> Result<(), std::collections::TryReserveError> {
        self.stack.subs.try_reserve(1)?;
        self.stack.subs.push(node);
        refresh_node_mirrors(self.current());
        Ok(())
    }

    pub(crate) fn pop_node(&mut self) {
        self.stack.subs.pop();
        refresh_node_mirrors(self.current());
    }

    pub(crate) fn command_id(&self) -> CommandId {
        self.current_command_id
    }

    pub(crate) fn set_command_id(&mut self, v: CommandId) {
        self.current_command_id = v;
        CUR_COMMAND_ID.set(v);
    }

    pub(crate) fn command_id_used(&self) -> bool {
        self.current_command_id_used
    }

    pub(crate) fn set_command_id_used(&mut self, v: bool) {
        self.current_command_id_used = v;
        CUR_COMMAND_ID_USED.set(v);
    }

    pub(crate) fn top_full_xid(&self) -> FullTransactionId {
        self.xact_top_full_transaction_id
    }

    pub(crate) fn set_top_full_xid(&mut self, v: FullTransactionId) {
        self.xact_top_full_transaction_id = v;
        TOP_FULL_XID.set(v);
    }

    pub(crate) fn reset_for_tests(&mut self) {
        *self = XactState::new();
        refresh_node_mirrors(self.current());
        CUR_COMMAND_ID.set(self.current_command_id);
        CUR_COMMAND_ID_USED.set(self.current_command_id_used);
        TOP_FULL_XID.set(self.xact_top_full_transaction_id);
    }
}

thread_local! {
    // UnsafeCell + ManuallyDrop, not RefCell (rule 10, lock's with_local
    // precedent): const-init + !needs_drop payload compiles each access to a
    // plain TLS address, no dtor-registration branch, no borrow flags. The
    // arena-less state leaks at thread exit exactly as C's globals do.
    static STATE: UnsafeCell<ManuallyDrop<XactState>> =
        const { UnsafeCell::new(ManuallyDrop::new(XactState::new())) };
    #[cfg(debug_assertions)]
    static XS_BUSY: Cell<bool> = const { Cell::new(false) };
}

/// Every closure is a leaf: no seam call, no call that may re-enter this
/// crate. XS_BUSY enforces in debug builds and under Miri.
pub(crate) fn xs<R>(f: impl FnOnce(&mut XactState) -> R) -> R {
    xs_ptr().with(f)
}

/// Session-memory teardown (FPBUDGET-1): free the transaction contexts at
/// clean task end. Contexts only — the state shell stays in TLS untouched
/// (nothing runs xact code after teardown; the thread is exiting).
pub fn session_mem_teardown() {
    xs(|s| {
        drop(s.top_transaction_context.take());
        drop(s.transaction_abort_context.take());
    });
}

/// The state's TLS address, resolved once per transaction phase and threaded
/// through the phase's helpers (C reads plain globals). Callouts between
/// `with` blocks may re-enter `xs`; only the brief `&mut` inside `with` must
/// stay a leaf (XS_BUSY).
#[derive(Clone, Copy)]
pub(crate) struct XsPtr(*mut XactState);

impl XsPtr {
    #[inline(always)]
    pub(crate) fn with<R>(self, f: impl FnOnce(&mut XactState) -> R) -> R {
        // Guard module Drop: BUSY must clear on panic unwind or every later
        // call — including abort cleanup — re-panics and the backend spins
        // (the snapmgr with_state wedge class).
        #[cfg(debug_assertions)]
        struct BusyReset;
        #[cfg(debug_assertions)]
        impl Drop for BusyReset {
            fn drop(&mut self) {
                XS_BUSY.set(false);
            }
        }
        #[cfg(debug_assertions)]
        let _busy = {
            assert!(!XS_BUSY.replace(true), "xs re-entered");
            BusyReset
        };
        // SAFETY: single-threaded backend TLS, live for the thread lifetime;
        // the leaf invariant excludes a second live &mut (XS_BUSY in debug).
        f(unsafe { &mut *self.0 })
    }
}

pub(crate) fn xs_ptr() -> XsPtr {
    XsPtr(STATE.with(|s| s.get()).cast::<XactState>())
}
