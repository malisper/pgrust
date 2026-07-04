#![allow(non_camel_case_types)]

use std::rc::Rc;

use tcop_dest::DestReceiver;
use types_core::CommandTag;
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::plannodes::PlannedStmt;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle};
use types_tuple::TupleDescData;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ProcessUtilityContext {
    PROCESS_UTILITY_TOPLEVEL = 0,
    PROCESS_UTILITY_QUERY,
    PROCESS_UTILITY_QUERY_NONATOMIC,
    PROCESS_UTILITY_SUBCOMMAND,
}

pub use ProcessUtilityContext::*;

seam_core::seam!(
    pub fn create_command_tag(parsetree: Node<'_>) -> CommandTag
);

seam_core::seam!(
    pub fn get_command_log_level(parsetree: Node<'_>) -> i32
);

seam_core::seam!(
    pub fn utility_returns_tuples(parsetree: Node<'_>) -> bool
);

seam_core::seam!(
    // UtilityContainsQuery (utility.c): the contained analyzed Query node.
    pub fn utility_contains_query<'mcx>(parsetree: Node<'mcx>) -> Option<Node<'mcx>>
);

seam_core::seam!(
    pub fn utility_tuple_descriptor(
        parsetree: Node<'_>,
    ) -> PgResult<Option<Rc<TupleDescData<'static>>>>
);

seam_core::seam!(
    // ProcessUtility (utility.c); receiver threaded by ref as in executor_run.
    // mcx is C's ambient CurrentMemoryContext at the call (the portal
    // context); it shares the receiver's lifetime because utility output
    // (tupdescs, slots, text datums) is allocated in it and fed to dest.
    pub fn process_utility<'p, 'a, 's, 'd, 'q, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstmt: &'p PlannedStmt<'a>,
        source_text: &'s str,
        read_only_tree: bool,
        context: ProcessUtilityContext,
        params: ParamListHandle,
        query_env: QueryEnvHandle,
        dest: &'d mut DestReceiver<'mcx>,
        qc: Option<&'q mut QueryCompletion>,
    ) -> PgResult<()>
);
