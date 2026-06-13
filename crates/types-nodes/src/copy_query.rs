//! The parse/plan/execute vocabulary the COPY-(query)-TO driver consumes
//! (`nodes/parsenodes.h`, `nodes/plannodes.h`, `executor/execdesc.h`), trimmed
//! to the fields copyto.c reads. The parser/planner/executor units that own
//! these types are unported; copyto reaches their functions through seams and
//! reads only these fields off the returned values.

use mcx::{PgBox, PgString};

use crate::execnodes::Opaque;
use crate::nodes::CmdType;
use types_tuple::heaptuple::TupleDesc;

/// `CURSOR_OPT_PARALLEL_OK` (`nodes/parsenodes.h`) — parallel mode OK.
pub const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

/// `ParseState` (`parser/parse_node.h`), trimmed to the one field the COPY
/// drivers read (`pstate->p_sourcetext`, the original query string passed to
/// analysis and planning). The parser unit owns the full structure.
pub struct ParseState<'mcx> {
    /// `const char *p_sourcetext` — source text of the query.
    pub p_sourcetext: PgString<'mcx>,
}

/// `QuerySource` (`nodes/parsenodes.h`) — where a rewritten query came from.
/// Values are PostgreSQL 18.3's enumeration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum QuerySource {
    /// `QSRC_ORIGINAL` — original parsetree (explicit query).
    QSRC_ORIGINAL = 0,
    /// `QSRC_PARSER` — added by parse analysis (now unused).
    QSRC_PARSER = 1,
    /// `QSRC_INSTEAD_RULE` — added by unconditional INSTEAD rule.
    QSRC_INSTEAD_RULE = 2,
    /// `QSRC_QUAL_INSTEAD_RULE` — added by conditional INSTEAD rule.
    QSRC_QUAL_INSTEAD_RULE = 3,
    /// `QSRC_NON_INSTEAD_RULE` — added by non-INSTEAD rule.
    QSRC_NON_INSTEAD_RULE = 4,
}

/// `Query` (`nodes/parsenodes.h`), trimmed to the fields the COPY-(query)-TO
/// validation reads after rewrite.
pub struct Query<'mcx> {
    /// `CmdType commandType`.
    pub commandType: CmdType,
    /// `QuerySource querySource`.
    pub querySource: QuerySource,
    /// `Node *utilityStmt` — the utility statement, with its node tag, when
    /// `commandType == CMD_UTILITY` (`None` otherwise). Only the tag is read
    /// (the SELECT-INTO `CreateTableAsStmt` check).
    pub utilityStmt: Option<crate::nodes::NodeTag>,
    /// `List *returningList` — `true` if non-NIL (the only thing copyto reads).
    pub has_returning_list: bool,
    /// Ties the `Query` to the context it (and its node tree) lives in; the
    /// rewrite output is allocated there.
    pub _marker: core::marker::PhantomData<&'mcx ()>,
}

/// `RawStmt` (`nodes/parsenodes.h`) — the raw parse tree handed to analysis.
/// Opaque to copyto, which only passes it to the analyze-and-rewrite seam.
pub struct RawStmt<'mcx> {
    /// `Node *stmt` — opaque parse tree node (owned by the parser unit).
    pub stmt: PgBox<'mcx, Opaque>,
}

/// `QueryDesc` (`executor/execdesc.h`), trimmed to the fields copyto reads.
/// The executor unit owns the full structure; copyto holds it as the executable
/// query handle, reading `tupDesc` after `ExecutorStart` and driving it through
/// the executor seams. `exec_token` is the executor unit's handle for the
/// started query state (the owned stand-in for the `QueryDesc *` the executor
/// keeps its `EState`/`DestReceiver` under).
pub struct QueryDesc<'mcx> {
    /// `TupleDesc tupDesc` — result tuple descriptor (set by `ExecutorStart`).
    pub tupDesc: TupleDesc<'mcx>,
    /// Opaque executor handle for this started query (set by the executor seam).
    pub exec_token: u64,
}

/// `T_CreateTableAsStmt` (`nodes/nodetags.h`) — value verified against
/// PostgreSQL 18.3's generated enumeration order. Used by the SELECT-INTO check.
pub const T_CreateTableAsStmt: crate::nodes::NodeTag = crate::nodes::NodeTag(242);
