//! TableFuncScan node vocabulary (nodes/plannodes.h / executor/execnodes.h /
//! executor/tablefunc.h).
//!
//! `TableFuncScan` scans a `RangeTableFunc` — the `XMLTABLE` / `JSON_TABLE`
//! table-producer functions.

use ::mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgString, PgVec};
use ::types_core::fmgr::FmgrInfo;
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::execexpr::ExprState;
use crate::execnodes::{Opaque, PlanStateData, ScanStateData};
use crate::funcapi::Tuplestorestate;
use crate::nodes::NodeTag;
use crate::primnodes::{TableFunc, TableFuncType};

/// `T_TableFuncScanState` (nodes/nodetags.h) — the executor-state node tag for
/// a TableFuncScan node. Verified against PostgreSQL 18.3.
pub const T_TableFuncScanState: NodeTag = NodeTag(414);

/// `TableFuncScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct TableFuncScan { Scan scan; TableFunc *tablefunc; }
///     TableFuncScan;
/// ```
#[derive(Debug)]
pub struct TableFuncScan<'mcx> {
    /// `Scan scan` — the abstract scan-node base.
    pub scan: crate::nodeindexscan::Scan<'mcx>,
    /// `TableFunc *tablefunc` — the table function node. The plan tree owns it;
    /// the executor reads it read-only at init.
    pub tablefunc: PgBox<'mcx, TableFunc<'mcx>>,
}

/// `TableFuncRoutine` selector (executor/tablefunc.h): C dispatches through a
/// `const TableFuncRoutine *` vtable, of which only two instances exist —
/// `XmlTableRoutine` (`xml.c`) and `JsonbTableRoutine` (`jsonpath_exec.c`).
/// The owned model carries that identity as this token; the vtable methods are
/// reached through the owner's seam crate, keyed by the kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TableFuncRoutineKind {
    /// `&XmlTableRoutine` (xml.c).
    XmlTable,
    /// `&JsonbTableRoutine` (jsonpath_exec.c).
    JsonbTable,
}

impl TableFuncRoutineKind {
    /// `tf->functype == TFT_XMLTABLE ? &XmlTableRoutine : &JsonbTableRoutine`
    /// — the routine selector for a `TableFuncType`.
    #[inline]
    pub fn from_functype(functype: TableFuncType) -> Self {
        match functype {
            TableFuncType::TFT_XMLTABLE => TableFuncRoutineKind::XmlTable,
            TableFuncType::TFT_JSON_TABLE => TableFuncRoutineKind::JsonbTable,
        }
    }
}

/// `TableFuncScanState` (execnodes.h):
///
/// ```c
/// typedef struct TableFuncScanState {
///     ScanState   ss;                 /* its first field is NodeTag */
///     ExprState  *docexpr;
///     ExprState  *rowexpr;
///     List       *colexprs;
///     List       *coldefexprs;
///     List       *colvalexprs;
///     List       *passingvalexprs;
///     List       *ns_names;
///     List       *ns_uris;
///     Bitmapset  *notnulls;
///     void       *opaque;
///     const struct TableFuncRoutine *routine;
///     FmgrInfo   *in_functions;
///     Oid        *typioparams;
///     int64       ordinal;
///     MemoryContext perTableCxt;
///     Tuplestorestate *tupstore;
/// } TableFuncScanState;
/// ```
///
/// In the owned tree, the `ExprState *` children are owned `ExprState` values
/// (`ExecInitExpr` output, allocated in the per-query context), and the
/// `colexprs`/`coldefexprs`/etc. lists carry `Option<ExprState<'mcx>>` to mirror
/// the C `NULL` cells. `routine` is the [`TableFuncRoutineKind`] token; the
/// builder's private space (`void *opaque`) is the type-erased [`Opaque`].
#[derive(Debug)]
pub struct TableFuncScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprState *docexpr` — state for document expression.
    pub docexpr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *rowexpr` — state for row-generating expression.
    pub rowexpr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `List *colexprs` — state for column-generating expressions (NULL cells
    /// allowed).
    pub colexprs: PgVec<'mcx, Option<ExprState<'mcx>>>,
    /// `List *coldefexprs` — state for column default expressions (NULL cells
    /// allowed).
    pub coldefexprs: PgVec<'mcx, Option<ExprState<'mcx>>>,
    /// `List *colvalexprs` — state for column value expressions.
    pub colvalexprs: PgVec<'mcx, Option<ExprState<'mcx>>>,
    /// `List *passingvalexprs` — state for PASSING argument expressions.
    pub passingvalexprs: PgVec<'mcx, Option<ExprState<'mcx>>>,
    /// `List *ns_names` — same as `TableFunc.ns_names` (DEFAULT namespace is a
    /// `None` cell).
    pub ns_names: PgVec<'mcx, Option<PgString<'mcx>>>,
    /// `List *ns_uris` — list of states of namespace URI exprs.
    pub ns_uris: PgVec<'mcx, ExprState<'mcx>>,
    /// `Bitmapset *notnulls` — nullability flag for each output column.
    pub notnulls: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `void *opaque` — table builder private space. `None` = the C `NULL`.
    pub opaque: Opaque,
    /// `const struct TableFuncRoutine *routine` — table builder methods,
    /// carried as the routine kind token. `None` until set in init.
    pub routine: Option<TableFuncRoutineKind>,
    /// `FmgrInfo *in_functions` — input function for each column.
    pub in_functions: PgVec<'mcx, FmgrInfo>,
    /// `Oid *typioparams` — typioparam for each column.
    pub typioparams: PgVec<'mcx, Oid>,
    /// `int64 ordinal` — row number to be output next.
    pub ordinal: i64,
    /// `MemoryContext perTableCxt` — per-table context. A real owned child
    /// context of `CurrentMemoryContext`.
    pub perTableCxt: Option<MemoryContext>,
    /// `Tuplestorestate *tupstore` — output tuple store. The carrier's empty
    /// state is the C `NULL`.
    pub tupstore: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
}

impl<'mcx> TableFuncScanState<'mcx> {
    /// `makeNode(TableFuncScanState)`-shaped construction: a palloc0 state with
    /// every list empty (the C `NIL`/`NULL`), allocated in `mcx`.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        TableFuncScanState {
            ss: ScanStateData::default(),
            docexpr: None,
            rowexpr: None,
            colexprs: PgVec::new_in(mcx),
            coldefexprs: PgVec::new_in(mcx),
            colvalexprs: PgVec::new_in(mcx),
            passingvalexprs: PgVec::new_in(mcx),
            ns_names: PgVec::new_in(mcx),
            ns_uris: PgVec::new_in(mcx),
            notnulls: None,
            opaque: Opaque::default(),
            routine: None,
            in_functions: PgVec::new_in(mcx),
            typioparams: PgVec::new_in(mcx),
            ordinal: 0,
            perTableCxt: None,
            tupstore: None,
        }
    }

    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

impl TableFuncScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TableFuncScan<'b>> {
        Ok(TableFuncScan {
            scan: self.scan.clone_in(mcx)?,
            tablefunc: alloc_in(mcx, self.tablefunc.clone_in(mcx)?)?,
        })
    }
}
