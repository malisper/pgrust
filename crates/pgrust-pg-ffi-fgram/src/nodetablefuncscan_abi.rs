//! TableFuncScan node ABI vocabulary (`nodeTableFuncscan.c`).
//!
//! The `TableFunc` primitive node (primnodes.h), the `TableFuncScan` plan node
//! (plannodes.h), the `TableFuncScanStateData` execution state (execnodes.h), and the
//! `TableFuncRoutine` method table (executor/tablefunc.h) cross the executor
//! crate boundary as `#[repr(C)]` structs with compile-time size/align asserts.
//!
//! The embedded scan/plan heads reuse the shared
//! [`crate::execnodes::ScanStateData`] / [`crate::execnodes::PlanStateData`] /
//! [`crate::execnodes::Scan`] layouts so a `*mut TableFuncScanStateData` can be
//! navigated identically to the C struct.

use core::ffi::{c_char, c_int, c_void};

use crate::execnodes::{PlanStateData, Scan, ScanStateData};
use crate::{Bitmapset, Datum, ExprState, FmgrInfo, List, MemoryContext, Node, NodeTag, Oid};

/// `TableFuncType` (primnodes.h): which table-producer function this is.
///
/// ```c
/// typedef enum TableFuncType
/// {
///     TFT_XMLTABLE,
///     TFT_JSON_TABLE,
/// } TableFuncType;
/// ```
pub type TableFuncType = c_int;

/// `TFT_XMLTABLE` — XMLTABLE table function.
pub const TFT_XMLTABLE: TableFuncType = 0;
/// `TFT_JSON_TABLE` — JSON_TABLE table function.
pub const TFT_JSON_TABLE: TableFuncType = 1;

/// `TableFunc` primitive node (primnodes.h): the XMLTABLE/JSON_TABLE descriptor
/// carried by a `TableFuncScan`. Pointer-typed fields cross the boundary as raw
/// pointers; only `functype`, `ordinalitycol`, and `location` are scalar.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableFunc {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `TableFuncType functype` — XMLTABLE or JSON_TABLE.
    pub functype: TableFuncType,
    /// `List *ns_uris` — list of namespace URI expressions.
    pub ns_uris: *mut List,
    /// `List *ns_names` — list of namespace names or NULL.
    pub ns_names: *mut List,
    /// `Node *docexpr` — input document expression.
    pub docexpr: *mut Node,
    /// `Node *rowexpr` — row filter expression.
    pub rowexpr: *mut Node,
    /// `List *colnames` — column names (list of `String`).
    pub colnames: *mut List,
    /// `List *coltypes` — OID list of column type OIDs.
    pub coltypes: *mut List,
    /// `List *coltypmods` — integer list of column typmods.
    pub coltypmods: *mut List,
    /// `List *colcollations` — OID list of column collation OIDs.
    pub colcollations: *mut List,
    /// `List *colexprs` — list of column filter expressions.
    pub colexprs: *mut List,
    /// `List *coldefexprs` — list of column default expressions.
    pub coldefexprs: *mut List,
    /// `List *colvalexprs` — JSON_TABLE: list of column value expressions.
    pub colvalexprs: *mut List,
    /// `List *passingvalexprs` — JSON_TABLE: list of PASSING argument expressions.
    pub passingvalexprs: *mut List,
    /// `Bitmapset *notnulls` — nullability flag for each output column.
    pub notnulls: *mut Bitmapset,
    /// `Node *plan` — JSON_TABLE plan.
    pub plan: *mut Node,
    /// `int ordinalitycol` — counts from 0; -1 if none specified.
    pub ordinalitycol: c_int,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: c_int,
}

/// `TableFuncScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct TableFuncScan
/// {
///     Scan        scan;
///     TableFunc  *tablefunc;  /* table function node */
/// } TableFuncScan;
/// ```
///
/// The leading `Scan` embeds `Plan`; the node layer reads the trailing
/// `tablefunc` and the embedded `scan.plan.qual`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableFuncScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan,
    /// `TableFunc *tablefunc` — the XMLTABLE/JSON_TABLE descriptor.
    pub tablefunc: *mut TableFunc,
}

/// `TableFuncScanState` (execnodes.h): per-node execution state for a
/// `TableFuncScan`. The embedded `ScanStateData` head crosses the boundary; the
/// `opaque` table-builder private space and `routine` method table travel as
/// opaque pointers (the routine is the [`TableFuncRoutine`] seam).
///
/// ```c
/// typedef struct TableFuncScanState
/// {
///     ScanState   ss;             /* its first field is NodeTag */
///     ExprState  *docexpr;        /* state for document expression */
///     ExprState  *rowexpr;        /* state for row-generating expression */
///     List       *colexprs;       /* state for column-generating expression */
///     List       *coldefexprs;    /* state for column default expressions */
///     List       *colvalexprs;    /* state for column value expressions */
///     List       *passingvalexprs;    /* state for PASSING argument expressions */
///     List       *ns_names;       /* same as TableFunc.ns_names */
///     List       *ns_uris;        /* list of states of namespace URI exprs */
///     Bitmapset  *notnulls;       /* nullability flag for each output column */
///     void       *opaque;         /* table builder private space */
///     const struct TableFuncRoutine *routine; /* table builder methods */
///     FmgrInfo   *in_functions;   /* input function for each column */
///     Oid        *typioparams;    /* typioparam for each column */
///     int64       ordinal;        /* row number to be output next */
///     MemoryContext perTableCxt;  /* per-table context */
///     Tuplestorestate *tupstore;  /* output tuple store */
/// } TableFuncScanState;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableFuncScanStateData {
    /// `ScanState ss` — its first field is the `PlanState`/`NodeTag` head.
    pub ss: ScanStateData,
    /// `ExprState *docexpr` — state for the document expression.
    pub docexpr: *mut ExprState,
    /// `ExprState *rowexpr` — state for the row-generating expression.
    pub rowexpr: *mut ExprState,
    /// `List *colexprs` — state for the column-generating expressions.
    pub colexprs: *mut List,
    /// `List *coldefexprs` — state for the column default expressions.
    pub coldefexprs: *mut List,
    /// `List *colvalexprs` — state for the column value expressions.
    pub colvalexprs: *mut List,
    /// `List *passingvalexprs` — state for the PASSING argument expressions.
    pub passingvalexprs: *mut List,
    /// `List *ns_names` — same as `TableFunc.ns_names`.
    pub ns_names: *mut List,
    /// `List *ns_uris` — list of states of namespace URI exprs.
    pub ns_uris: *mut List,
    /// `Bitmapset *notnulls` — nullability flag for each output column.
    pub notnulls: *mut Bitmapset,
    /// `void *opaque` — table builder private space.
    pub opaque: *mut c_void,
    /// `const struct TableFuncRoutine *routine` — table builder methods.
    pub routine: *const TableFuncRoutine,
    /// `FmgrInfo *in_functions` — input function for each column.
    pub in_functions: *mut FmgrInfo,
    /// `Oid *typioparams` — typioparam for each column.
    pub typioparams: *mut Oid,
    /// `int64 ordinal` — row number to be output next.
    pub ordinal: i64,
    /// `MemoryContext perTableCxt` — per-table context.
    pub perTableCxt: MemoryContext,
    /// `Tuplestorestate *tupstore` — output tuple store.
    pub tupstore: *mut c_void,
}

impl TableFuncScanStateData {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

/// `TableFuncRoutine` (executor/tablefunc.h): the function-pointer table used to
/// generate the contents of XMLTABLE/JSON_TABLE table-producer functions.
///
/// The methods carry an opaque `*mut TableFuncScanState` plus scalars; the node
/// crate only ever forwards a pointer through them, so they cross the boundary
/// as raw `extern "C"` fn pointers. `SetRowFilter` is the only optional method
/// (NULL when the builder has no row filter, e.g. JSON_TABLE in some forms);
/// the others are always present.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableFuncRoutine {
    /// `void (*InitOpaque)(struct TableFuncScanState *state, int natts)`
    pub InitOpaque: Option<unsafe extern "C" fn(*mut TableFuncScanStateData, c_int)>,
    /// `void (*SetDocument)(struct TableFuncScanState *state, Datum value)`
    pub SetDocument: Option<unsafe extern "C" fn(*mut TableFuncScanStateData, Datum)>,
    /// `void (*SetNamespace)(struct TableFuncScanState *state, const char *name,
    /// const char *uri)`
    pub SetNamespace:
        Option<unsafe extern "C" fn(*mut TableFuncScanStateData, *const c_char, *const c_char)>,
    /// `void (*SetRowFilter)(struct TableFuncScanState *state, const char *path)`
    /// — optional (NULL if the builder has no row filter).
    pub SetRowFilter: Option<unsafe extern "C" fn(*mut TableFuncScanStateData, *const c_char)>,
    /// `void (*SetColumnFilter)(struct TableFuncScanState *state, const char
    /// *path, int colnum)`
    pub SetColumnFilter:
        Option<unsafe extern "C" fn(*mut TableFuncScanStateData, *const c_char, c_int)>,
    /// `bool (*FetchRow)(struct TableFuncScanState *state)`
    pub FetchRow: Option<unsafe extern "C" fn(*mut TableFuncScanStateData) -> bool>,
    /// `Datum (*GetValue)(struct TableFuncScanState *state, int colnum, Oid
    /// typid, int32 typmod, bool *isnull)`
    pub GetValue: Option<
        unsafe extern "C" fn(*mut TableFuncScanStateData, c_int, Oid, i32, *mut bool) -> Datum,
    >,
    /// `void (*DestroyOpaque)(struct TableFuncScanState *state)`
    pub DestroyOpaque: Option<unsafe extern "C" fn(*mut TableFuncScanStateData)>,
}

const _: () = {
    // TableFunc primnode: 18 pointer-sized words + scalars. functype is an int at
    // offset 4 (after the NodeTag); the trailing ordinalitycol/location pack into
    // one 8-byte word.
    assert!(core::mem::offset_of!(TableFunc, type_) == 0);
    assert!(core::mem::offset_of!(TableFunc, functype) == 4);
    assert!(core::mem::offset_of!(TableFunc, ns_uris) == 8);
    assert!(core::mem::offset_of!(TableFunc, ns_names) == 16);
    assert!(core::mem::offset_of!(TableFunc, docexpr) == 24);
    assert!(core::mem::offset_of!(TableFunc, rowexpr) == 32);
    assert!(core::mem::offset_of!(TableFunc, colnames) == 40);
    assert!(core::mem::offset_of!(TableFunc, coltypes) == 48);
    assert!(core::mem::offset_of!(TableFunc, coltypmods) == 56);
    assert!(core::mem::offset_of!(TableFunc, colcollations) == 64);
    assert!(core::mem::offset_of!(TableFunc, colexprs) == 72);
    assert!(core::mem::offset_of!(TableFunc, coldefexprs) == 80);
    assert!(core::mem::offset_of!(TableFunc, colvalexprs) == 88);
    assert!(core::mem::offset_of!(TableFunc, passingvalexprs) == 96);
    assert!(core::mem::offset_of!(TableFunc, notnulls) == 104);
    assert!(core::mem::offset_of!(TableFunc, plan) == 112);
    assert!(core::mem::offset_of!(TableFunc, ordinalitycol) == 120);
    assert!(core::mem::offset_of!(TableFunc, location) == 124);
    assert!(core::mem::size_of::<TableFunc>() == 128);

    // TableFuncScan plan node: `scan` embeds `Plan`; total 120 bytes on LP64
    // (Scan is 112 bytes here — PlanNode is 104 + Index scanrelid + pad → 112).
    assert!(core::mem::offset_of!(TableFuncScan, scan) == 0);
    assert!(core::mem::offset_of!(TableFuncScan, tablefunc) == core::mem::size_of::<Scan>());

    // TableFuncScanStateData: `ss` (ScanStateData, 224 bytes) then 16 pointer/int64
    // words (docexpr..tupstore) → 224 + 16*8 = 352 bytes.
    assert!(core::mem::offset_of!(TableFuncScanStateData, ss) == 0);
    assert!(core::mem::offset_of!(TableFuncScanStateData, docexpr) == 224);
    assert!(core::mem::offset_of!(TableFuncScanStateData, rowexpr) == 232);
    assert!(core::mem::offset_of!(TableFuncScanStateData, colexprs) == 240);
    assert!(core::mem::offset_of!(TableFuncScanStateData, coldefexprs) == 248);
    assert!(core::mem::offset_of!(TableFuncScanStateData, colvalexprs) == 256);
    assert!(core::mem::offset_of!(TableFuncScanStateData, passingvalexprs) == 264);
    assert!(core::mem::offset_of!(TableFuncScanStateData, ns_names) == 272);
    assert!(core::mem::offset_of!(TableFuncScanStateData, ns_uris) == 280);
    assert!(core::mem::offset_of!(TableFuncScanStateData, notnulls) == 288);
    assert!(core::mem::offset_of!(TableFuncScanStateData, opaque) == 296);
    assert!(core::mem::offset_of!(TableFuncScanStateData, routine) == 304);
    assert!(core::mem::offset_of!(TableFuncScanStateData, in_functions) == 312);
    assert!(core::mem::offset_of!(TableFuncScanStateData, typioparams) == 320);
    assert!(core::mem::offset_of!(TableFuncScanStateData, ordinal) == 328);
    assert!(core::mem::offset_of!(TableFuncScanStateData, perTableCxt) == 336);
    assert!(core::mem::offset_of!(TableFuncScanStateData, tupstore) == 344);
    assert!(core::mem::size_of::<TableFuncScanStateData>() == 352);

    // TableFuncRoutine: 8 fn-pointer slots → 64 bytes.
    assert!(core::mem::size_of::<TableFuncRoutine>() == 64);
};
