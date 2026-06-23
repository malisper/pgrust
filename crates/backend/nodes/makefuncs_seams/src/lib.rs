//! Seam declarations for the `backend-nodes-makefuncs` unit
//! (`nodes/makefuncs.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Allocating constructors take the target context
//! handle (C: they palloc the node in `CurrentMemoryContext`).

use mcx::{Mcx, PgBox, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_tuple::Datum;
use ::nodes::nodes::Node;
use parsenodes::{Node as ParseNode, TypeName as ParseTypeName};

seam_core::seam!(
    /// `makeConst(consttype, consttypmod, constcollid, constlen, constvalue,
    /// constisnull, constbyval)` (makefuncs.c): build a `Const` node, allocated
    /// in `mcx`. Used by lsyscache.c's `get_typdefault`. `Err` carries OOM.
    pub fn make_const_node<'mcx>(
        mcx: Mcx<'mcx>,
        consttype: Oid,
        consttypmod: i32,
        constcollid: Oid,
        constlen: i32,
        constvalue: Datum<'mcx>,
        constisnull: bool,
        constbyval: bool,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `makeBoolExpr(AND_EXPR, args, location)` (makefuncs.c): build a
    /// `BoolExpr` node combining `args` with `AND_EXPR` at the source
    /// `location` (-1 for "unknown"). The node and its arg list are allocated
    /// in `mcx`. `Err` carries OOM.
    pub fn make_and_boolexpr<'mcx>(
        mcx: Mcx<'mcx>,
        args: PgVec<'mcx, Node<'mcx>>,
        location: i32,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `makeTypeNameFromNameList(names)` (makefuncs.c): build a `TypeName`
    /// node from a `List *` of `String` value nodes (`names`). The raw-parser
    /// node tree is owned plain-Rust (not `mcx`-allocated), so this returns the
    /// owned `TypeName`.
    pub fn make_type_name_from_name_list(names: Vec<ParseNode>) -> PgResult<ParseTypeName>
);
