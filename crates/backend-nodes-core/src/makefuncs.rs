//! Family: **makefuncs** — `nodes/makefuncs.c`, the node constructors.
//!
//! The `make*` constructors that `palloc` and populate a node. In the layered
//! owned-tree model the targets fall in three buckets:
//!
//! * **executable-expression nodes** (`makeVar`, `makeConst`, `makeBoolExpr`,
//!   `makeRelabelType`, `makeFuncExpr`, `make_opclause`, the boolean-clause
//!   helpers, `makeTargetEntry`, the JSON expression nodes) build a
//!   [`types_nodes::primnodes::Expr`] subtree. The Expr tree is lifetime-free
//!   (owned `Box`/`Vec`), so these are total constructors. Where the C returns
//!   the node through a `Node *`-typed API (`get_typdefault`, the partition-qual
//!   list), the seam wraps the Expr as [`Node::Expr`] and allocates the box in
//!   `mcx` — exactly the C `(Node *) expr` cast palloc'd in the current context.
//! * **plan/exec-state nodes** (`makeIndexInfo`) build a `types_nodes` exec
//!   struct.
//! * **raw-parser nodes** (`makeRangeVar`, `makeTypeName*`) build an owned
//!   plain-Rust parse node (`types_tuple::RangeVar`, `types_parsenodes::*`); no
//!   allocator.
//!
//! Owns the canonical `backend-nodes-makefuncs-seams`
//! (`make_const_node`, `make_and_boolexpr`, `make_type_name_from_name_list`),
//! installed in [`super::init_seams`].
//!
//! mirror-PG-and-panic: `makeConst`'s varlena-detoast branch
//! (`PG_DETOAST_DATUM`) delegates to the `backend-access-common-detoast` owner's
//! `detoast_attr` seam.

use mcx::{alloc_in, Mcx, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_core::catalog::BOOLOID;
use types_core::InvalidOid;
// Datum-unification: the owned `Const` carries the canonical unified value type
// [`Datum`] (`ByVal`/`ByRef`), and `make_const`/`make_const_node_seam` thread it
// end-to-end. The only residual use of the bare-word [`ScalarWord`] (the canonical
// `ByVal` arm's payload, `types_datum::Datum`) is the sanctioned varlena-pointer
// edge in `pg_detoast_datum`: a varlena `Datum` is a bare pointer into a varlena
// image, and the `detoast_attr` seam returns the fetched bytes through a leaked
// pointer word (the audited bare-word ABI edge), not a `ByRef` slice.
use types_datum::Datum as ScalarWord;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;

use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CoercionForm, Const, Expr, FuncExpr, JsonBehavior, JsonBehaviorType,
    JsonFormat, JsonFormatType, JsonEncoding, JsonIsPredicate, JsonValueExpr, JsonValueType,
    OpExpr, RelabelType, TargetEntry, Var, AND_EXPR, NOT_EXPR, OR_EXPR,
};
use types_nodes::execnodes::IndexInfo;
use types_tuple::access::{RangeVar, RELPERSISTENCE_PERMANENT};

use types_parsenodes::{Node as ParseNode, StringNode, TypeName};

use backend_access_common_detoast_seams as detoast_seam;

// ===========================================================================
// Expression-node constructors (build an owned `Expr` subtree).
// ===========================================================================

/// `makeVar(varno, varattno, vartype, vartypmod, varcollid, varlevelsup)`
/// (makefuncs.c) — create a `Var` node.
///
/// The trimmed [`Var`] carries the fields executor/optimizer readers consume
/// (including `varcollid`); `varreturningtype`, `varnullingrels`,
/// `varnosyn`/`varattnosyn` and `location` (which the C also sets to defaults)
/// are not modeled here.
pub fn make_var(
    varno: i32,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: Index,
) -> Var {
    Var {
        varno,
        varattno,
        vartype,
        vartypmod,
        varcollid,
        varlevelsup,
        ..Default::default()
    }
}

/// `makeConst(consttype, consttypmod, constcollid, constlen, constvalue,
/// constisnull, constbyval)` (makefuncs.c) — create a `Const` node.
///
/// If it's a varlena value (`constlen == -1`) and not null, force it to
/// non-expanded/non-toasted format (`PG_DETOAST_DATUM`) for representation
/// consistency, delegating the fetch/decompress to the `detoast` owner.
///
/// The trimmed [`Const`] carries
/// `consttype`/`consttypmod`/`constcollid`/`constvalue`/`constisnull`;
/// `constlen`/`constbyval`/`location` (also set by the C) are not modeled as
/// fields, but `constlen`/`constbyval` still drive the detoast decision exactly
/// as in the C.
pub fn make_const<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i32,
    mut constvalue: Datum<'mcx>,
    constisnull: bool,
    _constbyval: bool,
) -> PgResult<Const> {
    // if (!constisnull && constlen == -1)
    //     constvalue = PointerGetDatum(PG_DETOAST_DATUM(constvalue));
    //
    // The varlena `PG_DETOAST_DATUM` leg operates on the bare pointer word the
    // value's `ByVal` arm wraps (the sanctioned bare-word edge: a varlena Datum
    // is a pointer into a varlena image). A by-reference value here is the
    // execTuples canonical-carrier follow-on (#113): the detoast owner's seam
    // still takes a byte slice, so a `ByRef` image would already be the flat
    // bytes — no fetch/decompress would be needed (it cannot be external/
    // compressed). We therefore detoast only the by-value (pointer-word) form.
    if !constisnull && constlen == -1 {
        if let Datum::ByVal(word) = constvalue {
            constvalue =
                Datum::ByVal(pg_detoast_datum(mcx, ScalarWord::from_usize(word))?.as_usize());
        }
    }

    // The trimmed `Const.constvalue` field is typed `Datum<'static>` (the node
    // carries no lifetime parameter), so only the lifetime-free by-value arm
    // can be stored. The by-value word IS the canonical `ByVal` payload (a bare
    // machine word, or — for a varlena — a pointer into a varlena image that
    // outlives `mcx`), exactly C's `Const.constvalue` Datum. A by-reference
    // value would require a lifetime-carrying `Const`: the execTuples
    // canonical-carrier follow-on (#113). We record that edge rather than forge
    // a pointer across the lifetime boundary.
    let constvalue: Datum<'static> = match constvalue {
        Datum::ByVal(word) => Datum::ByVal(word),
        Datum::ByRef(_) => panic!(
            "make_const: by-reference Const value requires a lifetime-carrying \
             Const carrier (execTuples canonical-carrier follow-on, #113)"
        ),
    };

    Ok(Const {
        consttype,
        consttypmod,
        constcollid,
        constvalue,
        constisnull,
    })
}

/// `makeNullConst` / `makeBoolConst` build varlena/plain consts; `makeBoolConst`
/// is self-contained (it hardwires bool's storage like the C does), so it lives
/// here. `makeNullConst` needs `get_typlenbyval` from the lsyscache owner and
/// is built by that caller path, not modeled in this trimmed family.
///
/// `makeBoolConst(value, isnull)` (makefuncs.c) — a `Const` of type `bool`.
/// The C hardwires bool's `constlen == 1` / `constbyval == true`, so no
/// detoast and no allocation can occur.
pub fn make_bool_const(value: bool, isnull: bool) -> Const {
    // makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(value), isnull, true)
    Const {
        consttype: BOOLOID,
        consttypmod: -1,
        constcollid: InvalidOid,
        constvalue: Datum::from_bool(value),
        constisnull: isnull,
    }
}

/// `makeBoolExpr(boolop, args, location)` (makefuncs.c) — a `BoolExpr` node.
///
/// The trimmed [`BoolExpr`] carries `boolop`/`args`; `location` (set by the C)
/// is not modeled here.
pub fn make_bool_expr(boolop: BoolExprType, args: Vec<Expr>, _location: i32) -> Expr {
    Expr::BoolExpr(BoolExpr { boolop, args })
}

/// `make_andclause(andclauses)` (makefuncs.c) — `BoolExpr` with `AND_EXPR`.
pub fn make_andclause(andclauses: Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: AND_EXPR,
        args: andclauses,
    })
}

/// `make_orclause(orclauses)` (makefuncs.c) — `BoolExpr` with `OR_EXPR`.
pub fn make_orclause(orclauses: Vec<Expr>) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: OR_EXPR,
        args: orclauses,
    })
}

/// `make_notclause(notclause)` (makefuncs.c) — `BoolExpr` with `NOT_EXPR` over
/// the single negated expression (`list_make1(notclause)`).
pub fn make_notclause(notclause: Expr) -> Expr {
    Expr::BoolExpr(BoolExpr {
        boolop: NOT_EXPR,
        args: vec![notclause],
    })
}

/// `make_and_qual(qual1, qual2)` (makefuncs.c) — AND two qual conditions,
/// treating a `None` (C `NULL`) nodetree as TRUE.
pub fn make_and_qual(qual1: Option<Expr>, qual2: Option<Expr>) -> Option<Expr> {
    match (qual1, qual2) {
        // if (qual1 == NULL) return qual2;
        (None, q2) => q2,
        // if (qual2 == NULL) return qual1;
        (q1, None) => q1,
        // return (Node *) make_andclause(list_make2(qual1, qual2));
        (Some(q1), Some(q2)) => Some(make_andclause(vec![q1, q2])),
    }
}

/// `make_ands_explicit(andclauses)` (makefuncs.c) — convert an AND-semantics
/// expression list to an ordinary boolean expression. An empty list is TRUE.
pub fn make_ands_explicit(mut andclauses: Vec<Expr>) -> Expr {
    if andclauses.is_empty() {
        // return (Expr *) makeBoolConst(true, false);
        Expr::Const(make_bool_const(true, false))
    } else if andclauses.len() == 1 {
        // return (Expr *) linitial(andclauses);
        andclauses.remove(0)
    } else {
        // return make_andclause(andclauses);
        make_andclause(andclauses)
    }
}

/// `make_ands_implicit(clause)` (makefuncs.c) — convert an ordinary boolean
/// expression to an AND-semantics list. A `None`/constant-TRUE clause yields
/// the empty list (TRUE).
pub fn make_ands_implicit(clause: Option<Expr>) -> Vec<Expr> {
    match clause {
        // if (clause == NULL) return NIL; /* NULL -> NIL list == TRUE */
        None => Vec::new(),
        Some(Expr::BoolExpr(b)) if b.boolop == AND_EXPR => {
            // if (is_andclause(clause)) return ((BoolExpr *) clause)->args;
            b.args
        }
        // else if (IsA(clause, Const) && !constisnull && DatumGetBool(constvalue))
        //     return NIL; /* constant TRUE input -> NIL list */
        Some(Expr::Const(ref c)) if !c.constisnull && c.constvalue.as_bool() => Vec::new(),
        // else return list_make1(clause);
        Some(other) => vec![other],
    }
}

/// `makeRelabelType(arg, rtype, rtypmod, rcollid, rformat)` (makefuncs.c) — a
/// no-op binary-compatible coercion node.
///
/// The trimmed [`RelabelType`] carries every field the C sets except
/// `location` (set to -1 by the C).
pub fn make_relabel_type(
    arg: Expr,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
) -> Expr {
    Expr::RelabelType(RelabelType {
        arg: Some(Box::new(arg)),
        resulttype: rtype,
        resulttypmod: rtypmod,
        resultcollid: rcollid,
        relabelformat: rformat,
    })
}

/// `makeFuncExpr(funcid, rettype, args, funccollid, inputcollid, fformat)`
/// (makefuncs.c) — a function-call expression. `funcretset`/`funcvariadic` are
/// always `false` here (the only allowed case); `location` (set to -1 by the C)
/// is not modeled in the trimmed [`FuncExpr`].
pub fn make_func_expr(
    funcid: Oid,
    rettype: Oid,
    args: Vec<Expr>,
    funccollid: Oid,
    inputcollid: Oid,
    fformat: CoercionForm,
) -> Expr {
    Expr::FuncExpr(FuncExpr {
        funcid,
        funcresulttype: rettype,
        funcretset: false,
        funcvariadic: false,
        funcformat: fformat,
        funccollid,
        inputcollid,
        args,
    })
}

/// `make_opclause(opno, opresulttype, opretset, leftop, rightop, opcollid,
/// inputcollid)` (makefuncs.c) — an operator-invocation clause. Pass
/// `rightop == None` for a single-operand clause. `opfuncid` is left
/// `InvalidOid` (resolved later); `location` (set to -1 by the C) is not
/// modeled in the trimmed [`OpExpr`].
pub fn make_opclause(
    opno: Oid,
    opresulttype: Oid,
    opretset: bool,
    leftop: Expr,
    rightop: Option<Expr>,
    opcollid: Oid,
    inputcollid: Oid,
) -> Expr {
    let args = match rightop {
        // expr->args = list_make2(leftop, rightop);
        Some(r) => vec![leftop, r],
        // expr->args = list_make1(leftop);
        None => vec![leftop],
    };
    Expr::OpExpr(OpExpr {
        opno,
        opfuncid: InvalidOid,
        opresulttype,
        opretset,
        opcollid,
        inputcollid,
        args,
        location: -1,
    })
}

/// `makeTargetEntry(expr, resno, resname, resjunk)` (makefuncs.c) — a
/// `TargetEntry` node, allocated in `mcx` (the boxed child `expr` and `resname`
/// string live in the same context). The trimmed [`TargetEntry`] carries
/// `expr`/`resno`/`resname`/`resjunk`; `ressortgroupref`, `resorigtbl`,
/// `resorigcol` (set to 0/InvalidOid by the C) are not modeled here.
pub fn make_target_entry<'mcx>(
    mcx: Mcx<'mcx>,
    expr: Expr,
    resno: AttrNumber,
    resname: Option<&str>,
    resjunk: bool,
) -> PgResult<TargetEntry<'mcx>> {
    Ok(TargetEntry {
        expr: Some(alloc_in(mcx, expr)?),
        resno,
        resname: match resname {
            Some(s) => Some(PgString::from_str_in(s, mcx)?),
            None => None,
        },
        resjunk,
    })
}

/// `flatCopyTargetEntry(src_tle)` (makefuncs.c) — duplicate a `TargetEntry`
/// without copying substructure. Here the deep model has no shared-pointer
/// substructure to alias, so the C `memcpy` becomes a fallible deep copy into
/// `mcx` (`TargetEntry::clone_in`).
pub fn flat_copy_target_entry<'mcx>(
    mcx: Mcx<'mcx>,
    src_tle: &TargetEntry<'_>,
) -> PgResult<TargetEntry<'mcx>> {
    src_tle.clone_in(mcx)
}

// ===========================================================================
// JSON expression-node constructors.
// ===========================================================================

/// `makeJsonFormat(type, encoding, location)` (makefuncs.c) — a `JsonFormat`
/// node. `location` (set by the C) is not modeled in the trimmed [`JsonFormat`].
pub fn make_json_format(format_type: JsonFormatType, encoding: JsonEncoding, _location: i32) -> JsonFormat {
    JsonFormat {
        format_type,
        encoding,
    }
}

/// `makeJsonValueExpr(raw_expr, formatted_expr, format)` (makefuncs.c) — a
/// `JsonValueExpr` node.
pub fn make_json_value_expr(
    raw_expr: Option<Expr>,
    formatted_expr: Option<Expr>,
    format: Option<JsonFormat>,
) -> JsonValueExpr {
    JsonValueExpr {
        raw_expr: raw_expr.map(Box::new),
        formatted_expr: formatted_expr.map(Box::new),
        format,
    }
}

/// `makeJsonBehavior(btype, expr, location)` (makefuncs.c) — a `JsonBehavior`
/// node. `location` (set by the C) is not modeled; `coerce` is left at its
/// default (the C leaves it zero too).
pub fn make_json_behavior(btype: JsonBehaviorType, expr: Option<Expr>, _location: i32) -> JsonBehavior {
    JsonBehavior {
        btype,
        expr: expr.map(Box::new),
        coerce: false,
    }
}

/// `makeJsonIsPredicate(expr, format, item_type, unique_keys, location)`
/// (makefuncs.c) — a `JsonIsPredicate` node, returned as a `Node` in the C.
/// `location` (set by the C) is not modeled in the trimmed [`JsonIsPredicate`].
pub fn make_json_is_predicate(
    expr: Option<Expr>,
    format: Option<JsonFormat>,
    item_type: JsonValueType,
    unique_keys: bool,
    _location: i32,
) -> Expr {
    Expr::JsonIsPredicate(JsonIsPredicate {
        expr: expr.map(Box::new),
        format,
        item_type,
        unique_keys,
    })
}

// ===========================================================================
// Plan / exec-state node constructors.
// ===========================================================================

/// `makeIndexInfo(numattrs, numkeyattrs, amoid, expressions, predicates,
/// unique, nulls_not_distinct, isready, concurrent, summarizing,
/// withoutoverlaps)` (makefuncs.c) — an `IndexInfo` node.
///
/// The trimmed [`IndexInfo`] carries the build-state scalars the executor
/// consults; the expression/predicate lists, exclusion/unique op arrays, and
/// the memory-context handle (which the C also initializes) are not modeled
/// here. `ii_Summarizing`/`ii_WithoutOverlaps`/`ii_IndexUnchanged` are likewise
/// not modeled, so their inputs are accepted but unused.
pub fn make_index_info(
    numattrs: i32,
    numkeyattrs: i32,
    amoid: Oid,
    _expressions: (),
    _predicates: (),
    unique: bool,
    nulls_not_distinct: bool,
    isready: bool,
    concurrent: bool,
    _summarizing: bool,
    _withoutoverlaps: bool,
) -> IndexInfo {
    // Asserts mirrored from the C (ii_NumIndexKeyAttrs != 0,
    // ii_NumIndexKeyAttrs <= ii_NumIndexAttrs).
    debug_assert!(numkeyattrs != 0);
    debug_assert!(numkeyattrs <= numattrs);
    IndexInfo {
        ii_NumIndexAttrs: numattrs,
        ii_NumIndexKeyAttrs: numkeyattrs,
        ii_Unique: unique,
        ii_NullsNotDistinct: nulls_not_distinct,
        ii_ReadyForInserts: isready,
        ii_CheckedUnchanged: false,
        ii_IndexUnchanged: false,
        ii_Concurrent: concurrent,
        ii_BrokenHotChain: false,
        ii_ParallelWorkers: 0,
        ii_Am: amoid,
        ii_IndexAttrNumbers: Default::default(),
    }
}

// ===========================================================================
// Raw-parser node constructors (owned plain-Rust parse nodes).
// ===========================================================================

/// `makeRangeVar(schemaname, relname, location)` (makefuncs.c) — a `RangeVar`
/// node (oversimplified case): `catalogname`/`alias` NULL, `inh` true,
/// `relpersistence` permanent.
pub fn make_range_var(schemaname: Option<String>, relname: String, location: i32) -> RangeVar {
    RangeVar {
        catalogname: None,
        schemaname,
        relname,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location,
    }
}

/// `makeTypeName(typnam)` (makefuncs.c) — a `TypeName` for an unqualified name
/// (`makeTypeNameFromNameList(list_make1(makeString(typnam)))`).
pub fn make_type_name(typnam: String) -> TypeName {
    make_type_name_from_name_list(vec![ParseNode::String(StringNode { sval: Some(typnam) })])
}

/// `makeTypeNameFromNameList(names)` (makefuncs.c) — a `TypeName` from a `List`
/// of `String` value nodes. `typmods` defaulted to NIL, `typemod`/`location`
/// to -1.
pub fn make_type_name_from_name_list(names: Vec<ParseNode>) -> TypeName {
    TypeName {
        names,
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// `makeTypeNameFromOid(typeOid, typmod)` (makefuncs.c) — a `TypeName` for a
/// type already known by OID/typmod. `location` defaulted to -1.
pub fn make_type_name_from_oid(type_oid: Oid, typmod: i32) -> TypeName {
    TypeName {
        names: Vec::new(),
        typeOid: type_oid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: typmod,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

// ===========================================================================
// `PG_DETOAST_DATUM` — delegated to the `detoast` owner.
// ===========================================================================

/// `PointerGetDatum(PG_DETOAST_DATUM(d))` (fmgr.h) — return a fully
/// fetched-and-decompressed copy of the varlena datum `d` in `mcx`, or `d`
/// unchanged when it is already a plain (4-byte-header, uncompressed) varlena.
///
/// Mirrors the rangetypes precedent: inspect the varlena header to decide
/// whether a detoast is required, and when it is, delegate the actual
/// fetch/decompress to the `backend-access-common-detoast` owner's
/// `detoast_attr` seam, re-pointing the datum at the new `mcx` buffer.
fn pg_detoast_datum<'mcx>(mcx: Mcx<'mcx>, d: ScalarWord) -> PgResult<ScalarWord> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller guarantees `d` is a (non-null) varlena pointer datum
    // (`constlen == -1 && !constisnull`).
    unsafe {
        if varatt_is_external(p) {
            let len = varsize_external(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast_seam::detoast_attr::call(mcx, bytes)?;
            Ok(ScalarWord::from_usize(copy.leak().as_ptr() as usize))
        } else if !varatt_is_4b_u(p) && !varatt_is_1b(p) {
            // 4-byte compressed: the only remaining "extended" form. PG_DETOAST
            // _DATUM (unlike the _PACKED variant) also decompresses, which
            // detoast_attr does.
            let len = varsize_4b(p);
            let bytes = core::slice::from_raw_parts(p, len);
            let copy = detoast_seam::detoast_attr::call(mcx, bytes)?;
            Ok(ScalarWord::from_usize(copy.leak().as_ptr() as usize))
        } else {
            // Plain 4B or short 1B header: returned unchanged.
            Ok(d)
        }
    }
}

// varatt.h header predicates (little-endian; `VARATT_IS_*`). These are the
// standard varlena-header bit tests — Datum/varlena vocabulary, not detoast
// logic (the fetch/decompress is the detoast owner's). Mirrors the rangetypes
// port's inline helpers.

#[inline]
unsafe fn varatt_is_4b_u(ptr: *const u8) -> bool {
    // VARATT_IS_4B_U(PTR): ((PTR)->va_header & 0x03) == 0x00
    (*ptr & 0x03) == 0x00
}

#[inline]
unsafe fn varatt_is_1b(ptr: *const u8) -> bool {
    // VARATT_IS_1B(PTR): ((PTR)->va_header & 0x01) == 0x01
    (*ptr & 0x01) == 0x01
}

#[inline]
unsafe fn varatt_is_1b_e(ptr: *const u8) -> bool {
    // VARATT_IS_1B_E(PTR): ((PTR)->va_header) == 0x01
    *ptr == 0x01
}

#[inline]
unsafe fn varatt_is_external(ptr: *const u8) -> bool {
    // VARATT_IS_EXTERNAL(PTR): VARATT_IS_1B_E(PTR)
    varatt_is_1b_e(ptr)
}

#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    // VARSIZE_4B(PTR): (((varattrib_4b *)(PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF
    let header = (ptr as *const u32).read_unaligned();
    ((header >> 2) & 0x3FFF_FFFF) as usize
}

#[inline]
unsafe fn varsize_external(ptr: *const u8) -> usize {
    // VARHDRSZ_EXTERNAL (== 2) + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)).
    const VARHDRSZ_EXTERNAL: usize = 2;
    let tag = *ptr.add(1);
    let payload = match tag {
        1 => 16usize,      // VARTAG_INDIRECT
        2 | 3 => 16usize,  // VARTAG_EXPANDED_RO / _RW
        18 => 18usize,     // VARTAG_ONDISK
        other => other as usize,
    };
    VARHDRSZ_EXTERNAL + payload
}

// ===========================================================================
// Seam implementations (owned canonical seams).
// ===========================================================================

/// `make_const_node` seam — `makeConst(...)` returned through a `Node *`-typed
/// API (C: `get_typdefault` builds the literal default and returns it as
/// `Node *`). Builds the `Const` (with detoast where applicable) and wraps it
/// as [`Node::Expr`], allocated in `mcx`.
pub fn make_const_node_seam<'mcx>(
    mcx: Mcx<'mcx>,
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: i32,
    constvalue: Datum<'mcx>,
    constisnull: bool,
    constbyval: bool,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // The seam carries the canonical unified value, threaded straight into
    // `make_const` (which mirrors C's `Const.constvalue` Datum word for the
    // by-value arm and the by-reference image for `ByRef`).
    let c = make_const(
        mcx,
        consttype,
        consttypmod,
        constcollid,
        constlen,
        constvalue,
        constisnull,
        constbyval,
    )?;
    alloc_in(mcx, Node::Expr(Expr::Const(c)))
}

/// `make_and_boolexpr` seam — `makeBoolExpr(AND_EXPR, args, location)` returned
/// through a `Node *`-typed API (C: partition-qual assembly). Builds the
/// `BoolExpr` over the `mcx`-charged `args` list and wraps it as
/// [`Node::Expr`], allocated in `mcx`.
pub fn make_and_boolexpr_seam<'mcx>(
    mcx: Mcx<'mcx>,
    args: PgVec<'mcx, Node<'mcx>>,
    location: i32,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // The arg list crosses as `Node`s (the partition-qual elements). Each is a
    // `Node::Expr` (the qual clauses generate_partition_qual produced); unwrap
    // to the underlying `Expr` to populate the BoolExpr's `Vec<Expr>` args.
    let mut exprs: Vec<Expr> = Vec::with_capacity(args.len());
    for n in args.into_iter() {
        match n {
            Node::Expr(e) => exprs.push(e),
            // A non-expression `Node` in a boolean-AND arg list is a
            // model-impossible state (the C args are all `Expr *`).
            other => panic!(
                "make_and_boolexpr: AND argument is a non-expression node (tag {})",
                other.tag()
            ),
        }
    }
    let e = make_bool_expr(AND_EXPR, exprs, location);
    alloc_in(mcx, Node::Expr(e))
}

/// `make_type_name_from_name_list` seam — build a raw-parser `TypeName` from a
/// `List` of `String` value nodes. Owned plain-Rust (not `mcx`-allocated).
pub fn make_type_name_from_name_list_seam(names: Vec<ParseNode>) -> PgResult<TypeName> {
    Ok(make_type_name_from_name_list(names))
}
