//! Seam declarations for the `backend-parser-coerce` unit
//! (`parser/parse_coerce.c`), the type-coercion catalog lookups.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsestmt::ParseState;
use types_nodes::primnodes::{CoercionForm, Expr};
use types_parsenodes::CoercionContext;

/// `CoercionPathType` (parser/parse_coerce.h): the kind of coercion pathway
/// `find_coercion_pathway` resolved between two types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum CoercionPathType {
    /// `COERCION_PATH_NONE` (0) — failed to find any coercion pathway.
    None = 0,
    /// `COERCION_PATH_FUNC` (1) — apply the specified coercion function.
    Func = 1,
    /// `COERCION_PATH_RELABELTYPE` (2) — binary-compatible cast, no function.
    Relabeltype = 2,
    /// `COERCION_PATH_ARRAYCOERCE` (3) — need an `ArrayCoerceExpr` node.
    Arraycoerce = 3,
    /// `COERCION_PATH_COERCEVIAIO` (4) — need a `CoerceViaIO` node.
    Coerceviaio = 4,
}

seam_core::seam!(
    /// `find_coercion_pathway(targetTypeId, sourceTypeId, COERCION_IMPLICIT,
    /// &funcid)` (parse_coerce.c): determine how to coerce `source_type_id`
    /// to `target_type_id` under implicit context. Returns the pathway kind
    /// and (for `Func`) the coercion function OID, else `InvalidOid`. `Err`
    /// carries catcache-path `ereport(ERROR)`s.
    pub fn find_coercion_pathway_implicit(
        target_type_id: Oid,
        source_type_id: Oid,
    ) -> PgResult<(CoercionPathType, Oid)>
);

seam_core::seam!(
    /// `IsBinaryCoercible(srctype, targettype)` (parse_coerce.c): whether
    /// `srctype` is binary-coercible to `targettype` (identical types, an
    /// existing binary-coercible pg_cast entry, or `targettype` being a
    /// polymorphic/ANY pseudo-type that accepts `srctype`). `Err` carries
    /// catcache-path `ereport(ERROR)`s.
    pub fn is_binary_coercible(srctype: Oid, targettype: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `enforce_generic_type_consistency(actual_arg_types, declared_arg_types,
    /// nargs, rettype, allow_poly)` (parse_coerce.c): resolve any polymorphic
    /// (`ANY*`) entries in `declared_arg_types` against the concrete
    /// `actual_arg_types`, mutating `declared_arg_types` in place (the C output
    /// array used by `make_fn_arguments` as the cast destination) and returning
    /// the actual result type the polymorphic `rettype` resolves to.
    /// `allow_poly` is the C trailing `bool` (parse_oper.c always passes
    /// `false`). `Err` carries the inconsistent-polymorphic-types
    /// `ereport(ERROR)` surface.
    pub fn enforce_generic_type_consistency(
        actual_arg_types: &[types_core::Oid],
        declared_arg_types: &mut [types_core::Oid],
        nargs: i32,
        rettype: types_core::Oid,
        allow_poly: bool,
    ) -> PgResult<types_core::Oid>
);

// ---------------------------------------------------------------------------
// High-level coercion entry points consumed by parse_expr.c. These are the
// `parse_coerce.c` public surface (`coerce_to_boolean`, `coerce_to_specific_type`,
// `coerce_to_common_type`, `select_common_type`, `coerce_to_target_type`). The
// owning `backend-parser-coerce` unit is not yet ported; until it lands every
// call panics loudly.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `coerce_to_boolean(pstate, node, constructName)` (parse_coerce.c): coerce
    /// an expression to `bool`, raising the construct-named datatype-mismatch
    /// error if it cannot be. Returns the (possibly coerced) expression.
    pub fn coerce_to_boolean<'mcx>(
        pstate: &mut ParseState<'mcx>,
        node: Expr,
        construct_name: &str,
    ) -> PgResult<Expr>
);

seam_core::seam!(
    /// `coerce_to_specific_type(pstate, node, targetTypeId, constructName)`
    /// (parse_coerce.c): coerce to a specific built-in type for a named
    /// construct.
    pub fn coerce_to_specific_type<'mcx>(
        pstate: &mut ParseState<'mcx>,
        node: Expr,
        target_type_id: Oid,
        construct_name: &str,
    ) -> PgResult<Expr>
);

seam_core::seam!(
    /// `coerce_to_common_type(pstate, node, targetTypeId, context)`
    /// (parse_coerce.c): coerce a node to a previously-selected common type
    /// (CASE/COALESCE/GREATEST/LEAST/ARRAY/IN). `context` is the construct name
    /// for error text.
    pub fn coerce_to_common_type<'mcx>(
        pstate: &mut ParseState<'mcx>,
        node: Expr,
        target_type_id: Oid,
        context: &str,
    ) -> PgResult<Expr>
);

seam_core::seam!(
    /// `select_common_type(pstate, exprs, context, which_expr)` (parse_coerce.c):
    /// determine the common supertype of a list of already-transformed
    /// expressions. The C `Node **which_expr` out-parameter is dropped (the
    /// parse_expr.c call sites pass `NULL`). `context` is `None` when the C
    /// passes `NULL` (a no-common-type failure yields `InvalidOid` instead of an
    /// error — the `transformAExprIn` ScalarArrayOp probe).
    pub fn select_common_type<'mcx>(
        pstate: &mut ParseState<'mcx>,
        exprs: &[Expr],
        context: Option<&str>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `verify_common_type(common_type, exprs)` (parse_coerce.c): verify the
    /// selected common type actually works for every expression (the
    /// `transformAExprIn` ScalarArrayOp probe). Returns `false` when the type is
    /// unusable (fall through to the boolean tree), without raising.
    pub fn verify_common_type(common_type: Oid, exprs: &[Expr]) -> PgResult<bool>
);

seam_core::seam!(
    /// `coerce_to_target_type(pstate, expr, exprtype, targettype, targettypmod,
    /// ccontext, cformat, location)` (parse_coerce.c): the general explicit /
    /// assignment / implicit coercion driver. Returns `None` when no coercion is
    /// possible (the C NULL return — the caller raises the cannot-cast error).
    pub fn coerce_to_target_type<'mcx>(
        pstate: &mut ParseState<'mcx>,
        expr: Expr,
        exprtype: Oid,
        targettype: Oid,
        targettypmod: i32,
        ccontext: CoercionContext,
        cformat: CoercionForm,
        location: i32,
    ) -> PgResult<Option<Expr>>
);
