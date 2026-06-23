//! Seam declarations for the `backend-parser-small1` unit
//! (`parser/parse_node.c` + `parser/parse_merge.c`). The owning unit installs
//! the inward seams from its `init_seams()`; outward seams (whose real owner is
//! a not-yet-ported neighbor) panic loudly until that owner lands.

#![allow(non_snake_case)]

extern crate alloc;

use ::mcx::{Mcx, PgVec};
use ::types_cluster::ParseState;
use ::types_error::PgResult;
use ::nodes::primnodes::SubscriptingRef;
use ::nodes::rawnodes::A_Indices;

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): cursor position
    /// (1-based char index) for the error from a token location, or 0.
    pub fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> PgResult<i32>
);

seam_core::seam!(
    /// `pstate->p_coerce_param_hook(pstate, param, targetTypeId, targetTypeMod,
    /// location)` (parse_coerce.c `coerce_type`'s `IsA(node, Param)` arm). The C
    /// hook is a function pointer on `ParseState` installed by parse_param.c
    /// (`variable_coerce_param_hook`); the owned model reaches it from
    /// `coerce_type` (in the lower `backend-parser-coerce`) through this seam,
    /// dispatching on `pstate.p_ref_hook_state`. Returns the (possibly mutated)
    /// `Param` to use, or `None` to fall through to the normal coercion path —
    /// mirroring C's hook returning a transformed `Node *` or `NULL`. Only the
    /// variable-parameter case installs a coercion hook; the fixed-parameter and
    /// no-hook cases are an installed no-op returning `None`.
    pub fn coerce_param_hook(
        pstate: &ParseState<'_>,
        param: &::nodes::primnodes::Param,
        target_type_id: types_core::primitive::Oid,
        target_type_mod: i32,
        location: i32,
    ) -> PgResult<Option<::nodes::primnodes::Param>>
);

seam_core::seam!(
    /// `sbsroutines->transform(sbsref, indirection, pstate, isSlice, isAssignment)`
    /// (parse_node.c `transformContainerSubscripts`): the container-type-specific
    /// subscripting support method (fetched by `getSubscriptingRoutines`) that
    /// transforms the raw subscript list and fills in the result type on the
    /// `SubscriptingRef`. Mutates the node in place in C; here it takes the
    /// partially-built `SubscriptingRef` by value and returns the filled one.
    ///
    /// The transform method's C body (`array_subscript_transform` in arraysubs.c,
    /// `jsonb_subscript_transform` in jsonbsubs.c) transforms the raw subscript
    /// expressions via `transformExpr` + `coerce_to_target_type`/`coerce_type`,
    /// which are parser-layer entry points above the `utils/adt` handler files;
    /// the install therefore lives in `backend-parser-parse-expr` (which owns
    /// `transformExpr` and reaches the coerce seams) and dispatches on the
    /// container type's `SubscriptHandler`. `pstate` is `&mut` because
    /// `transformExpr` mutates it (`p_expr_kind`, `p_last_srf`), matching C's
    /// `ParseState *`.
    pub fn subscripting_transform<'mcx>(
        mcx: Mcx<'mcx>,
        sbsref: SubscriptingRef<'mcx>,
        indirection: &[A_Indices<'mcx>],
        pstate: &mut ParseState<'mcx>,
        is_slice: bool,
        is_assignment: bool,
    ) -> PgResult<SubscriptingRef<'mcx>>
);

seam_core::seam!(
    /// `DirectFunctionCall3(numeric_in, CStringGetDatum(str), InvalidOid, -1)`
    /// (parse_node.c `make_const`, `T_Float` oversize/non-integer arm): parse a
    /// decimal/scientific literal into a `numeric` value, returning the full
    /// on-disk `Numeric` varlena byte image (with the varlena length header).
    /// The owner is `backend-utils-adt-numeric` (`numeric_in`); `make_const`
    /// wraps the result into a by-reference `Datum::ByRef` for the `Const` node.
    pub fn numeric_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `DirectFunctionCall3(bit_in, CStringGetDatum(str), InvalidOid, -1)`
    /// (parse_node.c `make_const`, `T_BitString` arm): parse a bit-string literal
    /// (`B'101'` / `X'1F'`) into a `bit` value, returning the full on-disk
    /// `VarBit` varlena byte image (`[varsize_le | bit_len_le | data]`). The owner
    /// is `backend-utils-adt-varbit` (`bit_in`); `make_const` wraps the result
    /// into a by-reference `Datum::ByRef` for the `Const` node.
    pub fn bit_in<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>>
);
