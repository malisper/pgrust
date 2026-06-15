//! Seam declarations for the `backend-parser-parse-func` unit
//! (`parser/parse_func.c`): function-name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;
use types_namespace::FuncCandidateList;
use types_nodes::parsenodes::ObjectType;
use types_nodes::primnodes::Expr;
use types_opclass::ObjectWithArgs;
use types_parsenodes::ObjectWithArgs as ParseObjectWithArgs;
use types_parsenodes::ParseState;

seam_core::seam!(
    /// `LookupFuncName(funcname, nargs, argtypes, missing_ok)`
    /// (parse_func.c): resolve a possibly-qualified function name (a `List *`
    /// of `String` nodes, here the name components) with the given argument
    /// types to a `pg_proc` OID. With `missing_ok = false` a missing function
    /// raises (`Err`); with `missing_ok = true` it returns `InvalidOid`.
    pub fn lookup_func_name(
        funcname: &[PgString<'_>],
        nargs: i32,
        argtypes: &[Oid],
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupFuncWithArgs(OBJECT_FUNCTION, func, missing_ok)`
    /// (parse_func.c): resolve an `ObjectWithArgs` describing a plain function
    /// (the only object type opclasscmds.c uses) to its pg_proc OID. With
    /// `missing_ok = false` a missing function raises (`Err`); with
    /// `missing_ok = true` it returns `InvalidOid`.
    pub fn lookup_func_with_args(func: &ObjectWithArgs, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupFuncWithArgs(objtype, func, missing_ok)` (parse_func.c): the
    /// object-type-aware form `get_object_address` uses for the
    /// `OBJECT_AGGREGATE`/`OBJECT_FUNCTION`/`OBJECT_PROCEDURE`/`OBJECT_ROUTINE`
    /// arms — `objtype` selects which `pg_proc.prokind`s are acceptable and
    /// whether aggregate/window/ordered-set handling applies. `func` crosses as
    /// the parser's own [`ParseObjectWithArgs`] (the `castNode(ObjectWithArgs,
    /// object)` the C switch passes). With `missing_ok = false` a missing
    /// routine raises (`Err`); else `InvalidOid`.
    pub fn lookup_func_with_args_for_objtype(
        objtype: ObjectType,
        func: &ParseObjectWithArgs,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `func_match_argtypes(nargs, input_typeids, raw_candidates, &candidates)`
    /// (parse_func.c): from the `raw_candidates` list, keep only those whose
    /// declared argument types the `input_typeids` can be coerced to (directly
    /// or via implicit cast), returning the filtered candidate list (C returns
    /// the surviving count + the new list via the out-param; the owned port
    /// returns the list, whose `len()` is that count). Allocated in `mcx`.
    pub fn func_match_argtypes<'mcx>(
        mcx: Mcx<'mcx>,
        nargs: i32,
        input_typeids: &[Oid],
        raw_candidates: &FuncCandidateList<'mcx>,
    ) -> PgResult<FuncCandidateList<'mcx>>
);

seam_core::seam!(
    /// `func_select_candidate(nargs, input_typeids, candidates)`
    /// (parse_func.c): apply the ambiguous-function resolution heuristics to
    /// pick a single best candidate from `candidates`. `Ok(Some(oid))` is the
    /// chosen candidate's OID (the C returns the single-element list whose
    /// `->oid` the caller reads); `Ok(None)` is the C NULL return (no unique
    /// best candidate). Allocated in `mcx`. `Err` carries the catalog-lookup
    /// `ereport(ERROR)` surface.
    pub fn func_select_candidate<'mcx>(
        mcx: Mcx<'mcx>,
        nargs: i32,
        input_typeids: &[Oid],
        candidates: &FuncCandidateList<'mcx>,
    ) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types)`
    /// (parse_func.c): coerce each argument expression in `fargs` from its
    /// `actual_arg_types[i]` to the corresponding `declared_arg_types[i]`,
    /// applying the necessary cast/relabel in place (the C scribbles the coerced
    /// nodes back into the `fargs` list cells). `pstate` is the parse state
    /// (`None` when the C `pstate == NULL`); the C `coerce_type` calls reach it
    /// for error positioning via `p_sourcetext`. `Err` carries the
    /// cannot-coerce `ereport(ERROR)` surface.
    pub fn make_fn_arguments<'mcx>(
        pstate: Option<&mut ParseState<'mcx>>,
        fargs: &mut [Expr],
        actual_arg_types: &[Oid],
        declared_arg_types: &[Oid],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `check_srf_call_placement(pstate, last_srf, location)` (parse_func.c):
    /// verify that a set-returning function call appears in a context where it
    /// is allowed (and detect nested-SRF). `last_srf` is the saved
    /// `pstate->p_last_srf` from before the operator's arguments were
    /// transformed. The C reads `pstate->p_expr_kind` (to decide which contexts
    /// allow an SRF) and writes `pstate->p_hasTargetSRFs`, so `pstate` crosses
    /// as `&mut ParseState`. `Err` carries the disallowed-placement
    /// `ereport(ERROR)` surface.
    pub fn check_srf_call_placement<'mcx>(
        pstate: &mut ParseState<'mcx>,
        last_srf: Option<&Expr>,
        location: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `pstate->p_last_srf = (Node *) result;` (parse_oper.c make_op): record
    /// the just-built set-returning `OpExpr` as the parse state's last SRF, for
    /// nested-SRF error checks at higher levels. The C only performs this when
    /// `pstate != NULL`, so `pstate` crosses as `&mut ParseState`; the owner
    /// writes the real `ParseState->p_last_srf`.
    pub fn set_last_srf<'mcx>(pstate: &mut ParseState<'mcx>, result: &Expr) -> PgResult<()>
);
