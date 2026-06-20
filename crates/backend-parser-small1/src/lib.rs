//! Port of the `backend-parser-small1` unit (PostgreSQL 18.3): the small parser
//! support files bundled together —
//!
//! * `parser/parse_enr.c` — ephemeral-named-relation lookup helpers.
//! * `parser/scansup.c` — scanner support (identifier downcasing/truncation,
//!   the flex `{space}` predicate).
//! * `parser/parse_node.c` — `ParseState` lifecycle, `parser_errposition`, the
//!   error-position callback shim, container-subscript transforms, `make_const`.
//! * `parser/parse_param.c` — the fixed-parameter parser hook plus the
//!   post-analysis parameter checks and the extern-param probe (the F2 leg).
//! * `parser/parse_merge.c` — `transformMergeStmt` (sibling parser deps
//!   unported; mirror-PG-and-panic).
//!
//! # Seams installed (inward)
//!
//! * `backend_parser_small1_seams::parser_errposition` — the cursor-position
//!   helper (consumed by define / cluster / explain-state).
//! * `backend_parser_scansup_seams::truncate_identifier` — reconciled to live
//!   here (the parser-driver's lexer is the consumer).
//! * `backend_parser_analyze_seams::make_parsestate` — `ParseState` allocator.
//!
//! # Variable-parameter (F3): the shared-mutable type-array carrier
//!
//! `setup_parse_variable_parameters` / `variable_paramref_hook` /
//! `variable_coerce_param_hook` / `check_parameter_resolution_walker` MUTATE the
//! caller's `Oid *` parameter-type array in place. C models this with a
//! `VarParamState { Oid **paramTypes; int *numParams; }` aliasing the caller's
//! mutable array + count; the hooks `repalloc` and write through it so the caller
//! (`PrepareQuery`) reads the resolved types back after analysis.
//!
//! The owned model keeps that caller-aliasing semantics safely with a
//! [`types_nodes::parsestmt::VarParamState`] carrier — a single
//! `Rc<RefCell<Vec<Oid>>>` the caller constructs, hands to
//! `setup_parse_variable_parameters` (stored in `pstate.p_ref_hook_state`), and
//! reads back afterward; the `Vec`'s length is C's `*numParams`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec, MAX_ALLOC_SIZE};

use types_core::catalog::VOIDOID;
use types_core::fmgr::FLOAT8PASSBYVAL;
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::error::{
    ERRCODE_AMBIGUOUS_PARAMETER, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INTERNAL_ERROR,
    ERRCODE_NAME_TOO_LONG, ERRCODE_OUT_OF_MEMORY, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_PARAMETER,
};
use types_error::{ErrorLocation, PgError, PgResult, SoftErrorContext, ERROR, NOTICE};
use types_tuple::Datum;

use types_nodes::nodes::{ntag, Node};
use types_nodes::params::ParamRef;
use types_nodes::parsestmt::{ParseExprKind, ParseRefHookState, ParseState, VarParamState};
use types_nodes::primnodes::{Const, Expr, Param, SubscriptingRef, PARAM_EXTERN};
use types_nodes::rawnodes::{A_Const, A_Indices};
use types_nodes::queryenvironment::{EphemeralNamedRelationMetadataData, QueryEnvironment};
use types_nodes::copy_query::Query;

use types_tuple::heaptuple::{
    BITOID, BOOLOID, INT2ARRAYOID, INT2VECTOROID, INT4OID, INT8OID, MaxTupleAttributeNumber,
    NUMERICOID, OIDARRAYOID, OIDVECTOROID, UNKNOWNOID,
};

use types_storage::lock::NoLock;

use backend_utils_error::ereport;
use backend_nodes_core::makefuncs::make_const as makeConst;
use backend_nodes_core::nodefuncs::expr_location;

use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_mb_mbutils_seams as mb;
use backend_utils_misc_queryenvironment as queryenv;

/// `ErrorLocation` for an `ereport` in this unit (parser/parse_node.c et al.).
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/parser/scansup.c", lineno, funcname)
}

/// `palloc`/`repalloc` never return NULL (they `ereport(ERROR)` on exhaustion);
/// the `try_reserve` model surfaces that as a recoverable `Err`.
fn out_of_memory() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}

// ===========================================================================
// parse_enr.c — ephemeral named relations
// ===========================================================================

/// `name_matches_visible_ENR(pstate, refname)` (parse_enr.c) — true if an ENR
/// named `refname` is visible in the parse state's query environment.
pub fn name_matches_visible_ENR(pstate: &ParseState<'_>, refname: &str) -> bool {
    get_visible_ENR_metadata_of(pstate, refname).is_some()
}

/// `get_visible_ENR(pstate, refname)` (parse_enr.c) — the ENR metadata visible
/// under `refname`, or `None`.
///
/// The C returns a `EphemeralNamedRelationMetadata` pointer aliasing the env's
/// metadata; here we hand back the borrowed `&EphemeralNamedRelationMetadataData`
/// tied to the parse state.
pub fn get_visible_ENR<'e, 'mcx>(
    pstate: &'e ParseState<'mcx>,
    refname: &str,
) -> Option<&'e EphemeralNamedRelationMetadataData<'mcx>> {
    get_visible_ENR_metadata_of(pstate, refname)
}

/// `get_visible_ENR_metadata(pstate->p_queryEnv, refname)` — shared by both
/// public wrappers (C calls the queryenvironment helper directly with the env).
fn get_visible_ENR_metadata_of<'e, 'mcx>(
    pstate: &'e ParseState<'mcx>,
    refname: &str,
) -> Option<&'e EphemeralNamedRelationMetadataData<'mcx>> {
    queryenv::get_visible_ENR_metadata(pstate.p_queryEnv.as_deref(), refname)
}

// ===========================================================================
// scansup.c — scanner support
// ===========================================================================

/// `NAMEDATALEN` (pg_config_manual.h) — identifier byte limit.
const NAMEDATALEN: usize = 64;

/// `downcase_truncate_identifier(ident, len, warn)` (scansup.c) — downcase and
/// truncate an unquoted identifier (with truncation enabled). Returns the
/// palloc'd `char *` bytes (no trailing NUL — the carrier supplies it).
pub fn downcase_truncate_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    downcase_identifier(mcx, ident, warn, true)
}

/// `downcase_identifier(ident, len, warn, truncate)` (scansup.c) — the
/// downcasing workhorse. ASCII `A-Z` is folded unconditionally; high-bit bytes
/// are `tolower`'d only in a single-byte server encoding (matching the C
/// `enc_is_single_byte && IS_HIGHBIT_SET && isupper` guard). When `truncate`
/// and the result is at least `NAMEDATALEN` bytes, it is truncated.
pub fn downcase_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
    truncate: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let len = ident.len();
    // result = palloc(len + 1)
    let mut result: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    result.try_reserve(len + 1).map_err(|_| mcx.oom(len + 1))?;

    // enc_is_single_byte = pg_database_encoding_max_length() == 1;
    let enc_is_single_byte = mb::pg_database_encoding_max_length::call() == 1;

    for &b in ident.iter() {
        let mut ch = b;
        if ch.is_ascii_uppercase() {
            ch += b'a' - b'A';
        } else if enc_is_single_byte && is_highbit_set(ch) && locale_isupper(ch) {
            // tolower() for high-bit bytes (locale-aware), matching the repo's
            // libc convention (port-pgstrcasecmp::fold_to_lower).
            ch = locale_tolower(ch);
        }
        result.push(ch);
    }

    // if (i >= NAMEDATALEN && truncate) truncate_identifier(result, i, warn);
    if len >= NAMEDATALEN && truncate {
        let clipped = truncate_to_clip(&result, warn)?;
        result.truncate(clipped);
    }

    Ok(result)
}

/// `IS_HIGHBIT_SET(ch)` (c.h).
#[inline]
fn is_highbit_set(ch: u8) -> bool {
    ch & 0x80 != 0
}

/// `isupper((unsigned char) ch)` — the locale-aware C predicate, via `libc`
/// (the same direct-libc convention as `port-pgstrcasecmp::fold_to_lower`; no
/// seam, exactly as the C makes a direct `isupper` call).
#[inline]
fn locale_isupper(ch: u8) -> bool {
    unsafe { libc::isupper(i32::from(ch)) != 0 }
}

/// `tolower((unsigned char) ch)` — the locale-aware C fold, via `libc`.
#[inline]
fn locale_tolower(ch: u8) -> u8 {
    unsafe { libc::tolower(i32::from(ch)) as u8 }
}

/// `truncate_identifier(ident, len, warn)` (scansup.c) — truncate an identifier
/// to `NAMEDATALEN - 1` bytes on a multibyte-character boundary, emitting a
/// `NOTICE` if `warn` is set. Returns the (possibly truncated) bytes in `mcx`.
///
/// The seam contract (consumed by the parser-driver's lexer) and this in-crate
/// entry share the same logic.
pub fn truncate_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    ident: &[u8],
    warn: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    out.try_reserve(ident.len()).map_err(|_| mcx.oom(ident.len()))?;
    for &b in ident.iter() {
        out.push(b);
    }
    if ident.len() >= NAMEDATALEN {
        let clipped = truncate_to_clip(ident, warn)?;
        out.truncate(clipped);
    }
    Ok(out)
}

/// Shared clip-length computation (`pg_mbcliplen` + optional NOTICE) for a string
/// already known to be at least `NAMEDATALEN` bytes; returns the truncated byte
/// length.
fn truncate_to_clip(ident: &[u8], warn: bool) -> PgResult<usize> {
    let len = ident.len();
    // len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
    let clipped =
        mb::pg_mbcliplen::call(ident, len as i32, (NAMEDATALEN - 1) as i32) as usize;
    if warn {
        // ereport(NOTICE, ERRCODE_NAME_TOO_LONG,
        //   "identifier \"%s\" will be truncated to \"%.*s\"")
        let full = alloc::string::String::from_utf8_lossy(ident);
        let short = alloc::string::String::from_utf8_lossy(&ident[..clipped]);
        ereport(NOTICE)
            .errcode(ERRCODE_NAME_TOO_LONG)
            .errmsg(alloc::format!(
                "identifier \"{}\" will be truncated to \"{}\"",
                full, short
            ))
            .finish(errloc(99, "truncate_identifier"))?;
    }
    Ok(clipped)
}

/// `scanner_isspace(ch)` (scansup.c) — true if the flex scanner treats `ch` as
/// whitespace. Must match scan.l's `{space}` list.
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

// ===========================================================================
// parse_node.c — ParseState lifecycle, errposition, subscripts, make_const
// ===========================================================================

/// `make_parsestate(parentParseState)` (parse_node.c) — allocate and initialize
/// a new `ParseState`. The `palloc0` image with the two nonzero starts; when a
/// parent is given, the source text, parser hooks, ref-hook state and query
/// environment are inherited from it.
pub fn make_parsestate<'mcx>(
    mcx: Mcx<'mcx>,
    parent: Option<&ParseState<'mcx>>,
) -> PgResult<PgBox<'mcx, ParseState<'mcx>>> {
    let mut pstate = ParseState::new(mcx)?;

    if let Some(parent) = parent {
        // pstate->p_sourcetext = parentParseState->p_sourcetext;
        pstate.p_sourcetext = match parent.p_sourcetext.as_ref() {
            Some(s) => Some(s.clone_in(mcx)?),
            None => None,
        };
        // all hooks are copied from parent (fn pointers — Copy).
        pstate.p_pre_columnref_hook = parent.p_pre_columnref_hook;
        pstate.p_post_columnref_hook = parent.p_post_columnref_hook;
        pstate.p_paramref_hook = parent.p_paramref_hook;
        pstate.p_coerce_param_hook = parent.p_coerce_param_hook;

        // pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;
        //
        // C aliases the bare `void *` ref-hook state into the child so the
        // inherited column-ref / paramref hooks resolve against the same state
        // when they fire on embedded sub-statement analysis (e.g. PL/pgSQL's
        // `plpgsql_pre_column_ref` resolving a bareword against the function's
        // variable namespace while parse-analyzing an SPI-prepared statement
        // from `exec_stmt_execsql`). Every `ParseRefHookState` arm is built on
        // `Rc`-shared carriers (`VarParamState`'s `Rc<RefCell<Vec<Oid>>>`,
        // `PlpgsqlExprParseState`'s `Rc<RefCell<Vec<i32>>>` paramnos, the
        // `Rc<…>` type/name snapshots), so `clone()` reproduces C's pointer
        // aliasing exactly: the child shares the parent's mutable back-write
        // cells (the recorded paramnos / resolved types the installer reads back
        // after analysis) rather than forking a private copy.
        pstate.p_ref_hook_state = parent.p_ref_hook_state.clone();

        // pstate->p_queryEnv = parentParseState->p_queryEnv;
        //
        // The query environment "stays in context for the whole parse analysis"
        // (parse_node.c). The owned model holds it by value, so deep-copy it
        // into the child's arena. ENRs are read-only during analysis (looked up
        // by name in `parse_enr`), so a per-child copy is observationally
        // identical to C's shared pointer.
        pstate.p_queryEnv = match parent.p_queryEnv.as_deref() {
            Some(env) => Some(PgBox::try_new_in(env.clone_for_child(mcx)?, mcx)
                .map_err(|_| mcx.oom(core::mem::size_of::<QueryEnvironment>()))?),
            None => None,
        };

        // C aliases `parentParseState` as a live back-pointer; the owned model
        // holds it by value, so deep-copy the parent's read-only spine (range
        // table, namespaces, CTE namespace, containing CTE, source text). This
        // is what lets the parent-chain walks — notably the recursive-CTE
        // self-reference in `scanNameSpaceForCTE` — find the outer CTE namespace
        // from a child sub-statement state.
        pstate.parentParseState = Some(PgBox::try_new_in(parent.clone_read_spine(mcx)?, mcx)
            .map_err(|_| mcx.oom(core::mem::size_of::<ParseState>()))?);
    }

    PgBox::try_new_in(pstate, mcx).map_err(|_| mcx.oom(core::mem::size_of::<ParseState>()))
}

/// `free_parsestate(pstate)` (parse_node.c) — release a `ParseState` and its
/// subsidiary resources.
///
/// In the owned model the `ParseState` value is dropped (C's `pfree`); the only
/// subsidiary resource is the target relation, which is closed `NoLock`.
pub fn free_parsestate(pstate: PgBox<'_, ParseState<'_>>) -> PgResult<()> {
    let mut pstate = PgBox::into_inner(pstate);

    // Check that we did not produce too many resnos.
    if pstate.p_next_resno - 1 > MaxTupleAttributeNumber {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(alloc::format!(
                "target lists can have at most {} entries",
                MaxTupleAttributeNumber
            ))
            .into_error());
    }

    if let Some(rel) = pstate.p_target_relation.take() {
        backend_access_table_table::table_close(rel, NoLock)?;
    }

    // pfree(pstate) — the value drops here.
    Ok(())
}

/// `parser_errposition(pstate, location)` (parse_node.c) — a parse-analysis-time
/// cursor position. Converts a byte offset into the source string into a 1-based
/// character index for reporting; 0 if no location or no source text.
pub fn parser_errposition(pstate: &ParseState<'_>, location: i32) -> i32 {
    // No-op if location was not provided.
    if location < 0 {
        return 0;
    }
    // Can't do anything if source text is not available.
    let sourcetext = match pstate.p_sourcetext.as_ref() {
        Some(s) => s,
        None => return 0,
    };
    // pos = pg_mbstrlen_with_len(p_sourcetext, location) + 1;
    //
    // `p_sourcetext` is the query string, always valid in the server encoding
    // (it passed `pg_client_to_server` on receipt), so the
    // `report_invalid_encoding` path is dead here. If it somehow fires we report
    // "no position" (0) rather than escalate an error while building an error.
    match mb::pg_mbstrlen_with_len::call(sourcetext.as_bytes(), location) {
        Ok(n) => n + 1,
        Err(_) => 0,
    }
}

/// `setup_parser_errposition_callback` / `cancel_parser_errposition_callback` /
/// `pcb_error_callback` (parse_node.c) — the error-context shim that tags
/// non-parser errors thrown by callees with a parse error position.
///
/// `error_context_stack` is retired repo-wide (docs/query-lifecycle-raii.md):
/// error context attaches on propagation, not through an ambient callback chain.
/// These functions therefore have no ambient chain to push/pop — the location
/// tagging happens where the fallible callee returns (the seam carrying the
/// location, e.g. `numeric_in`/`bit_in`). They are retained as the (no-op)
/// C-structure image; the skipped `ERRCODE_QUERY_CANCELED` case of
/// `pcb_error_callback` likewise has no counterpart (no callback fires).
pub fn setup_parser_errposition_callback(_pstate: &ParseState<'_>, _location: i32) {}

/// See [`setup_parser_errposition_callback`].
pub fn cancel_parser_errposition_callback() {}

/// `transformContainerType(containerType, containerTypmod)` (parse_node.c) —
/// identify the actual container type for a subscripting operation, smashing any
/// domain to its base type and treating `int2vector`/`oidvector` as domains over
/// `int2[]`/`oid[]`.
pub fn transformContainerType(
    container_type: &mut Oid,
    container_typmod: &mut i32,
) -> PgResult<()> {
    // *containerType = getBaseTypeAndTypmod(*containerType, containerTypmod);
    let (base, typmod) = lsyscache::get_base_type_and_typmod::call(*container_type)?;
    *container_type = base;
    *container_typmod = typmod;

    // int2vector / oidvector are treated as domains over int2[] / oid[].
    if *container_type == INT2VECTOROID {
        *container_type = INT2ARRAYOID;
    } else if *container_type == OIDVECTOROID {
        *container_type = OIDARRAYOID;
    }
    Ok(())
}

/// `transformContainerSubscripts(pstate, containerBase, containerType,
/// containerTypMod, indirection, isAssignment)` (parse_node.c) — transform
/// container (array, etc) subscripting into a `SubscriptingRef`.
pub fn transformContainerSubscripts<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    container_base: Expr,
    mut container_type: Oid,
    mut container_typmod: i32,
    indirection: &[A_Indices<'mcx>],
    is_assignment: bool,
) -> PgResult<SubscriptingRef> {
    // Determine the actual container type, smashing any domain. In the
    // assignment case the caller already did this.
    if !is_assignment {
        transformContainerType(&mut container_type, &mut container_typmod)?;
    }

    // Verify that the container type is subscriptable, and get its support
    // functions and typelem.
    let routines = lsyscache::get_subscripting_routines::call(container_type)?;
    let element_type = match routines {
        Some((_sbsroutines, element_type)) => element_type,
        None => {
            // ereport(ERROR, DATATYPE_MISMATCH, "cannot subscript type %s ...")
            let tname =
                backend_utils_adt_format_type::format_type_be_owned(container_type)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(alloc::format!(
                    "cannot subscript type {} because it does not support subscripting",
                    tname
                ))
                .errposition(parser_errposition(
                    pstate,
                    expr_location(Some(&container_base))?,
                ))
                .into_error());
        }
    };

    // Detect whether any of the indirection items are slice specifiers.
    let mut is_slice = false;
    for ai in indirection.iter() {
        if ai.is_slice {
            is_slice = true;
            break;
        }
    }

    // Ready to build the SubscriptingRef node.
    let sbsref = SubscriptingRef {
        refcontainertype: container_type,
        refelemtype: element_type,
        // refrestype is to be set by container-specific logic.
        refrestype: InvalidOid,
        reftypmod: container_typmod,
        // refcollid will be set by parse_collate.c.
        refcollid: InvalidOid,
        // refupperindexpr, reflowerindexpr are set by container logic.
        refupperindexpr: Vec::new(),
        reflowerindexpr: Vec::new(),
        refexpr: Some(alloc::boxed::Box::new(container_base)),
        // caller will fill if it's an assignment.
        refassgnexpr: None,
    };

    // Call the container-type-specific logic (sbsroutines->transform) to
    // transform the subscripts and determine the subscripting result type. The
    // per-type subscript handler is unported — reach it through its outward seam.
    let sbsref = backend_parser_small1_seams::subscripting_transform::call(
        mcx,
        sbsref,
        indirection,
        pstate,
        is_slice,
        is_assignment,
    )?;

    // Verify we got a valid type.
    if !OidIsValid(sbsref.refrestype) {
        let tname = backend_utils_adt_format_type::format_type_be_owned(container_type)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(alloc::format!(
                "cannot subscript type {} because it does not support subscripting",
                tname
            ))
            .into_error());
    }

    Ok(sbsref)
}

/// `make_const(pstate, aconst)` (parse_node.c) — convert an `A_Const` grammar
/// node to a `Const` node of the "natural" type for the constant.
///
/// The integer / float-fits-int / boolean / NULL arms are pure. The string,
/// bitstring and numeric (oversize-float) arms call `DirectFunctionCall` of
/// `numeric_in` / `bit_in` / `CStringGetDatum` which produce a by-reference
/// `Datum`; the bitstring and numeric input-function calls reach their owners
/// through outward seams, with the literal's source location attached to any
/// error so a bad literal reports the same `LINE`/caret as C.
///
/// Takes `mcx` explicitly: C's `make_const` allocates the `Const` in the
/// caller's current context; the owned `ParseState` carries no arena field.
pub fn make_const<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_>,
    aconst: &A_Const<'_>,
) -> PgResult<Const> {
    // `setup_parser_errposition_callback(&pcbstate, pstate, aconst->location)`
    // arranges to report the literal's source location if the type's input
    // function (`numeric_in` / `bit_in`) raises an error. The ambient
    // error-context callback chain is retired here (docs/query-lifecycle-raii.md);
    // the location is instead attached at the point the fallible input-function
    // seam returns, exactly as `pcb_error_callback` does: tag the error with
    // `parser_errposition(pstate, aconst->location)` as the cursor position, but
    // only when the error has none of its own (C: `if (edata->cursorpos == 0)`).
    let attach_errpos = |mut e: PgError| -> PgError {
        if e.cursor_position().is_none() {
            let pos = parser_errposition(pstate, aconst.location);
            if pos > 0 {
                e = e.with_cursor_position(pos);
            }
        }
        e
    };
    if aconst.isnull {
        // return a null const: makeConst(UNKNOWNOID, -1, InvalidOid, -2,
        //   (Datum) 0, true, false); con->location = aconst->location;
        let mut con =
            makeConst(mcx, UNKNOWNOID, -1, InvalidOid, -2, Datum::from_usize(0), true, false)?;
        con.location = aconst.location;
        return Ok(con);
    }

    let val_node = aconst
        .val
        .as_deref()
        .expect("A_Const.val present when !isnull");

    let (val, typeid, typelen, typebyval): (Datum<'mcx>, Oid, i32, bool) = match val_node.node_tag() {
        ntag::T_Integer => {
            let i = val_node.expect_integer();
            // val = Int32GetDatum(intVal(&aconst->val));
            (Datum::from_i32(i.ival), INT4OID, 4, true)
        }
        ntag::T_Float => {
            let f = val_node.expect_float();
            // could be an oversize integer as well as a float ...
            // val64 = pg_strtoint64_safe(fval, &escontext);
            match pg_strtoint64_safe(f.fval.as_str()) {
                Some(val64) => {
                    // It might actually fit in int32.
                    let val32 = val64 as i32;
                    if val64 == val32 as i64 {
                        (Datum::from_i32(val32), INT4OID, 4, true)
                    } else {
                        (Datum::from_i64(val64), INT8OID, 8, FLOAT8PASSBYVAL != 0)
                    }
                }
                None => {
                    // val = DirectFunctionCall3(numeric_in,
                    //         CStringGetDatum(aconst->val.fval.fval),
                    //         ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                    // numeric_in returns the on-disk Numeric varlena byte image;
                    // it is a by-reference Datum.
                    let bytes = backend_parser_small1_seams::numeric_in::call(
                        mcx,
                        f.fval.as_str(),
                    )
                    .map_err(attach_errpos)?;
                    (Datum::ByRef(bytes), NUMERICOID, -1, false)
                }
            }
        }
        ntag::T_Boolean => {
            let b = val_node.expect_boolean();
            // val = BoolGetDatum(boolVal(&aconst->val));
            (Datum::from_bool(b.boolval), BOOLOID, 1, true)
        }
        ntag::T_String => {
            let s = val_node.expect_string();
            // C (parse_node.c make_const, T_String arm):
            //   val = CStringGetDatum(strVal(&aconst->val));
            //   typeid = UNKNOWNOID; /* will be coerced later */
            //   typelen = -2;        /* cstring-style varwidth */
            //   typebyval = false;
            // `CStringGetDatum` yields a by-reference (cstring pointer) Datum;
            // here the canonical `Datum::Cstring` arm carries the owned text.
            (Datum::from_cstring(alloc::string::String::from(s.sval.as_str())), UNKNOWNOID, -2, false)
        }
        ntag::T_BitString => {
            let b = val_node.expect_bitstring();
            // val = DirectFunctionCall3(bit_in,
            //         CStringGetDatum(aconst->val.bsval.bsval),
            //         ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            // bit_in returns the on-disk VarBit varlena byte image; it is a
            // by-reference Datum.
            let bytes = backend_parser_small1_seams::bit_in::call(
                mcx,
                b.bsval.as_str().as_bytes(),
            )
            .map_err(attach_errpos)?;
            (Datum::ByRef(bytes), BITOID, -1, false)
        }
        _ => {
            // elog(ERROR, "unrecognized node type: %d", nodeTag)
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(alloc::format!(
                    "unrecognized node type: {}",
                    val_node.node_tag().0
                ))
                .into_error());
        }
    };

    // con = makeConst(typeid, -1, InvalidOid, typelen, val, false, typebyval);
    // con->location = aconst->location;
    let mut con = makeConst(mcx, typeid, -1, InvalidOid, typelen, val, false, typebyval)?;
    con.location = aconst.location;
    Ok(con)
}

/// `pg_strtoint64_safe(str, escontext)` (numutils.c) — parse an integer that may
/// overflow `int64`; `None` on any (soft) parse failure (the C tests
/// `escontext.error_occurred`).
fn pg_strtoint64_safe(s: &str) -> Option<i64> {
    let mut escontext = SoftErrorContext::new(false);
    match backend_utils_adt_numutils::pg_strtoint64_safe(s, Some(&mut escontext)) {
        Ok(v) if !escontext.error_occurred() => Some(v),
        _ => None,
    }
}

// ===========================================================================
// parse_param.c (F2: fixed parameters + read-only post-analysis checks)
// ===========================================================================

pub use types_nodes::parsestmt::FixedParamState;

/// `setup_parse_fixed_parameters(pstate, paramTypes, numParams)` (parse_param.c)
/// — set up to process a query referencing a fixed list of parameter types.
///
/// C stores a `FixedParamState *` in `pstate->p_ref_hook_state` and installs
/// `fixed_paramref_hook`. The owned model installs the [`FixedParamState`] into
/// `pstate.p_ref_hook_state` (the `FixedParams` arm); the active ref-hook arm is
/// what selects the installed paramref hook, exactly as C's
/// `pstate->p_paramref_hook = fixed_paramref_hook` does (no `p_coerce_param_hook`
/// is set, matching C). The value-typed hook is reached from the parser via that
/// installed arm (see [`fixed_paramref_hook`]).
pub fn setup_parse_fixed_parameters(pstate: &mut ParseState<'_>, param_types: &[Oid]) {
    // parstate->paramTypes = paramTypes; parstate->numParams = numParams;
    // pstate->p_ref_hook_state = parstate;
    pstate.p_ref_hook_state = ParseRefHookState::FixedParams(FixedParamState::new(param_types));
    // pstate->p_paramref_hook = fixed_paramref_hook;
    // (selected by the FixedParams ref-hook arm; see transformParamRef)
    // /* no need to use p_coerce_param_hook */
}

/// `sql_fn_parser_setup(pstate, pinfo)` (executor/functions.c:340) — parser
/// setup hook for parsing a SQL-function body. Installs the post-columnref hook
/// (so a body bareword that names a function parameter resolves to its `$n`
/// Param) and the paramref hook (so `$n` resolves against the function's
/// argument types), with the [`SqlFnParseInfo`] as the ref-hook state.
///
/// `p_post_columnref_hook` / `p_paramref_hook` are set to the SQL-function
/// markers so the parser's hook gates fire; the active `SqlFunction` ref-hook arm
/// is what `transformColumnRef` / `transformParamRef` dispatch on (mirroring C's
/// `pstate->p_{post_columnref,paramref}_hook = sql_fn_{post_column_ref,param_ref}`
/// — the function pointer and the ref-hook state are set in lockstep).
///
/// [`SqlFnParseInfo`]: types_nodes::parsestmt::SqlFnParseInfo
pub fn setup_parse_sql_function(
    pstate: &mut ParseState<'_>,
    pinfo: types_nodes::parsestmt::SqlFnParseInfo,
) {
    // pstate->p_pre_columnref_hook = NULL;
    pstate.p_pre_columnref_hook = None;
    // pstate->p_post_columnref_hook = sql_fn_post_column_ref;
    pstate.p_post_columnref_hook =
        Some(sql_fn_post_column_ref_marker as types_nodes::parsestmt::PostParseColumnRefHook<'_>);
    // pstate->p_paramref_hook = sql_fn_param_ref;
    pstate.p_paramref_hook =
        Some(sql_fn_param_ref_marker as types_nodes::parsestmt::ParseParamRefHook<'_>);
    // /* no need to use p_coerce_param_hook */
    // pstate->p_ref_hook_state = pinfo;
    pstate.p_ref_hook_state = ParseRefHookState::SqlFunction(pinfo);
}

/// Marker for `pstate.p_post_columnref_hook` under SQL-function-body parsing. The
/// real dispatch reads the `SqlFunction` ref-hook arm in `transformColumnRef`
/// (parse_expr); this value only makes the hook gate fire.
fn sql_fn_post_column_ref_marker<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _cref: &types_nodes::rawnodes::ColumnRef<'mcx>,
    var: Option<types_nodes::nodes::NodePtr<'mcx>>,
) -> PgResult<Option<types_nodes::nodes::NodePtr<'mcx>>> {
    // Unreachable: the dispatch is by ref-hook arm, not by this function pointer.
    Ok(var)
}

/// Marker for `pstate.p_paramref_hook` under SQL-function-body parsing. The real
/// dispatch reads the `SqlFunction` ref-hook arm in `transformParamRef`.
fn sql_fn_param_ref_marker<'mcx>(
    _pstate: &mut ParseState<'mcx>,
    _pref: &types_nodes::rawnodes::ParamRef,
) -> PgResult<Option<types_nodes::nodes::NodePtr<'mcx>>> {
    Ok(None)
}

/// `fixed_paramref_hook(pstate, pref)` (parse_param.c) — transform a `ParamRef`
/// using fixed parameter types.
pub fn fixed_paramref_hook<'mcx>(
    pstate: &ParseState<'mcx>,
    parstate: &FixedParamState,
    pref: &ParamRef,
) -> PgResult<Param> {
    let paramno = pref.number;

    // Check parameter number is valid.
    if paramno <= 0
        || paramno as usize > parstate.param_types.len()
        || !OidIsValid(parstate.param_types[(paramno - 1) as usize])
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(alloc::format!("there is no parameter ${}", paramno))
            .errposition(parser_errposition(pstate, pref.location))
            .into_error());
    }

    let paramtype = parstate.param_types[(paramno - 1) as usize];
    Ok(Param {
        paramkind: PARAM_EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid: lsyscache::get_typcollation::call(paramtype)?,
        // param->location = pref->location;
        location: pref.location,
    })
}

/// `setup_parse_variable_parameters(pstate, paramTypes, numParams)`
/// (parse_param.c) — set up to process a query referencing a variable list of
/// parameter types, growing the caller's `Oid *` array as `$n` refs appear.
///
/// C allocates a `VarParamState` aliasing the caller's mutable `Oid **paramTypes`
/// / `int *numParams`, stores it in `pstate->p_ref_hook_state`, and installs
/// `variable_paramref_hook` + `variable_coerce_param_hook`. The owned model puts
/// the shared, growable type array in a [`VarParamState`] carrier (an
/// `Rc<RefCell<Vec<Oid>>>` the caller keeps a clone of, reading the resolved
/// types back after analysis — the `Vec`'s length is C's `*numParams`); the
/// installed hooks are [`variable_paramref_hook`] / [`variable_coerce_param_hook`]
/// reachable from the parser, exactly as the C wires the two function pointers.
pub fn setup_parse_variable_parameters(pstate: &mut ParseState<'_>, parstate: VarParamState) {
    // pstate->p_ref_hook_state = parstate;
    pstate.p_ref_hook_state = ParseRefHookState::VarParams(parstate);
    // pstate->p_paramref_hook = variable_paramref_hook;
    // pstate->p_coerce_param_hook = variable_coerce_param_hook;
    //
    // The owned ParseState's hook fields take a bare `fn` over the raw-Node
    // universe; the variable-parameter resolvers are value-typed (they return a
    // `Param`), so the parser reaches them directly via the installed
    // `VarParams` ref-hook state rather than through the `fn`-pointer slots. The
    // ref-hook state being `VarParams(..)` is the marker of that wiring (it is
    // the real artifact the C function pointers select).
}

/// `variable_paramref_hook(pstate, pref)` (parse_param.c) — transform a
/// `ParamRef` using variable parameter types, enlarging the shared type array as
/// needed and initializing newly-seen slots to `UNKNOWNOID`.
pub fn variable_paramref_hook(
    pstate: &ParseState<'_>,
    parstate: &VarParamState,
    pref: &ParamRef,
) -> PgResult<Param> {
    let paramno = pref.number;

    // Check parameter number is in range.
    //   if (paramno <= 0 || paramno > MaxAllocSize / sizeof(Oid)) ereport
    if paramno <= 0 || (paramno as usize) > MAX_ALLOC_SIZE / core::mem::size_of::<Oid>() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(alloc::format!("there is no parameter ${}", paramno))
            .errposition(parser_errposition(pstate, pref.location))
            .into_error());
    }

    let mut param_types = parstate.param_types.borrow_mut();

    // if (paramno > *parstate->numParams) { enlarge param array; *numParams = paramno }
    //
    // The shared Vec's length is *numParams; growing it (palloc0_array /
    // repalloc0_array) zero-fills the new slots (InvalidOid == 0).
    if (paramno as usize) > param_types.len() {
        let grow = paramno as usize - param_types.len();
        param_types.try_reserve(grow).map_err(|_| out_of_memory())?;
        param_types.resize(paramno as usize, InvalidOid);
    }

    // Locate param's slot in array; if not seen before, initialize to UNKNOWN.
    //   pptype = &(*paramTypes)[paramno - 1]; if (*pptype == InvalidOid) *pptype = UNKNOWNOID;
    let idx = (paramno - 1) as usize;
    if param_types[idx] == InvalidOid {
        param_types[idx] = UNKNOWNOID;
    }

    // If the argument is of type void and it's a procedure call, interpret it as
    // unknown. (JDBC hack — see also ParseFuncOrColumn.)
    if param_types[idx] == VOIDOID
        && pstate.p_expr_kind == ParseExprKind::EXPR_KIND_CALL_ARGUMENT
    {
        param_types[idx] = UNKNOWNOID;
    }

    let paramtype = param_types[idx];
    drop(param_types);

    // param = makeNode(Param); ...
    Ok(Param {
        paramkind: PARAM_EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid: lsyscache::get_typcollation::call(paramtype)?,
        location: pref.location,
    })
}

/// `variable_coerce_param_hook(pstate, param, targetTypeId, targetTypeMod,
/// location)` (parse_param.c) — coerce a `Param` to a query-requested type in the
/// varparams case, recording the deduced type back into the shared array (so
/// later refs + the caller see it). Returns the coerced `Param` (C's `Node*`), or
/// `None` to signal the caller to proceed with normal coercion.
///
/// `param` is mutated in place, exactly as the C hook updates `*param`.
pub fn variable_coerce_param_hook(
    pstate: &ParseState<'_>,
    parstate: &VarParamState,
    param: &mut Param,
    target_type_id: Oid,
    _target_type_mod: i32,
    location: i32,
) -> PgResult<Option<Param>> {
    if param.paramkind == PARAM_EXTERN && param.paramtype == UNKNOWNOID {
        // Input is a Param of previously undetermined type, and we want to
        // update our knowledge of the Param's type.
        let paramno = param.paramid;

        let mut param_types = parstate.param_types.borrow_mut();

        // if (paramno <= 0 || paramno > *parstate->numParams) ereport
        if paramno <= 0 || (paramno as usize) > param_types.len() {
            drop(param_types);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_PARAMETER)
                .errmsg(alloc::format!("there is no parameter ${}", paramno))
                .errposition(parser_errposition(pstate, param.location))
                .into_error());
        }

        let idx = (paramno - 1) as usize;
        let existing = param_types[idx];
        if existing == UNKNOWNOID {
            // We've successfully resolved the type.
            param_types[idx] = target_type_id;
        } else if existing == target_type_id {
            // We previously resolved the type, and it matches.
        } else {
            // Oops — inconsistent types deduced.
            drop(param_types);
            let was = backend_utils_adt_format_type::format_type_be_owned(existing)?;
            let now = backend_utils_adt_format_type::format_type_be_owned(target_type_id)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_AMBIGUOUS_PARAMETER)
                .errmsg(alloc::format!(
                    "inconsistent types deduced for parameter ${}",
                    paramno
                ))
                .errdetail(alloc::format!("{} versus {}", was, now))
                .errposition(parser_errposition(pstate, param.location))
                .into_error());
        }
        drop(param_types);

        param.paramtype = target_type_id;

        // Note: leaving paramtypmod -1 ensures a run-time length check/coercion
        // occurs if needed.
        param.paramtypmod = -1;

        // This module always sets a Param's collation to the type default.
        param.paramcollid = lsyscache::get_typcollation::call(param.paramtype)?;

        // Use the leftmost of the param's and coercion's locations.
        if location >= 0 && (param.location < 0 || location < param.location) {
            param.location = location;
        }

        return Ok(Some(param.clone()));
    }

    // Else signal to proceed with normal coercion.
    Ok(None)
}

/// `check_variable_parameters(pstate, query)` (parse_param.c) — verify consistent
/// variable-parameter type assignment after parsing. Walks the query tree
/// ensuring every `PARAM_EXTERN` `Param` matches its deduced type in the shared
/// array (some may still be UNKNOWN if nothing forced their coercion).
///
/// Note: intentionally does not check that all parameter positions were used, nor
/// that all got non-UNKNOWN types — the caller enforces that if it matters.
pub fn check_variable_parameters(pstate: &ParseState<'_>, query: &Query<'_>) -> PgResult<()> {
    let parstate = pstate
        .p_ref_hook_state
        .as_var_params()
        .expect("check_variable_parameters requires a VarParamState ref-hook state");

    // If numParams is zero then no Params were generated, so no work.
    if parstate.param_types.borrow().is_empty() {
        return Ok(());
    }

    // (void) query_tree_walker(query, check_parameter_resolution_walker, pstate, 0);
    let mut walk_err: PgResult<()> = Ok(());
    backend_nodes_core::node_walker::query_tree_walker(
        query,
        &mut |node| check_parameter_resolution_walker(node, pstate, parstate, &mut walk_err),
        0,
    );
    walk_err
}

/// `check_parameter_resolution_walker(node, pstate)` (parse_param.c, static) —
/// traverse a fully-analyzed tree verifying each `PARAM_EXTERN` `Param` matches
/// its deduced type in the shared array. Returns `true` to abort the walk (an
/// error was captured into `err`, mirroring the C `ereport(ERROR)` that
/// long-jumps out of the walk).
fn check_parameter_resolution_walker(
    node: &Node<'_>,
    pstate: &ParseState<'_>,
    parstate: &VarParamState,
    err: &mut PgResult<()>,
) -> bool {
    if err.is_err() {
        return true;
    }
    if let Some(param) = node_as_param(node) {
        if param.paramkind == PARAM_EXTERN {
            let paramno = param.paramid;
            let param_types = parstate.param_types.borrow();

            if paramno <= 0 || (paramno as usize) > param_types.len() {
                drop(param_types);
                *err = Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_PARAMETER)
                    .errmsg(alloc::format!("there is no parameter ${}", paramno))
                    .errposition(parser_errposition(pstate, param.location))
                    .into_error());
                return true;
            }

            if param.paramtype != param_types[(paramno - 1) as usize] {
                drop(param_types);
                *err = Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_PARAMETER)
                    .errmsg(alloc::format!(
                        "could not determine data type of parameter ${}",
                        paramno
                    ))
                    .errposition(parser_errposition(pstate, param.location))
                    .into_error());
                return true;
            }
        }
        return false;
    }
    if let Some(q) = node_as_query(node) {
        // Recurse into RTE subquery or not-yet-planned sublink subquery.
        return backend_nodes_core::node_walker::query_tree_walker(
            q,
            &mut |n| check_parameter_resolution_walker(n, pstate, parstate, err),
            0,
        );
    }
    backend_nodes_core::node_walker::expression_tree_walker(node, &mut |n| {
        check_parameter_resolution_walker(n, pstate, parstate, err)
    })
}

/// `query_contains_extern_params(query)` (parse_param.c) — true if a
/// fully-parsed query tree contains any `PARAM_EXTERN` `Param`.
pub fn query_contains_extern_params(query: &Query<'_>) -> bool {
    query_contains_extern_params_walker_query(query)
}

/// `query_contains_extern_params_walker(node, context)` (parse_param.c, static)
/// — over a `Node`. Returns true to abort the walk (found one).
fn query_contains_extern_params_walker(node: &Node<'_>) -> bool {
    if let Some(param) = node_as_param(node) {
        // if (param->paramkind == PARAM_EXTERN) return true;
        return param.paramkind == PARAM_EXTERN;
    }
    if let Some(q) = node_as_query(node) {
        // Recurse into RTE subquery or not-yet-planned sublink subquery.
        return query_contains_extern_params_walker_query(q);
    }
    backend_nodes_core::node_walker::expression_tree_walker(
        node,
        &mut query_contains_extern_params_walker,
    )
}

/// The `query_tree_walker(query, query_contains_extern_params_walker, ...)`
/// entry from a `Query`.
fn query_contains_extern_params_walker_query(query: &Query<'_>) -> bool {
    backend_nodes_core::node_walker::query_tree_walker(
        query,
        &mut query_contains_extern_params_walker,
        0,
    )
}

/// `IsA(node, Param)` projection over the `Node` universe.
fn node_as_param<'a>(node: &'a Node<'_>) -> Option<&'a Param> {
    node.as_param()
}

/// `IsA(node, Query)` projection over the `Node` universe.
fn node_as_query<'a, 'mcx>(node: &'a Node<'mcx>) -> Option<&'a Query<'mcx>> {
    node.as_query()
}

// ===========================================================================
// parse_merge.c — transformMergeStmt (sibling parser deps unported)
// ===========================================================================

/// `transformMergeStmt(pstate, stmt)` (parse_merge.c) — analyze a MERGE
/// statement into a `Query`.
///
/// Its body orchestrates `transformFromClause` / `setTargetTable` /
/// `transformWhereClause` / `transformTargetList` / `transformExpr` and the CTE
/// machinery — every one of those owners (parse_clause.c, parse_relation.c,
/// parse_target.c, parse_expr.c's full path, analyze.c) is unported. Faithful
/// mirror-PG-and-panic until that sibling parser layer lands.
pub fn transformMergeStmt() -> PgResult<()> {
    panic!(
        "transformMergeStmt (parse_merge.c) orchestrates the unported sibling \
         parser layer (parse_clause/parse_relation/parse_target/analyze); \
         mirror-PG-and-panic until those owners land"
    )
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Convert the infallible C `parser_errposition` into the seam's `PgResult<i32>`
/// contract (the C never errors here, so this always succeeds).
fn parser_errposition_seam(pstate: &ParseState<'_>, location: i32) -> PgResult<i32> {
    Ok(parser_errposition(pstate, location))
}

/// Install this unit's inward seams.
pub fn init_seams() {
    backend_parser_small1_seams::parser_errposition::set(parser_errposition_seam);
    // collationcmds.c (DefineCollation) re-declares `parser_errposition`
    // (parse_node.c) without a `ParseState` (its port drops the pstate arg); with
    // no source text reachable the C reduces to `location < 0 ? 0 : location`.
    backend_commands_collationcmds_seams::parser_errposition::set(|location| {
        if location < 0 {
            0
        } else {
            location
        }
    });
    // functioncmds.c / aggregatecmds.c call `parser_errposition(pstate,
    // location)` (parse_node.c). The installer carries the active query string
    // (`pstate->p_sourcetext`) so the seam reproduces the full C body:
    //   if (location < 0) return 0;
    //   if (p_sourcetext == NULL) return 0;
    //   pos = pg_mbstrlen_with_len(p_sourcetext, location) + 1;
    backend_commands_functioncmds_seams::parser_errposition::set(|source, location| {
        if location < 0 {
            return 0;
        }
        match source {
            // `p_sourcetext` is valid in the server encoding, so the
            // `report_invalid_encoding` path is dead; report 0 ("no position")
            // if it somehow fires rather than escalate while building an error.
            Some(s) => match mb::pg_mbstrlen_with_len::call(s.as_bytes(), location) {
                Ok(n) => n + 1,
                Err(_) => 0,
            },
            None => 0,
        }
    });
    backend_parser_scansup_seams::truncate_identifier::set(truncate_identifier);
    backend_parser_scansup_seams::downcase_truncate_identifier::set(downcase_truncate_identifier);
    backend_parser_analyze_seams::make_parsestate::set(make_parsestate);
    backend_parser_small1_seams::coerce_param_hook::set(coerce_param_hook_seam);
}

/// `coerce_type`'s `pstate->p_coerce_param_hook(...)` dispatch (parse_coerce.c).
/// C selects the installed hook by which `setup_parse_*_parameters` ran; the
/// owned model dispatches on `pstate.p_ref_hook_state`. Only the
/// variable-parameter case (`setup_parse_variable_parameters`) installs a
/// coercion hook (`variable_coerce_param_hook`); the fixed-parameter, SQL-
/// function, and no-hook cases have no `p_coerce_param_hook` (C: "no need to use
/// p_coerce_param_hook"), so they return `None` to fall through to normal
/// coercion — exactly what a NULL `p_coerce_param_hook` does.
fn coerce_param_hook_seam(
    pstate: &types_nodes::parsestmt::ParseState<'_>,
    param: &types_nodes::primnodes::Param,
    target_type_id: Oid,
    target_type_mod: i32,
    location: i32,
) -> PgResult<Option<types_nodes::primnodes::Param>> {
    match pstate.p_ref_hook_state.as_var_params() {
        Some(parstate) => {
            // VarParamState is an `Rc` carrier; clone it out so the call doesn't
            // alias the `&pstate` borrow `variable_coerce_param_hook` also needs
            // (for parser_errposition). The shared `Oid` array is the same.
            let parstate = parstate.clone();
            let mut p = param.clone();
            variable_coerce_param_hook(
                pstate,
                &parstate,
                &mut p,
                target_type_id,
                target_type_mod,
                location,
            )
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests;
