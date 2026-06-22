//! `backend-pl-plpgsql-comp` — the PL/pgSQL compiler (`pl_comp.c`).
//!
//! Turns a parse tree (from the grammar) into a `PLpgSQL_function`: builds the
//! datum array, resolves the namespace, handles `%TYPE`/`%ROWTYPE`, compiles
//! the SQL-expression substrate, and assembles the executable function.
//!
//! This crate INSTALLS the scanner/grammar callbacks declared in
//! `backend-pl-plpgsql-comp-seams` (`plpgsql_parse_word`/`dblword`/`tripword`,
//! the `plpgsql_curr_compile->…` accessors, the `plpgsql_build_*` builders, the
//! `%TYPE`/`%ROWTYPE` resolvers, the err-condition lookups, and the
//! datum-array bookkeeping) from [`init_seams`].
//!
//! ## Global compile-time state
//!
//! `pl_comp.c` keeps its working state in per-backend module statics
//! (`plpgsql_Datums`, `plpgsql_nDatums`, `datums_alloc`/`datums_last`,
//! `plpgsql_curr_compile`, `plpgsql_error_funcname`, `plpgsql_DumpExecTree`,
//! `plpgsql_check_syntax`, `plpgsql_compile_tmp_cxt`). The compiler is
//! explicitly non-reentrant, so these are modeled as thread-locals owned here.

#![allow(non_camel_case_types, non_snake_case)]

mod mem;
pub mod rowtupdesc_table;
mod seam;

/// The custom-GUC assign-hook targets (`plpgsql_variable_conflict` /
/// `plpgsql_print_strict_params` / `plpgsql_extra_warnings` /
/// `plpgsql_extra_errors`): the compiler's per-backend copies of the
/// `pl_handler.c` GUC globals it reads while assembling a function. The handler
/// (the layer above) writes these through its GUC assign hooks. Exposed so the
/// handler's `DefineCustom*Variable` assign hooks update the values the compiler
/// actually reads.
pub use seam::{
    set_plpgsql_extra_errors, set_plpgsql_extra_warnings, set_plpgsql_print_strict_params,
    set_plpgsql_variable_conflict,
};

use core::cell::RefCell;

use types_core::Oid;
use types_datum::Datum;
pub use types_error::ERRCODE_UNDEFINED_COLUMN;
use types_error::{
    PgError, PgResult, SqlState, ERRCODE_ERROR_IN_ASSIGNMENT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_FUNCTION_DEFINITION, ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_TABLE,
    ERRCODE_WRONG_OBJECT_TYPE,
};
use types_plpgsql::*;

use backend_pl_plpgsql_funcs as funcs;

use seam::{oid_is_valid, INVALID_OID};

// ---------------------------------------------------------------------------
// Hardwired type OIDs (catalog/pg_type.h) referenced by the compiler.
// ---------------------------------------------------------------------------
const BOOLOID: Oid = 16;
const NAMEOID: Oid = 19;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const RECORDOID: Oid = 2249;
const VOIDOID: Oid = 2278;
const TRIGGEROID: Oid = 2279;
const EVENT_TRIGGEROID: Oid = 3838;
const TEXTARRAYOID: Oid = 1009;

// Polymorphic-type OIDs (catalog/pg_type.h).
const ANYARRAYOID: Oid = 2277;
const ANYELEMENTOID: Oid = 2283;
const ANYNONARRAYOID: Oid = 2776;
const ANYENUMOID: Oid = 3500;
const ANYRANGEOID: Oid = 3831;
const ANYCOMPATIBLEOID: Oid = 5077;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
const ANYCOMPATIBLERANGEOID: Oid = 5080;
const ANYMULTIRANGEOID: Oid = 4537;
const ANYCOMPATIBLEMULTIRANGEOID: Oid = 4538;

// Substitution OIDs used in validation mode for polymorphic returns.
const INT4ARRAYOID: Oid = 1007;
const INT4RANGEOID: Oid = 3904;
const INT4MULTIRANGEOID: Oid = 4451;

// pg_proc.prokind / provolatile codes (catalog/pg_proc.h).
const PROKIND_FUNCTION: u8 = b'f';
const PROKIND_PROCEDURE: u8 = b'p';
const PROVOLATILE_VOLATILE: u8 = b'v';

// pg_type.typtype codes (catalog/pg_type.h's TYPTYPE_*).
const TYPTYPE_BASE: i8 = b'b' as i8;
const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
const TYPTYPE_DOMAIN: i8 = b'd' as i8;
const TYPTYPE_ENUM: i8 = b'e' as i8;
const TYPTYPE_PSEUDO: i8 = b'p' as i8;
const TYPTYPE_RANGE: i8 = b'r' as i8;
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

// pg_type.typstorage codes.
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;

/// `INVALID_TUPLEDESC_IDENTIFIER` (access/tupdesc.h).
const INVALID_TUPLEDESC_IDENTIFIER: u64 = 1;

/// `IsPolymorphicType(typid)` — true for the polymorphic pseudo-types.
fn is_polymorphic_type(typid: Oid) -> bool {
    matches!(
        typid,
        ANYELEMENTOID | ANYARRAYOID | ANYNONARRAYOID | ANYENUMOID | ANYRANGEOID | ANYMULTIRANGEOID
    ) || matches!(
        typid,
        ANYCOMPATIBLEOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}

// ===========================================================================
// Global compile state (pl_comp.c module statics) — thread-locals.
// ===========================================================================

thread_local! {
    /// `static int datums_alloc;`
    static DATUMS_ALLOC: RefCell<i32> = const { RefCell::new(0) };
    /// `int plpgsql_nDatums;`
    static PLPGSQL_N_DATUMS: RefCell<i32> = const { RefCell::new(0) };
    /// `PLpgSQL_datum **plpgsql_Datums;`
    static PLPGSQL_DATUMS: RefCell<Vec<PLpgSQL_datum>> = const { RefCell::new(Vec::new()) };
    /// `static int datums_last;`
    static DATUMS_LAST: RefCell<i32> = const { RefCell::new(0) };

    /// `char *plpgsql_error_funcname;`
    static PLPGSQL_ERROR_FUNCNAME: RefCell<Option<String>> = const { RefCell::new(None) };
    /// Tracks `plpgsql_latest_lineno(yyscanner)` for the compile error-context
    /// callback. `plpgsql_scanner_init` resets `cur_line_num` to 1, so a
    /// semantic compile error raised before/around the parse reports "near line
    /// 1"; the parse advances it.
    static PLPGSQL_LATEST_LINENO: RefCell<i32> = const { RefCell::new(1) };
    /// `bool plpgsql_DumpExecTree = false;`
    static PLPGSQL_DUMP_EXEC_TREE: RefCell<bool> = const { RefCell::new(false) };
    /// `bool plpgsql_check_syntax = false;`
    static PLPGSQL_CHECK_SYNTAX: RefCell<bool> = const { RefCell::new(false) };

    /// `PLpgSQL_function *plpgsql_curr_compile;`
    static PLPGSQL_CURR_COMPILE: RefCell<Option<PLpgSQL_function>> = const { RefCell::new(None) };

    /// `IdentifierLookup plpgsql_IdentifierLookup;` — in the repo's scanner the
    /// authoritative copy lives on the scanner instance; the compiler's
    /// dotted-name resolvers consult this thread-local mirror (defaulting to
    /// `IDENTIFIER_LOOKUP_NORMAL`, the value outside DECLARE sections).
    static IDENTIFIER_LOOKUP: RefCell<IdentifierLookup> =
        const { RefCell::new(IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL) };
}

/// Read `plpgsql_IdentifierLookup`.
fn plpgsql_identifier_lookup() -> IdentifierLookup {
    IDENTIFIER_LOOKUP.with(|f| *f.borrow())
}

/// Set `plpgsql_IdentifierLookup` (the scanner/grammar mode mirror).
pub fn set_plpgsql_identifier_lookup(mode: IdentifierLookup) {
    IDENTIFIER_LOOKUP.with(|f| *f.borrow_mut() = mode);
}

/// Read `plpgsql_DumpExecTree`.
pub fn plpgsql_dump_exec_tree() -> bool {
    PLPGSQL_DUMP_EXEC_TREE.with(|f| *f.borrow())
}
/// `plpgsql_DumpExecTree = value` (`#option dump`).
pub fn set_dump_exec_tree(value: bool) {
    PLPGSQL_DUMP_EXEC_TREE.with(|f| *f.borrow_mut() = value);
}
/// Read `plpgsql_check_syntax`.
pub fn plpgsql_check_syntax() -> bool {
    PLPGSQL_CHECK_SYNTAX.with(|f| *f.borrow())
}
fn set_check_syntax(value: bool) {
    PLPGSQL_CHECK_SYNTAX.with(|f| *f.borrow_mut() = value);
}
/// Read `plpgsql_error_funcname`.
pub fn plpgsql_error_funcname() -> Option<String> {
    PLPGSQL_ERROR_FUNCNAME.with(|f| f.borrow().clone())
}

/// Set the tracked `plpgsql_latest_lineno` (called by the parse with the
/// scanner's final value; reset to 1 at the start of each compile).
fn set_latest_lineno(lineno: i32) {
    PLPGSQL_LATEST_LINENO.with(|f| *f.borrow_mut() = lineno);
}
fn latest_lineno() -> i32 {
    PLPGSQL_LATEST_LINENO.with(|f| *f.borrow())
}

/// `plpgsql_compile_error_callback` (pl_comp.c) — the "near line N" fallback
/// the error_context_stack callback adds for any error raised during a compile.
/// Mirrors the callback's final `errcontext(...)`: applied once, and only if the
/// parse phase did not already attach the (transposed) "compilation of …" line.
fn add_compile_error_context(e: PgError) -> PgError {
    if e.context()
        .is_some_and(|c| c.contains("compilation of PL/pgSQL function"))
    {
        return e;
    }
    if let Some(funcname) = plpgsql_error_funcname() {
        return e.with_context(format!(
            "compilation of PL/pgSQL function \"{funcname}\" near line {}",
            latest_lineno()
        ));
    }
    e
}

/// Run a compile body under the `plpgsql_compile_error_callback` error-context
/// scope. Catches both the `Err(PgError)` channel (parse-phase faults) and the
/// `panic_any(PgError)` channel (`ereport_error` semantic faults — the trigtype
/// and return-type checks) so either way the "compilation of … near line N"
/// fallback is attached before the error propagates to the handler's
/// `catch_unwind`. A non-`PgError` panic is resumed unchanged.
fn with_compile_error_context<F>(f: F) -> PgResult<PLpgSQL_function>
where
    F: FnOnce() -> PgResult<PLpgSQL_function>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r.map_err(add_compile_error_context),
        Err(payload) => match payload.downcast::<PgError>() {
            Ok(e) => std::panic::panic_any(add_compile_error_context(*e)),
            Err(other) => std::panic::resume_unwind(other),
        },
    }
}

/// `ereport(ERROR, (errcode(code), errmsg(msg)))` — raise a structured error
/// from a compiler path that has a `()` return.  The SQLSTATE rides the
/// `PgError.sqlstate` field (shown only at verbose verbosity), exactly as the C
/// `errcode()`; it must never be concatenated into the message text.  Dispatched
/// over the `panic_any(PgError)` channel the handler's `catch_unwind` catches,
/// mirroring C's ereport longjmp.
fn ereport_error(code: SqlState, msg: String) -> ! {
    std::panic::panic_any(PgError::error(msg).with_sqlstate(code));
}

// ===========================================================================
// add_parameter_name + add_dummy_return
// ===========================================================================

/// `add_parameter_name` — add a name for a function parameter to the function's
/// namespace, checking for duplicates first.
fn add_parameter_name(itemtype: PLpgSQL_nsitem_type, itemno: i32, name: &str) {
    let dup = funcs::plpgsql_ns_top()
        .map(|top| funcs::plpgsql_ns_lookup(&top, true, name, None, None, None).is_some())
        .unwrap_or(false);
    if dup {
        ereport_error(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            format!("parameter name \"{name}\" used more than once"),
        );
    }
    funcs::plpgsql_ns_additem(itemtype, itemno, name);
}

/// `add_dummy_return` — add a dummy RETURN statement to the current function's
/// body so control may fall off the end without an explicit RETURN.
fn add_dummy_return() {
    // If the outer block has an EXCEPTION clause, or has a label, wrap it in a
    // new outer block so the added RETURN behaves correctly.
    let needs_wrap = curr_compile_with_action(|action| {
        action.exceptions.is_some() || action.label.is_some()
    });
    if needs_wrap {
        let stmtid = curr_compile_next_stmtid();
        set_curr_compile_field(|f| {
            let old_action = f.action.take().expect("function->action != NULL");
            let new_block = mem::boxed(PLpgSQL_stmt_block {
                cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_BLOCK,
                lineno: 0,
                stmtid,
                label: None,
                body: mem::vfrom([PLpgSQL_stmt::Block(old_action)]),
                n_initvars: 0,
                initvarnos: Vec::new(),
                exceptions: None,
            });
            f.action = Some(new_block);
        });
    }

    let needs_return = curr_compile_with_action(|action| {
        action.body.is_empty() || !matches!(action.body.last(), Some(PLpgSQL_stmt::Return(_)))
    });
    if needs_return {
        let stmtid = curr_compile_next_stmtid();
        let retvarno = curr_compile_field(|f| f.out_param_varno);
        let new_ret = PLpgSQL_stmt::Return(mem::boxed(PLpgSQL_stmt_return {
            cmd_type: PLpgSQL_stmt_type::PLPGSQL_STMT_RETURN,
            lineno: 0,
            stmtid,
            expr: None,
            retvarno,
        }));
        set_curr_compile_field(|f| {
            f.action
                .as_mut()
                .expect("function->action != NULL")
                .body
                .push(new_ret);
        });
    }
}

// ===========================================================================
// Scanner namespace-resolution callbacks (plpgsql_parse_word/dblword/tripword)
// ===========================================================================

/// `plpgsql_parse_word` — postparse a single word.  Resolves to a datum if it
/// names a known variable; otherwise returns the literal word.
pub fn plpgsql_parse_word(word1: &str, yytxt: &str, lookup: bool) -> comp_seams::WordResolution {
    use comp_seams::WordResolution;
    // We should not lookup variables in DECLARE sections.
    if lookup && plpgsql_identifier_lookup() == IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL {
        if let Some(top) = funcs::plpgsql_ns_top() {
            if let Some(item) = funcs::plpgsql_ns_lookup(&top, false, word1, None, None, None) {
                match item.itemtype {
                    PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR
                    | PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC => {
                        return WordResolution::Datum(PLwdatum {
                            datum: Some(item.itemno as u64),
                            ident: Some(mem::sdup(word1)),
                            quoted: yytxt.starts_with('"'),
                            idents: Vec::new(),
                        });
                    }
                    PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL => {
                        panic!("plpgsql_ns_lookup returned a label");
                    }
                }
            }
        }
    }

    WordResolution::Word(PLword {
        ident: mem::sdup(word1),
        quoted: yytxt.starts_with('"'),
    })
}

/// `plpgsql_parse_dblword` — same lookup for two dotted words `word1.word2`.
pub fn plpgsql_parse_dblword(word1: &str, word2: &str) -> comp_seams::CwordResolution {
    use comp_seams::CwordResolution;
    let idents = mem::vfrom([mem::sdup(word1), mem::sdup(word2)]);

    if plpgsql_identifier_lookup() != IdentifierLookup::IDENTIFIER_LOOKUP_DECLARE {
        if let Some(top) = funcs::plpgsql_ns_top() {
            let mut nnames: i32 = 0;
            if let Some(item) =
                funcs::plpgsql_ns_lookup(&top, false, word1, Some(word2), None, Some(&mut nnames))
            {
                match item.itemtype {
                    PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR => {
                        return CwordResolution::Datum(PLwdatum {
                            datum: Some(item.itemno as u64),
                            ident: None,
                            quoted: false,
                            idents,
                        });
                    }
                    PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC => {
                        let itemno = item.itemno;
                        let datum_dno = if nnames == 1 {
                            plpgsql_build_recfield(itemno, word2)
                        } else {
                            itemno
                        };
                        return CwordResolution::Datum(PLwdatum {
                            datum: Some(datum_dno as u64),
                            ident: None,
                            quoted: false,
                            idents,
                        });
                    }
                    PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL => {}
                }
            }
        }
    }

    CwordResolution::Cword(PLcword { idents })
}

/// `plpgsql_parse_tripword` — same lookup for three dotted words.
pub fn plpgsql_parse_tripword(
    word1: &str,
    word2: &str,
    word3: &str,
) -> comp_seams::CwordResolution {
    use comp_seams::CwordResolution;
    if plpgsql_identifier_lookup() != IdentifierLookup::IDENTIFIER_LOOKUP_DECLARE {
        if let Some(top) = funcs::plpgsql_ns_top() {
            let mut nnames: i32 = 0;
            if let Some(item) = funcs::plpgsql_ns_lookup(
                &top,
                false,
                word1,
                Some(word2),
                Some(word3),
                Some(&mut nnames),
            ) {
                if item.itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC {
                    let itemno = item.itemno;
                    let (datum_dno, idents) = if nnames == 1 {
                        let new = plpgsql_build_recfield(itemno, word2);
                        (new, mem::vfrom([mem::sdup(word1), mem::sdup(word2)]))
                    } else {
                        let new = plpgsql_build_recfield(itemno, word3);
                        (
                            new,
                            mem::vfrom([mem::sdup(word1), mem::sdup(word2), mem::sdup(word3)]),
                        )
                    };
                    return CwordResolution::Datum(PLwdatum {
                        datum: Some(datum_dno as u64),
                        ident: None,
                        quoted: false,
                        idents,
                    });
                }
            }
        }
    }

    CwordResolution::Cword(PLcword {
        idents: mem::vfrom([mem::sdup(word1), mem::sdup(word2), mem::sdup(word3)]),
    })
}

// ===========================================================================
// %TYPE / %ROWTYPE resolvers
// ===========================================================================

/// `plpgsql_parse_wordtype` — the scanner found `word%TYPE`.
pub fn plpgsql_parse_wordtype(ident: &str) -> PgResult<Box<PLpgSQL_type>> {
    if let Some(top) = funcs::plpgsql_ns_top() {
        if let Some(item) = funcs::plpgsql_ns_lookup(&top, false, ident, None, None, None) {
            match item.itemtype {
                PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR => {
                    return Ok(mem::boxed(datum_var_datatype(item.itemno)));
                }
                PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC => {
                    return Ok(mem::boxed(datum_rec_datatype(item.itemno)));
                }
                PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_LABEL => {}
            }
        }
    }
    Err(PgError::error(format!("variable \"{ident}\" does not exist"))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT))
}

/// `plpgsql_parse_cwordtype` — `%TYPE` for a block-qualified var or a column.
pub fn plpgsql_parse_cwordtype(idents: &[String]) -> PgResult<Box<PLpgSQL_type>> {
    if idents.len() == 2 {
        if let Some(top) = funcs::plpgsql_ns_top() {
            let mut nnames: i32 = 0;
            if let Some(item) = funcs::plpgsql_ns_lookup(
                &top,
                false,
                &idents[0],
                Some(&idents[1]),
                None,
                Some(&mut nnames),
            ) {
                if item.itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR {
                    return Ok(mem::boxed(datum_var_datatype(item.itemno)));
                } else if item.itemtype == PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC && nnames == 2 {
                    return Ok(mem::boxed(datum_rec_datatype(item.itemno)));
                }
            }
        }
    }
    // First word (or all-but-last words) could also be a table name; the last
    // ident is the column.  (C: makeRangeVar over a 2-name list uses the first
    // as the relname and the second as the field; for >2 names it strips the
    // last and resolves the rest as a qualified rel name.)
    let (rvnames, fldname): (&[String], &str) = if idents.len() == 2 {
        (&idents[0..1], idents[1].as_str())
    } else {
        // list_length(idents) > 2
        (&idents[..idents.len() - 1], idents[idents.len() - 1].as_str())
    };
    let rvname_refs: Vec<&str> = rvnames.iter().map(String::as_str).collect();
    // C: RangeVarGetRelid(relvar, NoLock, false) — the relation must exist.
    let class_oid = seam::qualified_relname_get_relid(&rvname_refs, false)?;
    // relvar->relname is the last component of the rel-name portion (C reads
    // relvar->relname for the diagnostic).
    let relname = rvname_refs.last().copied().unwrap_or("");
    seam::column_atttype(class_oid, relname, fldname)
}

/// `plpgsql_parse_wordrowtype` — the scanner found `word%ROWTYPE`.
pub fn plpgsql_parse_wordrowtype(ident: &str) -> PgResult<Box<PLpgSQL_type>> {
    // C: RelnameGetRelid(ident); a missing relation is "relation does not exist".
    let class_oid = seam::relname_get_relid(ident)?;
    if !oid_is_valid(class_oid) {
        return Err(PgError::error(format!("relation \"{ident}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE));
    }
    let typ_oid = seam::get_rel_type_id(class_oid);
    if !oid_is_valid(typ_oid) {
        return Err(
            PgError::error(format!("relation \"{ident}\" does not have a composite type"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        );
    }
    Ok(plpgsql_build_datatype_internal(typ_oid, -1, INVALID_OID, None))
}

/// `plpgsql_parse_cwordrowtype` — `compositeword%ROWTYPE` (qualified table).
pub fn plpgsql_parse_cwordrowtype(idents: &[String]) -> PgResult<Box<PLpgSQL_type>> {
    // Qualified table name -> makeRangeVarFromNameList -> RangeVarGetRelid
    // (NoLock, missing_ok=false) -> get_rel_type_id.
    let idents_refs: Vec<&str> = idents.iter().map(String::as_str).collect();
    let class_oid = seam::qualified_relname_get_relid(&idents_refs, false)?;
    // relvar->relname is the last component (the diagnostic's relation name).
    let relname = idents.last().map(String::as_str).unwrap_or("");
    let typ_oid = seam::get_rel_type_id(class_oid);
    if !oid_is_valid(typ_oid) {
        return Err(
            PgError::error(format!("relation \"{relname}\" does not have a composite type"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        );
    }
    Ok(plpgsql_build_datatype_internal(typ_oid, -1, INVALID_OID, None))
}

// ===========================================================================
// Datum-array builders
// ===========================================================================

/// `plpgsql_build_variable` — build a datum-array entry of a given datatype.
/// Returns an owned `PLpgSQL_variable` snapshot (dtype/dno/identity).
pub fn plpgsql_build_variable(
    refname: &str,
    lineno: i32,
    dtype: Box<PLpgSQL_type>,
    add2namespace: bool,
) -> PLpgSQL_variable {
    match dtype.ttype {
        PLpgSQL_type_type::PLPGSQL_TTYPE_SCALAR => {
            let var = PLpgSQL_var {
                dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR,
                dno: 0,
                refname: mem::sdup(refname),
                lineno,
                isconst: false,
                notnull: false,
                default_val: None,
                datatype: Some(dtype),
                cursor_explicit_expr: None,
                cursor_explicit_argrow: 0,
                cursor_options: 0,
                value: Datum::null(),
                isnull: true,
                freeval: false,
                value_byref: None,
                promise: PLpgSQL_promise_type::PLPGSQL_PROMISE_NONE,
            };
            let dno = plpgsql_adddatum(PLpgSQL_datum::Var(mem::boxed(var)));
            if add2namespace {
                funcs::plpgsql_ns_additem(PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR, dno, refname);
            }
            PLpgSQL_variable {
                dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR,
                dno,
                refname: mem::sdup(refname),
                lineno,
                isconst: false,
                notnull: false,
                default_val: None,
            }
        }
        PLpgSQL_type_type::PLPGSQL_TTYPE_REC => {
            let typoid = dtype.typoid;
            let dno = plpgsql_build_record(refname, lineno, Some(dtype), typoid, add2namespace);
            PLpgSQL_variable {
                dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_REC,
                dno,
                refname: mem::sdup(refname),
                lineno,
                isconst: false,
                notnull: false,
                default_val: None,
            }
        }
        PLpgSQL_type_type::PLPGSQL_TTYPE_PSEUDO => {
            ereport_error(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "variable \"{refname}\" has pseudo-type {}",
                    seam::format_type_be(dtype.typoid)
                ),
            );
        }
    }
}

/// `plpgsql_build_record` — build an empty named record variable.  Returns dno.
pub fn plpgsql_build_record(
    refname: &str,
    lineno: i32,
    dtype: Option<Box<PLpgSQL_type>>,
    rectypeid: Oid,
    add2namespace: bool,
) -> i32 {
    let rec = PLpgSQL_rec {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_REC,
        dno: 0,
        refname: mem::sdup(refname),
        lineno,
        isconst: false,
        notnull: false,
        default_val: None,
        datatype: dtype,
        rectypeid,
        firstfield: -1,
        erh: None,
    };
    let dno = plpgsql_adddatum(PLpgSQL_datum::Rec(mem::boxed(rec)));
    if add2namespace {
        funcs::plpgsql_ns_additem(PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC, dno, refname);
    }
    dno
}

/// `build_row_from_vars` — build a row-variable from component variables (dnos).
///
/// Ports the full `pl_comp.c:1838` body: per-member `typoid`/`typmod`/`typcoll`
/// extraction (validating the member dtype) and the `TupleDescInitEntry` /
/// `TupleDescInitEntryCollation` rowtupdesc build.  The rowtupdesc itself is the
/// genuine composite `TupleDesc`; in `types-plpgsql` that field is an opaque
/// handle with no in-repo constructor, so the actual `CreateTemplateTupleDesc`
/// build is a tupdesc-owner callee reached via [`seam::build_row_tupledesc`]
/// (mirror-PG-and-panic until the handle model unifies).  The member-type
/// extraction is done here regardless, matching the C control flow.
fn build_row_from_vars(vars: &[i32]) -> PgResult<PLpgSQL_row> {
    let numvars = vars.len() as i32;
    let mut fieldnames: Vec<String> = mem::vwithcap(numvars as usize);
    let mut varnos: Vec<i32> = mem::vwithcap(numvars as usize);
    let mut members: Vec<RowMember> = mem::vwithcap(numvars as usize);

    for &var_dno in vars {
        // Member vars of a row should never be const.
        debug_assert!(!datum_variable_isconst(var_dno));

        let (typoid, typmod, typcoll) = match datum_dtype_of(var_dno) {
            PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE => {
                let dt = datum_var_datatype(var_dno);
                (dt.typoid, dt.atttypmod, dt.collation)
            }
            PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
                // shouldn't need to revalidate rectypeid already...
                // composite types have no collation; typmod unknown.
                (datum_rec_rectypeid(var_dno), -1, INVALID_OID)
            }
            other => panic!("unrecognized dtype: {other:?}"),
        };

        let refname = datum_variable_refname(var_dno);
        members.push(RowMember {
            attname: mem::sdup(&refname),
            typoid,
            typmod,
            typcoll,
        });
        mem::vpush(&mut fieldnames, mem::sdup(&refname));
        mem::vpush(&mut varnos, var_dno);
    }

    let rowtupdesc = seam::build_row_tupledesc(&members)?;

    Ok(PLpgSQL_row {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW,
        dno: 0,
        refname: mem::sdup("(unnamed row)"),
        lineno: -1,
        isconst: false,
        notnull: false,
        default_val: None,
        rowtupdesc,
        nfields: numvars,
        fieldnames,
        varnos,
    })
}

/// One member column of a row's `rowtupdesc` (the per-member type facts the
/// `TupleDescInitEntry` / `TupleDescInitEntryCollation` calls consume).
pub struct RowMember {
    pub attname: String,
    pub typoid: Oid,
    pub typmod: i32,
    pub typcoll: Oid,
}

/// `plpgsql_build_recfield` — build (or reuse) a RECFIELD datum for the named
/// field of a record variable.  Returns the recfield datum's dno.
pub fn plpgsql_build_recfield(rec_dno: i32, fldname: &str) -> i32 {
    let mut i = datum_rec_firstfield(rec_dno);
    while i >= 0 {
        debug_assert_eq!(datum_dtype_of(i), PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD);
        debug_assert_eq!(datum_recfield_recparentno(i), rec_dno);
        if datum_recfield_fieldname(i) == fldname {
            return i;
        }
        i = datum_recfield_nextfield(i);
    }

    let recfield = PLpgSQL_recfield {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD,
        dno: 0,
        fieldname: mem::sdup(fldname),
        recparentno: rec_dno,
        nextfield: -1,
        rectupledescid: INVALID_TUPLEDESC_IDENTIFIER,
        finfo: ExpandedRecordFieldInfo::default(),
    };
    let dno = plpgsql_adddatum(PLpgSQL_datum::Recfield(mem::boxed(recfield)));

    let old_first = datum_rec_firstfield(rec_dno);
    set_datum_recfield_nextfield(dno, old_first);
    set_datum_rec_firstfield(rec_dno, dno);
    dno
}

/// `plpgsql_build_datatype` — build a `PLpgSQL_type` for a type OID.
pub fn plpgsql_build_datatype_internal(
    type_oid: Oid,
    typmod: i32,
    collation: Oid,
    origtypname: Option<TypeName>,
) -> Box<PLpgSQL_type> {
    let form = seam::pg_type_form(type_oid);
    build_datatype(&form, typmod, collation, origtypname)
}

/// `build_datatype` — make a `PLpgSQL_type` from a `pg_type` row.
fn build_datatype(
    form: &types_tuple::pg_type::FormData_pg_type,
    typmod: i32,
    collation: Oid,
    origtypname: Option<TypeName>,
) -> Box<PLpgSQL_type> {
    if !form.typisdefined {
        ereport_error(
            ERRCODE_UNDEFINED_OBJECT,
            format!("type \"{}\" is only a shell", seam::typname_string(form)),
        );
    }

    let ttype = match form.typtype {
        TYPTYPE_BASE | TYPTYPE_ENUM | TYPTYPE_RANGE | TYPTYPE_MULTIRANGE => {
            PLpgSQL_type_type::PLPGSQL_TTYPE_SCALAR
        }
        TYPTYPE_COMPOSITE => PLpgSQL_type_type::PLPGSQL_TTYPE_REC,
        TYPTYPE_DOMAIN => {
            if seam::type_is_rowtype(form.typbasetype) {
                PLpgSQL_type_type::PLPGSQL_TTYPE_REC
            } else {
                PLpgSQL_type_type::PLPGSQL_TTYPE_SCALAR
            }
        }
        TYPTYPE_PSEUDO => {
            if form.oid == RECORDOID {
                PLpgSQL_type_type::PLPGSQL_TTYPE_REC
            } else {
                PLpgSQL_type_type::PLPGSQL_TTYPE_PSEUDO
            }
        }
        other => panic!("unrecognized typtype: {other}"),
    };

    let mut typ_collation = form.typcollation;
    if oid_is_valid(collation) && oid_is_valid(typ_collation) {
        typ_collation = collation;
    }

    // Detect if type is true array, or domain thereof.
    let typisarray = if form.typtype == TYPTYPE_BASE {
        seam::is_true_array_type(form) && form.typstorage != TYPSTORAGE_PLAIN
    } else if form.typtype == TYPTYPE_DOMAIN {
        form.typlen == -1
            && form.typstorage != TYPSTORAGE_PLAIN
            && oid_is_valid(seam::get_base_element_type(form.typbasetype))
    } else {
        false
    };

    // If it's a named composite type (or domain over one), find the typcache
    // entry and record the current tupdesc ID, so we can detect changes
    // (including drops). (C build_datatype: lookup_type_cache(typoid,
    // TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO), chaining to the domain
    // base for a domain; raise "type is not composite" when tupDesc is NULL.)
    let (tcache, tupdesc_id) =
        if ttype == PLpgSQL_type_type::PLPGSQL_TTYPE_REC && form.oid != RECORDOID {
            seam::composite_tupdesc_id(form.oid)
        } else {
            (None, 0)
        };

    mem::boxed(PLpgSQL_type {
        typname: seam::typname_string(form),
        typoid: form.oid,
        ttype,
        typlen: form.typlen,
        typbyval: form.typbyval,
        typtype: form.typtype as u8,
        collation: typ_collation,
        typisarray,
        atttypmod: typmod,
        origtypname,
        tcache,
        tupdesc_id,
    })
}

/// `plpgsql_build_datatype_arrayof` — build the array type over `dtype`.
pub fn plpgsql_build_datatype_arrayof(dtype: Box<PLpgSQL_type>) -> PgResult<Box<PLpgSQL_type>> {
    if dtype.typisarray {
        return Ok(dtype);
    }
    let array_typeid = seam::get_array_type(dtype.typoid);
    if !oid_is_valid(array_typeid) {
        return Err(PgError::error(format!(
            "could not find array type for data type {}",
            seam::format_type_be(dtype.typoid)
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(plpgsql_build_datatype_internal(
        array_typeid,
        dtype.atttypmod,
        dtype.collation,
        None,
    ))
}

// ===========================================================================
// Error-condition lookups
// ===========================================================================

/// `plpgsql_recognize_err_condition` — check a condition name and translate it
/// to SQLSTATE.  Returns the first match.
pub fn plpgsql_recognize_err_condition(condname: &str, allow_sqlstate: bool) -> PgResult<i32> {
    if allow_sqlstate
        && condname.len() == 5
        && condname
            .bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    {
        let b = condname.as_bytes();
        return Ok(make_sqlstate(b[0], b[1], b[2], b[3], b[4]));
    }

    if let Some(sqlerrstate) = exception_label_lookup(condname) {
        return Ok(sqlerrstate);
    }
    Err(
        PgError::error(format!("unrecognized exception condition \"{condname}\""))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

/// `plpgsql_parse_err_condition` — `PLpgSQL_condition` entry(s) for a name.
pub fn plpgsql_parse_err_condition(condname: &str) -> PgResult<PLpgSQL_condition> {
    // XXX Eventually we will want to look for user-defined exception names here.
    if condname == "others" {
        return Ok(PLpgSQL_condition {
            sqlerrstate: PLPGSQL_OTHERS,
            condname: mem::sdup(condname),
            next: None,
        });
    }

    let mut head: Option<Box<PLpgSQL_condition>> = None;
    for sqlerrstate in exception_label_lookup_all(condname) {
        head = Some(mem::boxed(PLpgSQL_condition {
            sqlerrstate,
            condname: mem::sdup(condname),
            next: head.take(),
        }));
    }

    match head {
        Some(p) => Ok(*p),
        None => Err(
            PgError::error(format!("unrecognized exception condition \"{condname}\""))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ),
    }
}

// ===========================================================================
// Datum-list bookkeeping
// ===========================================================================

/// `plpgsql_start_datums` — initialize the datum list at compile startup.
pub fn plpgsql_start_datums() {
    DATUMS_ALLOC.with(|a| *a.borrow_mut() = 128);
    PLPGSQL_N_DATUMS.with(|n| *n.borrow_mut() = 0);
    PLPGSQL_DATUMS.with(|d| {
        let mut v = d.borrow_mut();
        v.clear();
        mem::vreserve(&mut v, 128);
    });
    DATUMS_LAST.with(|l| *l.borrow_mut() = 0);
}

/// `plpgsql_adddatum` — add a datum to the compiler's datum list, set its dno.
pub fn plpgsql_adddatum(newdatum: PLpgSQL_datum) -> i32 {
    let dno = PLPGSQL_N_DATUMS.with(|n| *n.borrow());
    let mut newdatum = newdatum;
    set_datum_head_dno(&mut newdatum, dno);
    PLPGSQL_DATUMS.with(|d| mem::vpush(&mut d.borrow_mut(), newdatum));
    PLPGSQL_N_DATUMS.with(|n| *n.borrow_mut() = dno + 1);
    DATUMS_ALLOC.with(|a| {
        let mut alloc = a.borrow_mut();
        if dno + 1 > *alloc {
            *alloc *= 2;
        }
    });
    dno
}

/// `plpgsql_finish_datums` — copy completed datum info into the function struct.
pub fn plpgsql_finish_datums() {
    let n = PLPGSQL_N_DATUMS.with(|n| *n.borrow());
    let datums: Vec<PLpgSQL_datum> = PLPGSQL_DATUMS.with(|d| d.borrow().clone());

    let mut copiable_size: Size = 0;
    for d in &datums {
        match d {
            PLpgSQL_datum::Var(_) => {
                copiable_size += maxalign(core::mem::size_of::<PLpgSQL_var>());
            }
            PLpgSQL_datum::Rec(_) => {
                copiable_size += maxalign(core::mem::size_of::<PLpgSQL_rec>());
            }
            _ => {}
        }
    }

    set_curr_compile_field(|f| {
        f.ndatums = n;
        f.datums = datums.clone();
        f.copiable_size = copiable_size;
    });
}

/// `plpgsql_add_initdatums` — make an array of the dnos of all initializable
/// datums created since the last call.  If `collect` is false, just forget the
/// recent datums (the `varnos == NULL` path).
pub fn plpgsql_add_initdatums(collect: bool) -> Vec<i32> {
    let datums_last = DATUMS_LAST.with(|l| *l.borrow());
    let n_datums = PLPGSQL_N_DATUMS.with(|n| *n.borrow());
    let mut varnos: Vec<i32> = Vec::new();

    if collect {
        PLPGSQL_DATUMS.with(|d| {
            let d = d.borrow();
            for i in (datums_last as usize)..(n_datums as usize) {
                match datum_head_dtype(&d[i]) {
                    PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
                    | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
                        mem::vpush(&mut varnos, datum_head_dno(&d[i]));
                    }
                    _ => {}
                }
            }
        });
    }

    DATUMS_LAST.with(|l| *l.borrow_mut() = n_datums);
    varnos
}

// ===========================================================================
// plpgsql_curr_compile accessors + datum-array element accessors
// ===========================================================================

fn set_curr_compile(func: PLpgSQL_function) {
    PLPGSQL_CURR_COMPILE.with(|c| *c.borrow_mut() = Some(func));
}

fn take_curr_compile() -> PLpgSQL_function {
    PLPGSQL_CURR_COMPILE.with(|c| c.borrow_mut().take().expect("plpgsql_curr_compile set"))
}

/// Read a field of `plpgsql_curr_compile`.
pub fn curr_compile_field<T>(f: impl FnOnce(&PLpgSQL_function) -> T) -> T {
    PLPGSQL_CURR_COMPILE.with(|c| {
        let b = c.borrow();
        f(b.as_ref().expect("plpgsql_curr_compile set"))
    })
}

/// Mutate `plpgsql_curr_compile`.
pub fn set_curr_compile_field(f: impl FnOnce(&mut PLpgSQL_function)) {
    PLPGSQL_CURR_COMPILE.with(|c| {
        let mut b = c.borrow_mut();
        f(b.as_mut().expect("plpgsql_curr_compile set"));
    });
}

fn curr_compile_with_action<T>(f: impl FnOnce(&PLpgSQL_stmt_block) -> T) -> T {
    curr_compile_field(|func| f(func.action.as_deref().expect("function->action != NULL")))
}

/// `++plpgsql_curr_compile->nstatements`.
pub fn curr_compile_next_stmtid() -> u32 {
    PLPGSQL_CURR_COMPILE.with(|c| {
        let mut b = c.borrow_mut();
        let f = b.as_mut().expect("plpgsql_curr_compile set");
        f.nstatements += 1;
        f.nstatements
    })
}

/// `plpgsql_curr_compile != NULL`.
pub fn curr_compile_in_progress() -> bool {
    PLPGSQL_CURR_COMPILE.with(|c| c.borrow().is_some())
}

/// `plpgsql_nDatums`.
pub fn plpgsql_ndatums() -> i32 {
    PLPGSQL_N_DATUMS.with(|n| *n.borrow())
}

// --- datum-array element accessors -----------------------------------------

fn set_datum_head_dno(datum: &mut PLpgSQL_datum, dno: i32) {
    match datum {
        PLpgSQL_datum::Var(v) => v.dno = dno,
        PLpgSQL_datum::Row(r) => r.dno = dno,
        PLpgSQL_datum::Rec(r) => r.dno = dno,
        PLpgSQL_datum::Recfield(r) => r.dno = dno,
    }
}

fn datum_head_dno(datum: &PLpgSQL_datum) -> i32 {
    match datum {
        PLpgSQL_datum::Var(v) => v.dno,
        PLpgSQL_datum::Row(r) => r.dno,
        PLpgSQL_datum::Rec(r) => r.dno,
        PLpgSQL_datum::Recfield(r) => r.dno,
    }
}

fn datum_head_dtype(datum: &PLpgSQL_datum) -> PLpgSQL_datum_type {
    match datum {
        PLpgSQL_datum::Var(v) => v.dtype,
        PLpgSQL_datum::Row(r) => r.dtype,
        PLpgSQL_datum::Rec(r) => r.dtype,
        PLpgSQL_datum::Recfield(r) => r.dtype,
    }
}

/// `plpgsql_Datums[dno]->dtype`.
pub fn datum_dtype_of(dno: i32) -> PLpgSQL_datum_type {
    PLPGSQL_DATUMS.with(|d| datum_head_dtype(&d.borrow()[dno as usize]))
}

/// `(PLpgSQL_variable *) plpgsql_Datums[dno]` — owned snapshot of the header.
pub fn datum_as_variable(dno: i32) -> PLpgSQL_variable {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => PLpgSQL_variable {
                dtype: v.dtype,
                dno: v.dno,
                refname: v.refname.clone(),
                lineno: v.lineno,
                isconst: v.isconst,
                notnull: v.notnull,
                default_val: v.default_val.clone(),
            },
            PLpgSQL_datum::Row(r) => PLpgSQL_variable {
                dtype: r.dtype,
                dno: r.dno,
                refname: r.refname.clone(),
                lineno: r.lineno,
                isconst: false,
                notnull: false,
                default_val: None,
            },
            PLpgSQL_datum::Rec(r) => PLpgSQL_variable {
                dtype: r.dtype,
                dno: r.dno,
                refname: r.refname.clone(),
                lineno: r.lineno,
                isconst: false,
                notnull: false,
                default_val: None,
            },
            PLpgSQL_datum::Recfield(_) => panic!("datum {dno} is a RECFIELD, not a variable"),
        }
    })
}

/// `((PLpgSQL_var *) plpgsql_Datums[dno])->datatype->typoid`.
pub fn var_datatype_typoid(dno: i32) -> Oid {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.datatype.as_ref().map(|t| t.typoid).unwrap_or(0),
            _ => panic!("datum {dno} is not a PLpgSQL_var"),
        }
    })
}

/// `((PLpgSQL_var *) plpgsql_Datums[dno])->cursor_explicit_expr != NULL`.
pub fn var_has_explicit_expr(dno: i32) -> bool {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.cursor_explicit_expr.is_some(),
            _ => panic!("datum {dno} is not a PLpgSQL_var"),
        }
    })
}

/// `((PLpgSQL_var *) plpgsql_Datums[dno])->cursor_explicit_argrow`.
pub fn var_cursor_explicit_argrow(dno: i32) -> i32 {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.cursor_explicit_argrow,
            _ => panic!("datum {dno} is not a PLpgSQL_var"),
        }
    })
}

/// `((PLpgSQL_variable *) plpgsql_Datums[dno])->refname`.
pub fn var_refname(dno: i32) -> String {
    datum_variable_refname(dno)
}

/// `((PLpgSQL_row *) plpgsql_Datums[argrow])->fieldnames`.
pub fn cursor_argrow_fieldnames(argrow: i32) -> Vec<String> {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[argrow as usize] {
            PLpgSQL_datum::Row(r) => r.fieldnames.clone(),
            _ => panic!("datum {argrow} is not a PLpgSQL_row"),
        }
    })
}

/// `var->isconst`/`notnull`/`default_val =` (post-declaration assignments).
pub fn plpgsql_var_set_decl_props(
    dno: i32,
    isconst: bool,
    notnull: bool,
    default_val: Option<Box<PLpgSQL_expr>>,
) {
    PLPGSQL_DATUMS.with(|d| {
        let mut b = d.borrow_mut();
        match &mut b[dno as usize] {
            PLpgSQL_datum::Var(v) => {
                v.isconst = isconst;
                v.notnull = notnull;
                v.default_val = default_val;
            }
            PLpgSQL_datum::Rec(r) => {
                r.isconst = isconst;
                r.notnull = notnull;
                r.default_val = default_val;
            }
            _ => panic!("datum {dno} is not a scalar/record variable"),
        }
    });
}

/// `mark_expr_as_assignment_source(var->default_val, (PLpgSQL_datum *) var)`.
pub fn mark_var_default_as_assignment_source(dno: i32) {
    let mut default_val = PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.default_val.clone(),
            PLpgSQL_datum::Rec(r) => r.default_val.clone(),
            _ => None,
        }
    });
    if let Some(expr) = default_val.as_mut() {
        mark_expr_as_assignment_source(expr, dno);
        PLPGSQL_DATUMS.with(|d| {
            let mut b = d.borrow_mut();
            match &mut b[dno as usize] {
                PLpgSQL_datum::Var(v) => v.default_val = Some(expr.clone()),
                PLpgSQL_datum::Rec(r) => r.default_val = Some(expr.clone()),
                _ => {}
            }
        });
    }
}

/// `check_assignable(plpgsql_Datums[dno], location)` — error if not assignable.
pub fn check_assignable(dno: i32, location: i32) {
    let dtype = datum_dtype_of(dno);
    match dtype {
        PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR
        | PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE
        | PLpgSQL_datum_type::PLPGSQL_DTYPE_REC => {
            let (isconst, refname) = PLPGSQL_DATUMS.with(|d| {
                let b = d.borrow();
                match &b[dno as usize] {
                    PLpgSQL_datum::Var(v) => (v.isconst, v.refname.clone()),
                    PLpgSQL_datum::Rec(r) => (r.isconst, r.refname.clone()),
                    _ => unreachable!("dtype matched VAR/PROMISE/REC"),
                }
            });
            if isconst {
                // C `parser_errposition(location)` rides the separate
                // position field (shown only at verbose verbosity), never the
                // message text. The scanner needed to translate `location` to a
                // byte position is not threaded into this path, so the position
                // hint is omitted; the message text is what the regress diff
                // checks.
                let _ = location;
                ereport_error(
                    ERRCODE_ERROR_IN_ASSIGNMENT,
                    format!("variable \"{refname}\" is declared CONSTANT"),
                );
            }
        }
        PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW => {}
        PLpgSQL_datum_type::PLPGSQL_DTYPE_RECFIELD => {
            let recparentno = PLPGSQL_DATUMS.with(|d| {
                let b = d.borrow();
                match &b[dno as usize] {
                    PLpgSQL_datum::Recfield(r) => r.recparentno,
                    _ => unreachable!("dtype matched RECFIELD"),
                }
            });
            check_assignable(recparentno, location);
        }
    }
}

/// `mark_expr_as_assignment_source(expr, plpgsql_Datums[target_dno])`.
pub fn mark_expr_as_assignment_source(expr: &mut PLpgSQL_expr, target_dno: i32) {
    if datum_dtype_of(target_dno) == PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR {
        expr.target_param = target_dno;
        expr.target_is_local = true;
    } else {
        expr.target_param = -1;
        expr.target_is_local = false;
    }
}

/// PERFORM's source rewrite: overwrite the leading "PERFORM" of `expr.query`
/// with " SELECT" and left-justify (the `stmt_perform` grammar action).
pub fn perform_rewrite_query(expr: &mut PLpgSQL_expr) {
    // The query begins with "PERFORM"; C overwrites the keyword in place with
    // a leading space + "SELECT" then advances past the leading whitespace.
    // "PERFORM" (7) -> " SELECT" (7) keeps the byte length identical.
    const PERFORM: &str = "PERFORM";
    const SELECT: &str = " SELECT";
    if let Some(pos) = ascii_ci_find(&expr.query, PERFORM) {
        let mut bytes = expr.query.clone().into_bytes();
        bytes[pos..pos + PERFORM.len()].copy_from_slice(SELECT.as_bytes());
        expr.query = String::from_utf8(bytes).expect("ascii overwrite stays valid utf8");
    }
}

/// Case-insensitive byte search for the first occurrence of `needle` in `hay`.
fn ascii_ci_find(hay: &str, needle: &str) -> Option<usize> {
    let h = hay.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || h.len() < n.len() {
        return None;
    }
    'outer: for i in 0..=(h.len() - n.len()) {
        for j in 0..n.len() {
            if h[i + j].to_ascii_uppercase() != n[j].to_ascii_uppercase() {
                continue 'outer;
            }
        }
        return Some(i);
    }
    None
}

// --- private datum readers (pl_comp.c reads on plpgsql_Datums[]) -------------

fn datum_var_datatype(dno: i32) -> PLpgSQL_type {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => (**v.datatype.as_ref().expect("var->datatype")).clone(),
            _ => panic!("datum {dno} is not a PLpgSQL_var"),
        }
    })
}

fn datum_rec_datatype(dno: i32) -> PLpgSQL_type {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Rec(r) => (**r.datatype.as_ref().expect("rec->datatype")).clone(),
            _ => panic!("datum {dno} is not a PLpgSQL_rec"),
        }
    })
}

fn datum_rec_rectypeid(dno: i32) -> Oid {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Rec(r) => r.rectypeid,
            _ => panic!("datum {dno} is not a PLpgSQL_rec"),
        }
    })
}

fn datum_rec_firstfield(dno: i32) -> i32 {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Rec(r) => r.firstfield,
            _ => panic!("datum {dno} is not a PLpgSQL_rec"),
        }
    })
}

/// Build one of the DML/event-trigger promise variables (`build_trigger_promise`
/// in `plpgsql_compile_callback`): build a scalar var of the given type, then
/// flip the datum to a PROMISE with the given promise kind.
fn build_trigger_promise(name: &str, type_oid: Oid, collation: Oid, promise: PLpgSQL_promise_type) {
    let dt = plpgsql_build_datatype_internal(type_oid, -1, collation, None);
    let var = plpgsql_build_variable(name, 0, dt, true);
    debug_assert_eq!(var.dtype, PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR);
    let dno = var.dno;
    PLPGSQL_DATUMS.with(|d| {
        let mut b = d.borrow_mut();
        match &mut b[dno as usize] {
            PLpgSQL_datum::Var(v) => {
                v.dtype = PLpgSQL_datum_type::PLPGSQL_DTYPE_PROMISE;
                v.promise = promise;
            }
            _ => panic!("trigger promise datum {dno} is not a PLpgSQL_var"),
        }
    });
}

/// The DML-trigger arm of `plpgsql_compile_callback` (sets the NEW/OLD records
/// and the `tg_*` promise variables).  `pronargs` must be 0 for a trigger.
fn compile_dml_trigger_setup(pronargs: i32) {
    plpgsql_start_datums();

    set_curr_compile_field(|f| {
        f.fn_rettype = INVALID_OID;
        f.fn_retbyval = false;
        f.fn_retistuple = true;
        f.fn_retisdomain = false;
        f.fn_retset = false;
    });

    if pronargs != 0 {
        std::panic::panic_any(
            PgError::error("trigger functions cannot have declared arguments")
                .with_sqlstate(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .with_hint(
                    "The arguments of the trigger can be accessed through TG_NARGS and TG_ARGV instead.",
                ),
        );
    }

    let rec = plpgsql_build_record("new", 0, None, RECORDOID, true);
    set_curr_compile_field(|f| f.new_varno = rec);
    let rec = plpgsql_build_record("old", 0, None, RECORDOID, true);
    set_curr_compile_field(|f| f.old_varno = rec);

    let coll = curr_compile_field(|f| f.fn_input_collation);
    use PLpgSQL_promise_type::*;
    build_trigger_promise("tg_name", NAMEOID, coll, PLPGSQL_PROMISE_TG_NAME);
    build_trigger_promise("tg_when", TEXTOID, coll, PLPGSQL_PROMISE_TG_WHEN);
    build_trigger_promise("tg_level", TEXTOID, coll, PLPGSQL_PROMISE_TG_LEVEL);
    build_trigger_promise("tg_op", TEXTOID, coll, PLPGSQL_PROMISE_TG_OP);
    build_trigger_promise("tg_relid", OIDOID, INVALID_OID, PLPGSQL_PROMISE_TG_RELID);
    build_trigger_promise("tg_relname", NAMEOID, coll, PLPGSQL_PROMISE_TG_TABLE_NAME);
    build_trigger_promise("tg_table_name", NAMEOID, coll, PLPGSQL_PROMISE_TG_TABLE_NAME);
    build_trigger_promise("tg_table_schema", NAMEOID, coll, PLPGSQL_PROMISE_TG_TABLE_SCHEMA);
    build_trigger_promise("tg_nargs", INT4OID, INVALID_OID, PLPGSQL_PROMISE_TG_NARGS);
    build_trigger_promise("tg_argv", TEXTARRAYOID, coll, PLPGSQL_PROMISE_TG_ARGV);
}

/// The event-trigger arm of `plpgsql_compile_callback`.
fn compile_event_trigger_setup(pronargs: i32) {
    plpgsql_start_datums();

    set_curr_compile_field(|f| {
        f.fn_rettype = VOIDOID;
        f.fn_retbyval = false;
        f.fn_retistuple = true;
        f.fn_retisdomain = false;
        f.fn_retset = false;
    });

    if pronargs != 0 {
        ereport_error(
            ERRCODE_INVALID_FUNCTION_DEFINITION,
            "event trigger functions cannot have declared arguments".to_string(),
        );
    }

    let coll = curr_compile_field(|f| f.fn_input_collation);
    use PLpgSQL_promise_type::*;
    build_trigger_promise("tg_event", TEXTOID, coll, PLPGSQL_PROMISE_TG_EVENT);
    build_trigger_promise("tg_tag", TEXTOID, coll, PLPGSQL_PROMISE_TG_TAG);
}

fn set_datum_rec_firstfield(dno: i32, val: i32) {
    PLPGSQL_DATUMS.with(|d| {
        let mut b = d.borrow_mut();
        match &mut b[dno as usize] {
            PLpgSQL_datum::Rec(r) => r.firstfield = val,
            _ => panic!("datum {dno} is not a PLpgSQL_rec"),
        }
    });
}

fn datum_recfield_recparentno(dno: i32) -> i32 {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Recfield(r) => r.recparentno,
            _ => panic!("datum {dno} is not a PLpgSQL_recfield"),
        }
    })
}

fn datum_recfield_nextfield(dno: i32) -> i32 {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Recfield(r) => r.nextfield,
            _ => panic!("datum {dno} is not a PLpgSQL_recfield"),
        }
    })
}

fn set_datum_recfield_nextfield(dno: i32, val: i32) {
    PLPGSQL_DATUMS.with(|d| {
        let mut b = d.borrow_mut();
        match &mut b[dno as usize] {
            PLpgSQL_datum::Recfield(r) => r.nextfield = val,
            _ => panic!("datum {dno} is not a PLpgSQL_recfield"),
        }
    });
}

fn datum_recfield_fieldname(dno: i32) -> String {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Recfield(r) => r.fieldname.clone(),
            _ => panic!("datum {dno} is not a PLpgSQL_recfield"),
        }
    })
}

fn datum_variable_refname(dno: i32) -> String {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.refname.clone(),
            PLpgSQL_datum::Row(r) => r.refname.clone(),
            PLpgSQL_datum::Rec(r) => r.refname.clone(),
            PLpgSQL_datum::Recfield(r) => r.fieldname.clone(),
        }
    })
}

fn datum_variable_isconst(dno: i32) -> bool {
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => v.isconst,
            PLpgSQL_datum::Row(r) => r.isconst,
            PLpgSQL_datum::Rec(r) => r.isconst,
            PLpgSQL_datum::Recfield(_) => false,
        }
    })
}

// ===========================================================================
// Small helpers
// ===========================================================================

/// `MAKE_SQLSTATE(a,b,c,d,e)` (utils/errcodes.h) — pack 5 chars into an int.
fn make_sqlstate(a: u8, b: u8, c: u8, d: u8, e: u8) -> i32 {
    fn enc(ch: u8) -> i32 {
        ((ch as i32) - ('0' as i32)) & 0x3F
    }
    enc(a) + (enc(b) << 6) + (enc(c) << 12) + (enc(d) << 18) + (enc(e) << 24)
}

/// `MAXALIGN(len)` — round up to MAXIMUM_ALIGNOF (8 on LP64).
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// One entry of the exception-label table (`exception_label_map`, generated
/// from `plerrcodes.h`).  Looked up by condition name.
mod errcodes;

/// `exception_label_lookup(condname)` — first SQLSTATE for the condition name.
fn exception_label_lookup(condname: &str) -> Option<i32> {
    errcodes::EXCEPTION_LABEL_MAP
        .iter()
        .find(|(label, _)| *label == condname)
        .map(|(_, code)| *code)
}

/// All SQLSTATEs the condition name maps to (a few names cover several codes).
fn exception_label_lookup_all(condname: &str) -> Vec<i32> {
    errcodes::EXCEPTION_LABEL_MAP
        .iter()
        .filter(|(label, _)| *label == condname)
        .map(|(_, code)| *code)
        .collect()
}

// ===========================================================================
// Builders backing the grammar's cursor / loop / exception / INTO seams.
// (In C these live partly in pl_gram.y's actions; ported here faithfully and
// installed via the comp-seams the grammar calls.)
// ===========================================================================

/// `plpgsql_build_cursor_variable` — build a refcursor VAR with its bound query.
pub fn plpgsql_build_cursor_variable(
    refname: &str,
    lineno: i32,
    typoid: Oid,
    cursor_query: Option<Box<PLpgSQL_expr>>,
    argrow: i32,
    cursor_options: i32,
) -> i32 {
    let dt = plpgsql_build_datatype_internal(typoid, -1, INVALID_OID, None);
    // build the scalar refcursor var, then attach its cursor properties.
    let var = plpgsql_build_variable(refname, lineno, dt, true);
    let dno = var.dno;
    PLPGSQL_DATUMS.with(|d| {
        let mut b = d.borrow_mut();
        match &mut b[dno as usize] {
            PLpgSQL_datum::Var(v) => {
                v.cursor_explicit_expr = cursor_query;
                v.cursor_explicit_argrow = argrow;
                v.cursor_options = cursor_options;
            }
            _ => panic!("cursor datum {dno} is not a PLpgSQL_var"),
        }
    });
    dno
}

/// Build the unnamed ROW datum collecting a cursor's scalar args.
pub fn plpgsql_build_cursor_arg_row(lineno: i32, args: Vec<i32>) -> PgResult<i32> {
    let mut row = build_row_from_vars(&args)?;
    row.lineno = lineno;
    Ok(plpgsql_adddatum(PLpgSQL_datum::Row(mem::boxed(row))))
}

/// Build the record loop variable for `FOR rec IN <query>` loops.
pub fn plpgsql_build_record_for_loop(name: &str, lineno: i32) -> Option<PLpgSQL_variable> {
    let dno = plpgsql_build_record(name, lineno, None, RECORDOID, true);
    Some(datum_as_variable(dno))
}

/// Build the integer FOR loop's private variable (INT4 by default).
pub fn plpgsql_build_int_loop_var(name: &str, lineno: i32, typoid: Oid) -> PLpgSQL_var {
    let dt = plpgsql_build_datatype_internal(typoid, -1, INVALID_OID, None);
    let var = plpgsql_build_variable(name, lineno, dt, true);
    let dno = var.dno;
    PLPGSQL_DATUMS.with(|d| {
        let b = d.borrow();
        match &b[dno as usize] {
            PLpgSQL_datum::Var(v) => (**v).clone(),
            _ => panic!("int loop datum {dno} is not a PLpgSQL_var"),
        }
    })
}

/// Build a special EXCEPTION variable (`sqlstate`/`sqlerrm`).  Returns its dno.
pub fn plpgsql_build_exc_special_var(
    name: &str,
    lineno: i32,
    typoid: Oid,
    collation: Oid,
) -> i32 {
    let dt = plpgsql_build_datatype_internal(typoid, -1, collation, None);
    let var = plpgsql_build_variable(name, lineno, dt, true);
    var.dno
}

/// `plpgsql_build_into_row` — build the ROW datum for a multi-target INTO list.
pub fn plpgsql_build_into_row(lineno: i32, fieldnames: Vec<String>, varnos: Vec<i32>) -> i32 {
    let nfields = varnos.len() as i32;
    let row = PLpgSQL_row {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW,
        dno: 0,
        refname: mem::sdup("(unnamed row)"),
        lineno,
        isconst: false,
        notnull: false,
        default_val: None,
        rowtupdesc: None,
        nfields,
        fieldnames,
        varnos,
    };
    plpgsql_adddatum(PLpgSQL_datum::Row(mem::boxed(row)))
}

/// `make_scalar_list1` — wrap a single scalar into a ROW datum.  Returns dno.
pub fn make_scalar_list1(name: &str, scalar_dno: i32, lineno: i32, _location: i32) -> i32 {
    let row = PLpgSQL_row {
        dtype: PLpgSQL_datum_type::PLPGSQL_DTYPE_ROW,
        dno: 0,
        refname: mem::sdup(name),
        lineno,
        isconst: false,
        notnull: false,
        default_val: None,
        rowtupdesc: None,
        nfields: 1,
        fieldnames: mem::vfrom([mem::sdup(name)]),
        varnos: mem::vfrom([scalar_dno]),
    };
    plpgsql_adddatum(PLpgSQL_datum::Row(mem::boxed(row)))
}

/// `plpgsql_check_shadowvar` — decide whether `name` shadows an outer variable.
///
/// Mirrors the `decl_varname` action in `pl_gram.y`: only when the current
/// compile's `extra_warnings` / `extra_errors` carry `PLPGSQL_XCHECK_SHADOWVAR`
/// do we look the name up in the *enclosing* scope (`local_scope = false`); if
/// found, the `extra_errors` bit promotes the WARNING to an ERROR.  The grammar
/// performs the positioned `ereport` from the returned action.
pub fn plpgsql_check_shadowvar(name: &str) -> comp_seams::ShadowVarAction {
    let extra_warnings = curr_compile_field(|f| f.extra_warnings);
    let extra_errors = curr_compile_field(|f| f.extra_errors);

    if (extra_warnings & PLPGSQL_XCHECK_SHADOWVAR) == 0
        && (extra_errors & PLPGSQL_XCHECK_SHADOWVAR) == 0
    {
        return comp_seams::ShadowVarAction::None;
    }

    let shadowed = funcs::plpgsql_ns_top()
        .map(|top| funcs::plpgsql_ns_lookup(&top, false, name, None, None, None).is_some())
        .unwrap_or(false);
    if !shadowed {
        return comp_seams::ShadowVarAction::None;
    }

    if (extra_errors & PLPGSQL_XCHECK_SHADOWVAR) != 0 {
        comp_seams::ShadowVarAction::Error
    } else {
        comp_seams::ShadowVarAction::Warning
    }
}

// ===========================================================================
// Public compile entry points.
// ===========================================================================

/// `plpgsql_compile` — make an execution tree for a PL/pgSQL function.
///
/// `funccache.c` manages re-use of existing `PLpgSQL_function` caches: in
/// PL/pgSQL `fn_extra` is used directly as the pointer to the long-lived cache
/// entry, so this dispatches `cached_function_compile` (which calls back into
/// `plpgsql_compile_callback` on a cache miss) and then saves the result in
/// `fn_extra` to avoid the lookup on subsequent calls.
///
/// The funccache cache driver works on a `CachedFunctionRef`
/// (`types-funccache`), but `PLpgSQL_function` does not yet implement the
/// `CachedFunctionPayload` bridge (the funccache↔plpgsql opaque-`cfunc` header
/// unification keystone, also gating `plpgsql_free_function_memory`'s
/// `cfunc->use_count`), and the owned fcinfo model carries no `fn_extra`
/// channel.  Until those land the cache dispatch + `fn_extra` save cross
/// [`seam::compile_cached`], which mirror-PG-and-panics; the compile *logic*
/// lives in [`plpgsql_compile_from_source`] / [`compile_scalar_function_setup`].
pub fn plpgsql_compile(
    fcinfo: &types_nodes::fmgr::FunctionCallInfoBaseData<'_>,
    for_validator: bool,
) -> PLpgSQL_function {
    // function = cached_function_compile(fcinfo, fcinfo->flinfo->fn_extra,
    //     plpgsql_compile_callback, plpgsql_delete_callback,
    //     sizeof(PLpgSQL_function), false, forValidator);
    // ... fcinfo->flinfo->fn_extra = function;  (save to avoid re-search)
    seam::compile_cached(fcinfo, for_validator)
}

/// The owned `pg_proc` facts a first-call PL/pgSQL compile needs (the
/// integration layer reads these from the real `pg_proc` row and hands them
/// here, replacing the fmgr/funccache/syscache plumbing of `plpgsql_compile`).
#[derive(Clone, Debug)]
pub struct ProcCompileFacts {
    pub proname: String,
    pub fn_oid: Oid,
    pub fn_input_collation: Oid,
    pub prosrc: String,
    pub prorettype: Oid,
    pub proretset: bool,
    pub prokind: u8,
    pub provolatile: u8,
    pub pronargs: i32,
    pub argtypes: Vec<Oid>,
    pub argnames: Vec<String>,
    pub argmodes: Vec<u8>,
    /// `PLPGSQL_NOT_TRIGGER` / `PLPGSQL_DML_TRIGGER` / `PLPGSQL_EVENT_TRIGGER`
    /// (from `CALLED_AS_TRIGGER`/`CALLED_AS_EVENT_TRIGGER` on `fcinfo`).
    pub fn_is_trigger: PLpgSQL_trigtype,
    /// `forValidator` — compiling for `CREATE FUNCTION`-time validation.
    pub for_validator: bool,
    /// For a non-validator compile of a polymorphic-return function, the actual
    /// return type resolved from the call expression (`get_fn_expr_rettype`);
    /// `None` (InvalidOid) for the non-polymorphic case or when the integration
    /// layer could not resolve it (the validator path substitutes instead).
    pub resolved_rettype: Oid,
    /// For a non-validator compile of a function with polymorphic argument
    /// types, the actual argument types resolved from the call expression by the
    /// integration layer (`resolve_polymorphic_argtypes` over `fcinfo->flinfo->
    /// fn_expr`). When non-empty this list (length == declared `argtypes`) is
    /// used in place of the declared types so that each `any*` pseudo-type is
    /// the concrete type the call passes. Empty for the validator path (which
    /// substitutes the int4 family per `plpgsql_resolve_polymorphic_argtypes`'s
    /// `forValidator` branch) or when no polymorphic argument is present.
    pub resolved_argtypes: Vec<Oid>,
}

/// `plpgsql_compile_inline` — make an execution tree for an anonymous code
/// block (`DO`).  Generally parallel to the non-trigger compile.
pub fn plpgsql_compile_inline(proc_source: String) -> PgResult<PLpgSQL_function> {
    set_latest_lineno(1);
    with_compile_error_context(|| plpgsql_compile_inline_inner(proc_source))
}

fn plpgsql_compile_inline_inner(proc_source: String) -> PgResult<PLpgSQL_function> {
    let func_name = "inline_code_block";

    PLPGSQL_ERROR_FUNCNAME.with(|f| *f.borrow_mut() = Some(mem::sdup(func_name)));
    // Do extra syntax checking if check_function_bodies is on.
    set_check_syntax(seam::check_function_bodies());

    let mut function = new_zeroed_function();
    function.fn_signature = mem::sdup(func_name);
    function.fn_is_trigger = PLpgSQL_trigtype::PLPGSQL_NOT_TRIGGER;
    function.fn_input_collation = INVALID_OID;
    function.out_param_varno = -1;
    function.resolve_option = seam::plpgsql_variable_conflict();
    function.print_strict_params = seam::plpgsql_print_strict_params();
    // don't do extra validation for inline code
    function.extra_warnings = 0;
    function.extra_errors = 0;
    function.nstatements = 0;
    function.requires_procedure_resowner = false;
    function.has_exception_block = false;

    funcs::plpgsql_ns_init();
    funcs::plpgsql_ns_push(Some(func_name), PLpgSQL_label_type::PLPGSQL_LABEL_BLOCK);
    set_dump_exec_tree(false);

    // Set up as though in a function returning VOID.
    function.fn_rettype = VOIDOID;
    function.fn_retset = false;
    function.fn_retistuple = false;
    function.fn_retisdomain = false;
    function.fn_prokind = PROKIND_FUNCTION;
    function.fn_retbyval = true;
    function.fn_rettyplen = core::mem::size_of::<i32>() as i32;
    function.fn_readonly = false;

    set_curr_compile(function);

    plpgsql_start_datums();

    let found_dt = plpgsql_build_datatype_internal(BOOLOID, -1, INVALID_OID, None);
    let var = plpgsql_build_variable("found", 0, found_dt, true);
    set_curr_compile_field(|f| f.found_varno = var.dno);

    let action = parse_function_body(&proc_source)?;
    set_curr_compile_field(|f| f.action = Some(action));

    if curr_compile_field(|f| f.fn_rettype) == VOIDOID {
        add_dummy_return();
    }

    set_curr_compile_field(|f| f.fn_nargs = 0);
    plpgsql_finish_datums();

    if curr_compile_field(|f| f.has_exception_block) {
        let ctx = mcx::MemoryContext::new("PL/pgSQL mark-local-targets");
        with_curr_compile_mut(|func| {
            let _ = funcs::plpgsql_mark_local_assignment_targets(ctx.mcx(), func);
        });
    }

    if plpgsql_dump_exec_tree() {
        curr_compile_field(funcs::plpgsql_dumptree);
    }

    PLPGSQL_ERROR_FUNCNAME.with(|f| *f.borrow_mut() = None);
    set_check_syntax(false);

    Ok(take_curr_compile())
}

/// `plpgsql_compile` cold path, owned-inputs form (`plpgsql_compile_callback`'s
/// non-trigger scalar/procedure branch).  Trigger / event-trigger functions
/// take the gated trigtype branches and are not handled on this cold path.
pub fn plpgsql_compile_from_source(facts: &ProcCompileFacts) -> PgResult<PLpgSQL_function> {
    // `plpgsql_scanner_init` resets cur_line_num to 1 before the trigtype checks
    // and the parse; mirror that so an early semantic compile error reports
    // "near line 1". The error-context callback wraps the whole compile.
    set_latest_lineno(1);
    with_compile_error_context(|| plpgsql_compile_from_source_inner(facts))
}

fn plpgsql_compile_from_source_inner(facts: &ProcCompileFacts) -> PgResult<PLpgSQL_function> {
    let mut num_out_args: i32 = 0;
    let mut in_arg_varnos: Vec<i32> = Vec::new();
    let mut out_arg_variables: Vec<i32> = Vec::new();

    PLPGSQL_ERROR_FUNCNAME.with(|f| *f.borrow_mut() = Some(facts.proname.clone()));
    // Do extra syntax checks when validating the function definition.
    set_check_syntax(facts.for_validator);

    let mut function = new_zeroed_function();
    // C: `function->fn_signature = format_procedure(fcinfo->flinfo->fn_oid)` —
    // the function's printable name with its IN-argument type list, used in the
    // PL/pgSQL error-context line. Build it in a transient context and copy the
    // owned String into the function (which outlives the temp context).
    function.fn_signature = {
        let ctx = mcx::MemoryContext::new("PL/pgSQL fn_signature");
        let s = backend_utils_adt_regproc_seams::format_procedure::call(ctx.mcx(), facts.fn_oid)?;
        s.as_str().to_string()
    };
    function.fn_oid = facts.fn_oid;
    function.fn_input_collation = facts.fn_input_collation;
    function.out_param_varno = -1;
    function.resolve_option = seam::plpgsql_variable_conflict();
    function.print_strict_params = seam::plpgsql_print_strict_params();
    // only promote extra warnings and errors at CREATE FUNCTION time
    function.extra_warnings = if facts.for_validator {
        seam::plpgsql_extra_warnings()
    } else {
        0
    };
    function.extra_errors = if facts.for_validator {
        seam::plpgsql_extra_errors()
    } else {
        0
    };
    function.fn_is_trigger = facts.fn_is_trigger;
    function.fn_prokind = facts.prokind;
    function.nstatements = 0;
    function.requires_procedure_resowner = false;
    function.has_exception_block = false;

    funcs::plpgsql_ns_init();
    funcs::plpgsql_ns_push(Some(&facts.proname), PLpgSQL_label_type::PLPGSQL_LABEL_BLOCK);
    set_dump_exec_tree(false);

    set_curr_compile(function);

    // Branch on the function's trigtype (the three arms of the C
    // `switch (function->fn_is_trigger)` in `plpgsql_compile_callback`).
    if facts.fn_is_trigger == PLpgSQL_trigtype::PLPGSQL_DML_TRIGGER {
        compile_dml_trigger_setup(facts.pronargs);
    } else if facts.fn_is_trigger == PLpgSQL_trigtype::PLPGSQL_EVENT_TRIGGER {
        compile_event_trigger_setup(facts.pronargs);
    } else {
        compile_scalar_function_setup(
            facts,
            &mut num_out_args,
            &mut in_arg_varnos,
            &mut out_arg_variables,
        )?;
    }

    set_curr_compile_field(|f| f.fn_readonly = facts.provolatile != PROVOLATILE_VOLATILE);

    let found_dt = plpgsql_build_datatype_internal(BOOLOID, -1, INVALID_OID, None);
    let var = plpgsql_build_variable("found", 0, found_dt, true);
    set_curr_compile_field(|f| f.found_varno = var.dno);

    let action = parse_function_body(&facts.prosrc)?;
    set_curr_compile_field(|f| f.action = Some(action));

    if num_out_args > 0
        || curr_compile_field(|f| f.fn_rettype) == VOIDOID
        || curr_compile_field(|f| f.fn_retset)
    {
        add_dummy_return();
    }

    set_curr_compile_field(|f| f.fn_nargs = facts.pronargs);
    let fn_nargs = curr_compile_field(|f| f.fn_nargs);
    set_curr_compile_field(|f| {
        for i in 0..(fn_nargs as usize) {
            f.fn_argvarnos[i] = in_arg_varnos[i];
        }
    });

    plpgsql_finish_datums();

    if curr_compile_field(|f| f.has_exception_block) {
        let ctx = mcx::MemoryContext::new("PL/pgSQL mark-local-targets");
        with_curr_compile_mut(|func| {
            let _ = funcs::plpgsql_mark_local_assignment_targets(ctx.mcx(), func);
        });
    }

    if plpgsql_dump_exec_tree() {
        curr_compile_field(funcs::plpgsql_dumptree);
    }

    PLPGSQL_ERROR_FUNCNAME.with(|f| *f.borrow_mut() = None);
    set_check_syntax(false);

    Ok(take_curr_compile())
}

/// `plpgsql_resolve_polymorphic_argtypes` (pl_comp.c) — given the declared
/// argument types of the function being compiled, substitute each polymorphic
/// pseudo-type (`anyelement`/`anyarray`/`anyenum`/`anyrange`/`anymultirange` and
/// the `anycompatible*` family) with the concrete type the call actually passes.
///
/// In the normal (non-validator) call path this is the actual-type resolution
/// from `fcinfo->flinfo->fn_expr` via `resolve_polymorphic_argtypes`
/// (funcapi.c); the integration layer performs that resolution against the call
/// expression and hands the result in `facts.resolved_argtypes`, so here we copy
/// those concrete types in (and ereport if they could not be determined).
///
/// In the validator path there is no call expression, so — exactly as the C
/// `forValidator` branch — we arbitrarily assume we are dealing with the integer
/// family. Note `ANYENUMOID` maps to `INT4OID` (the C comment marks this as
/// "XXX dubious", but it is what CREATE FUNCTION validation does), which is what
/// lets a `CREATE FUNCTION f(x anyenum) ... LANGUAGE plpgsql` validate.
fn plpgsql_resolve_polymorphic_argtypes(facts: &ProcCompileFacts, argtypes: &mut [Oid]) {
    if !facts.for_validator {
        // Normal case: the integration layer resolved the actual argument types
        // from the call expression (the C `resolve_polymorphic_argtypes(numargs,
        // argtypes, argmodes, call_expr)` call). If any declared type is
        // polymorphic, `resolved_argtypes` must be populated with the concretes.
        let needs_resolution = argtypes.iter().any(|&t| is_polymorphic_type(t));
        if needs_resolution {
            if facts.resolved_argtypes.len() != argtypes.len() {
                ereport_error(
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "could not determine actual argument type for polymorphic function \"{}\"",
                        plpgsql_error_funcname().unwrap_or_default()
                    ),
                );
            }
            for (i, dst) in argtypes.iter_mut().enumerate() {
                if is_polymorphic_type(*dst) {
                    let concrete = facts.resolved_argtypes[i];
                    if !oid_is_valid(concrete) {
                        ereport_error(
                            ERRCODE_FEATURE_NOT_SUPPORTED,
                            format!(
                                "could not determine actual argument type for polymorphic function \"{}\"",
                                plpgsql_error_funcname().unwrap_or_default()
                            ),
                        );
                    }
                    *dst = concrete;
                }
            }
        }
    } else {
        // Special validation mode --- arbitrarily assume we are dealing with the
        // integer family (mirrors pl_comp.c's `forValidator` switch).
        for t in argtypes.iter_mut() {
            match *t {
                ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID | ANYCOMPATIBLEOID
                | ANYCOMPATIBLENONARRAYOID => {
                    *t = INT4OID;
                }
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID => {
                    *t = INT4ARRAYOID;
                }
                ANYRANGEOID | ANYCOMPATIBLERANGEOID => {
                    *t = INT4RANGEOID;
                }
                ANYMULTIRANGEOID | ANYCOMPATIBLEMULTIRANGEOID => {
                    *t = INT4MULTIRANGEOID;
                }
                _ => {}
            }
        }
    }
}

/// The non-trigger (`PLPGSQL_NOT_TRIGGER`) arm of `plpgsql_compile_callback`:
/// build the argument variables, handle OUT params, resolve the return type.
fn compile_scalar_function_setup(
    facts: &ProcCompileFacts,
    num_out_args: &mut i32,
    in_arg_varnos: &mut Vec<i32>,
    out_arg_variables: &mut Vec<i32>,
) -> PgResult<()> {
    plpgsql_start_datums();

    // Resolve any polymorphic argument types to the concrete call types (or the
    // int4 family in validation mode) before building the argument variables.
    // (the C `plpgsql_resolve_polymorphic_argtypes(numargs, argtypes, argmodes,
    // fcinfo->flinfo->fn_expr, forValidator, ...)` call in do_compile)
    let mut argtypes = facts.argtypes.clone();
    plpgsql_resolve_polymorphic_argtypes(facts, &mut argtypes);
    let argnames = facts.argnames.clone();
    let argmodes = facts.argmodes.clone();
    let numargs = argtypes.len() as i32;

    const PROARGMODE_IN: u8 = b'i';
    const PROARGMODE_OUT: u8 = b'o';
    const PROARGMODE_INOUT: u8 = b'b';
    const PROARGMODE_VARIADIC: u8 = b'v';
    const PROARGMODE_TABLE: u8 = b't';

    for i in 0..(numargs as usize) {
        let argtypeid = argtypes[i];
        let argmode = if !argmodes.is_empty() {
            argmodes[i]
        } else {
            PROARGMODE_IN
        };

        let buf = mem::sdup(&format!("${}", i + 1));
        let argdtype = plpgsql_build_datatype_internal(
            argtypeid,
            -1,
            curr_compile_field(|f| f.fn_input_collation),
            None,
        );
        if argdtype.ttype == PLpgSQL_type_type::PLPGSQL_TTYPE_PSEUDO {
            ereport_error(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot accept type {}",
                    seam::format_type_be(argtypeid)
                ),
            );
        }

        let refname = if !argnames.is_empty() && !argnames[i].is_empty() {
            argnames[i].clone()
        } else {
            buf.clone()
        };
        let argvariable = plpgsql_build_variable(&refname, 0, argdtype, false);
        let (argitemtype, argv_dno) = {
            let itemtype = if argvariable.dtype == PLpgSQL_datum_type::PLPGSQL_DTYPE_VAR {
                PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_VAR
            } else {
                assert_eq!(argvariable.dtype, PLpgSQL_datum_type::PLPGSQL_DTYPE_REC);
                PLpgSQL_nsitem_type::PLPGSQL_NSTYPE_REC
            };
            (itemtype, argvariable.dno)
        };

        if argmode == PROARGMODE_IN
            || argmode == PROARGMODE_INOUT
            || argmode == PROARGMODE_VARIADIC
        {
            mem::vpush(in_arg_varnos, argv_dno);
        }
        if argmode == PROARGMODE_OUT || argmode == PROARGMODE_INOUT || argmode == PROARGMODE_TABLE {
            mem::vpush(out_arg_variables, argv_dno);
            *num_out_args += 1;
        }

        add_parameter_name(argitemtype, argv_dno, &buf);
        if !argnames.is_empty() && !argnames[i].is_empty() {
            add_parameter_name(argitemtype, argv_dno, &argnames[i]);
        }
    }

    if *num_out_args > 1
        || (*num_out_args == 1 && curr_compile_field(|f| f.fn_prokind) == PROKIND_PROCEDURE)
    {
        let row = build_row_from_vars(out_arg_variables)?;
        let row_dno = plpgsql_adddatum(PLpgSQL_datum::Row(mem::boxed(row)));
        set_curr_compile_field(|f| f.out_param_varno = row_dno);
    } else if *num_out_args == 1 {
        set_curr_compile_field(|f| f.out_param_varno = out_arg_variables[0]);
    }

    // Check for a polymorphic returntype. If found, use the actual returntype
    // type from the caller's FuncExpr node, if we have one. (In validation mode
    // we arbitrarily assume we are dealing with integers.)
    let mut rettypeid = facts.prorettype;
    if is_polymorphic_type(rettypeid) {
        if facts.for_validator {
            if rettypeid == ANYARRAYOID || rettypeid == ANYCOMPATIBLEARRAYOID {
                rettypeid = INT4ARRAYOID;
            } else if rettypeid == ANYRANGEOID || rettypeid == ANYCOMPATIBLERANGEOID {
                rettypeid = INT4RANGEOID;
            } else if rettypeid == ANYMULTIRANGEOID {
                rettypeid = INT4MULTIRANGEOID;
            } else {
                // ANYELEMENT or ANYNONARRAY or ANYCOMPATIBLE
                rettypeid = INT4OID;
            }
        } else {
            // get_fn_expr_rettype(fcinfo->flinfo) — resolved from the call
            // expression by the integration layer and passed in `resolved_rettype`.
            rettypeid = facts.resolved_rettype;
            if !oid_is_valid(rettypeid) {
                ereport_error(
                    ERRCODE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "could not determine actual return type for polymorphic function \"{}\"",
                        plpgsql_error_funcname().unwrap_or_default()
                    ),
                );
            }
        }
    }

    // Normal function has a defined returntype.
    set_curr_compile_field(|f| f.fn_rettype = rettypeid);
    set_curr_compile_field(|f| f.fn_retset = facts.proretset);

    let type_form = seam::pg_type_form(rettypeid);
    // Disallow pseudotype result, except VOID or RECORD.
    // (note we already replaced polymorphic types)
    if type_form.typtype as i8 == TYPTYPE_PSEUDO {
        if rettypeid == VOIDOID || rettypeid == RECORDOID {
            // okay
        } else if rettypeid == TRIGGEROID || rettypeid == EVENT_TRIGGEROID {
            ereport_error(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                "trigger functions can only be called as triggers".to_string(),
            );
        } else {
            ereport_error(
                ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "PL/pgSQL functions cannot return type {}",
                    seam::format_type_be(rettypeid)
                ),
            );
        }
    }

    set_curr_compile_field(|f| f.fn_retistuple = seam::type_is_rowtype(rettypeid));
    set_curr_compile_field(|f| f.fn_retisdomain = type_form.typtype as i8 == TYPTYPE_DOMAIN);
    set_curr_compile_field(|f| f.fn_retbyval = type_form.typbyval);
    set_curr_compile_field(|f| f.fn_rettyplen = type_form.typlen as i32);

    // Install $0 reference, but only for polymorphic return types, and not when
    // the return is specified through an output parameter.
    if is_polymorphic_type(facts.prorettype) && *num_out_args == 0 {
        let dt = build_datatype(
            &type_form,
            -1,
            curr_compile_field(|f| f.fn_input_collation),
            None,
        );
        let _ = plpgsql_build_variable("$0", 0, dt, true);
    }

    Ok(())
}

fn with_curr_compile_mut(f: impl FnOnce(&mut PLpgSQL_function)) {
    set_curr_compile_field(f)
}

/// Scan + parse the function body text into the top-level block (the
/// `plpgsql_scanner_init` -> `plpgsql_yyparse` -> `plpgsql_scanner_finish`
/// sequence of `plpgsql_compile_callback`).  Runs inside the seams the grammar
/// fires, which are installed by the time a compile runs.
fn parse_function_body(src: &str) -> PgResult<Box<PLpgSQL_stmt_block>> {
    set_plpgsql_identifier_lookup(IdentifierLookup::IDENTIFIER_LOOKUP_NORMAL);
    let scanbuf = scanbuf_bytes(src);
    // The scanner allocates per-token strings in this arena; the owned AST it
    // returns is on the builtin allocator, so the arena may drop at parse end.
    let ctx = mcx::MemoryContext::new("PL/pgSQL function parse");
    let scanner = backend_pl_plpgsql_scanner::plpgsql_scanner_init(ctx.mcx(), &scanbuf, src);
    // In C the grammar errors via `ereport(ERROR)`, which longjmps out of the
    // compile through `plpgsql_compile_error_callback`.  That callback runs
    // `function_parse_error_transpose` to relocate the body-relative cursor
    // position to a position in the original CREATE FUNCTION / DO text (and to
    // drop the "internal query" framing on success).  `plpgsql_yyparse` returns
    // the same `PgError` it would have raised; run the transpose on it before
    // propagating, mirroring the C error-context callback.
    backend_pl_plpgsql_gram::plpgsql_yyparse_with_lineno(scanner).map_err(|(e, latest_lineno)| {
        // Record the scanner's final line for any later compile error-context.
        set_latest_lineno(latest_lineno);
        // `plpgsql_compile_error_callback` (pl_comp.c): first try to relocate the
        // body-relative cursor position into the original CREATE FUNCTION / DO
        // text. If `function_parse_error_transpose` reports a syntax-error
        // position (C `return true`), the callback returns without adding any
        // "near line N" context. Otherwise — the common case for a *semantic*
        // compile error with no cursor position (e.g. "too many parameters
        // specified for RAISE") — it falls back to an
        // `errcontext("compilation of PL/pgSQL function \"%s\" near line %d", …)`.
        //
        // The value-form transpose returns the error unchanged exactly when it
        // would have returned C-false (no original cursor/internal position), so
        // detect that by checking whether the error carried a position before the
        // transpose.
        let had_position =
            e.cursor_position.unwrap_or(0) > 0 || e.internal_position.unwrap_or(0) > 0;
        let e = comp_seams::function_parse_error_transpose::call(src, e)
            .unwrap_or_else(|fallback| fallback);
        if !had_position {
            if let Some(funcname) = plpgsql_error_funcname() {
                return e.with_context(format!(
                    "compilation of PL/pgSQL function \"{funcname}\" near line {latest_lineno}"
                ));
            }
        }
        e
    })
}

/// The NUL-terminated scan buffer the core lexer scans (the bytes of `src`).
fn scanbuf_bytes(src: &str) -> Vec<u8> {
    let mut v = mem::vwithcap(src.len() + 1);
    v.extend_from_slice(src.as_bytes());
    v.push(0);
    v
}

/// Build a fresh zeroed `PLpgSQL_function` (mirrors `palloc0`).
fn new_zeroed_function() -> PLpgSQL_function {
    PLpgSQL_function {
        cfunc: CachedFunction(0),
        fn_signature: String::new(),
        fn_oid: INVALID_OID,
        fn_is_trigger: PLpgSQL_trigtype::PLPGSQL_NOT_TRIGGER,
        fn_input_collation: INVALID_OID,
        fn_cxt: None,
        fn_rettype: INVALID_OID,
        fn_rettyplen: 0,
        fn_retbyval: false,
        fn_retistuple: false,
        fn_retisdomain: false,
        fn_retset: false,
        fn_readonly: false,
        fn_prokind: 0,
        fn_nargs: 0,
        fn_argvarnos: [0; FUNC_MAX_ARGS],
        out_param_varno: -1,
        found_varno: 0,
        new_varno: 0,
        old_varno: 0,
        resolve_option: PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR,
        print_strict_params: false,
        extra_warnings: 0,
        extra_errors: 0,
        ndatums: 0,
        datums: Vec::new(),
        copiable_size: 0,
        action: None,
        nstatements: 0,
        requires_procedure_resowner: false,
        has_exception_block: false,
        cur_estate: None,
    }
}

// ===========================================================================
// Seam installation — wire every backend-pl-plpgsql-comp-seams declaration to
// its real implementation in this crate.
// ===========================================================================

use backend_pl_plpgsql_comp_seams as comp_seams;

/// Install the compiler's inward callbacks (the scanner/grammar resolvers and
/// builders).  Wired into `seams-init::init_all()`.
pub fn init_seams() {
    comp_seams::plpgsql_parse_word::set(|word1, yytxt, lookup| {
        Ok(plpgsql_parse_word(word1, yytxt, lookup))
    });
    comp_seams::plpgsql_parse_dblword::set(|w1, w2| Ok(plpgsql_parse_dblword(w1, w2)));
    comp_seams::plpgsql_parse_tripword::set(|w1, w2, w3| Ok(plpgsql_parse_tripword(w1, w2, w3)));

    comp_seams::set_dump_exec_tree::set(set_dump_exec_tree);
    comp_seams::set_identifier_lookup::set(set_plpgsql_identifier_lookup);
    comp_seams::curr_compile_set_print_strict_params::set(|v| {
        set_curr_compile_field(|f| f.print_strict_params = v)
    });
    comp_seams::curr_compile_set_resolve_option::set(|v| {
        set_curr_compile_field(|f| f.resolve_option = v)
    });
    comp_seams::curr_compile_set_requires_procedure_resowner::set(|| {
        set_curr_compile_field(|f| f.requires_procedure_resowner = true)
    });
    comp_seams::curr_compile_set_has_exception_block::set(|| {
        set_curr_compile_field(|f| f.has_exception_block = true)
    });
    comp_seams::curr_compile_next_stmtid::set(curr_compile_next_stmtid);
    comp_seams::curr_compile_handle::set(|| {
        if curr_compile_in_progress() {
            Some(0)
        } else {
            None
        }
    });
    comp_seams::curr_compile_fn_input_collation::set(|| curr_compile_field(|f| f.fn_input_collation));
    comp_seams::curr_compile_fn_retset::set(|| curr_compile_field(|f| f.fn_retset));
    comp_seams::curr_compile_fn_rettype::set(|| curr_compile_field(|f| f.fn_rettype));
    comp_seams::curr_compile_fn_prokind::set(|| curr_compile_field(|f| f.fn_prokind));
    comp_seams::curr_compile_out_param_varno::set(|| curr_compile_field(|f| f.out_param_varno));
    comp_seams::plpgsql_ndatums::set(plpgsql_ndatums);

    comp_seams::plpgsql_build_variable::set(|refname, lineno, dtype, add2ns| {
        let v = plpgsql_build_variable(refname, lineno, Box::new(dtype), add2ns);
        Ok(v.dno)
    });
    comp_seams::plpgsql_var_set_decl_props::set(plpgsql_var_set_decl_props);
    comp_seams::mark_var_default_as_assignment_source::set(mark_var_default_as_assignment_source);
    comp_seams::plpgsql_build_cursor_variable::set(
        |refname, lineno, typoid, cursor_query, argrow, opts| {
            Ok(plpgsql_build_cursor_variable(
                refname,
                lineno,
                typoid,
                cursor_query,
                argrow,
                opts,
            ))
        },
    );
    comp_seams::plpgsql_build_cursor_arg_row::set(|lineno, args| {
        plpgsql_build_cursor_arg_row(lineno, args)
    });
    comp_seams::plpgsql_build_datatype_arrayof::set(|elem| {
        plpgsql_build_datatype_arrayof(Box::new(elem))
    });
    comp_seams::plpgsql_build_datatype::set(|typoid, typmod, collation| {
        Ok(plpgsql_build_datatype_internal(typoid, typmod, collation, None))
    });
    comp_seams::plpgsql_parse_wordtype::set(plpgsql_parse_wordtype);
    comp_seams::plpgsql_parse_wordrowtype::set(plpgsql_parse_wordrowtype);
    comp_seams::plpgsql_parse_cwordtype::set(plpgsql_parse_cwordtype);
    comp_seams::plpgsql_parse_cwordrowtype::set(plpgsql_parse_cwordrowtype);

    comp_seams::plpgsql_build_record_for_loop::set(|name, lineno| {
        Ok(plpgsql_build_record_for_loop(name, lineno).map(Box::new))
    });
    comp_seams::plpgsql_build_int_loop_var::set(|name, lineno, typoid| {
        Ok(plpgsql_build_int_loop_var(name, lineno, typoid))
    });
    comp_seams::plpgsql_build_exc_special_var::set(|name, lineno, typoid, collation| {
        Ok(plpgsql_build_exc_special_var(name, lineno, typoid, collation))
    });
    comp_seams::plpgsql_build_into_row::set(|lineno, fieldnames, varnos| {
        Ok(plpgsql_build_into_row(lineno, fieldnames, varnos))
    });
    comp_seams::make_scalar_list1::set(|name, scalar_dno, lineno, location| {
        Ok(make_scalar_list1(name, scalar_dno, lineno, location))
    });

    comp_seams::datum_dtype::set(datum_dtype_of);
    comp_seams::datum_as_variable::set(datum_as_variable);
    comp_seams::var_datatype_typoid::set(var_datatype_typoid);
    comp_seams::var_has_explicit_expr::set(var_has_explicit_expr);
    comp_seams::var_cursor_explicit_argrow::set(var_cursor_explicit_argrow);
    comp_seams::var_refname::set(var_refname);
    comp_seams::cursor_argrow_fieldnames::set(cursor_argrow_fieldnames);

    comp_seams::plpgsql_add_initdatums_forget::set(|| {
        plpgsql_add_initdatums(false);
    });
    comp_seams::plpgsql_add_initdatums_collect::set(|| plpgsql_add_initdatums(true));
    comp_seams::plpgsql_check_shadowvar::set(plpgsql_check_shadowvar);

    comp_seams::plpgsql_parse_err_condition::set(plpgsql_parse_err_condition);
    comp_seams::plpgsql_recognize_err_condition::set(|condname, allow| {
        plpgsql_recognize_err_condition(condname, allow)?;
        Ok(())
    });
    comp_seams::check_assignable::set(|dno, location| {
        check_assignable(dno, location);
        Ok(())
    });
    comp_seams::mark_expr_as_assignment_source::set(mark_expr_as_assignment_source);
    comp_seams::perform_rewrite_query::set(perform_rewrite_query);
    comp_seams::check_sql_expr::set(|stmt, parse_mode, location| {
        // C `check_sql_expr` returns immediately unless `plpgsql_check_syntax`
        // (set only for a forValidator compile); skip the raw-parse otherwise.
        if !plpgsql_check_syntax() {
            return Ok(());
        }
        seam::check_sql_expr(stmt, parse_mode, location)
    });
    comp_seams::parse_datatype::set(|string, location| seam::parse_datatype(string, location));
    comp_seams::get_collation_oid::set(|names, missing_ok| {
        seam::get_collation_oid(names, missing_ok)
    });
    comp_seams::quote_identifier::set(seam::quote_identifier);
}
