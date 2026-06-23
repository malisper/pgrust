//! Seam declarations for the PL/pgSQL compiler unit (`pl_comp.c`)'s
//! scanner-callback surface.
//!
//! `pl_scanner.c`'s `plpgsql_yylex` postparses each identifier (or compound
//! dotted identifier) by calling back into the compiler to resolve it against
//! the function's variable namespace: `plpgsql_parse_word` for a simple name,
//! `plpgsql_parse_dblword` for `A.B`, and `plpgsql_parse_tripword` for `A.B.C`.
//! Those resolvers live in `pl_comp.c` (the `backend-pl-plpgsql-comp` unit),
//! which in turn consumes the scanner's keyword tables — a cycle. The scanner
//! therefore reaches them through these seams; the compiler unit installs them
//! from its `init_seams()` when it lands. Until then a call panics loudly
//! (mirror-PG-and-panic).
//!
//! ## Modeling the C out-parameter contract
//!
//! Each C resolver returns `bool` (matched a datum?) and fills *one* of two
//! out-parameters:
//!   * on `true`  -> `*wdatum` (a [`PLwdatum`]) is filled, identifying the
//!     resolved variable;
//!   * on `false` -> the word/cword out-parameter (`*word` [`PLword`] for the
//!     simple case, `*cword` [`PLcword`] for the compound cases) is filled with
//!     the un-resolved identifier text the grammar will report.
//!
//! We carry that as a [`WordResolution`] tagged result so the (mutually
//! exclusive) out-parameters are expressed by construction rather than by a
//! `bool` plus two maybe-initialized slots.

use ::types_error::{PgError, PgResult};
use ::plpgsql::{
    IdentifierLookup, Oid, PLcword, PLpgSQL_condition, PLpgSQL_datum_type, PLpgSQL_expr,
    PLpgSQL_resolve_option, PLpgSQL_type, PLpgSQL_var, PLpgSQL_variable, PLwdatum, PLword,
    RawParseMode,
};

/// Result of resolving a single identifier (`plpgsql_parse_word`).
///
/// `Datum` mirrors the C `true` return (the `*wdatum` out-parameter was
/// filled); `Word` mirrors the `false` return (the `*word` out-parameter was
/// filled with the literal identifier).
pub enum WordResolution {
    Datum(PLwdatum),
    Word(PLword),
}

/// Result of resolving a compound (dotted) identifier (`plpgsql_parse_dblword`
/// / `plpgsql_parse_tripword`).
///
/// `Datum` mirrors the C `true` return (`*wdatum` filled); `Cword` mirrors the
/// `false` return (`*cword` filled with the dotted-name component list).
pub enum CwordResolution {
    Datum(PLwdatum),
    Cword(PLcword),
}

seam_core::seam!(
    /// `plpgsql_parse_word(word1, yytxt, lookup, wdatum, word)` (`pl_comp.c`):
    /// postparse a single identifier. `word1` is the (possibly downcased,
    /// truncated) identifier; `yytxt` is the original token text used to test
    /// whether the identifier was double-quoted; `lookup` requests a variable
    /// lookup (suppressed at statement start where the name can't be a
    /// variable). Returns [`WordResolution::Datum`] when the name resolves to a
    /// PL/pgSQL variable, else [`WordResolution::Word`].
    pub fn plpgsql_parse_word(
        word1: &str,
        yytxt: &str,
        lookup: bool,
    ) -> PgResult<WordResolution>
);

seam_core::seam!(
    /// `plpgsql_parse_dblword(word1, word2, wdatum, cword)` (`pl_comp.c`):
    /// the same lookup for a two-component dotted name `word1.word2`.
    pub fn plpgsql_parse_dblword(word1: &str, word2: &str) -> PgResult<CwordResolution>
);

seam_core::seam!(
    /// `plpgsql_parse_tripword(word1, word2, word3, wdatum, cword)`
    /// (`pl_comp.c`): the same lookup for a three-component dotted name
    /// `word1.word2.word3`.
    pub fn plpgsql_parse_tripword(
        word1: &str,
        word2: &str,
        word3: &str,
    ) -> PgResult<CwordResolution>
);

// ===========================================================================
// Compiler global compile-state accessors (`plpgsql_curr_compile` family) +
// builders / catalog resolvers / SQL-engine helpers the grammar's actions call.
//
// These all live in `pl_comp.c` (the `backend-pl-plpgsql-comp` unit), which is
// not yet ported. Until it lands and installs them from its `init_seams()`, a
// call panics loudly (mirror-PG-and-panic). They are declared here because the
// grammar (`pl_gram.y`, the `backend-pl-plpgsql-gram` unit) reaches them across
// the gram↔comp cycle.
// ===========================================================================

seam_core::seam!(
    /// `plpgsql_DumpExecTree = value` (`#option dump`).
    pub fn set_dump_exec_tree(value: bool)
);

seam_core::seam!(
    /// `plpgsql_IdentifierLookup = mode` — the grammar toggles this global as it
    /// enters/leaves DECLARE sections and SQL-expression scans. The
    /// authoritative copy lives in the compiler unit (consulted by
    /// `plpgsql_parse_word`/`dblword`/`tripword`), so the grammar writes it
    /// through this seam rather than the (vestigial) scanner-instance mirror.
    pub fn set_identifier_lookup(mode: IdentifierLookup)
);

seam_core::seam!(
    /// `plpgsql_curr_compile->print_strict_params = value`.
    pub fn curr_compile_set_print_strict_params(value: bool)
);

seam_core::seam!(
    /// `plpgsql_curr_compile->resolve_option = value`.
    pub fn curr_compile_set_resolve_option(value: PLpgSQL_resolve_option)
);

seam_core::seam!(
    /// `plpgsql_curr_compile->requires_procedure_resowner = true`.
    pub fn curr_compile_set_requires_procedure_resowner()
);

seam_core::seam!(
    /// `plpgsql_curr_compile->has_exception_block = true`.
    pub fn curr_compile_set_has_exception_block()
);

seam_core::seam!(
    /// `++plpgsql_curr_compile->nstatements` (the per-statement id allocator).
    pub fn curr_compile_next_stmtid() -> u32
);

seam_core::seam!(
    /// `plpgsql_curr_compile` itself, as the opaque back-reference stored in
    /// `PLpgSQL_expr.func` (`Option<u64>`); `None` when no compile is active.
    pub fn curr_compile_handle() -> Option<u64>
);

seam_core::seam!(
    /// `plpgsql_curr_compile->fn_input_collation`.
    pub fn curr_compile_fn_input_collation() -> Oid
);

seam_core::seam!(
    /// `plpgsql_curr_compile->fn_retset`.
    pub fn curr_compile_fn_retset() -> bool
);

seam_core::seam!(
    /// `plpgsql_curr_compile->fn_rettype`.
    pub fn curr_compile_fn_rettype() -> Oid
);

seam_core::seam!(
    /// `plpgsql_curr_compile->fn_prokind`.
    pub fn curr_compile_fn_prokind() -> u8
);

seam_core::seam!(
    /// `plpgsql_curr_compile->out_param_varno`.
    pub fn curr_compile_out_param_varno() -> i32
);

seam_core::seam!(
    /// `plpgsql_nDatums` — number of datums currently allocated (used to name
    /// the implicit CASE test variable).
    pub fn plpgsql_ndatums() -> i32
);

seam_core::seam!(
    /// `plpgsql_build_variable(refname, lineno, dtype, add2namespace)`
    /// (`pl_comp.c`): build a variable datum and return its dno.
    pub fn plpgsql_build_variable(
        refname: &str,
        lineno: i32,
        dtype: PLpgSQL_type,
        add2namespace: bool,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// Set `var->isconst`/`var->notnull`/`var->default_val` on the just-built
    /// variable `dno` (the `decl_statement` post-build assignments).
    pub fn plpgsql_var_set_decl_props(
        dno: i32,
        isconst: bool,
        notnull: bool,
        default_val: Option<Box<PLpgSQL_expr>>,
    )
);

seam_core::seam!(
    /// `mark_expr_as_assignment_source(var->default_val, (PLpgSQL_datum *) var)`
    /// for the variable `dno`.
    pub fn mark_var_default_as_assignment_source(dno: i32)
);

seam_core::seam!(
    /// `plpgsql_build_variable` for a cursor variable (refcursor type) with its
    /// explicit bound query / arg-row / options; returns the new dno.
    pub fn plpgsql_build_cursor_variable(
        refname: &str,
        lineno: i32,
        typoid: Oid,
        cursor_query: Option<Box<PLpgSQL_expr>>,
        argrow: i32,
        cursor_options: i32,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// Build the unnamed ROW datum collecting a cursor's scalar args; returns
    /// its dno.
    pub fn plpgsql_build_cursor_arg_row(lineno: i32, args: Vec<i32>) -> PgResult<i32>
);

seam_core::seam!(
    /// `plpgsql_build_datatype(get_array_type(elem->typoid), -1, ...)` — build
    /// the array type over `elem`.
    pub fn plpgsql_build_datatype_arrayof(elem: PLpgSQL_type) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `plpgsql_build_datatype(typoid, typmod, collation, NULL)`.
    pub fn plpgsql_build_datatype(
        typoid: Oid,
        typmod: i32,
        collation: Oid,
    ) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `plpgsql_parse_wordtype(dtname)` — resolve `dtname%TYPE`.
    pub fn plpgsql_parse_wordtype(dtname: &str) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `plpgsql_parse_wordrowtype(dtname)` — resolve `dtname%ROWTYPE`.
    pub fn plpgsql_parse_wordrowtype(dtname: &str) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `plpgsql_parse_cwordtype(dtnames)` — resolve `a.b%TYPE`.
    pub fn plpgsql_parse_cwordtype(dtnames: &[String]) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `plpgsql_parse_cwordrowtype(dtnames)` — resolve `a.b%ROWTYPE`.
    pub fn plpgsql_parse_cwordrowtype(dtnames: &[String]) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// Build the record loop variable for `FOR rec IN <query>` / cursor loops;
    /// returns the loop variable (or `None` when not applicable).
    pub fn plpgsql_build_record_for_loop(
        name: &str,
        lineno: i32,
    ) -> PgResult<Option<Box<PLpgSQL_variable>>>
);

seam_core::seam!(
    /// Build the integer FOR loop's private INT4 variable.
    pub fn plpgsql_build_int_loop_var(
        name: &str,
        lineno: i32,
        typoid: Oid,
    ) -> PgResult<PLpgSQL_var>
);

seam_core::seam!(
    /// Build a special EXCEPTION variable (`sqlstate`/`sqlerrm`); returns its dno.
    pub fn plpgsql_build_exc_special_var(
        name: &str,
        lineno: i32,
        typoid: Oid,
        collation: Oid,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `plpgsql_build_into_row(lineno, fieldnames, varnos)` — build the ROW
    /// datum for a multi-target INTO list; returns its dno.
    pub fn plpgsql_build_into_row(
        lineno: i32,
        fieldnames: Vec<String>,
        varnos: Vec<i32>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `make_scalar_list1(initial_name, initial_datum, lineno, location)` —
    /// wrap a single scalar into a ROW datum; returns its dno.
    pub fn make_scalar_list1(
        name: &str,
        scalar_dno: i32,
        lineno: i32,
        location: i32,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `plpgsql_Datums[dno]->dtype` — the datum kind of the datum at `dno`.
    pub fn datum_dtype(dno: i32) -> PLpgSQL_datum_type
);

seam_core::seam!(
    /// `(PLpgSQL_variable *) plpgsql_Datums[dno]` — an owned snapshot of the
    /// variable header at `dno`.
    pub fn datum_as_variable(dno: i32) -> PLpgSQL_variable
);

seam_core::seam!(
    /// `((PLpgSQL_var *) plpgsql_Datums[dno])->datatype->typoid`.
    pub fn var_datatype_typoid(dno: i32) -> Oid
);

seam_core::seam!(
    /// `((PLpgSQL_var *) plpgsql_Datums[dno])->cursor_explicit_expr != NULL`.
    pub fn var_has_explicit_expr(dno: i32) -> bool
);

seam_core::seam!(
    /// `((PLpgSQL_var *) plpgsql_Datums[dno])->cursor_explicit_argrow`.
    pub fn var_cursor_explicit_argrow(dno: i32) -> i32
);

seam_core::seam!(
    /// `((PLpgSQL_var *) plpgsql_Datums[dno])->refname`.
    pub fn var_refname(dno: i32) -> String
);

seam_core::seam!(
    /// The field names of a cursor's argument ROW datum (`argrow` dno).
    pub fn cursor_argrow_fieldnames(argrow: i32) -> Vec<String>
);

seam_core::seam!(
    /// `plpgsql_add_initdatums(NULL)` — forget the datums created before the
    /// block (the `decl_start` reset).
    pub fn plpgsql_add_initdatums_forget()
);

seam_core::seam!(
    /// `plpgsql_add_initdatums(&varnos)` — collect the dnos of variables
    /// declared in this block that need initialization.
    pub fn plpgsql_add_initdatums_collect() -> Vec<i32>
);

/// Decision returned by [`plpgsql_check_shadowvar`]: whether `name` shadows an
/// outer variable and, if so, whether the `plpgsql.extra_*` GUC bits request a
/// WARNING or an ERROR.  The grammar (which owns the scanner position needed for
/// `parser_errposition`) performs the actual `ereport`.
pub enum ShadowVarAction {
    /// No outer variable is shadowed, or the SHADOWVAR check is disabled.
    None,
    /// `extra_warnings & PLPGSQL_XCHECK_SHADOWVAR` (and not extra_errors) — warn.
    Warning,
    /// `extra_errors & PLPGSQL_XCHECK_SHADOWVAR` — raise an ERROR.
    Error,
}

seam_core::seam!(
    /// `plpgsql_check_shadowvar(name)` — decide whether `name` shadows an outer
    /// variable in the current compile namespace and, if so, whether the
    /// `extra_warnings`/`extra_errors` SHADOWVAR bits request a WARNING or ERROR.
    /// Mirrors the `decl_varname` action in `pl_gram.y`.
    pub fn plpgsql_check_shadowvar(name: &str) -> ShadowVarAction
);

seam_core::seam!(
    /// `plpgsql_parse_err_condition(condname)` — resolve a named SQL error
    /// condition into a `PLpgSQL_condition` (a list of SQLSTATEs).
    pub fn plpgsql_parse_err_condition(condname: &str) -> PgResult<PLpgSQL_condition>
);

seam_core::seam!(
    /// `plpgsql_recognize_err_condition(condname, allow_sqlstate)` — verify the
    /// condition name is recognized (RAISE's condition-name validation).
    pub fn plpgsql_recognize_err_condition(
        condname: &str,
        allow_sqlstate: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `check_assignable(plpgsql_Datums[dno], location)` — error if the datum is
    /// not assignable (e.g. CONST).
    pub fn check_assignable(dno: i32, location: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `mark_expr_as_assignment_source(expr, plpgsql_Datums[target_dno])` —
    /// record that `expr` feeds the assignment target `target_dno`.
    pub fn mark_expr_as_assignment_source(expr: &mut PLpgSQL_expr, target_dno: i32)
);

seam_core::seam!(
    /// PERFORM's source rewrite: overwrite the leading "PERFORM" of `expr.query`
    /// with " SELECT" and left-justify (`stmt_perform` action).
    pub fn perform_rewrite_query(expr: &mut PLpgSQL_expr)
);

seam_core::seam!(
    /// `check_sql_expr(stmt, parse_mode, cursorpos)` — raw-parse the SQL text
    /// for syntax only (no analysis), reporting errors with the right position.
    pub fn check_sql_expr(
        stmt: &str,
        parse_mode: RawParseMode,
        location: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `parse_datatype(string, location)` — parse a (non-`%TYPE`) datatype name
    /// into a `PLpgSQL_type`.
    pub fn parse_datatype(string: &str, location: i32) -> PgResult<Box<PLpgSQL_type>>
);

seam_core::seam!(
    /// `get_collation_oid(names, missing_ok)` — resolve a COLLATE name list.
    pub fn get_collation_oid(names: &[String], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `quote_identifier(ident)` — SQL-quote an identifier if needed (used to
    /// build the positional cursor-argument list text).
    pub fn quote_identifier(ident: &str) -> String
);

seam_core::seam!(
    /// `function_parse_error_transpose(prosrc)` (pg_proc.c): relocate a PL/pgSQL
    /// compile (syntax) error from the function body's internal cursor position
    /// to a cursor position in the original CREATE FUNCTION / DO command text,
    /// matching C's `plpgsql_compile_error_callback`.  Value-form: takes and
    /// returns the in-flight `PgError` (the SDK's `PgResult` error model).
    /// Installed by `backend-catalog-pg-proc` (which owns the body and the
    /// active-portal-text reader).
    pub fn function_parse_error_transpose(prosrc: &str, err: PgError) -> PgResult<PgError>
);
