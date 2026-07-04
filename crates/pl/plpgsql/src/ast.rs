// plpgsql.h compile-output structures, phase-1 subset.
//
// Std collections justification (AGENTS.md rule 3): the compiled function is
// a cold, backend-lifetime artifact mirroring C's dedicated func_cxt (freed
// wholesale on recompile — Drop here); it is outside context accounting and
// never allocated per row.
use types_core::Oid;

pub type Dno = i32;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TypeKind {
    Scalar,
    Rec,
    Pseudo,
}

// PLpgSQL_type; typinput resolved once at compile (C fmgr_info into func_cxt).
#[derive(Clone, Debug)]
pub struct PlType {
    pub typoid: Oid,
    pub ttype: TypeKind,
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: i8,
    pub collation: Oid,
    pub typisarray: bool,
    pub atttypmod: i32,
    pub typinput: Oid,
    pub typioparam: Oid,
}

// PLpgSQL_expr. `ns` indexes the function's namespace arena (the item
// visible at parse time); plan/simple-expr state lives in exec-side slots
// keyed by expr_id (interior runtime state kept out of the shared AST).
#[derive(Debug)]
pub struct PlExpr {
    pub query: String,
    pub parse_mode: parser_seams::RawParseMode,
    pub ns: i32,
    pub expr_id: u32,
    /// RAW_PARSE_PLPGSQL_ASSIGN target datum (C target_param), -1 if none.
    pub target_param: Dno,
}

pub const PROMISE_NONE: i32 = 0;
pub const PROMISE_TG_NAME: i32 = 1;
pub const PROMISE_TG_WHEN: i32 = 2;
pub const PROMISE_TG_LEVEL: i32 = 3;
pub const PROMISE_TG_OP: i32 = 4;
pub const PROMISE_TG_RELID: i32 = 5;
pub const PROMISE_TG_TABLE_NAME: i32 = 6;
pub const PROMISE_TG_TABLE_SCHEMA: i32 = 7;
pub const PROMISE_TG_NARGS: i32 = 8;
pub const PROMISE_TG_ARGV: i32 = 9;

#[derive(Debug)]
pub struct PlVar {
    pub dno: Dno,
    pub refname: String,
    pub lineno: i32,
    pub datatype: PlType,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<PlExpr>,
    pub promise: i32,
    pub cursor_explicit_expr: Option<PlExpr>,
    pub cursor_explicit_argrow: Dno,
    pub cursor_options: i32,
}

#[derive(Debug)]
pub struct PlRow {
    pub dno: Dno,
    pub refname: String,
    pub lineno: i32,
    pub fieldnames: Vec<String>,
    pub varnos: Vec<Dno>,
}

#[derive(Debug)]
pub struct PlRec {
    pub dno: Dno,
    pub refname: String,
    pub lineno: i32,
    /// RECORDOID unless declared with a named composite type (%ROWTYPE etc.).
    pub rectypeid: Oid,
    pub datatype: Option<PlType>,
}

#[derive(Debug)]
pub struct PlRecField {
    pub dno: Dno,
    pub recparentno: Dno,
    pub fieldname: String,
}

#[derive(Debug)]
pub enum PlDatum {
    Var(PlVar),
    Row(PlRow),
    Rec(PlRec),
    RecField(PlRecField),
}

impl PlDatum {
    pub fn dno(&self) -> Dno {
        match self {
            PlDatum::Var(v) => v.dno,
            PlDatum::Row(r) => r.dno,
            PlDatum::Rec(r) => r.dno,
            PlDatum::RecField(f) => f.dno,
        }
    }

    pub fn refname(&self) -> &str {
        match self {
            PlDatum::Var(v) => &v.refname,
            PlDatum::Row(r) => &r.refname,
            PlDatum::Rec(r) => &r.refname,
            PlDatum::RecField(f) => &f.fieldname,
        }
    }
}

// PLpgSQL_nsitem, arena-index chained (C is pointer-chained).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NsType {
    Label,
    Var,
    Row,
    Rec,
}

#[derive(Debug)]
pub struct NsItem {
    pub itemtype: NsType,
    pub itemno: i32,
    pub name: String,
    pub prev: i32,
}

// FetchDirection (parsenodes.h) + FETCH_ALL.
pub const FETCH_FORWARD: i32 = 0;
pub const FETCH_BACKWARD: i32 = 1;
pub const FETCH_ABSOLUTE: i32 = 2;
pub const FETCH_RELATIVE: i32 = 3;
pub const FETCH_ALL: i64 = i64::MAX;

pub const GETDIAG_ROW_COUNT: i32 = 0;
pub const GETDIAG_ROUTINE_OID: i32 = 1;
pub const GETDIAG_CONTEXT: i32 = 2;
pub const GETDIAG_ERROR_CONTEXT: i32 = 3;
pub const GETDIAG_ERROR_DETAIL: i32 = 4;
pub const GETDIAG_ERROR_HINT: i32 = 5;
pub const GETDIAG_RETURNED_SQLSTATE: i32 = 6;
pub const GETDIAG_COLUMN_NAME: i32 = 7;
pub const GETDIAG_CONSTRAINT_NAME: i32 = 8;
pub const GETDIAG_DATATYPE_NAME: i32 = 9;
pub const GETDIAG_MESSAGE_TEXT: i32 = 10;
pub const GETDIAG_TABLE_NAME: i32 = 11;
pub const GETDIAG_SCHEMA_NAME: i32 = 12;

#[derive(Debug)]
pub struct GetDiagItem {
    pub kind: i32,
    pub target: Dno,
}

// Loop bodies carry the enclosing label for EXIT/CONTINUE matching.
#[derive(Debug)]
pub enum PlStmt {
    Block(PlBlock),
    Assign {
        lineno: i32,
        varno: Dno,
        expr: PlExpr,
    },
    If {
        lineno: i32,
        cond: PlExpr,
        then_body: Vec<PlStmt>,
        elsifs: Vec<(PlExpr, Vec<PlStmt>)>,
        else_body: Option<Vec<PlStmt>>,
    },
    Loop {
        lineno: i32,
        label: Option<String>,
        body: Vec<PlStmt>,
    },
    While {
        lineno: i32,
        label: Option<String>,
        cond: PlExpr,
        body: Vec<PlStmt>,
    },
    ForI {
        lineno: i32,
        label: Option<String>,
        var: Dno,
        lower: PlExpr,
        upper: PlExpr,
        step: Option<PlExpr>,
        reverse: bool,
        body: Vec<PlStmt>,
    },
    ForS {
        lineno: i32,
        label: Option<String>,
        /// Rec or Row datum receiving each result row.
        var: Dno,
        query: PlExpr,
        body: Vec<PlStmt>,
    },
    Case {
        lineno: i32,
        t_expr: Option<PlExpr>,
        t_varno: Dno,
        whens: Vec<(PlExpr, Vec<PlStmt>)>,
        have_else: bool,
        else_stmts: Vec<PlStmt>,
    },
    ForEachA {
        lineno: i32,
        label: Option<String>,
        varno: Dno,
        slice: i32,
        expr: PlExpr,
        body: Vec<PlStmt>,
    },
    ExitContinue {
        lineno: i32,
        is_exit: bool,
        label: Option<String>,
        cond: Option<PlExpr>,
    },
    Return {
        lineno: i32,
        expr: Option<PlExpr>,
        retvarno: Dno,
    },
    Raise {
        lineno: i32,
        elog_level: i32,
        condname: Option<String>,
        message: Option<String>,
        params: Vec<PlExpr>,
        options: Vec<RaiseOption>,
    },
    Assert {
        lineno: i32,
        cond: PlExpr,
        message: Option<PlExpr>,
    },
    ExecSql {
        lineno: i32,
        sqlstmt: PlExpr,
        mod_stmt: bool,
        into: bool,
        strict: bool,
        target: Dno,
    },
    Perform {
        lineno: i32,
        expr: PlExpr,
    },
    Call {
        lineno: i32,
        expr: PlExpr,
        is_call: bool,
    },
    Commit {
        lineno: i32,
        chain: bool,
    },
    Rollback {
        lineno: i32,
        chain: bool,
    },
    GetDiag {
        lineno: i32,
        is_stacked: bool,
        items: Vec<GetDiagItem>,
    },
    DynExecute {
        lineno: i32,
        query: PlExpr,
        into: bool,
        strict: bool,
        target: Dno,
        params: Vec<PlExpr>,
    },
    ReturnNext {
        lineno: i32,
        expr: Option<PlExpr>,
        retvarno: Dno,
    },
    ReturnQuery {
        lineno: i32,
        query: Option<PlExpr>,
        dynquery: Option<PlExpr>,
        params: Vec<PlExpr>,
    },
    Open {
        lineno: i32,
        curvar: Dno,
        cursor_options: i32,
        argquery: Option<PlExpr>,
        query: Option<PlExpr>,
        dynquery: Option<PlExpr>,
        params: Vec<PlExpr>,
    },
    Fetch {
        lineno: i32,
        target: Dno,
        curvar: Dno,
        direction: i32,
        how_many: i64,
        expr: Option<PlExpr>,
        is_move: bool,
        returns_multiple_rows: bool,
    },
    Close {
        lineno: i32,
        curvar: Dno,
    },
    ForC {
        lineno: i32,
        label: Option<String>,
        var: Dno,
        curvar: Dno,
        argquery: Option<PlExpr>,
        body: Vec<PlStmt>,
    },
    DynForS {
        lineno: i32,
        label: Option<String>,
        var: Dno,
        query: PlExpr,
        params: Vec<PlExpr>,
        body: Vec<PlStmt>,
    },
}

/// PLpgSQL_condition; sqlerrstate 0 is OTHERS.
#[derive(Debug)]
pub struct PlCondition {
    pub sqlerrstate: types_error::SqlState,
    pub condname: String,
}

#[derive(Debug)]
pub struct PlException {
    pub lineno: i32,
    pub conditions: Vec<PlCondition>,
    pub action: Vec<PlStmt>,
}

#[derive(Debug)]
pub struct ExceptionBlock {
    pub sqlstate_varno: Dno,
    pub sqlerrm_varno: Dno,
    pub exc_list: Vec<PlException>,
}

pub const PLPGSQL_RAISEOPTION_ERRCODE: i32 = 0;
pub const PLPGSQL_RAISEOPTION_MESSAGE: i32 = 1;
pub const PLPGSQL_RAISEOPTION_DETAIL: i32 = 2;
pub const PLPGSQL_RAISEOPTION_HINT: i32 = 3;
pub const PLPGSQL_RAISEOPTION_COLUMN: i32 = 4;
pub const PLPGSQL_RAISEOPTION_CONSTRAINT: i32 = 5;
pub const PLPGSQL_RAISEOPTION_DATATYPE: i32 = 6;
pub const PLPGSQL_RAISEOPTION_TABLE: i32 = 7;
pub const PLPGSQL_RAISEOPTION_SCHEMA: i32 = 8;

#[derive(Debug)]
pub struct RaiseOption {
    pub opt_type: i32,
    pub expr: PlExpr,
}

#[derive(Debug)]
pub struct PlBlock {
    pub lineno: i32,
    pub label: Option<String>,
    pub body: Vec<PlStmt>,
    /// dnos of block-local variables to initialize on entry.
    pub initvarnos: Vec<Dno>,
    pub exceptions: Option<ExceptionBlock>,
}

pub fn stmt_lineno(s: &PlStmt) -> i32 {
    match s {
        PlStmt::Block(b) => b.lineno,
        PlStmt::Assign { lineno, .. }
        | PlStmt::If { lineno, .. }
        | PlStmt::Loop { lineno, .. }
        | PlStmt::While { lineno, .. }
        | PlStmt::ForI { lineno, .. }
        | PlStmt::ForS { lineno, .. }
        | PlStmt::ExitContinue { lineno, .. }
        | PlStmt::Return { lineno, .. }
        | PlStmt::Raise { lineno, .. }
        | PlStmt::Assert { lineno, .. }
        | PlStmt::ExecSql { lineno, .. }
        | PlStmt::Perform { lineno, .. }
        | PlStmt::Call { lineno, .. }
        | PlStmt::Commit { lineno, .. }
        | PlStmt::Rollback { lineno, .. }
        | PlStmt::GetDiag { lineno, .. }
        | PlStmt::Case { lineno, .. }
        | PlStmt::ForEachA { lineno, .. }
        | PlStmt::ReturnNext { lineno, .. }
        | PlStmt::ReturnQuery { lineno, .. }
        | PlStmt::Open { lineno, .. }
        | PlStmt::Fetch { lineno, .. }
        | PlStmt::Close { lineno, .. }
        | PlStmt::ForC { lineno, .. }
        | PlStmt::DynForS { lineno, .. }
        | PlStmt::DynExecute { lineno, .. } => *lineno,
    }
}

pub fn stmt_typename(s: &PlStmt) -> &'static str {
    // plpgsql_stmt_typename (pl_funcs.c) subset — context-line vocabulary.
    match s {
        PlStmt::Block(_) => "statement block",
        PlStmt::Assign { .. } => "assignment",
        PlStmt::If { .. } => "IF",
        PlStmt::Loop { .. } => "LOOP",
        PlStmt::While { .. } => "WHILE",
        PlStmt::ForI { .. } => "FOR with integer loop variable",
        PlStmt::ForS { .. } => "FOR over SELECT rows",
        PlStmt::Case { .. } => "CASE",
        PlStmt::ForEachA { .. } => "FOREACH over array",
        PlStmt::ExitContinue { is_exit: true, .. } => "EXIT",
        PlStmt::ExitContinue { is_exit: false, .. } => "CONTINUE",
        PlStmt::Return { .. } => "RETURN",
        PlStmt::Raise { .. } => "RAISE",
        PlStmt::Assert { .. } => "ASSERT",
        PlStmt::ExecSql { .. } => "SQL statement",
        PlStmt::Perform { .. } => "PERFORM",
        PlStmt::Call { is_call: true, .. } => "CALL",
        PlStmt::Call { is_call: false, .. } => "DO",
        PlStmt::Commit { .. } => "COMMIT",
        PlStmt::Rollback { .. } => "ROLLBACK",
        PlStmt::GetDiag { is_stacked: false, .. } => "GET DIAGNOSTICS",
        PlStmt::GetDiag { is_stacked: true, .. } => "GET STACKED DIAGNOSTICS",
        PlStmt::DynExecute { .. } => "EXECUTE",
        PlStmt::ReturnNext { .. } => "RETURN NEXT",
        PlStmt::ReturnQuery { .. } => "RETURN QUERY",
        PlStmt::Open { .. } => "OPEN",
        PlStmt::Fetch { is_move: false, .. } => "FETCH",
        PlStmt::Fetch { is_move: true, .. } => "MOVE",
        PlStmt::Close { .. } => "CLOSE",
        PlStmt::ForC { .. } => "FOR over cursor",
        PlStmt::DynForS { .. } => "FOR over EXECUTE statement",
    }
}

// fn_is_trigger; the cache entry must carry trigger-ness so the handler
// picks the right exec path on cache hits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FnTrigger {
    NotTrigger,
    DmlTrigger,
    EventTrigger { tg_event_varno: Dno, tg_tag_varno: Dno },
}

// PLpgSQL_function (phase-1 fields).
#[derive(Debug)]
pub struct PlFunction {
    pub fn_signature: String,
    pub fn_oid: Oid,
    pub fn_xmin: u32,
    pub fn_tid: (u32, u16),
    pub fn_input_collation: Oid,
    pub fn_rettype: Oid,
    pub fn_rettyplen: i16,
    pub fn_retbyval: bool,
    pub fn_retistuple: bool,
    pub fn_retisdomain: bool,
    pub fn_retset: bool,
    pub fn_readonly: bool,
    pub fn_prokind: i8,
    pub fn_is_trigger: FnTrigger,
    pub fn_nargs: i16,
    /// All signature args in order (IN and OUT); $n resolves through these.
    pub fn_argvarnos: Vec<Dno>,
    /// Parallel to fn_argvarnos: does the arg consume an fcinfo slot.
    pub fn_arg_is_input: Vec<bool>,
    pub new_varno: Dno,
    pub old_varno: Dno,
    pub found_varno: Dno,
    pub out_param_varno: Dno,
    pub datums: Vec<PlDatum>,
    pub ns: Vec<NsItem>,
    pub action: PlBlock,
    pub resolve_option: i32,
    pub print_strict_params: bool,
    pub nstatements: u32,
    /// Every expr's id (exec-side plan-table cleanup on recompile).
    pub expr_ids: Vec<u32>,
}
