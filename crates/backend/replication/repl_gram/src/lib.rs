//! Parser for the WalSender replication commands — `repl_gram.y`.
//!
//! `repl_gram.y` is a Bison LALR(1) grammar that recognizes the fixed set of
//! commands a WAL sender accepts (`IDENTIFY_SYSTEM`, `BASE_BACKUP`,
//! `START_REPLICATION`, `CREATE_REPLICATION_SLOT`, `DROP_REPLICATION_SLOT`,
//! `ALTER_REPLICATION_SLOT`, `READ_REPLICATION_SLOT`, `TIMELINE_HISTORY`,
//! `SHOW`, `UPLOAD_MANIFEST`) and builds the `replnodes.h` command node written
//! through `*replication_parse_result_p`.
//!
//! # Differences from the C source
//!
//!  * **No Bison runtime.** The LALR(1) state machine of `repl_gram.y` is small
//!    enough that a hand-written recursive-descent parser reproduces the *exact*
//!    accepted language. Every production is transcribed 1:1; each parser method
//!    names the production it implements.
//!  * **Tokens.** The scanner (`repl_scanner.l`) is a separate unit. The grammar
//!    obtains the token stream through the
//!    [`replication_lex_all`](::repl_scanner_seams::replication_lex_all)
//!    seam (panics until the scanner lands) and consumes it left-to-right with
//!    one token of lookahead, exactly as Bison drives `replication_yylex`.
//!  * **Error reporting.** `repl_gram.y`'s `ereport(ERROR, ERRCODE_SYNTAX_ERROR,
//!    ...)` (via `replication_yyerror`) and unwind becomes a recoverable
//!    [`PgError`](::utils_error::PgError) carrying `ERRCODE_SYNTAX_ERROR`,
//!    returned to the caller.
//!  * **`psprintf("%s.%s", ...)`** for the dotted `var_name` of `SHOW a.b.c` is
//!    ordinary string formatting, ported inline.
//!  * **Memory.** Bison's `palloc`/`pfree` (it allocates nothing across parser
//!    calls) become owned `String`/`Vec`; option-list growth uses `try_reserve`
//!    so a genuine OOM returns a recoverable error rather than aborting.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use ::utils_error::ereport;
use ::types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};

use ::parsenodes::{Boolean, DefElem, Integer, Node, StringNode, DEFELEM_UNSPEC};
use ::replication::repl_token::Token;
use ::replication::replnodes::{
    AlterReplicationSlotCmd, BaseBackupCmd, CreateReplicationSlotCmd, DropReplicationSlotCmd,
    ReadReplicationSlotCmd, ReplCommand, ReplicationKind, StartReplicationCmd, TimeLineHistoryCmd,
    VariableShowStmt,
};

use ::repl_scanner_seams::{
    replication_lex_all, replication_scanner_is_replication_command,
};

#[cfg(test)]
mod tests;

/// `replication_yyerror(..., message)` (`repl_scanner.l`): the WalSender
/// parser reports every syntax problem as `ereport(ERROR,
/// errcode(ERRCODE_SYNTAX_ERROR), errmsg_internal("%s", message))`. The port
/// returns that as a recoverable [`PgError`].
fn replication_yyerror(message: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg_internal(message)
        .into_error()
}

// ---------------------------------------------------------------------------
// Node-construction helpers (`nodes/makefuncs.h` / `nodes/value.h`) used by the
// grammar's semantic actions. Pure owned-value construction over the central
// parse-node vocabulary — the `makeString`, `makeInteger`, `makeBoolean`,
// `makeDefElem` of the C actions.
// ---------------------------------------------------------------------------

/// `makeString(str)` wrapped into a `Node` (the grammar uses
/// `(Node *) makeString(...)` for DefElem args).
fn make_string_node(s: String) -> Node {
    Node::String(StringNode { sval: Some(s) })
}

/// `makeInteger(i)` wrapped into a `Node`.
fn make_integer_node(i: i32) -> Node {
    Node::Integer(Integer { ival: i })
}

/// `makeBoolean(val)` wrapped into a `Node`.
fn make_boolean_node(val: bool) -> Node {
    Node::Boolean(Boolean { boolval: val })
}

/// `makeDefElem(name, arg, -1)`.
fn make_def_elem(name: String, arg: Option<Node>) -> DefElem {
    DefElem {
        defnamespace: None,
        defname: Some(name),
        arg: arg.map(alloc::boxed::Box::new),
        defaction: DEFELEM_UNSPEC,
        location: -1,
    }
}

/// Fallibly append a `DefElem` to an option list (`lappend` / `list_make1`).
fn list_append(list: &mut Vec<DefElem>, elem: DefElem) -> PgResult<()> {
    list.try_reserve(1)
        .map_err(|_| PgError::error("out of memory"))?;
    list.push(elem);
    Ok(())
}

// ===========================================================================
// Parser (repl_gram.y)
// ===========================================================================

/// Recursive-descent parser for the WalSender command grammar — the analogue of
/// the LALR(1) machine generated from `repl_gram.y`. It walks the token stream
/// produced by the scanner with one token of lookahead and builds a
/// [`ReplCommand`].
struct Parser {
    tokens: Vec<Token>,
    /// Cursor into `tokens`; `tokens[pos]` is the current lookahead (`yychar`).
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    /// Peek the current lookahead token. Past end of the stream the scanner
    /// already appended a [`Token::Eof`], so this always succeeds.
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    /// Consume and return the current token, advancing the cursor.
    fn bump(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    /// The generic Bison "syntax error". `repl_gram.y` has no custom message for
    /// production mismatches; the default `replication_yyerror(..., "syntax
    /// error")` fires with `ERRCODE_SYNTAX_ERROR`.
    fn syntax_error(&self) -> PgError {
        replication_yyerror("syntax error")
    }

    /// Expect a single, specific character token (`(`, `)`, `,`, `.`).
    fn expect_char(&mut self, want: u8) -> PgResult<()> {
        match self.bump() {
            Token::Char(c) if c == want => Ok(()),
            _ => Err(self.syntax_error()),
        }
    }

    /// `firstcmd: command opt_semicolon` — parse exactly one command, then an
    /// optional `;`, then end of input.
    fn parse_first_cmd(&mut self) -> PgResult<ReplCommand> {
        let cmd = self.parse_command()?;
        self.parse_opt_semicolon();
        // After `command opt_semicolon` Bison expects `$end`; trailing tokens
        // are a syntax error.
        if *self.peek() != Token::Eof {
            return Err(self.syntax_error());
        }
        Ok(cmd)
    }

    /// `opt_semicolon: ';' | /* EMPTY */`.
    fn parse_opt_semicolon(&mut self) {
        if matches!(self.peek(), Token::Char(b';')) {
            self.bump();
        }
    }

    /// `command:` — dispatch on the first keyword to the matching production.
    fn parse_command(&mut self) -> PgResult<ReplCommand> {
        match self.peek() {
            Token::IdentifySystem => self.parse_identify_system(),
            Token::BaseBackup => self.parse_base_backup(),
            Token::StartReplication => self.parse_start_replication(),
            Token::CreateReplicationSlot => self.parse_create_replication_slot(),
            Token::DropReplicationSlot => self.parse_drop_replication_slot(),
            Token::AlterReplicationSlot => self.parse_alter_replication_slot(),
            Token::ReadReplicationSlot => self.parse_read_replication_slot(),
            Token::TimelineHistory => self.parse_timeline_history(),
            Token::Show => self.parse_show(),
            Token::UploadManifest => self.parse_upload_manifest(),
            _ => Err(self.syntax_error()),
        }
    }

    /// `identify_system: K_IDENTIFY_SYSTEM` -> `makeNode(IdentifySystemCmd)`.
    fn parse_identify_system(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_IDENTIFY_SYSTEM
        Ok(ReplCommand::IdentifySystem)
    }

    /// `read_replication_slot: K_READ_REPLICATION_SLOT var_name`.
    fn parse_read_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_READ_REPLICATION_SLOT
        let slotname = self.parse_var_name()?;
        Ok(ReplCommand::ReadReplicationSlot(ReadReplicationSlotCmd {
            slotname: Some(slotname),
        }))
    }

    /// `show: K_SHOW var_name` -> `VariableShowStmt { name }`.
    fn parse_show(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_SHOW
        let name = self.parse_var_name()?;
        Ok(ReplCommand::VariableShow(VariableShowStmt { name }))
    }

    /// `var_name: IDENT | var_name '.' IDENT { psprintf("%s.%s", $1, $3) }`.
    fn parse_var_name(&mut self) -> PgResult<String> {
        // First component must be an IDENT.
        let mut name = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error()),
        };
        // Left-recursive `var_name '.' IDENT`: fold each `.IDENT` suffix.
        while matches!(self.peek(), Token::Char(b'.')) {
            self.bump(); // '.'
            let next = match self.bump() {
                Token::Ident(s) => s,
                _ => return Err(self.syntax_error()),
            };
            name = format!("{name}.{next}");
        }
        Ok(name)
    }

    /// `base_backup:`
    ///   `K_BASE_BACKUP '(' generic_option_list ')'`
    ///   `| K_BASE_BACKUP`
    fn parse_base_backup(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_BASE_BACKUP
        let mut options = Vec::new();
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump(); // '('
            options = self.parse_generic_option_list()?;
            self.expect_char(b')')?;
        }
        Ok(ReplCommand::BaseBackup(BaseBackupCmd { options }))
    }

    /// `create_replication_slot:`
    ///   `K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_PHYSICAL create_slot_options`
    ///   `| K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_LOGICAL IDENT create_slot_options`
    fn parse_create_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_CREATE_REPLICATION_SLOT
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error()),
        };
        let temporary = self.parse_opt_temporary();
        match self.bump() {
            Token::Physical => {
                let options = self.parse_create_slot_options()?;
                Ok(ReplCommand::CreateReplicationSlot(
                    CreateReplicationSlotCmd {
                        kind: ReplicationKind::REPLICATION_KIND_PHYSICAL,
                        slotname: Some(slotname),
                        temporary,
                        plugin: None,
                        options,
                    },
                ))
            }
            Token::Logical => {
                let plugin = match self.bump() {
                    Token::Ident(s) => s,
                    _ => return Err(self.syntax_error()),
                };
                let options = self.parse_create_slot_options()?;
                Ok(ReplCommand::CreateReplicationSlot(
                    CreateReplicationSlotCmd {
                        kind: ReplicationKind::REPLICATION_KIND_LOGICAL,
                        slotname: Some(slotname),
                        temporary,
                        plugin: Some(plugin),
                        options,
                    },
                ))
            }
            _ => Err(self.syntax_error()),
        }
    }

    /// `create_slot_options:`
    ///   `'(' generic_option_list ')'`
    ///   `| create_slot_legacy_opt_list`
    fn parse_create_slot_options(&mut self) -> PgResult<Vec<DefElem>> {
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump(); // '('
            let list = self.parse_generic_option_list()?;
            self.expect_char(b')')?;
            Ok(list)
        } else {
            self.parse_create_slot_legacy_opt_list()
        }
    }

    /// `create_slot_legacy_opt_list:`
    ///   `create_slot_legacy_opt_list create_slot_legacy_opt`
    ///   `| /* EMPTY */`
    ///
    /// A possibly-empty run of legacy keyword options, each lowered to a DefElem.
    fn parse_create_slot_legacy_opt_list(&mut self) -> PgResult<Vec<DefElem>> {
        let mut list = Vec::new();
        loop {
            let elem = match self.peek() {
                // K_EXPORT_SNAPSHOT -> snapshot = "export"
                Token::ExportSnapshot => {
                    self.bump();
                    make_def_elem(
                        String::from("snapshot"),
                        Some(make_string_node(String::from("export"))),
                    )
                }
                // K_NOEXPORT_SNAPSHOT -> snapshot = "nothing"
                Token::NoexportSnapshot => {
                    self.bump();
                    make_def_elem(
                        String::from("snapshot"),
                        Some(make_string_node(String::from("nothing"))),
                    )
                }
                // K_USE_SNAPSHOT -> snapshot = "use"
                Token::UseSnapshot => {
                    self.bump();
                    make_def_elem(
                        String::from("snapshot"),
                        Some(make_string_node(String::from("use"))),
                    )
                }
                // K_RESERVE_WAL -> reserve_wal = true
                Token::ReserveWal => {
                    self.bump();
                    make_def_elem(String::from("reserve_wal"), Some(make_boolean_node(true)))
                }
                // K_TWO_PHASE -> two_phase = true
                Token::TwoPhase => {
                    self.bump();
                    make_def_elem(String::from("two_phase"), Some(make_boolean_node(true)))
                }
                // /* EMPTY */ — end of the legacy option run.
                _ => break,
            };
            list_append(&mut list, elem)?;
        }
        Ok(list)
    }

    /// `drop_replication_slot:`
    ///   `K_DROP_REPLICATION_SLOT IDENT`
    ///   `| K_DROP_REPLICATION_SLOT IDENT K_WAIT`
    fn parse_drop_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_DROP_REPLICATION_SLOT
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error()),
        };
        let wait = if matches!(self.peek(), Token::Wait) {
            self.bump(); // K_WAIT
            true
        } else {
            false
        };
        Ok(ReplCommand::DropReplicationSlot(DropReplicationSlotCmd {
            slotname: Some(slotname),
            wait,
        }))
    }

    /// `alter_replication_slot:`
    ///   `K_ALTER_REPLICATION_SLOT IDENT '(' generic_option_list ')'`
    fn parse_alter_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_ALTER_REPLICATION_SLOT
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error()),
        };
        self.expect_char(b'(')?;
        let options = self.parse_generic_option_list()?;
        self.expect_char(b')')?;
        Ok(ReplCommand::AlterReplicationSlot(AlterReplicationSlotCmd {
            slotname: Some(slotname),
            options,
        }))
    }

    /// `start_replication:`
    ///   `K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline`
    /// `start_logical_replication:`
    ///   `K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options`
    ///
    /// Both productions begin `K_START_REPLICATION`; they diverge on whether the
    /// slot clause is followed (eventually) by `K_LOGICAL`. The LALR table
    /// resolves this with lookahead; the recursive-descent equivalent reads
    /// `opt_slot` then branches on `K_LOGICAL` (logical) vs `opt_physical RECPTR`
    /// (physical).
    fn parse_start_replication(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_START_REPLICATION

        // `opt_slot: K_SLOT IDENT | /* EMPTY */`  (shared prefix of both rules:
        // physical's `opt_slot` and logical's mandatory `K_SLOT IDENT`).
        let had_slot_keyword = matches!(self.peek(), Token::Slot);
        let slotname = if had_slot_keyword {
            self.bump(); // K_SLOT
            match self.bump() {
                Token::Ident(s) => Some(s),
                _ => return Err(self.syntax_error()),
            }
        } else {
            None
        };

        // Logical form: `K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR
        // plugin_options`. The `start_logical_replication` production REQUIRES
        // the `K_SLOT IDENT` clause, so a bare `START_REPLICATION LOGICAL ...`
        // matches no production and is a syntax error, exactly as Bison reports.
        if matches!(self.peek(), Token::Logical) {
            if !had_slot_keyword {
                return Err(self.syntax_error());
            }
            self.bump(); // K_LOGICAL
            let startpoint = self.expect_recptr()?;
            let options = self.parse_plugin_options()?;
            return Ok(ReplCommand::StartReplication(StartReplicationCmd {
                kind: ReplicationKind::REPLICATION_KIND_LOGICAL,
                slotname,
                startpoint,
                timeline: 0,
                options,
            }));
        }

        // Physical form: `opt_slot opt_physical RECPTR opt_timeline`.
        // `opt_physical: K_PHYSICAL | /* EMPTY */`
        if matches!(self.peek(), Token::Physical) {
            self.bump(); // K_PHYSICAL
        }
        let startpoint = self.expect_recptr()?;
        let timeline = self.parse_opt_timeline()?;
        Ok(ReplCommand::StartReplication(StartReplicationCmd {
            kind: ReplicationKind::REPLICATION_KIND_PHYSICAL,
            slotname,
            startpoint,
            timeline,
            options: Vec::new(),
        }))
    }

    /// Expect and consume a `RECPTR` token.
    fn expect_recptr(&mut self) -> PgResult<types_core::primitive::XLogRecPtr> {
        match self.bump() {
            Token::Recptr(r) => Ok(r),
            _ => Err(self.syntax_error()),
        }
    }

    /// `opt_timeline: K_TIMELINE UCONST { if ($2 <= 0) ereport(... "invalid
    /// timeline %u" ...); } | /* EMPTY */ { 0 }`.
    fn parse_opt_timeline(&mut self) -> PgResult<types_core::primitive::TimeLineID> {
        if matches!(self.peek(), Token::Timeline) {
            self.bump(); // K_TIMELINE
            let val = self.expect_uconst()?;
            // C: `if ($2 <= 0)` — `$2` is uint32 so this is `== 0`.
            if val == 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("invalid timeline {val}"))
                    .into_error());
            }
            Ok(val)
        } else {
            Ok(0)
        }
    }

    /// `opt_temporary: K_TEMPORARY { true } | /* EMPTY */ { false }`.
    fn parse_opt_temporary(&mut self) -> bool {
        if matches!(self.peek(), Token::Temporary) {
            self.bump(); // K_TEMPORARY
            true
        } else {
            false
        }
    }

    /// Expect and consume a `UCONST` token.
    fn expect_uconst(&mut self) -> PgResult<u32> {
        match self.bump() {
            Token::Uconst(u) => Ok(u),
            _ => Err(self.syntax_error()),
        }
    }

    /// `timeline_history: K_TIMELINE_HISTORY UCONST` — note the `$2 <= 0` guard
    /// `ereport(... "invalid timeline %u" ...)`.
    fn parse_timeline_history(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_TIMELINE_HISTORY
        let timeline = self.expect_uconst()?;
        if timeline == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid timeline {timeline}"))
                .into_error());
        }
        Ok(ReplCommand::TimeLineHistory(TimeLineHistoryCmd { timeline }))
    }

    /// `upload_manifest: K_UPLOAD_MANIFEST` -> `makeNode(UploadManifestCmd)`.
    fn parse_upload_manifest(&mut self) -> PgResult<ReplCommand> {
        self.bump(); // K_UPLOAD_MANIFEST
        Ok(ReplCommand::UploadManifest)
    }

    /// `plugin_options:`
    ///   `'(' plugin_opt_list ')'`
    ///   `| /* EMPTY */ { NIL }`
    fn parse_plugin_options(&mut self) -> PgResult<Vec<DefElem>> {
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump(); // '('
            let list = self.parse_plugin_opt_list()?;
            self.expect_char(b')')?;
            Ok(list)
        } else {
            Ok(Vec::new())
        }
    }

    /// `plugin_opt_list:`
    ///   `plugin_opt_elem { list_make1($1) }`
    ///   `| plugin_opt_list ',' plugin_opt_elem { lappend($1, $3) }`
    fn parse_plugin_opt_list(&mut self) -> PgResult<Vec<DefElem>> {
        let mut list = Vec::new();
        let first = self.parse_plugin_opt_elem()?;
        list_append(&mut list, first)?;
        while matches!(self.peek(), Token::Char(b',')) {
            self.bump(); // ','
            let elem = self.parse_plugin_opt_elem()?;
            list_append(&mut list, elem)?;
        }
        Ok(list)
    }

    /// `plugin_opt_elem: IDENT plugin_opt_arg { makeDefElem($1, $2, -1) }`.
    fn parse_plugin_opt_elem(&mut self) -> PgResult<DefElem> {
        let name = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error()),
        };
        let arg = self.parse_plugin_opt_arg();
        Ok(make_def_elem(name, arg))
    }

    /// `plugin_opt_arg: SCONST { (Node *) makeString($1) } | /* EMPTY */ { NULL }`.
    fn parse_plugin_opt_arg(&mut self) -> Option<Node> {
        if matches!(self.peek(), Token::Sconst(_)) {
            match self.bump() {
                Token::Sconst(s) => Some(make_string_node(s)),
                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    /// `generic_option_list:`
    ///   `generic_option_list ',' generic_option { lappend($1, $3) }`
    ///   `| generic_option { list_make1($1) }`
    fn parse_generic_option_list(&mut self) -> PgResult<Vec<DefElem>> {
        let mut list = Vec::new();
        let first = self.parse_generic_option()?;
        list_append(&mut list, first)?;
        while matches!(self.peek(), Token::Char(b',')) {
            self.bump(); // ','
            let elem = self.parse_generic_option()?;
            list_append(&mut list, elem)?;
        }
        Ok(list)
    }

    /// `generic_option:`
    ///   `ident_or_keyword                 { makeDefElem($1, NULL, -1) }`
    ///   `| ident_or_keyword IDENT         { makeDefElem($1, (Node *) makeString($2), -1) }`
    ///   `| ident_or_keyword SCONST        { makeDefElem($1, (Node *) makeString($2), -1) }`
    ///   `| ident_or_keyword UCONST        { makeDefElem($1, (Node *) makeInteger($2), -1) }`
    fn parse_generic_option(&mut self) -> PgResult<DefElem> {
        let name = self.parse_ident_or_keyword()?;
        let arg = match self.peek() {
            Token::Ident(_) => match self.bump() {
                Token::Ident(s) => Some(make_string_node(s)),
                _ => unreachable!(),
            },
            Token::Sconst(_) => match self.bump() {
                Token::Sconst(s) => Some(make_string_node(s)),
                _ => unreachable!(),
            },
            Token::Uconst(_) => match self.bump() {
                // makeInteger($2): C `$2` is uint32, stored into Integer.ival
                // (an `int`); reproduce the bit-preserving narrowing to i32.
                Token::Uconst(u) => Some(make_integer_node(u as i32)),
                _ => unreachable!(),
            },
            // No value argument follows.
            _ => None,
        };
        Ok(make_def_elem(name, arg))
    }

    /// `ident_or_keyword:`
    ///   `IDENT { $1 }`
    ///   `| K_BASE_BACKUP { "base_backup" }`
    ///   `| ... (every keyword folds to its lowercase spelling) ...`
    fn parse_ident_or_keyword(&mut self) -> PgResult<String> {
        let s = match self.bump() {
            Token::Ident(s) => s,
            Token::BaseBackup => String::from("base_backup"),
            Token::IdentifySystem => String::from("identify_system"),
            Token::Show => String::from("show"),
            Token::StartReplication => String::from("start_replication"),
            Token::CreateReplicationSlot => String::from("create_replication_slot"),
            Token::DropReplicationSlot => String::from("drop_replication_slot"),
            Token::AlterReplicationSlot => String::from("alter_replication_slot"),
            Token::TimelineHistory => String::from("timeline_history"),
            Token::Wait => String::from("wait"),
            Token::Timeline => String::from("timeline"),
            Token::Physical => String::from("physical"),
            Token::Logical => String::from("logical"),
            Token::Slot => String::from("slot"),
            Token::ReserveWal => String::from("reserve_wal"),
            Token::Temporary => String::from("temporary"),
            Token::TwoPhase => String::from("two_phase"),
            Token::ExportSnapshot => String::from("export_snapshot"),
            Token::NoexportSnapshot => String::from("noexport_snapshot"),
            Token::UseSnapshot => String::from("use_snapshot"),
            Token::UploadManifest => String::from("upload_manifest"),
            // K_READ_REPLICATION_SLOT is the one command keyword NOT in the
            // `ident_or_keyword` production, so it is a syntax error here.
            _ => return Err(self.syntax_error()),
        };
        Ok(s)
    }
}

// ===========================================================================
// Public entry points.
// ===========================================================================

/// Parse a single WalSender replication command string into a [`ReplCommand`].
///
/// The analogue of the C call sequence: `replication_scanner_init(cmd_string,
/// &scanner)` then `replication_yyparse(&result, scanner)` (the Bison entry),
/// then `replication_scanner_finish(scanner)`. On a lexical or grammatical
/// problem it returns an `Err` carrying `ERRCODE_SYNTAX_ERROR`, exactly as the C
/// parser's `ereport(ERROR)` would (but recoverably, as a value).
///
/// The scanner is reached through the `replication_lex_all` seam, which panics
/// until the `backend-replication-repl-scanner` unit lands.
pub fn replication_parse(cmd_string: &str) -> PgResult<ReplCommand> {
    let tokens = replication_lex_all::call(cmd_string)?;
    parse_tokens(tokens)
}

/// Run the grammar over an already-lexed token stream (terminated by
/// [`Token::Eof`]). Split out from [`replication_parse`] so the grammar can be
/// driven without the scanner seam (the parser's `replication_yyparse` body
/// proper, separate from the scanner driver).
pub fn parse_tokens(tokens: Vec<Token>) -> PgResult<ReplCommand> {
    let mut parser = Parser::new(tokens);
    parser.parse_first_cmd()
}

/// `replication_scanner_is_replication_command(yyscanner)` — the WalSender-vs-SQL
/// gate. Reports whether the first token of `cmd_string` is one of the
/// replication-command keywords. Delegates to the scanner seam.
pub fn is_replication_command(cmd_string: &str) -> PgResult<bool> {
    replication_scanner_is_replication_command::call(cmd_string)
}
