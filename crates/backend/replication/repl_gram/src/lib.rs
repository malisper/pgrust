//! Port of `repl_gram.y`: hand-written recursive-descent parser (no Bison
//! runtime), one production per method. Tokens come from the sibling
//! `repl_scanner` crate as a direct dependency.
//!
//! C-divergence: `repl_gram.y`'s option lists are `types_nodes::DefElem`,
//! which needs a caller-supplied `Mcx`; this leaf grammar has no such caller
//! yet, so options are a local [`ReplOption`]/[`ReplOptionArg`] instead.

#![allow(non_snake_case)]

use std::string::String;
use std::vec::Vec;

use elog::ereport;
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};

use repl_scanner::Token;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationKind {
    REPLICATION_KIND_PHYSICAL,
    REPLICATION_KIND_LOGICAL,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplOptionArg {
    Str(String),
    Int(i32),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReplOption {
    pub name: String,
    pub arg: Option<ReplOptionArg>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BaseBackupCmd {
    pub options: Vec<ReplOption>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateReplicationSlotCmd {
    pub kind: ReplicationKind,
    pub slotname: Option<String>,
    pub temporary: bool,
    pub plugin: Option<String>,
    pub options: Vec<ReplOption>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropReplicationSlotCmd {
    pub slotname: Option<String>,
    pub wait: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterReplicationSlotCmd {
    pub slotname: Option<String>,
    pub options: Vec<ReplOption>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadReplicationSlotCmd {
    pub slotname: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartReplicationCmd {
    pub kind: ReplicationKind,
    pub slotname: Option<String>,
    pub startpoint: XLogRecPtr,
    pub timeline: TimeLineID,
    pub options: Vec<ReplOption>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimeLineHistoryCmd {
    pub timeline: TimeLineID,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariableShowStmt {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplCommand {
    IdentifySystem,
    BaseBackup(BaseBackupCmd),
    CreateReplicationSlot(CreateReplicationSlotCmd),
    DropReplicationSlot(DropReplicationSlotCmd),
    AlterReplicationSlot(AlterReplicationSlotCmd),
    ReadReplicationSlot(ReadReplicationSlotCmd),
    StartReplication(StartReplicationCmd),
    TimeLineHistory(TimeLineHistoryCmd),
    VariableShow(VariableShowStmt),
    UploadManifest,
}

// Recoverable PgError, not an unwind.
fn replication_yyerror(message: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg_internal(message)
        .into_error()
}

fn make_def(name: String, arg: Option<ReplOptionArg>) -> ReplOption {
    ReplOption { name, arg }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    // `mem::replace`, not `.clone()`: tokens are read exactly once, left to right.
    fn bump(&mut self) -> Token {
        if self.pos < self.tokens.len() {
            let tok = std::mem::replace(&mut self.tokens[self.pos], Token::Eof);
            self.pos += 1;
            tok
        } else {
            Token::Eof
        }
    }

    fn syntax_error(&self) -> PgError {
        replication_yyerror("syntax error")
    }

    fn expect_char(&mut self, want: u8) -> PgResult<()> {
        match self.bump() {
            Token::Char(c) if c == want => Ok(()),
            _ => Err(self.syntax_error().into()),
        }
    }

    fn parse_first_cmd(&mut self) -> PgResult<ReplCommand> {
        let cmd = self.parse_command()?;
        self.parse_opt_semicolon();
        if *self.peek() != Token::Eof {
            return Err(self.syntax_error().into());
        }
        Ok(cmd)
    }

    fn parse_opt_semicolon(&mut self) {
        if matches!(self.peek(), Token::Char(b';')) {
            self.bump();
        }
    }

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
            _ => Err(self.syntax_error().into()),
        }
    }

    fn parse_identify_system(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        Ok(ReplCommand::IdentifySystem)
    }

    fn parse_read_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let slotname = self.parse_var_name()?;
        Ok(ReplCommand::ReadReplicationSlot(ReadReplicationSlotCmd {
            slotname: Some(slotname),
        }))
    }

    fn parse_show(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let name = self.parse_var_name()?;
        Ok(ReplCommand::VariableShow(VariableShowStmt { name }))
    }

    fn parse_var_name(&mut self) -> PgResult<String> {
        let mut name = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error().into()),
        };
        while matches!(self.peek(), Token::Char(b'.')) {
            self.bump();
            let next = match self.bump() {
                Token::Ident(s) => s,
                _ => return Err(self.syntax_error().into()),
            };
            name = format!("{name}.{next}");
        }
        Ok(name)
    }

    fn parse_base_backup(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let mut options = Vec::new();
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump();
            options = self.parse_generic_option_list()?;
            self.expect_char(b')')?;
        }
        Ok(ReplCommand::BaseBackup(BaseBackupCmd { options }))
    }

    fn parse_create_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error().into()),
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
                    _ => return Err(self.syntax_error().into()),
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
            _ => Err(self.syntax_error().into()),
        }
    }

    fn parse_create_slot_options(&mut self) -> PgResult<Vec<ReplOption>> {
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump();
            let list = self.parse_generic_option_list()?;
            self.expect_char(b')')?;
            Ok(list)
        } else {
            self.parse_create_slot_legacy_opt_list()
        }
    }

    fn parse_create_slot_legacy_opt_list(&mut self) -> PgResult<Vec<ReplOption>> {
        let mut list = Vec::new();
        loop {
            let elem = match self.peek() {
                Token::ExportSnapshot => {
                    self.bump();
                    make_def(
                        String::from("snapshot"),
                        Some(ReplOptionArg::Str(String::from("export"))),
                    )
                }
                Token::NoexportSnapshot => {
                    self.bump();
                    make_def(
                        String::from("snapshot"),
                        Some(ReplOptionArg::Str(String::from("nothing"))),
                    )
                }
                Token::UseSnapshot => {
                    self.bump();
                    make_def(
                        String::from("snapshot"),
                        Some(ReplOptionArg::Str(String::from("use"))),
                    )
                }
                Token::ReserveWal => {
                    self.bump();
                    make_def(String::from("reserve_wal"), Some(ReplOptionArg::Bool(true)))
                }
                Token::TwoPhase => {
                    self.bump();
                    make_def(String::from("two_phase"), Some(ReplOptionArg::Bool(true)))
                }
                _ => break,
            };
            list.push(elem);
        }
        Ok(list)
    }

    fn parse_drop_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error().into()),
        };
        let wait = if matches!(self.peek(), Token::Wait) {
            self.bump();
            true
        } else {
            false
        };
        Ok(ReplCommand::DropReplicationSlot(DropReplicationSlotCmd {
            slotname: Some(slotname),
            wait,
        }))
    }

    fn parse_alter_replication_slot(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let slotname = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error().into()),
        };
        self.expect_char(b'(')?;
        let options = self.parse_generic_option_list()?;
        self.expect_char(b')')?;
        Ok(ReplCommand::AlterReplicationSlot(AlterReplicationSlotCmd {
            slotname: Some(slotname),
            options,
        }))
    }

    /// `start_replication: K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline`
    /// `start_logical_replication: K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options`
    ///
    /// Both begin `K_START_REPLICATION`; they diverge on whether the slot
    /// clause is followed by `K_LOGICAL`. Read `opt_slot`, then branch on
    /// `K_LOGICAL` (logical) vs `opt_physical RECPTR` (physical).
    fn parse_start_replication(&mut self) -> PgResult<ReplCommand> {
        self.bump();

        let had_slot_keyword = matches!(self.peek(), Token::Slot);
        let slotname = if had_slot_keyword {
            self.bump();
            match self.bump() {
                Token::Ident(s) => Some(s),
                _ => return Err(self.syntax_error().into()),
            }
        } else {
            None
        };

        // `start_logical_replication` REQUIRES the `K_SLOT IDENT` clause, so
        // a bare `START_REPLICATION LOGICAL ...` matches no production.
        if matches!(self.peek(), Token::Logical) {
            if !had_slot_keyword {
                return Err(self.syntax_error().into());
            }
            self.bump();
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

        if matches!(self.peek(), Token::Physical) {
            self.bump();
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

    fn expect_recptr(&mut self) -> PgResult<XLogRecPtr> {
        match self.bump() {
            Token::Recptr(r) => Ok(r),
            _ => Err(self.syntax_error().into()),
        }
    }

    fn parse_opt_timeline(&mut self) -> PgResult<TimeLineID> {
        if matches!(self.peek(), Token::Timeline) {
            self.bump();
            let val = self.expect_uconst()?;
            // `$2` is uint32, so `<= 0` is `== 0`.
            if val == 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("invalid timeline {val}"))
                    .into_error()
                    .into());
            }
            Ok(val)
        } else {
            Ok(0)
        }
    }

    fn parse_opt_temporary(&mut self) -> bool {
        if matches!(self.peek(), Token::Temporary) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_uconst(&mut self) -> PgResult<u32> {
        match self.bump() {
            Token::Uconst(u) => Ok(u),
            _ => Err(self.syntax_error().into()),
        }
    }

    fn parse_timeline_history(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        let timeline = self.expect_uconst()?;
        if timeline == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid timeline {timeline}"))
                .into_error()
                .into());
        }
        Ok(ReplCommand::TimeLineHistory(TimeLineHistoryCmd { timeline }))
    }

    fn parse_upload_manifest(&mut self) -> PgResult<ReplCommand> {
        self.bump();
        Ok(ReplCommand::UploadManifest)
    }

    fn parse_plugin_options(&mut self) -> PgResult<Vec<ReplOption>> {
        if matches!(self.peek(), Token::Char(b'(')) {
            self.bump();
            let list = self.parse_plugin_opt_list()?;
            self.expect_char(b')')?;
            Ok(list)
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_plugin_opt_list(&mut self) -> PgResult<Vec<ReplOption>> {
        let mut list = Vec::new();
        list.push(self.parse_plugin_opt_elem()?);
        while matches!(self.peek(), Token::Char(b',')) {
            self.bump();
            list.push(self.parse_plugin_opt_elem()?);
        }
        Ok(list)
    }

    fn parse_plugin_opt_elem(&mut self) -> PgResult<ReplOption> {
        let name = match self.bump() {
            Token::Ident(s) => s,
            _ => return Err(self.syntax_error().into()),
        };
        let arg = self.parse_plugin_opt_arg();
        Ok(make_def(name, arg))
    }

    fn parse_plugin_opt_arg(&mut self) -> Option<ReplOptionArg> {
        if matches!(self.peek(), Token::Sconst(_)) {
            match self.bump() {
                Token::Sconst(s) => Some(ReplOptionArg::Str(s)),
                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    fn parse_generic_option_list(&mut self) -> PgResult<Vec<ReplOption>> {
        let mut list = Vec::new();
        list.push(self.parse_generic_option()?);
        while matches!(self.peek(), Token::Char(b',')) {
            self.bump();
            list.push(self.parse_generic_option()?);
        }
        Ok(list)
    }

    fn parse_generic_option(&mut self) -> PgResult<ReplOption> {
        let name = self.parse_ident_or_keyword()?;
        let arg = match self.peek() {
            Token::Ident(_) => match self.bump() {
                Token::Ident(s) => Some(ReplOptionArg::Str(s)),
                _ => unreachable!(),
            },
            Token::Sconst(_) => match self.bump() {
                Token::Sconst(s) => Some(ReplOptionArg::Str(s)),
                _ => unreachable!(),
            },
            Token::Uconst(_) => match self.bump() {
                // `makeInteger($2)`: C's `$2` is uint32 stored into an `int`
                // field; reproduce the bit-preserving narrowing to i32.
                Token::Uconst(u) => Some(ReplOptionArg::Int(u as i32)),
                _ => unreachable!(),
            },
            _ => None,
        };
        Ok(make_def(name, arg))
    }

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
            // `ident_or_keyword` production.
            _ => return Err(self.syntax_error().into()),
        };
        Ok(s)
    }
}

/// Parse a single WalSender replication command string into a [`ReplCommand`].
///
/// The analogue of `replication_scanner_init` + `replication_yyparse` +
/// `replication_scanner_finish`. Returns a recoverable `Err` carrying
/// `ERRCODE_SYNTAX_ERROR` on a lexical or grammatical problem, exactly as the
/// C parser's `ereport(ERROR)` would (but as a value, not an unwind).
pub fn replication_parse(cmd_string: &str) -> PgResult<ReplCommand> {
    let tokens = repl_scanner::replication_lex_all(cmd_string)?;
    parse_tokens(tokens)
}

/// Run the grammar over an already-lexed token stream (terminated by
/// [`Token::Eof`]); the `replication_yyparse` body proper, without the
/// scanner driver.
pub fn parse_tokens(tokens: Vec<Token>) -> PgResult<ReplCommand> {
    let mut parser = Parser::new(tokens);
    parser.parse_first_cmd()
}

/// `replication_scanner_is_replication_command`: the WalSender-vs-SQL gate.
pub fn is_replication_command(cmd_string: &str) -> PgResult<bool> {
    repl_scanner::is_replication_command(cmd_string)
}
