//! The replication-command lexer token (`repl_scanner.l`'s `replication_yylex`
//! return value plus the matching `yylval` payload), shared between the scanner
//! (`repl_scanner.l`, which produces tokens) and the grammar (`repl_gram.y`,
//! which consumes them).
//!
//! `repl_gram.y` declares keyword tokens as the `K_*` family and non-keyword
//! tokens (`SCONST`, `IDENT`, `UCONST`, `RECPTR`) carrying a `yylval` payload;
//! single-character tokens are returned as their ASCII byte. We model the token
//! *kind* together with its payload as a Rust enum.

extern crate alloc;

use alloc::string::String;

use ::types_core::primitive::XLogRecPtr;

/// A lexed replication-command token. Mirrors `replication_yylex()`'s `int`
/// token code together with the matching `yylval` union member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    // Keyword tokens (the `K_*` family from `repl_gram.y`).
    BaseBackup,
    IdentifySystem,
    ReadReplicationSlot,
    Show,
    Timeline,
    StartReplication,
    CreateReplicationSlot,
    DropReplicationSlot,
    AlterReplicationSlot,
    TimelineHistory,
    Physical,
    ReserveWal,
    Logical,
    Slot,
    Temporary,
    TwoPhase,
    ExportSnapshot,
    NoexportSnapshot,
    UseSnapshot,
    Wait,
    UploadManifest,

    /// `SCONST` — a single-quoted string literal (`yylval->str`).
    Sconst(String),
    /// `IDENT` — a folded identifier or double-quoted delimited identifier
    /// (`yylval->str`).
    Ident(String),
    /// `UCONST` — an unsigned decimal integer (`yylval->uintval`).
    Uconst(u32),
    /// `RECPTR` — a `%X/%X` WAL location (`yylval->recptr`).
    Recptr(XLogRecPtr),

    /// Any single character not otherwise recognized, returned as itself by the
    /// `.` flex rule (e.g. `(`, `)`, `,`, `.`, `;`).
    Char(u8),

    /// The end-of-input sentinel. flex's `<<EOF>>` rule calls `yyterminate()`,
    /// which makes `yylex` return 0; the parser checks for end of input via this.
    Eof,
}

impl Token {
    /// Whether this token is one of the WalSender command-introducing keywords —
    /// the set tested by `replication_scanner_is_replication_command`.
    pub fn is_replication_command_first(&self) -> bool {
        matches!(
            self,
            Token::IdentifySystem
                | Token::BaseBackup
                | Token::StartReplication
                | Token::CreateReplicationSlot
                | Token::DropReplicationSlot
                | Token::AlterReplicationSlot
                | Token::ReadReplicationSlot
                | Token::TimelineHistory
                | Token::UploadManifest
                | Token::Show
        )
    }
}
