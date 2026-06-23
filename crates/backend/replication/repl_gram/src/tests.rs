//! Grammar tests driving `parse_tokens` over hand-built token streams (the
//! `replication_yyparse` body without the scanner driver).

use super::*;
use replication::repl_token::Token;

fn parse(toks: Vec<Token>) -> PgResult<ReplCommand> {
    parse_tokens(toks)
}

/// `IDENTIFY_SYSTEM`
#[test]
fn identify_system() {
    let cmd = parse(alloc::vec![Token::IdentifySystem, Token::Eof]).unwrap();
    assert_eq!(cmd, ReplCommand::IdentifySystem);
}

/// `IDENTIFY_SYSTEM;` — `opt_semicolon` accepted.
#[test]
fn identify_system_with_semicolon() {
    let cmd = parse(alloc::vec![
        Token::IdentifySystem,
        Token::Char(b';'),
        Token::Eof,
    ])
    .unwrap();
    assert_eq!(cmd, ReplCommand::IdentifySystem);
}

/// Trailing junk after a complete command is a syntax error.
#[test]
fn trailing_token_is_error() {
    let r = parse(alloc::vec![
        Token::IdentifySystem,
        Token::Ident(String::from("x")),
        Token::Eof,
    ]);
    assert!(r.is_err());
}

/// `SHOW a.b.c` folds the dotted name.
#[test]
fn show_dotted_var_name() {
    let cmd = parse(alloc::vec![
        Token::Show,
        Token::Ident(String::from("a")),
        Token::Char(b'.'),
        Token::Ident(String::from("b")),
        Token::Char(b'.'),
        Token::Ident(String::from("c")),
        Token::Eof,
    ])
    .unwrap();
    assert_eq!(
        cmd,
        ReplCommand::VariableShow(VariableShowStmt {
            name: String::from("a.b.c"),
        })
    );
}

/// `READ_REPLICATION_SLOT s`
#[test]
fn read_replication_slot() {
    let cmd = parse(alloc::vec![
        Token::ReadReplicationSlot,
        Token::Ident(String::from("s")),
        Token::Eof,
    ])
    .unwrap();
    assert_eq!(
        cmd,
        ReplCommand::ReadReplicationSlot(ReadReplicationSlotCmd {
            slotname: Some(String::from("s")),
        })
    );
}

/// `BASE_BACKUP` bare and with a parenthesized option list.
#[test]
fn base_backup() {
    let bare = parse(alloc::vec![Token::BaseBackup, Token::Eof]).unwrap();
    assert_eq!(bare, ReplCommand::BaseBackup(BaseBackupCmd::default()));

    // BASE_BACKUP ( LABEL 'x', PROGRESS )
    let cmd = parse(alloc::vec![
        Token::BaseBackup,
        Token::Char(b'('),
        Token::Ident(String::from("label")),
        Token::Sconst(String::from("x")),
        Token::Char(b','),
        Token::Ident(String::from("progress")),
        Token::Char(b')'),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::BaseBackup(c) => {
            assert_eq!(c.options.len(), 2);
            assert_eq!(c.options[0].defname.as_deref(), Some("label"));
            assert!(matches!(c.options[0].arg.as_deref(), Some(Node::String(_))));
            assert_eq!(c.options[1].defname.as_deref(), Some("progress"));
            assert!(c.options[1].arg.is_none());
        }
        _ => panic!("expected BaseBackup"),
    }
}

/// `CREATE_REPLICATION_SLOT s TEMPORARY PHYSICAL RESERVE_WAL` — legacy opts.
#[test]
fn create_physical_slot_legacy_opts() {
    let cmd = parse(alloc::vec![
        Token::CreateReplicationSlot,
        Token::Ident(String::from("s")),
        Token::Temporary,
        Token::Physical,
        Token::ReserveWal,
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::CreateReplicationSlot(c) => {
            assert_eq!(c.kind, ReplicationKind::REPLICATION_KIND_PHYSICAL);
            assert_eq!(c.slotname.as_deref(), Some("s"));
            assert!(c.temporary);
            assert!(c.plugin.is_none());
            assert_eq!(c.options.len(), 1);
            assert_eq!(c.options[0].defname.as_deref(), Some("reserve_wal"));
        }
        _ => panic!("expected CreateReplicationSlot"),
    }
}

/// `CREATE_REPLICATION_SLOT s LOGICAL plug ( snapshot 'use' )`.
#[test]
fn create_logical_slot_with_options() {
    let cmd = parse(alloc::vec![
        Token::CreateReplicationSlot,
        Token::Ident(String::from("s")),
        Token::Logical,
        Token::Ident(String::from("plug")),
        Token::Char(b'('),
        Token::Ident(String::from("snapshot")),
        Token::Sconst(String::from("use")),
        Token::Char(b')'),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::CreateReplicationSlot(c) => {
            assert_eq!(c.kind, ReplicationKind::REPLICATION_KIND_LOGICAL);
            assert!(!c.temporary);
            assert_eq!(c.plugin.as_deref(), Some("plug"));
            assert_eq!(c.options.len(), 1);
        }
        _ => panic!("expected CreateReplicationSlot"),
    }
}

/// `DROP_REPLICATION_SLOT s WAIT`.
#[test]
fn drop_replication_slot_wait() {
    let cmd = parse(alloc::vec![
        Token::DropReplicationSlot,
        Token::Ident(String::from("s")),
        Token::Wait,
        Token::Eof,
    ])
    .unwrap();
    assert_eq!(
        cmd,
        ReplCommand::DropReplicationSlot(DropReplicationSlotCmd {
            slotname: Some(String::from("s")),
            wait: true,
        })
    );
}

/// `ALTER_REPLICATION_SLOT s ( failover )`.
#[test]
fn alter_replication_slot() {
    let cmd = parse(alloc::vec![
        Token::AlterReplicationSlot,
        Token::Ident(String::from("s")),
        Token::Char(b'('),
        Token::Ident(String::from("failover")),
        Token::Char(b')'),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::AlterReplicationSlot(c) => {
            assert_eq!(c.slotname.as_deref(), Some("s"));
            assert_eq!(c.options.len(), 1);
        }
        _ => panic!("expected AlterReplicationSlot"),
    }
}

/// `START_REPLICATION 0/0 TIMELINE 1` (physical, anonymous).
#[test]
fn start_physical_replication() {
    let cmd = parse(alloc::vec![
        Token::StartReplication,
        Token::Recptr(0x1234_5678),
        Token::Timeline,
        Token::Uconst(1),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::StartReplication(c) => {
            assert_eq!(c.kind, ReplicationKind::REPLICATION_KIND_PHYSICAL);
            assert!(c.slotname.is_none());
            assert_eq!(c.startpoint, 0x1234_5678);
            assert_eq!(c.timeline, 1);
            assert!(c.options.is_empty());
        }
        _ => panic!("expected StartReplication"),
    }
}

/// `START_REPLICATION SLOT s LOGICAL 0/0 ( opt 'v' )` (logical).
#[test]
fn start_logical_replication() {
    let cmd = parse(alloc::vec![
        Token::StartReplication,
        Token::Slot,
        Token::Ident(String::from("s")),
        Token::Logical,
        Token::Recptr(16),
        Token::Char(b'('),
        Token::Ident(String::from("opt")),
        Token::Sconst(String::from("v")),
        Token::Char(b')'),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::StartReplication(c) => {
            assert_eq!(c.kind, ReplicationKind::REPLICATION_KIND_LOGICAL);
            assert_eq!(c.slotname.as_deref(), Some("s"));
            assert_eq!(c.startpoint, 16);
            assert_eq!(c.timeline, 0);
            assert_eq!(c.options.len(), 1);
        }
        _ => panic!("expected StartReplication"),
    }
}

/// Bare `START_REPLICATION LOGICAL ...` (no SLOT clause) is a syntax error.
#[test]
fn start_logical_without_slot_is_error() {
    let r = parse(alloc::vec![
        Token::StartReplication,
        Token::Logical,
        Token::Recptr(16),
        Token::Eof,
    ]);
    assert!(r.is_err());
}

/// `TIMELINE_HISTORY 0` is rejected (`$2 <= 0`).
#[test]
fn timeline_history_zero_is_error() {
    let r = parse(alloc::vec![
        Token::TimelineHistory,
        Token::Uconst(0),
        Token::Eof,
    ]);
    assert!(r.is_err());
}

/// `TIMELINE_HISTORY 3`.
#[test]
fn timeline_history_ok() {
    let cmd = parse(alloc::vec![
        Token::TimelineHistory,
        Token::Uconst(3),
        Token::Eof,
    ])
    .unwrap();
    assert_eq!(
        cmd,
        ReplCommand::TimeLineHistory(TimeLineHistoryCmd { timeline: 3 })
    );
}

/// `UPLOAD_MANIFEST`.
#[test]
fn upload_manifest() {
    let cmd = parse(alloc::vec![Token::UploadManifest, Token::Eof]).unwrap();
    assert_eq!(cmd, ReplCommand::UploadManifest);
}

/// A keyword used as a `generic_option` name folds to its lowercase spelling,
/// and a UCONST value becomes a makeInteger node.
#[test]
fn generic_option_keyword_name_and_integer_value() {
    // BASE_BACKUP ( WAIT 5 )  -> defname "wait", arg Integer(5)
    let cmd = parse(alloc::vec![
        Token::BaseBackup,
        Token::Char(b'('),
        Token::Wait,
        Token::Uconst(5),
        Token::Char(b')'),
        Token::Eof,
    ])
    .unwrap();
    match cmd {
        ReplCommand::BaseBackup(c) => {
            assert_eq!(c.options.len(), 1);
            assert_eq!(c.options[0].defname.as_deref(), Some("wait"));
            assert_eq!(
                c.options[0].arg.as_deref(),
                Some(&Node::Integer(Integer { ival: 5 }))
            );
        }
        _ => panic!("expected BaseBackup"),
    }
}

/// An empty / unrecognized command is a syntax error.
#[test]
fn empty_input_is_error() {
    assert!(parse(alloc::vec![Token::Eof]).is_err());
    assert!(parse(alloc::vec![Token::Ident(String::from("x")), Token::Eof]).is_err());
}
