//! Grammar tests driving `parse_tokens` over hand-built token streams (the
//! `replication_yyparse` body without the scanner driver).

use super::*;

fn parse(toks: Vec<Token>) -> PgResult<ReplCommand> {
    parse_tokens(toks)
}

#[test]
fn identify_system() {
    let cmd = parse(vec![Token::IdentifySystem, Token::Eof]).unwrap();
    assert_eq!(cmd, ReplCommand::IdentifySystem);
}

#[test]
fn identify_system_with_semicolon() {
    let cmd = parse(vec![Token::IdentifySystem, Token::Char(b';'), Token::Eof]).unwrap();
    assert_eq!(cmd, ReplCommand::IdentifySystem);
}

#[test]
fn trailing_token_is_error() {
    let r = parse(vec![
        Token::IdentifySystem,
        Token::Ident(String::from("x")),
        Token::Eof,
    ]);
    assert!(r.is_err());
}

#[test]
fn show_dotted_var_name() {
    let cmd = parse(vec![
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

#[test]
fn read_replication_slot() {
    let cmd = parse(vec![
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

#[test]
fn base_backup() {
    let bare = parse(vec![Token::BaseBackup, Token::Eof]).unwrap();
    assert_eq!(bare, ReplCommand::BaseBackup(BaseBackupCmd::default()));

    // BASE_BACKUP ( LABEL 'x', PROGRESS )
    let cmd = parse(vec![
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
            assert_eq!(c.options[0].name, "label");
            assert!(matches!(c.options[0].arg, Some(ReplOptionArg::Str(_))));
            assert_eq!(c.options[1].name, "progress");
            assert!(c.options[1].arg.is_none());
        }
        _ => panic!("expected BaseBackup"),
    }
}

#[test]
fn create_physical_slot_legacy_opts() {
    let cmd = parse(vec![
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
            assert_eq!(c.options[0].name, "reserve_wal");
        }
        _ => panic!("expected CreateReplicationSlot"),
    }
}

#[test]
fn create_logical_slot_with_options() {
    let cmd = parse(vec![
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

#[test]
fn drop_replication_slot_wait() {
    let cmd = parse(vec![
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

#[test]
fn alter_replication_slot() {
    let cmd = parse(vec![
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

#[test]
fn start_physical_replication() {
    let cmd = parse(vec![
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

#[test]
fn start_logical_replication() {
    let cmd = parse(vec![
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

#[test]
fn start_logical_without_slot_is_error() {
    let r = parse(vec![
        Token::StartReplication,
        Token::Logical,
        Token::Recptr(16),
        Token::Eof,
    ]);
    assert!(r.is_err());
}

#[test]
fn timeline_history_zero_is_error() {
    let r = parse(vec![Token::TimelineHistory, Token::Uconst(0), Token::Eof]);
    assert!(r.is_err());
}

#[test]
fn timeline_history_ok() {
    let cmd = parse(vec![Token::TimelineHistory, Token::Uconst(3), Token::Eof]).unwrap();
    assert_eq!(
        cmd,
        ReplCommand::TimeLineHistory(TimeLineHistoryCmd { timeline: 3 })
    );
}

#[test]
fn upload_manifest() {
    let cmd = parse(vec![Token::UploadManifest, Token::Eof]).unwrap();
    assert_eq!(cmd, ReplCommand::UploadManifest);
}

#[test]
fn generic_option_keyword_name_and_integer_value() {
    // BASE_BACKUP ( WAIT 5 )  -> defname "wait", arg Int(5)
    let cmd = parse(vec![
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
            assert_eq!(c.options[0].name, "wait");
            assert_eq!(c.options[0].arg, Some(ReplOptionArg::Int(5)));
        }
        _ => panic!("expected BaseBackup"),
    }
}

#[test]
fn empty_input_is_error() {
    assert!(parse(vec![Token::Eof]).is_err());
    assert!(parse(vec![Token::Ident(String::from("x")), Token::Eof]).is_err());
}
