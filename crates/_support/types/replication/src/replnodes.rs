//! Replication-command parse nodes (`nodes/replnodes.h`) and the WalSender
//! command vocabulary shared between the replication scanner/grammar
//! (`repl_scanner.l` / `repl_gram.y`) and `walsender.c`'s
//! `exec_replication_command`.
//!
//! The C grammar builds a `Node *` per command (`makeNode(IdentifySystemCmd)`
//! etc.) and stores it through `*replication_parse_result_p`. We model the
//! parse result as the [`ReplCommand`] enum (the `Node *` tag becomes the enum
//! discriminant) carrying one owned per-command struct, mirroring
//! `replnodes.h` field-for-field.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::{TimeLineID, XLogRecPtr};
use parsenodes::DefElem;

/// `typedef enum ReplicationKind` (`nodes/replnodes.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ReplicationKind {
    REPLICATION_KIND_PHYSICAL = 0,
    REPLICATION_KIND_LOGICAL = 1,
}
pub use ReplicationKind::{REPLICATION_KIND_LOGICAL, REPLICATION_KIND_PHYSICAL};

/// `IDENTIFY_SYSTEM` command (`IdentifySystemCmd`) — carries no fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentifySystemCmd;

/// `BASE_BACKUP [ ( option ... ) ]` (`BaseBackupCmd`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BaseBackupCmd {
    /// `List *options` — the parenthesized generic-option list (NIL when absent).
    pub options: Vec<DefElem>,
}

/// `CREATE_REPLICATION_SLOT` command (`CreateReplicationSlotCmd`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateReplicationSlotCmd {
    /// `char *slotname`.
    pub slotname: Option<String>,
    /// `ReplicationKind kind`.
    pub kind: ReplicationKind,
    /// `char *plugin` — only for LOGICAL slots.
    pub plugin: Option<String>,
    /// `bool temporary`.
    pub temporary: bool,
    /// `List *options`.
    pub options: Vec<DefElem>,
}

/// `DROP_REPLICATION_SLOT slot [WAIT]` (`DropReplicationSlotCmd`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropReplicationSlotCmd {
    /// `char *slotname`.
    pub slotname: Option<String>,
    /// `bool wait`.
    pub wait: bool,
}

/// `ALTER_REPLICATION_SLOT slot (options)` (`AlterReplicationSlotCmd`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterReplicationSlotCmd {
    /// `char *slotname`.
    pub slotname: Option<String>,
    /// `List *options`.
    pub options: Vec<DefElem>,
}

/// `START_REPLICATION ...` (`StartReplicationCmd`) — physical or logical.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartReplicationCmd {
    /// `ReplicationKind kind`.
    pub kind: ReplicationKind,
    /// `char *slotname` (NULL for an anonymous physical start).
    pub slotname: Option<String>,
    /// `TimeLineID timeline` (0 when unspecified).
    pub timeline: TimeLineID,
    /// `XLogRecPtr startpoint`.
    pub startpoint: XLogRecPtr,
    /// `List *options` — logical plugin options (NIL for physical).
    pub options: Vec<DefElem>,
}

/// `READ_REPLICATION_SLOT slot` (`ReadReplicationSlotCmd`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadReplicationSlotCmd {
    /// `char *slotname`.
    pub slotname: Option<String>,
}

/// `TIMELINE_HISTORY tli` (`TimeLineHistoryCmd`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeLineHistoryCmd {
    /// `TimeLineID timeline`.
    pub timeline: TimeLineID,
}

/// `UPLOAD_MANIFEST` command (`UploadManifestCmd`) — carries no fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadManifestCmd;

/// `VARIABLE SHOW` (`SHOW setting`) statement (`VariableShowStmt`,
/// `nodes/parsenodes.h`) — the replication grammar builds it for `SHOW name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariableShowStmt {
    /// `char *name` — the (possibly dotted) GUC name.
    pub name: String,
}

/// The replication-command parse result — the `Node *` written through
/// `*replication_parse_result_p`, tagged by command kind.
/// `exec_replication_command` dispatches on this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplCommand {
    /// `T_IdentifySystemCmd`.
    IdentifySystem,
    /// `T_BaseBackupCmd`.
    BaseBackup(BaseBackupCmd),
    /// `T_CreateReplicationSlotCmd`.
    CreateReplicationSlot(CreateReplicationSlotCmd),
    /// `T_DropReplicationSlotCmd`.
    DropReplicationSlot(DropReplicationSlotCmd),
    /// `T_AlterReplicationSlotCmd`.
    AlterReplicationSlot(AlterReplicationSlotCmd),
    /// `T_StartReplicationCmd`.
    StartReplication(StartReplicationCmd),
    /// `T_ReadReplicationSlotCmd`.
    ReadReplicationSlot(ReadReplicationSlotCmd),
    /// `T_TimeLineHistoryCmd`.
    TimeLineHistory(TimeLineHistoryCmd),
    /// `T_UploadManifestCmd`.
    UploadManifest,
    /// `T_VariableShowStmt`.
    VariableShow(VariableShowStmt),
}
