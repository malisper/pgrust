//! Constant vocabulary local to the `utility.c` port.
//!
//! `utility.c` reads a handful of enums / flag sets: the `LogStmtLevel`
//! (`tcop/tcopprot.h`), the `COMMAND_*` read-only classification flags
//! (`tcop/utility.h`), the `ProcessUtilityContext` scalar (`tcop/utility.h`),
//! the `LockClauseStrength` enumerators (`nodes/lockoptions.h`), and the full
//! `CMDTAG_*` enumerator set (`tcop/cmdtaglist.h`). Each value is verified
//! against PostgreSQL 18.3.
//!
//! `CommandTag` is `::types_core::cmdtag::CommandTag` — a `CommandTag(i32)`
//! newtype whose value is the 0-based list-position from `cmdtaglist.h` (the
//! same numbering `backend-tcop-cmdtag`'s `TAG_BEHAVIOR` table indexes).

pub use ::nodes::parsestmt::ProcessUtilityContext;
use ::types_core::cmdtag::CommandTag;
use ::types_core::Oid;

/* tcop/utility.h — ProcessUtility read-only classification flags. */

/// `COMMAND_OK_IN_READ_ONLY_TXN` (0x0001).
pub const COMMAND_OK_IN_READ_ONLY_TXN: i32 = 0x0001;
/// `COMMAND_OK_IN_PARALLEL_MODE` (0x0002).
pub const COMMAND_OK_IN_PARALLEL_MODE: i32 = 0x0002;
/// `COMMAND_OK_IN_RECOVERY` (0x0004).
pub const COMMAND_OK_IN_RECOVERY: i32 = 0x0004;
/// `COMMAND_IS_STRICTLY_READ_ONLY` — OK in read-only txn, parallel, recovery.
pub const COMMAND_IS_STRICTLY_READ_ONLY: i32 =
    COMMAND_OK_IN_READ_ONLY_TXN | COMMAND_OK_IN_RECOVERY | COMMAND_OK_IN_PARALLEL_MODE;
/// `COMMAND_IS_NOT_READ_ONLY` (0).
pub const COMMAND_IS_NOT_READ_ONLY: i32 = 0;

/* tcop/tcopprot.h — LogStmtLevel. */

/// `typedef enum LogStmtLevel` (`tcop/tcopprot.h`).
pub type LogStmtLevel = i32;
/// `LOGSTMT_NONE` — log no statements.
pub const LOGSTMT_NONE: LogStmtLevel = 0;
/// `LOGSTMT_DDL` — log data-definition statements.
pub const LOGSTMT_DDL: LogStmtLevel = 1;
/// `LOGSTMT_MOD` — log modification statements, plus DDL.
pub const LOGSTMT_MOD: LogStmtLevel = 2;
/// `LOGSTMT_ALL` — log all statements.
pub const LOGSTMT_ALL: LogStmtLevel = 3;

/* nodes/lockoptions.h — LockClauseStrength enumerators read by CreateCommandTag.
 * storage/lockdefs.h — RowExclusiveLock, read by ClassifyUtilityCommandAsReadOnly. */

/// `LCS_FORKEYSHARE` — FOR KEY SHARE.
pub const LCS_FORKEYSHARE: i32 = 1;
/// `LCS_FORSHARE` — FOR SHARE.
pub const LCS_FORSHARE: i32 = 2;
/// `LCS_FORNOKEYUPDATE` — FOR NO KEY UPDATE.
pub const LCS_FORNOKEYUPDATE: i32 = 3;
/// `LCS_FORUPDATE` — FOR UPDATE.
pub const LCS_FORUPDATE: i32 = 4;

/// `RowExclusiveLock` (`storage/lockdefs.h`).
pub const ROW_EXCLUSIVE_LOCK: i32 = 3;

/* catalog OIDs read by utility.c. */

/// `RECORDOID` (`catalog/pg_type_d.h`) — anonymous-record pseudo-type; used by
/// `UtilityReturnsTuples` for `CALL`.
pub const RECORDOID: Oid = 2_249;

/// `ROLE_PG_CHECKPOINT` OID (`catalog/pg_authid_d.h`) — predefined role allowed
/// to run CHECKPOINT.
pub const ROLE_PG_CHECKPOINT: Oid = 4_544;

/* tcop/cmdtaglist.h — CommandTag enumerators (value = 0-based list position),
 * verified against PostgreSQL 18.3 cmdtaglist.h. */

pub const CMDTAG_UNKNOWN: CommandTag = CommandTag(0);
pub const CMDTAG_ALTER_AGGREGATE: CommandTag = CommandTag(2);
pub const CMDTAG_ALTER_CAST: CommandTag = CommandTag(3);
pub const CMDTAG_ALTER_COLLATION: CommandTag = CommandTag(4);
pub const CMDTAG_ALTER_CONVERSION: CommandTag = CommandTag(6);
pub const CMDTAG_ALTER_DATABASE: CommandTag = CommandTag(7);
pub const CMDTAG_ALTER_DEFAULT_PRIVILEGES: CommandTag = CommandTag(8);
pub const CMDTAG_ALTER_DOMAIN: CommandTag = CommandTag(9);
pub const CMDTAG_ALTER_EVENT_TRIGGER: CommandTag = CommandTag(10);
pub const CMDTAG_ALTER_EXTENSION: CommandTag = CommandTag(11);
pub const CMDTAG_ALTER_FOREIGN_DATA_WRAPPER: CommandTag = CommandTag(12);
pub const CMDTAG_ALTER_FOREIGN_TABLE: CommandTag = CommandTag(13);
pub const CMDTAG_ALTER_FUNCTION: CommandTag = CommandTag(14);
pub const CMDTAG_ALTER_INDEX: CommandTag = CommandTag(15);
pub const CMDTAG_ALTER_LANGUAGE: CommandTag = CommandTag(16);
pub const CMDTAG_ALTER_LARGE_OBJECT: CommandTag = CommandTag(17);
pub const CMDTAG_ALTER_MATERIALIZED_VIEW: CommandTag = CommandTag(18);
pub const CMDTAG_ALTER_OPERATOR: CommandTag = CommandTag(19);
pub const CMDTAG_ALTER_OPERATOR_CLASS: CommandTag = CommandTag(20);
pub const CMDTAG_ALTER_OPERATOR_FAMILY: CommandTag = CommandTag(21);
pub const CMDTAG_ALTER_POLICY: CommandTag = CommandTag(22);
pub const CMDTAG_ALTER_PROCEDURE: CommandTag = CommandTag(23);
pub const CMDTAG_ALTER_PUBLICATION: CommandTag = CommandTag(24);
pub const CMDTAG_ALTER_ROLE: CommandTag = CommandTag(25);
pub const CMDTAG_ALTER_ROUTINE: CommandTag = CommandTag(26);
pub const CMDTAG_ALTER_RULE: CommandTag = CommandTag(27);
pub const CMDTAG_ALTER_SCHEMA: CommandTag = CommandTag(28);
pub const CMDTAG_ALTER_SEQUENCE: CommandTag = CommandTag(29);
pub const CMDTAG_ALTER_SERVER: CommandTag = CommandTag(30);
pub const CMDTAG_ALTER_STATISTICS: CommandTag = CommandTag(31);
pub const CMDTAG_ALTER_SUBSCRIPTION: CommandTag = CommandTag(32);
pub const CMDTAG_ALTER_SYSTEM: CommandTag = CommandTag(33);
pub const CMDTAG_ALTER_TABLE: CommandTag = CommandTag(34);
pub const CMDTAG_ALTER_TABLESPACE: CommandTag = CommandTag(35);
pub const CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION: CommandTag = CommandTag(36);
pub const CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY: CommandTag = CommandTag(37);
pub const CMDTAG_ALTER_TEXT_SEARCH_PARSER: CommandTag = CommandTag(38);
pub const CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE: CommandTag = CommandTag(39);
pub const CMDTAG_ALTER_TRIGGER: CommandTag = CommandTag(41);
pub const CMDTAG_ALTER_TYPE: CommandTag = CommandTag(42);
pub const CMDTAG_ALTER_USER_MAPPING: CommandTag = CommandTag(43);
pub const CMDTAG_ALTER_VIEW: CommandTag = CommandTag(44);
pub const CMDTAG_ANALYZE: CommandTag = CommandTag(45);
pub const CMDTAG_BEGIN: CommandTag = CommandTag(46);
pub const CMDTAG_CALL: CommandTag = CommandTag(47);
pub const CMDTAG_CHECKPOINT: CommandTag = CommandTag(48);
pub const CMDTAG_CLOSE_CURSOR: CommandTag = CommandTag(50);
pub const CMDTAG_CLOSE_CURSOR_ALL: CommandTag = CommandTag(51);
pub const CMDTAG_CLUSTER: CommandTag = CommandTag(52);
pub const CMDTAG_COMMENT: CommandTag = CommandTag(53);
pub const CMDTAG_COMMIT: CommandTag = CommandTag(54);
pub const CMDTAG_COMMIT_PREPARED: CommandTag = CommandTag(55);
pub const CMDTAG_COPY: CommandTag = CommandTag(56);
pub const CMDTAG_CREATE_ACCESS_METHOD: CommandTag = CommandTag(58);
pub const CMDTAG_CREATE_AGGREGATE: CommandTag = CommandTag(59);
pub const CMDTAG_CREATE_CAST: CommandTag = CommandTag(60);
pub const CMDTAG_CREATE_COLLATION: CommandTag = CommandTag(61);
pub const CMDTAG_CREATE_CONVERSION: CommandTag = CommandTag(63);
pub const CMDTAG_CREATE_DATABASE: CommandTag = CommandTag(64);
pub const CMDTAG_CREATE_DOMAIN: CommandTag = CommandTag(65);
pub const CMDTAG_CREATE_EVENT_TRIGGER: CommandTag = CommandTag(66);
pub const CMDTAG_CREATE_EXTENSION: CommandTag = CommandTag(67);
pub const CMDTAG_CREATE_FOREIGN_DATA_WRAPPER: CommandTag = CommandTag(68);
pub const CMDTAG_CREATE_FOREIGN_TABLE: CommandTag = CommandTag(69);
pub const CMDTAG_CREATE_FUNCTION: CommandTag = CommandTag(70);
pub const CMDTAG_CREATE_INDEX: CommandTag = CommandTag(71);
pub const CMDTAG_CREATE_LANGUAGE: CommandTag = CommandTag(72);
pub const CMDTAG_CREATE_MATERIALIZED_VIEW: CommandTag = CommandTag(73);
pub const CMDTAG_CREATE_OPERATOR: CommandTag = CommandTag(74);
pub const CMDTAG_CREATE_OPERATOR_CLASS: CommandTag = CommandTag(75);
pub const CMDTAG_CREATE_OPERATOR_FAMILY: CommandTag = CommandTag(76);
pub const CMDTAG_CREATE_POLICY: CommandTag = CommandTag(77);
pub const CMDTAG_CREATE_PROCEDURE: CommandTag = CommandTag(78);
pub const CMDTAG_CREATE_PUBLICATION: CommandTag = CommandTag(79);
pub const CMDTAG_CREATE_ROLE: CommandTag = CommandTag(80);
pub const CMDTAG_CREATE_RULE: CommandTag = CommandTag(82);
pub const CMDTAG_CREATE_SCHEMA: CommandTag = CommandTag(83);
pub const CMDTAG_CREATE_SEQUENCE: CommandTag = CommandTag(84);
pub const CMDTAG_CREATE_SERVER: CommandTag = CommandTag(85);
pub const CMDTAG_CREATE_STATISTICS: CommandTag = CommandTag(86);
pub const CMDTAG_CREATE_SUBSCRIPTION: CommandTag = CommandTag(87);
pub const CMDTAG_CREATE_TABLE: CommandTag = CommandTag(88);
pub const CMDTAG_CREATE_TABLE_AS: CommandTag = CommandTag(89);
pub const CMDTAG_CREATE_TABLESPACE: CommandTag = CommandTag(90);
pub const CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION: CommandTag = CommandTag(91);
pub const CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY: CommandTag = CommandTag(92);
pub const CMDTAG_CREATE_TEXT_SEARCH_PARSER: CommandTag = CommandTag(93);
pub const CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE: CommandTag = CommandTag(94);
pub const CMDTAG_CREATE_TRANSFORM: CommandTag = CommandTag(95);
pub const CMDTAG_CREATE_TRIGGER: CommandTag = CommandTag(96);
pub const CMDTAG_CREATE_TYPE: CommandTag = CommandTag(97);
pub const CMDTAG_CREATE_USER_MAPPING: CommandTag = CommandTag(98);
pub const CMDTAG_CREATE_VIEW: CommandTag = CommandTag(99);
pub const CMDTAG_DEALLOCATE: CommandTag = CommandTag(100);
pub const CMDTAG_DEALLOCATE_ALL: CommandTag = CommandTag(101);
pub const CMDTAG_DECLARE_CURSOR: CommandTag = CommandTag(102);
pub const CMDTAG_DELETE: CommandTag = CommandTag(103);
pub const CMDTAG_DISCARD_ALL: CommandTag = CommandTag(105);
pub const CMDTAG_DISCARD_PLANS: CommandTag = CommandTag(106);
pub const CMDTAG_DISCARD_SEQUENCES: CommandTag = CommandTag(107);
pub const CMDTAG_DISCARD_TEMP: CommandTag = CommandTag(108);
pub const CMDTAG_DO: CommandTag = CommandTag(109);
pub const CMDTAG_DROP_ACCESS_METHOD: CommandTag = CommandTag(110);
pub const CMDTAG_DROP_AGGREGATE: CommandTag = CommandTag(111);
pub const CMDTAG_DROP_CAST: CommandTag = CommandTag(112);
pub const CMDTAG_DROP_COLLATION: CommandTag = CommandTag(113);
pub const CMDTAG_DROP_CONVERSION: CommandTag = CommandTag(115);
pub const CMDTAG_DROP_DATABASE: CommandTag = CommandTag(116);
pub const CMDTAG_DROP_DOMAIN: CommandTag = CommandTag(117);
pub const CMDTAG_DROP_EVENT_TRIGGER: CommandTag = CommandTag(118);
pub const CMDTAG_DROP_EXTENSION: CommandTag = CommandTag(119);
pub const CMDTAG_DROP_FOREIGN_DATA_WRAPPER: CommandTag = CommandTag(120);
pub const CMDTAG_DROP_FOREIGN_TABLE: CommandTag = CommandTag(121);
pub const CMDTAG_DROP_FUNCTION: CommandTag = CommandTag(122);
pub const CMDTAG_DROP_INDEX: CommandTag = CommandTag(123);
pub const CMDTAG_DROP_LANGUAGE: CommandTag = CommandTag(124);
pub const CMDTAG_DROP_MATERIALIZED_VIEW: CommandTag = CommandTag(125);
pub const CMDTAG_DROP_OPERATOR: CommandTag = CommandTag(126);
pub const CMDTAG_DROP_OPERATOR_CLASS: CommandTag = CommandTag(127);
pub const CMDTAG_DROP_OPERATOR_FAMILY: CommandTag = CommandTag(128);
pub const CMDTAG_DROP_OWNED: CommandTag = CommandTag(129);
pub const CMDTAG_DROP_POLICY: CommandTag = CommandTag(130);
pub const CMDTAG_DROP_PROCEDURE: CommandTag = CommandTag(131);
pub const CMDTAG_DROP_PUBLICATION: CommandTag = CommandTag(132);
pub const CMDTAG_DROP_ROLE: CommandTag = CommandTag(133);
pub const CMDTAG_DROP_ROUTINE: CommandTag = CommandTag(134);
pub const CMDTAG_DROP_RULE: CommandTag = CommandTag(135);
pub const CMDTAG_DROP_SCHEMA: CommandTag = CommandTag(136);
pub const CMDTAG_DROP_SEQUENCE: CommandTag = CommandTag(137);
pub const CMDTAG_DROP_SERVER: CommandTag = CommandTag(138);
pub const CMDTAG_DROP_STATISTICS: CommandTag = CommandTag(139);
pub const CMDTAG_DROP_SUBSCRIPTION: CommandTag = CommandTag(140);
pub const CMDTAG_DROP_TABLE: CommandTag = CommandTag(141);
pub const CMDTAG_DROP_TABLESPACE: CommandTag = CommandTag(142);
pub const CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION: CommandTag = CommandTag(143);
pub const CMDTAG_DROP_TEXT_SEARCH_DICTIONARY: CommandTag = CommandTag(144);
pub const CMDTAG_DROP_TEXT_SEARCH_PARSER: CommandTag = CommandTag(145);
pub const CMDTAG_DROP_TEXT_SEARCH_TEMPLATE: CommandTag = CommandTag(146);
pub const CMDTAG_DROP_TRANSFORM: CommandTag = CommandTag(147);
pub const CMDTAG_DROP_TRIGGER: CommandTag = CommandTag(148);
pub const CMDTAG_DROP_TYPE: CommandTag = CommandTag(149);
pub const CMDTAG_DROP_USER_MAPPING: CommandTag = CommandTag(150);
pub const CMDTAG_DROP_VIEW: CommandTag = CommandTag(151);
pub const CMDTAG_EXECUTE: CommandTag = CommandTag(152);
pub const CMDTAG_EXPLAIN: CommandTag = CommandTag(153);
pub const CMDTAG_FETCH: CommandTag = CommandTag(154);
pub const CMDTAG_GRANT: CommandTag = CommandTag(155);
pub const CMDTAG_GRANT_ROLE: CommandTag = CommandTag(156);
pub const CMDTAG_IMPORT_FOREIGN_SCHEMA: CommandTag = CommandTag(157);
pub const CMDTAG_INSERT: CommandTag = CommandTag(158);
pub const CMDTAG_LISTEN: CommandTag = CommandTag(159);
pub const CMDTAG_LOAD: CommandTag = CommandTag(160);
pub const CMDTAG_LOCK_TABLE: CommandTag = CommandTag(161);
pub const CMDTAG_MERGE: CommandTag = CommandTag(162);
pub const CMDTAG_MOVE: CommandTag = CommandTag(164);
pub const CMDTAG_NOTIFY: CommandTag = CommandTag(165);
pub const CMDTAG_PREPARE: CommandTag = CommandTag(166);
pub const CMDTAG_PREPARE_TRANSACTION: CommandTag = CommandTag(167);
pub const CMDTAG_REASSIGN_OWNED: CommandTag = CommandTag(168);
pub const CMDTAG_REFRESH_MATERIALIZED_VIEW: CommandTag = CommandTag(169);
pub const CMDTAG_REINDEX: CommandTag = CommandTag(170);
pub const CMDTAG_RELEASE: CommandTag = CommandTag(171);
pub const CMDTAG_RESET: CommandTag = CommandTag(172);
pub const CMDTAG_REVOKE: CommandTag = CommandTag(173);
pub const CMDTAG_REVOKE_ROLE: CommandTag = CommandTag(174);
pub const CMDTAG_ROLLBACK: CommandTag = CommandTag(175);
pub const CMDTAG_ROLLBACK_PREPARED: CommandTag = CommandTag(176);
pub const CMDTAG_SAVEPOINT: CommandTag = CommandTag(177);
pub const CMDTAG_SECURITY_LABEL: CommandTag = CommandTag(178);
pub const CMDTAG_SELECT: CommandTag = CommandTag(179);
pub const CMDTAG_SELECT_FOR_KEY_SHARE: CommandTag = CommandTag(180);
pub const CMDTAG_SELECT_FOR_NO_KEY_UPDATE: CommandTag = CommandTag(181);
pub const CMDTAG_SELECT_FOR_SHARE: CommandTag = CommandTag(182);
pub const CMDTAG_SELECT_FOR_UPDATE: CommandTag = CommandTag(183);
pub const CMDTAG_SELECT_INTO: CommandTag = CommandTag(184);
pub const CMDTAG_SET: CommandTag = CommandTag(185);
pub const CMDTAG_SET_CONSTRAINTS: CommandTag = CommandTag(186);
pub const CMDTAG_SHOW: CommandTag = CommandTag(187);
pub const CMDTAG_START_TRANSACTION: CommandTag = CommandTag(188);
pub const CMDTAG_TRUNCATE_TABLE: CommandTag = CommandTag(189);
pub const CMDTAG_UNLISTEN: CommandTag = CommandTag(190);
pub const CMDTAG_UPDATE: CommandTag = CommandTag(191);
pub const CMDTAG_VACUUM: CommandTag = CommandTag(192);
