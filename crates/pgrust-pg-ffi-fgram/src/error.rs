use core::ffi::{c_char, c_int, c_void};

use crate::{MemoryContextData, NodeTag};

pub const T_ErrorSaveContext: NodeTag = 447;

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ErrorLevel(pub c_int);

pub const DEBUG5: ErrorLevel = ErrorLevel(10);
pub const DEBUG4: ErrorLevel = ErrorLevel(11);
pub const DEBUG3: ErrorLevel = ErrorLevel(12);
pub const DEBUG2: ErrorLevel = ErrorLevel(13);
pub const DEBUG1: ErrorLevel = ErrorLevel(14);
pub const LOG: ErrorLevel = ErrorLevel(15);
pub const LOG_SERVER_ONLY: ErrorLevel = ErrorLevel(16);
pub const COMMERROR: ErrorLevel = LOG_SERVER_ONLY;
pub const INFO: ErrorLevel = ErrorLevel(17);
pub const NOTICE: ErrorLevel = ErrorLevel(18);
pub const WARNING: ErrorLevel = ErrorLevel(19);
pub const PGWARNING: ErrorLevel = WARNING;
pub const WARNING_CLIENT_ONLY: ErrorLevel = ErrorLevel(20);
pub const ERROR: ErrorLevel = ErrorLevel(21);
pub const PGERROR: ErrorLevel = ERROR;
pub const FATAL: ErrorLevel = ErrorLevel(22);
pub const PANIC: ErrorLevel = ErrorLevel(23);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlState(pub c_int);

pub const ERRCODE_SUCCESSFUL_COMPLETION: SqlState = make_sqlstate(*b"00000");
pub const ERRCODE_WARNING: SqlState = make_sqlstate(*b"01000");
pub const ERRCODE_FEATURE_NOT_SUPPORTED: SqlState = make_sqlstate(*b"0A000");
pub const ERRCODE_CONNECTION_FAILURE: SqlState = make_sqlstate(*b"08006");
pub const ERRCODE_PROTOCOL_VIOLATION: SqlState = make_sqlstate(*b"08P01");
pub const ERRCODE_ACTIVE_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25001");
pub const ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION: SqlState = make_sqlstate(*b"28000");
pub const ERRCODE_INVALID_TRANSACTION_STATE: SqlState = make_sqlstate(*b"25000");
pub const ERRCODE_NO_ACTIVE_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25P01");
pub const ERRCODE_IN_FAILED_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25P02");
/// `ERRCODE_S_E_INVALID_SPECIFICATION` (Class 3B -- Savepoint Exception).
pub const ERRCODE_S_E_INVALID_SPECIFICATION: SqlState = make_sqlstate(*b"3B001");
pub const ERRCODE_READ_ONLY_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25006");
pub const ERRCODE_NO_DATA: SqlState = make_sqlstate(*b"02000");
pub const ERRCODE_DATA_EXCEPTION: SqlState = make_sqlstate(*b"22000");
pub const ERRCODE_SYNTAX_ERROR: SqlState = make_sqlstate(*b"42601");
pub const ERRCODE_AMBIGUOUS_FUNCTION: SqlState = make_sqlstate(*b"42725");
pub const ERRCODE_INVALID_FUNCTION_DEFINITION: SqlState = make_sqlstate(*b"42P13");
pub const ERRCODE_TOO_MANY_ARGUMENTS: SqlState = make_sqlstate(*b"54023");
pub const ERRCODE_UNDEFINED_FUNCTION: SqlState = make_sqlstate(*b"42883");
pub const ERRCODE_DUPLICATE_FUNCTION: SqlState = make_sqlstate(*b"42723");
pub const ERRCODE_GROUPING_ERROR: SqlState = make_sqlstate(*b"42803");
pub const ERRCODE_UNDEFINED_OBJECT: SqlState = make_sqlstate(*b"42704");
pub const ERRCODE_UNDEFINED_SCHEMA: SqlState = make_sqlstate(*b"3F000");
pub const ERRCODE_UNDEFINED_COLUMN: SqlState = make_sqlstate(*b"42703");
pub const ERRCODE_DATATYPE_MISMATCH: SqlState = make_sqlstate(*b"42804");
pub const ERRCODE_DUPLICATE_OBJECT: SqlState = make_sqlstate(*b"42710");
/// Class 23 - Integrity Constraint Violation: unique_violation.
pub const ERRCODE_UNIQUE_VIOLATION: SqlState = make_sqlstate(*b"23505");
pub const ERRCODE_INSUFFICIENT_PRIVILEGE: SqlState = make_sqlstate(*b"42501");
pub const ERRCODE_INVALID_NAME: SqlState = make_sqlstate(*b"42602");
pub const ERRCODE_CHARACTER_NOT_IN_REPERTOIRE: SqlState = make_sqlstate(*b"22021");
pub const ERRCODE_UNTRANSLATABLE_CHARACTER: SqlState = make_sqlstate(*b"22P05");
pub const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: SqlState = make_sqlstate(*b"22003");
// Additive: SQLSTATE needed by the sequence generator (commands/sequence.c);
// see errcodes.txt -- class 22 data_exception, 2200H.
pub const ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"2200H");
pub const ERRCODE_INVALID_PARAMETER_VALUE: SqlState = make_sqlstate(*b"22023");
// Additive: SQLSTATE needed by the row-locking executor node (nodeLockRows.c);
// see errcodes.txt -- class 40 transaction_rollback.
pub const ERRCODE_T_R_SERIALIZATION_FAILURE: SqlState = make_sqlstate(*b"40001");
// Additive: SQLSTATE needed by the catalog index-scan engine (genam.c
// HandleConcurrentAbort); see errcodes.txt -- class 40 transaction_rollback.
pub const ERRCODE_TRANSACTION_ROLLBACK: SqlState = make_sqlstate(*b"40000");
// Additive: SQLSTATEs needed by the LIMIT/OFFSET executor node (nodeLimit.c);
// see errcodes.txt -- class 22 data_exception.
pub const ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: SqlState = make_sqlstate(*b"2201W");
pub const ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: SqlState = make_sqlstate(*b"2201X");
// Additive: SQLSTATEs needed by the full-text-search (tsvector/tsquery) port
// (see errcodes.txt -- class 22 data_exception).
pub const ERRCODE_ARRAY_SUBSCRIPT_ERROR: SqlState = make_sqlstate(*b"2202E");
pub const ERRCODE_NULL_VALUE_NOT_ALLOWED: SqlState = make_sqlstate(*b"22004");
pub const ERRCODE_ZERO_LENGTH_CHARACTER_STRING: SqlState = make_sqlstate(*b"2200F");
// Additive: SQLSTATEs needed by the numeric port (see errcodes.txt).
pub const ERRCODE_DIVISION_BY_ZERO: SqlState = make_sqlstate(*b"22012");
pub const ERRCODE_INVALID_ARGUMENT_FOR_LOG: SqlState = make_sqlstate(*b"2201E");
/// Alias matching the spelled-out errcodes.txt name (`invalid_argument_for_logarithm`).
pub const ERRCODE_INVALID_ARGUMENT_FOR_LOGARITHM: SqlState = ERRCODE_INVALID_ARGUMENT_FOR_LOG;
pub const ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION: SqlState = make_sqlstate(*b"2201F");
pub const ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: SqlState = make_sqlstate(*b"2201G");
pub const ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE: SqlState = make_sqlstate(*b"22013");
pub const ERRCODE_INVALID_BINARY_REPRESENTATION: SqlState = make_sqlstate(*b"22P03");
pub const ERRCODE_INVALID_TEXT_REPRESENTATION: SqlState = make_sqlstate(*b"22P02");
/// Additive: SQLSTATE needed by the COPY parser ports (`copyfromparse.c`;
/// see errcodes.txt: `bad_copy_file_format` in class 22 -- data_exception).
pub const ERRCODE_BAD_COPY_FILE_FORMAT: SqlState = make_sqlstate(*b"22P04");
// Additive: SQLSTATE needed by the name.c port (see errcodes.txt:
// `name_too_long` in class 42 -- syntax_error_or_access_rule_violation).
pub const ERRCODE_NAME_TOO_LONG: SqlState = make_sqlstate(*b"42622");
// Additive: SQLSTATEs needed by the parser ports (parse_node/type/coerce/oper/
// collate.c; see errcodes.txt).
pub const ERRCODE_TOO_MANY_COLUMNS: SqlState = make_sqlstate(*b"54011");
pub const ERRCODE_CANNOT_COERCE: SqlState = make_sqlstate(*b"42846");
pub const ERRCODE_COLLATION_MISMATCH: SqlState = make_sqlstate(*b"42P21");
// Additive: SQLSTATEs needed by the array + json ports (arrayfuncs.c /
// arrayutils.c + json; `array_element_error`/`array_subscript_error` share 2202E).
pub const ERRCODE_ARRAY_ELEMENT_ERROR: SqlState = make_sqlstate(*b"2202E");
// (dup removed by merge resolution) pub const ERRCODE_ARRAY_SUBSCRIPT_ERROR: SqlState = make_sqlstate(*b"2202E");
// (dup removed by merge resolution) pub const ERRCODE_NULL_VALUE_NOT_ALLOWED: SqlState = make_sqlstate(*b"22004");
// Additive: SQLSTATEs needed by the nodeSamplescan.c port (see errcodes.txt --
// class 22 data_exception).
pub const ERRCODE_INVALID_TABLESAMPLE_ARGUMENT: SqlState = make_sqlstate(*b"2202H");
pub const ERRCODE_INVALID_TABLESAMPLE_REPEAT: SqlState = make_sqlstate(*b"2202G");
// Additive: SQLSTATEs needed by the xml.c port (see errcodes.txt).
// Class 10 - XQuery Error.
pub const ERRCODE_INVALID_ARGUMENT_FOR_XQUERY: SqlState = make_sqlstate(*b"10608");
// Class 21 - Cardinality Violation.
pub const ERRCODE_CARDINALITY_VIOLATION: SqlState = make_sqlstate(*b"21000");
// Class 22 - Data Exception (XML subcodes).
pub const ERRCODE_NOT_AN_XML_DOCUMENT: SqlState = make_sqlstate(*b"2200L");
pub const ERRCODE_INVALID_XML_DOCUMENT: SqlState = make_sqlstate(*b"2200M");
pub const ERRCODE_INVALID_XML_CONTENT: SqlState = make_sqlstate(*b"2200N");
pub const ERRCODE_INVALID_XML_COMMENT: SqlState = make_sqlstate(*b"2200S");
pub const ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION: SqlState = make_sqlstate(*b"2200T");
// Class 24 / 34 - cursor errors (used by cursor_to_xml*).
pub const ERRCODE_INVALID_CURSOR_STATE: SqlState = make_sqlstate(*b"24000");
pub const ERRCODE_UNDEFINED_CURSOR: SqlState = make_sqlstate(*b"34000");
// Additive: SQLSTATE needed by the portalmem.c port (CreatePortal — a portal of
// the same name already exists; errcodes.txt class 42 "syntax error or access
// rule violation", subclass `duplicate_cursor`).
pub const ERRCODE_DUPLICATE_CURSOR: SqlState = make_sqlstate(*b"42P03");
// Additive: SQLSTATEs needed by the datetime port (see errcodes.txt).
pub const ERRCODE_DATETIME_VALUE_OUT_OF_RANGE: SqlState = make_sqlstate(*b"22008");
/// `datetime_field_overflow` -- shares SQLSTATE 22008 with
/// `ERRCODE_DATETIME_VALUE_OUT_OF_RANGE` (errcodes.txt); C uses this named code
/// for the date/time field-overflow paths.
pub const ERRCODE_DATETIME_FIELD_OVERFLOW: SqlState = make_sqlstate(*b"22008");
pub const ERRCODE_INVALID_DATETIME_FORMAT: SqlState = make_sqlstate(*b"22007");
pub const ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE: SqlState = make_sqlstate(*b"22009");
pub const ERRCODE_INTERVAL_FIELD_OVERFLOW: SqlState = make_sqlstate(*b"22015");
// Additive: SQLSTATEs needed by the json/jsonb/jsonpath ports (errcodes.txt,
// class 22 "data exception").
pub const ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE: SqlState = make_sqlstate(*b"22030");
pub const ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION: SqlState =
    make_sqlstate(*b"22031");
pub const ERRCODE_INVALID_JSON_TEXT: SqlState = make_sqlstate(*b"22032");
pub const ERRCODE_INVALID_SQL_JSON_SUBSCRIPT: SqlState = make_sqlstate(*b"22033");
pub const ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM: SqlState = make_sqlstate(*b"22034");
pub const ERRCODE_NO_SQL_JSON_ITEM: SqlState = make_sqlstate(*b"22035");
pub const ERRCODE_NON_NUMERIC_SQL_JSON_ITEM: SqlState = make_sqlstate(*b"22036");
pub const ERRCODE_NON_UNIQUE_KEYS_IN_A_JSON_OBJECT: SqlState = make_sqlstate(*b"22037");
pub const ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED: SqlState = make_sqlstate(*b"22038");
pub const ERRCODE_SQL_JSON_ARRAY_NOT_FOUND: SqlState = make_sqlstate(*b"22039");
pub const ERRCODE_SQL_JSON_MEMBER_NOT_FOUND: SqlState = make_sqlstate(*b"2203A");
pub const ERRCODE_SQL_JSON_NUMBER_NOT_FOUND: SqlState = make_sqlstate(*b"2203B");
pub const ERRCODE_SQL_JSON_OBJECT_NOT_FOUND: SqlState = make_sqlstate(*b"2203C");
pub const ERRCODE_TOO_MANY_JSON_ARRAY_ELEMENTS: SqlState = make_sqlstate(*b"2203D");
pub const ERRCODE_TOO_MANY_JSON_OBJECT_MEMBERS: SqlState = make_sqlstate(*b"2203E");
pub const ERRCODE_SQL_JSON_SCALAR_REQUIRED: SqlState = make_sqlstate(*b"2203F");
pub const ERRCODE_SQL_JSON_ITEM_CANNOT_BE_CAST_TO_TARGET_TYPE: SqlState = make_sqlstate(*b"2203G");

pub const ERRCODE_CONFIG_FILE_ERROR: SqlState = make_sqlstate(*b"F0000");
// Additive: SQLSTATE needed by the copyto.c port (ClosePipeToProgram — class 38
// external_routine_exception; see errcodes.txt).
pub const ERRCODE_EXTERNAL_ROUTINE_EXCEPTION: SqlState = make_sqlstate(*b"38000");
pub const ERRCODE_WRONG_OBJECT_TYPE: SqlState = make_sqlstate(*b"42809");
pub const ERRCODE_LOCK_NOT_AVAILABLE: SqlState = make_sqlstate(*b"55P03");
pub const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: SqlState = make_sqlstate(*b"55000");
// Additive: SQLSTATE needed by the pg_upgrade_support.c port (CHECK_IS_BINARY_UPGRADE
// raises this when the server is not in binary-upgrade mode; errcodes.txt --
// class 55 object_not_in_prerequisite_state, `cant_change_runtime_param`).
pub const ERRCODE_CANT_CHANGE_RUNTIME_PARAM: SqlState = make_sqlstate(*b"55P02");
// Additive: SQLSTATE needed by the tablespace port (CREATE TABLESPACE — directory
// already in use as a tablespace).
pub const ERRCODE_OBJECT_IN_USE: SqlState = make_sqlstate(*b"55006");
pub const ERRCODE_QUERY_CANCELED: SqlState = make_sqlstate(*b"57014");
pub const ERRCODE_ADMIN_SHUTDOWN: SqlState = make_sqlstate(*b"57P01");
pub const ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT: SqlState = make_sqlstate(*b"25P03");
pub const ERRCODE_TRANSACTION_TIMEOUT: SqlState = make_sqlstate(*b"25P04");
pub const ERRCODE_IDLE_SESSION_TIMEOUT: SqlState = make_sqlstate(*b"57P05");
pub const ERRCODE_INSUFFICIENT_RESOURCES: SqlState = make_sqlstate(*b"53000");
pub const ERRCODE_TOO_MANY_CONNECTIONS: SqlState = make_sqlstate(*b"53300");
pub const ERRCODE_CONFIGURATION_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"53400");
pub const ERRCODE_DISK_FULL: SqlState = make_sqlstate(*b"53100");
pub const ERRCODE_OUT_OF_MEMORY: SqlState = make_sqlstate(*b"53200");
pub const ERRCODE_PROGRAM_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"54000");
pub const ERRCODE_STATEMENT_TOO_COMPLEX: SqlState = make_sqlstate(*b"54001");
pub const ERRCODE_INTERNAL_ERROR: SqlState = make_sqlstate(*b"XX000");
pub const ERRCODE_DATA_CORRUPTED: SqlState = make_sqlstate(*b"XX001");
pub const ERRCODE_INDEX_CORRUPTED: SqlState = make_sqlstate(*b"XX002");
// Additive: SQLSTATEs needed by the nbtree access-method port (errcodes.txt).
pub const ERRCODE_INVALID_OBJECT_DEFINITION: SqlState = make_sqlstate(*b"42P17");
// Additive: SQLSTATE needed by the partitioning ports (check_new_partition_bound).
pub const ERRCODE_CHECK_VIOLATION: SqlState = make_sqlstate(*b"23514");
pub const ERRCODE_IO_ERROR: SqlState = make_sqlstate(*b"58030");
pub const ERRCODE_SYSTEM_ERROR: SqlState = make_sqlstate(*b"58000");
pub const ERRCODE_UNDEFINED_FILE: SqlState = make_sqlstate(*b"58P01");
pub const ERRCODE_DUPLICATE_FILE: SqlState = make_sqlstate(*b"58P02");
pub const ERRCODE_FILE_NAME_TOO_LONG: SqlState = make_sqlstate(*b"58P03");
// Additive: SQLSTATEs needed by the adt-batch2 port (see errcodes.txt).
pub const ERRCODE_STRING_DATA_RIGHT_TRUNCATION: SqlState = make_sqlstate(*b"22001");
pub const ERRCODE_STRING_DATA_LENGTH_MISMATCH: SqlState = make_sqlstate(*b"22026");
pub const ERRCODE_SUBSTRING_ERROR: SqlState = make_sqlstate(*b"22011");
// ERRCODE_ARRAY_SUBSCRIPT_ERROR already defined above (array port block).
pub const ERRCODE_INDETERMINATE_COLLATION: SqlState = make_sqlstate(*b"42P22");
pub const ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE: SqlState = make_sqlstate(*b"55P04");
// Additive: SQLSTATEs needed by the acl port (see errcodes.txt).
pub const ERRCODE_INVALID_GRANT_OPERATION: SqlState = make_sqlstate(*b"0LP01");
pub const ERRCODE_INVALID_GRANTOR: SqlState = make_sqlstate(*b"0L000");
pub const ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST: SqlState = make_sqlstate(*b"2BP01");
pub const ERRCODE_RESERVED_NAME: SqlState = make_sqlstate(*b"42939");
// Additive: SQLSTATEs needed by the windowfuncs port (see errcodes.txt:
// `invalid_argument_for_ntile_function` / `invalid_argument_for_nth_value_function`,
// class 22 -- data_exception).
pub const ERRCODE_INVALID_ARGUMENT_FOR_NTILE: SqlState = make_sqlstate(*b"22014");
pub const ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE: SqlState = make_sqlstate(*b"22016");
// Additive: SQLSTATEs needed by the parse_cte.c / parse_relation.c ports (errcodes.txt).
pub const ERRCODE_DUPLICATE_ALIAS: SqlState = make_sqlstate(*b"42712");
pub const ERRCODE_INVALID_RECURSION: SqlState = make_sqlstate(*b"42P19");
pub const ERRCODE_DUPLICATE_COLUMN: SqlState = make_sqlstate(*b"42701");
pub const ERRCODE_INVALID_COLUMN_REFERENCE: SqlState = make_sqlstate(*b"42P10");
pub const ERRCODE_AMBIGUOUS_COLUMN: SqlState = make_sqlstate(*b"42702");
pub const ERRCODE_AMBIGUOUS_ALIAS: SqlState = make_sqlstate(*b"42P09");
pub const ERRCODE_UNDEFINED_TABLE: SqlState = make_sqlstate(*b"42P01");
pub const ERRCODE_INVALID_TABLE_DEFINITION: SqlState = make_sqlstate(*b"42P16");
// Additive: SQL/JSON SQLSTATEs needed by the json-query port (jsonfuncs.c /
// jsonpath_exec.c; see errcodes.txt, class 22 data_exception).
// Additive: SQLSTATEs needed by the executor main loop (execMain.c) — class 23
// integrity_constraint_violation and class 44 with_check_option_violation; see
// errcodes.txt.
pub const ERRCODE_NOT_NULL_VIOLATION: SqlState = make_sqlstate(*b"23502");
pub const ERRCODE_EXCLUSION_VIOLATION: SqlState = make_sqlstate(*b"23P01");
pub const ERRCODE_WITH_CHECK_OPTION_VIOLATION: SqlState = make_sqlstate(*b"44000");
// Additive: SQLSTATEs needed by the executor expression ports (execExpr.c /
// execExprInterp.c; see errcodes.txt).
pub const ERRCODE_WINDOWING_ERROR: SqlState = make_sqlstate(*b"42P20");
// Additive: SQLSTATEs needed by the backend-commands ports (errcodes.txt).
// comment.c (COMMENT ON DATABASE dump work-around).
pub const ERRCODE_UNDEFINED_DATABASE: SqlState = make_sqlstate(*b"3D000");
// dbcommands.c (CREATE/RENAME DATABASE name conflict).
pub const ERRCODE_DUPLICATE_DATABASE: SqlState = make_sqlstate(*b"42P04");
// schemacmds.c (CREATE SCHEMA duplicate).
pub const ERRCODE_DUPLICATE_SCHEMA: SqlState = make_sqlstate(*b"42P06");
// createas.c (CREATE TABLE AS already-exists).
pub const ERRCODE_DUPLICATE_TABLE: SqlState = make_sqlstate(*b"42P07");
// portalcmds.c (empty cursor name); distinct named code, same value as
// ERRCODE_UNDEFINED_CURSOR (34000).
pub const ERRCODE_INVALID_CURSOR_NAME: SqlState = make_sqlstate(*b"34000");
// prepare.c (PREPARE/EXECUTE/DEALLOCATE).
pub const ERRCODE_INVALID_PSTATEMENT_DEFINITION: SqlState = make_sqlstate(*b"42P14");
pub const ERRCODE_DUPLICATE_PSTATEMENT: SqlState = make_sqlstate(*b"42P05");
pub const ERRCODE_UNDEFINED_PSTATEMENT: SqlState = make_sqlstate(*b"26000");
// constraint.c (deferred unique/exclusion trigger protocol).
pub const ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED: SqlState = make_sqlstate(*b"39P01");
// execSRF.c (set-returning-function value-per-call / materialize protocol).
pub const ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED: SqlState = make_sqlstate(*b"39P02");
// trigger.c (BEFORE trigger modified a row already modified by this command).
pub const ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION: SqlState = make_sqlstate(*b"27000");
// Additive: SQLSTATEs needed by the ri_triggers.c port (referential-integrity
// FK enforcement; see errcodes.txt class 23 -- integrity_constraint_violation).
// (UNIQUE_VIOLATION / E_R_I_E_TRIGGER_PROTOCOL_VIOLATED / INVALID_OBJECT_DEFINITION
// are already defined above, so only the two genuinely-new codes are added here.)
pub const ERRCODE_RESTRICT_VIOLATION: SqlState = make_sqlstate(*b"23001");
pub const ERRCODE_FOREIGN_KEY_VIOLATION: SqlState = make_sqlstate(*b"23503");

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorField(pub c_int);

pub const PG_DIAG_SCHEMA_NAME: ErrorField = ErrorField(b's' as c_int);
pub const PG_DIAG_TABLE_NAME: ErrorField = ErrorField(b't' as c_int);
pub const PG_DIAG_COLUMN_NAME: ErrorField = ErrorField(b'c' as c_int);
pub const PG_DIAG_DATATYPE_NAME: ErrorField = ErrorField(b'd' as c_int);
pub const PG_DIAG_CONSTRAINT_NAME: ErrorField = ErrorField(b'n' as c_int);

pub const LOG_DESTINATION_STDERR: c_int = 1;
pub const LOG_DESTINATION_SYSLOG: c_int = 2;
pub const LOG_DESTINATION_EVENTLOG: c_int = 4;
pub const LOG_DESTINATION_CSVLOG: c_int = 8;
pub const LOG_DESTINATION_JSONLOG: c_int = 16;

pub const fn pg_sixbit(ch: u8) -> c_int {
    ((ch as c_int) - (b'0' as c_int)) & 0x3f
}

pub const fn pg_unsixbit(value: c_int) -> u8 {
    (((value & 0x3f) + (b'0' as c_int)) & 0xff) as u8
}

pub const fn make_sqlstate(chars: [u8; 5]) -> SqlState {
    SqlState(
        pg_sixbit(chars[0])
            + (pg_sixbit(chars[1]) << 6)
            + (pg_sixbit(chars[2]) << 12)
            + (pg_sixbit(chars[3]) << 18)
            + (pg_sixbit(chars[4]) << 24),
    )
}

pub const fn unpack_sqlstate(sqlstate: SqlState) -> [u8; 5] {
    let value = sqlstate.0;
    [
        pg_unsixbit(value),
        pg_unsixbit(value >> 6),
        pg_unsixbit(value >> 12),
        pg_unsixbit(value >> 18),
        pg_unsixbit(value >> 24),
    ]
}

pub const fn errcode_to_category(sqlstate: SqlState) -> SqlState {
    SqlState(sqlstate.0 & ((1 << 12) - 1))
}

pub const fn errcode_is_category(sqlstate: SqlState) -> bool {
    (sqlstate.0 & !((1 << 12) - 1)) == 0
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(*mut c_void)>,
    pub arg: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ErrorData {
    pub elevel: c_int,
    pub output_to_server: bool,
    pub output_to_client: bool,
    pub hide_stmt: bool,
    pub hide_ctx: bool,
    pub filename: *const c_char,
    pub lineno: c_int,
    pub funcname: *const c_char,
    pub domain: *const c_char,
    pub context_domain: *const c_char,
    pub sqlerrcode: c_int,
    pub message: *mut c_char,
    pub detail: *mut c_char,
    pub detail_log: *mut c_char,
    pub hint: *mut c_char,
    pub context: *mut c_char,
    pub backtrace: *mut c_char,
    pub message_id: *const c_char,
    pub schema_name: *mut c_char,
    pub table_name: *mut c_char,
    pub column_name: *mut c_char,
    pub datatype_name: *mut c_char,
    pub constraint_name: *mut c_char,
    pub cursorpos: c_int,
    pub internalpos: c_int,
    pub internalquery: *mut c_char,
    pub saved_errno: c_int,
    pub assoc_context: *mut MemoryContextData,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ErrorSaveContext {
    pub type_: NodeTag,
    pub error_occurred: bool,
    pub details_wanted: bool,
    pub error_data: *mut ErrorData,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PgrustErrorData {
    pub elevel: c_int,
    pub sqlerrcode: c_int,
    pub message: *mut c_char,
    pub detail: *mut c_char,
    pub detail_log: *mut c_char,
    pub hint: *mut c_char,
    pub context: *mut c_char,
    pub backtrace: *mut c_char,
    pub message_id: *mut c_char,
    pub filename: *mut c_char,
    pub lineno: c_int,
    pub funcname: *mut c_char,
    pub domain: *mut c_char,
    pub context_domain: *mut c_char,
    pub hide_stmt: bool,
    pub hide_ctx: bool,
    pub saved_errno: c_int,
    pub has_saved_errno: bool,
    pub cursorpos: c_int,
    pub internalpos: c_int,
    pub internalquery: *mut c_char,
    pub schema_name: *mut c_char,
    pub table_name: *mut c_char,
    pub column_name: *mut c_char,
    pub datatype_name: *mut c_char,
    pub constraint_name: *mut c_char,
}

impl PgrustErrorData {
    pub const fn empty() -> Self {
        Self {
            elevel: ERROR.0,
            sqlerrcode: ERRCODE_INTERNAL_ERROR.0,
            message: core::ptr::null_mut(),
            detail: core::ptr::null_mut(),
            detail_log: core::ptr::null_mut(),
            hint: core::ptr::null_mut(),
            context: core::ptr::null_mut(),
            backtrace: core::ptr::null_mut(),
            message_id: core::ptr::null_mut(),
            filename: core::ptr::null_mut(),
            lineno: 0,
            funcname: core::ptr::null_mut(),
            domain: core::ptr::null_mut(),
            context_domain: core::ptr::null_mut(),
            hide_stmt: false,
            hide_ctx: false,
            saved_errno: 0,
            has_saved_errno: false,
            cursorpos: 0,
            internalpos: 0,
            internalquery: core::ptr::null_mut(),
            schema_name: core::ptr::null_mut(),
            table_name: core::ptr::null_mut(),
            column_name: core::ptr::null_mut(),
            datatype_name: core::ptr::null_mut(),
            constraint_name: core::ptr::null_mut(),
        }
    }
}

impl Default for PgrustErrorData {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_round_trips() {
        assert_eq!(unpack_sqlstate(make_sqlstate(*b"XX000")), *b"XX000");
        assert_eq!(unpack_sqlstate(ERRCODE_WARNING), *b"01000");
    }

    #[test]
    fn error_data_layout_matches_generated_shape() {
        assert_eq!(core::mem::size_of::<ErrorData>(), 184);
        assert_eq!(core::mem::align_of::<ErrorData>(), 8);
        assert_eq!(core::mem::size_of::<ErrorContextCallback>(), 24);
        assert_eq!(core::mem::size_of::<PgrustErrorData>(), 176);
        assert_eq!(core::mem::align_of::<PgrustErrorData>(), 8);
    }
}
