//! Error-reporting vocabulary from `utils/elog.h` and `utils/errcodes.h`.
//!
//! The `ERRCODE_*` table below is generated from Postgres's
//! `src/backend/utils/errcodes.txt` (the source from which the build generates
//! `errcodes.h`) and must stay a complete, value-exact copy of it.

/// Error severity level (`elog.h`). Values must match the C `#define`s: level
/// comparisons (`>= ERROR`, etc.) are pervasive semantics, not just labels.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ErrorLevel(pub i32);

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

/// A SQLSTATE packed into an `i32` with the `MAKE_SQLSTATE` six-bit encoding.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlState(pub i32);

// Class 00 - Successful Completion
pub const ERRCODE_SUCCESSFUL_COMPLETION: SqlState = make_sqlstate(*b"00000");

// Class 01 - Warning
pub const ERRCODE_WARNING: SqlState = make_sqlstate(*b"01000");
pub const ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED: SqlState = make_sqlstate(*b"0100C");
pub const ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING: SqlState = make_sqlstate(*b"01008");
pub const ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION: SqlState = make_sqlstate(*b"01003");
pub const ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED: SqlState = make_sqlstate(*b"01007");
pub const ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED: SqlState = make_sqlstate(*b"01006");
pub const ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION: SqlState = make_sqlstate(*b"01004");
pub const ERRCODE_WARNING_DEPRECATED_FEATURE: SqlState = make_sqlstate(*b"01P01");

// Class 02 - No Data (this is also a warning class per the SQL standard)
pub const ERRCODE_NO_DATA: SqlState = make_sqlstate(*b"02000");
pub const ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED: SqlState = make_sqlstate(*b"02001");

// Class 03 - SQL Statement Not Yet Complete
pub const ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE: SqlState = make_sqlstate(*b"03000");

// Class 08 - Connection Exception
pub const ERRCODE_CONNECTION_EXCEPTION: SqlState = make_sqlstate(*b"08000");
pub const ERRCODE_CONNECTION_DOES_NOT_EXIST: SqlState = make_sqlstate(*b"08003");
pub const ERRCODE_CONNECTION_FAILURE: SqlState = make_sqlstate(*b"08006");
pub const ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: SqlState = make_sqlstate(*b"08001");
pub const ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION: SqlState = make_sqlstate(*b"08004");
pub const ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN: SqlState = make_sqlstate(*b"08007");
pub const ERRCODE_PROTOCOL_VIOLATION: SqlState = make_sqlstate(*b"08P01");

// Class 09 - Triggered Action Exception
pub const ERRCODE_TRIGGERED_ACTION_EXCEPTION: SqlState = make_sqlstate(*b"09000");

// Class 0A - Feature Not Supported
pub const ERRCODE_FEATURE_NOT_SUPPORTED: SqlState = make_sqlstate(*b"0A000");

// Class 0B - Invalid Transaction Initiation
pub const ERRCODE_INVALID_TRANSACTION_INITIATION: SqlState = make_sqlstate(*b"0B000");

// Class 0F - Locator Exception
pub const ERRCODE_LOCATOR_EXCEPTION: SqlState = make_sqlstate(*b"0F000");
pub const ERRCODE_L_E_INVALID_SPECIFICATION: SqlState = make_sqlstate(*b"0F001");

// Class 0L - Invalid Grantor
pub const ERRCODE_INVALID_GRANTOR: SqlState = make_sqlstate(*b"0L000");
pub const ERRCODE_INVALID_GRANT_OPERATION: SqlState = make_sqlstate(*b"0LP01");

// Class 0P - Invalid Role Specification
pub const ERRCODE_INVALID_ROLE_SPECIFICATION: SqlState = make_sqlstate(*b"0P000");

// Class 0Z - Diagnostics Exception
pub const ERRCODE_DIAGNOSTICS_EXCEPTION: SqlState = make_sqlstate(*b"0Z000");
pub const ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER: SqlState = make_sqlstate(*b"0Z002");

// Class 10 - XQuery Error
pub const ERRCODE_INVALID_ARGUMENT_FOR_XQUERY: SqlState = make_sqlstate(*b"10608");

// Class 20 - Case Not Found
pub const ERRCODE_CASE_NOT_FOUND: SqlState = make_sqlstate(*b"20000");

// Class 21 - Cardinality Violation
pub const ERRCODE_CARDINALITY_VIOLATION: SqlState = make_sqlstate(*b"21000");

// Class 22 - Data Exception
pub const ERRCODE_DATA_EXCEPTION: SqlState = make_sqlstate(*b"22000");
pub const ERRCODE_ARRAY_ELEMENT_ERROR: SqlState = make_sqlstate(*b"2202E");
pub const ERRCODE_ARRAY_SUBSCRIPT_ERROR: SqlState = make_sqlstate(*b"2202E");
pub const ERRCODE_CHARACTER_NOT_IN_REPERTOIRE: SqlState = make_sqlstate(*b"22021");
pub const ERRCODE_DATETIME_FIELD_OVERFLOW: SqlState = make_sqlstate(*b"22008");
pub const ERRCODE_DATETIME_VALUE_OUT_OF_RANGE: SqlState = make_sqlstate(*b"22008");
pub const ERRCODE_DIVISION_BY_ZERO: SqlState = make_sqlstate(*b"22012");
pub const ERRCODE_ERROR_IN_ASSIGNMENT: SqlState = make_sqlstate(*b"22005");
pub const ERRCODE_ESCAPE_CHARACTER_CONFLICT: SqlState = make_sqlstate(*b"2200B");
pub const ERRCODE_INDICATOR_OVERFLOW: SqlState = make_sqlstate(*b"22022");
pub const ERRCODE_INTERVAL_FIELD_OVERFLOW: SqlState = make_sqlstate(*b"22015");
pub const ERRCODE_INVALID_ARGUMENT_FOR_LOG: SqlState = make_sqlstate(*b"2201E");
pub const ERRCODE_INVALID_ARGUMENT_FOR_NTILE: SqlState = make_sqlstate(*b"22014");
pub const ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE: SqlState = make_sqlstate(*b"22016");
pub const ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION: SqlState = make_sqlstate(*b"2201F");
pub const ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: SqlState = make_sqlstate(*b"2201G");
pub const ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST: SqlState = make_sqlstate(*b"22018");
pub const ERRCODE_INVALID_DATETIME_FORMAT: SqlState = make_sqlstate(*b"22007");
pub const ERRCODE_INVALID_ESCAPE_CHARACTER: SqlState = make_sqlstate(*b"22019");
pub const ERRCODE_INVALID_ESCAPE_OCTET: SqlState = make_sqlstate(*b"2200D");
pub const ERRCODE_INVALID_ESCAPE_SEQUENCE: SqlState = make_sqlstate(*b"22025");
pub const ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER: SqlState = make_sqlstate(*b"22P06");
pub const ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE: SqlState = make_sqlstate(*b"22010");
pub const ERRCODE_INVALID_PARAMETER_VALUE: SqlState = make_sqlstate(*b"22023");
pub const ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE: SqlState = make_sqlstate(*b"22013");
pub const ERRCODE_INVALID_REGULAR_EXPRESSION: SqlState = make_sqlstate(*b"2201B");
pub const ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: SqlState = make_sqlstate(*b"2201W");
pub const ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: SqlState = make_sqlstate(*b"2201X");
pub const ERRCODE_INVALID_TABLESAMPLE_ARGUMENT: SqlState = make_sqlstate(*b"2202H");
pub const ERRCODE_INVALID_TABLESAMPLE_REPEAT: SqlState = make_sqlstate(*b"2202G");
pub const ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE: SqlState = make_sqlstate(*b"22009");
pub const ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER: SqlState = make_sqlstate(*b"2200C");
pub const ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH: SqlState = make_sqlstate(*b"2200G");
pub const ERRCODE_NULL_VALUE_NOT_ALLOWED: SqlState = make_sqlstate(*b"22004");
pub const ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER: SqlState = make_sqlstate(*b"22002");
pub const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: SqlState = make_sqlstate(*b"22003");
pub const ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"2200H");
pub const ERRCODE_STRING_DATA_LENGTH_MISMATCH: SqlState = make_sqlstate(*b"22026");
pub const ERRCODE_STRING_DATA_RIGHT_TRUNCATION: SqlState = make_sqlstate(*b"22001");
pub const ERRCODE_SUBSTRING_ERROR: SqlState = make_sqlstate(*b"22011");
pub const ERRCODE_TRIM_ERROR: SqlState = make_sqlstate(*b"22027");
pub const ERRCODE_UNTERMINATED_C_STRING: SqlState = make_sqlstate(*b"22024");
pub const ERRCODE_ZERO_LENGTH_CHARACTER_STRING: SqlState = make_sqlstate(*b"2200F");
pub const ERRCODE_FLOATING_POINT_EXCEPTION: SqlState = make_sqlstate(*b"22P01");
pub const ERRCODE_INVALID_TEXT_REPRESENTATION: SqlState = make_sqlstate(*b"22P02");
pub const ERRCODE_INVALID_BINARY_REPRESENTATION: SqlState = make_sqlstate(*b"22P03");
pub const ERRCODE_BAD_COPY_FILE_FORMAT: SqlState = make_sqlstate(*b"22P04");
pub const ERRCODE_UNTRANSLATABLE_CHARACTER: SqlState = make_sqlstate(*b"22P05");
pub const ERRCODE_NOT_AN_XML_DOCUMENT: SqlState = make_sqlstate(*b"2200L");
pub const ERRCODE_INVALID_XML_DOCUMENT: SqlState = make_sqlstate(*b"2200M");
pub const ERRCODE_INVALID_XML_CONTENT: SqlState = make_sqlstate(*b"2200N");
pub const ERRCODE_INVALID_XML_COMMENT: SqlState = make_sqlstate(*b"2200S");
pub const ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION: SqlState = make_sqlstate(*b"2200T");
pub const ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE: SqlState = make_sqlstate(*b"22030");
pub const ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION: SqlState = make_sqlstate(*b"22031");
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

// Class 23 - Integrity Constraint Violation
pub const ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION: SqlState = make_sqlstate(*b"23000");
pub const ERRCODE_RESTRICT_VIOLATION: SqlState = make_sqlstate(*b"23001");
pub const ERRCODE_NOT_NULL_VIOLATION: SqlState = make_sqlstate(*b"23502");
pub const ERRCODE_FOREIGN_KEY_VIOLATION: SqlState = make_sqlstate(*b"23503");
pub const ERRCODE_UNIQUE_VIOLATION: SqlState = make_sqlstate(*b"23505");
pub const ERRCODE_CHECK_VIOLATION: SqlState = make_sqlstate(*b"23514");
pub const ERRCODE_EXCLUSION_VIOLATION: SqlState = make_sqlstate(*b"23P01");

// Class 24 - Invalid Cursor State
pub const ERRCODE_INVALID_CURSOR_STATE: SqlState = make_sqlstate(*b"24000");

// Class 25 - Invalid Transaction State
pub const ERRCODE_INVALID_TRANSACTION_STATE: SqlState = make_sqlstate(*b"25000");
pub const ERRCODE_ACTIVE_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25001");
pub const ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE: SqlState = make_sqlstate(*b"25002");
pub const ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL: SqlState = make_sqlstate(*b"25008");
pub const ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION: SqlState = make_sqlstate(*b"25003");
pub const ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION: SqlState = make_sqlstate(*b"25004");
pub const ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION: SqlState = make_sqlstate(*b"25005");
pub const ERRCODE_READ_ONLY_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25006");
pub const ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED: SqlState = make_sqlstate(*b"25007");
pub const ERRCODE_NO_ACTIVE_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25P01");
pub const ERRCODE_IN_FAILED_SQL_TRANSACTION: SqlState = make_sqlstate(*b"25P02");
pub const ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT: SqlState = make_sqlstate(*b"25P03");
pub const ERRCODE_TRANSACTION_TIMEOUT: SqlState = make_sqlstate(*b"25P04");

// Class 26 - Invalid SQL Statement Name
pub const ERRCODE_INVALID_SQL_STATEMENT_NAME: SqlState = make_sqlstate(*b"26000");

// Class 27 - Triggered Data Change Violation
pub const ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION: SqlState = make_sqlstate(*b"27000");

// Class 28 - Invalid Authorization Specification
pub const ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION: SqlState = make_sqlstate(*b"28000");
pub const ERRCODE_INVALID_PASSWORD: SqlState = make_sqlstate(*b"28P01");

// Class 2B - Dependent Privilege Descriptors Still Exist
pub const ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST: SqlState = make_sqlstate(*b"2B000");
pub const ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST: SqlState = make_sqlstate(*b"2BP01");

// Class 2D - Invalid Transaction Termination
pub const ERRCODE_INVALID_TRANSACTION_TERMINATION: SqlState = make_sqlstate(*b"2D000");

// Class 2F - SQL Routine Exception
pub const ERRCODE_SQL_ROUTINE_EXCEPTION: SqlState = make_sqlstate(*b"2F000");
pub const ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT: SqlState = make_sqlstate(*b"2F005");
pub const ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED: SqlState = make_sqlstate(*b"2F002");
pub const ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED: SqlState = make_sqlstate(*b"2F003");
pub const ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED: SqlState = make_sqlstate(*b"2F004");

// Class 34 - Invalid Cursor Name
pub const ERRCODE_INVALID_CURSOR_NAME: SqlState = make_sqlstate(*b"34000");

// Class 38 - External Routine Exception
pub const ERRCODE_EXTERNAL_ROUTINE_EXCEPTION: SqlState = make_sqlstate(*b"38000");
pub const ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED: SqlState = make_sqlstate(*b"38001");
pub const ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED: SqlState = make_sqlstate(*b"38002");
pub const ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED: SqlState = make_sqlstate(*b"38003");
pub const ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED: SqlState = make_sqlstate(*b"38004");

// Class 39 - External Routine Invocation Exception
pub const ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: SqlState = make_sqlstate(*b"39000");
pub const ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED: SqlState = make_sqlstate(*b"39001");
pub const ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED: SqlState = make_sqlstate(*b"39004");
pub const ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED: SqlState = make_sqlstate(*b"39P01");
pub const ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED: SqlState = make_sqlstate(*b"39P02");
pub const ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED: SqlState = make_sqlstate(*b"39P03");

// Class 3B - Savepoint Exception
pub const ERRCODE_SAVEPOINT_EXCEPTION: SqlState = make_sqlstate(*b"3B000");
pub const ERRCODE_S_E_INVALID_SPECIFICATION: SqlState = make_sqlstate(*b"3B001");

// Class 3D - Invalid Catalog Name
pub const ERRCODE_INVALID_CATALOG_NAME: SqlState = make_sqlstate(*b"3D000");

// Class 3F - Invalid Schema Name
pub const ERRCODE_INVALID_SCHEMA_NAME: SqlState = make_sqlstate(*b"3F000");

// Class 40 - Transaction Rollback
pub const ERRCODE_TRANSACTION_ROLLBACK: SqlState = make_sqlstate(*b"40000");
pub const ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION: SqlState = make_sqlstate(*b"40002");
pub const ERRCODE_T_R_SERIALIZATION_FAILURE: SqlState = make_sqlstate(*b"40001");
pub const ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN: SqlState = make_sqlstate(*b"40003");
pub const ERRCODE_T_R_DEADLOCK_DETECTED: SqlState = make_sqlstate(*b"40P01");

// Class 42 - Syntax Error or Access Rule Violation
pub const ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: SqlState = make_sqlstate(*b"42000");
pub const ERRCODE_SYNTAX_ERROR: SqlState = make_sqlstate(*b"42601");
pub const ERRCODE_INSUFFICIENT_PRIVILEGE: SqlState = make_sqlstate(*b"42501");
pub const ERRCODE_CANNOT_COERCE: SqlState = make_sqlstate(*b"42846");
pub const ERRCODE_GROUPING_ERROR: SqlState = make_sqlstate(*b"42803");
pub const ERRCODE_WINDOWING_ERROR: SqlState = make_sqlstate(*b"42P20");
pub const ERRCODE_INVALID_RECURSION: SqlState = make_sqlstate(*b"42P19");
pub const ERRCODE_INVALID_FOREIGN_KEY: SqlState = make_sqlstate(*b"42830");
pub const ERRCODE_INVALID_NAME: SqlState = make_sqlstate(*b"42602");
pub const ERRCODE_NAME_TOO_LONG: SqlState = make_sqlstate(*b"42622");
pub const ERRCODE_RESERVED_NAME: SqlState = make_sqlstate(*b"42939");
pub const ERRCODE_DATATYPE_MISMATCH: SqlState = make_sqlstate(*b"42804");
pub const ERRCODE_INDETERMINATE_DATATYPE: SqlState = make_sqlstate(*b"42P18");
pub const ERRCODE_COLLATION_MISMATCH: SqlState = make_sqlstate(*b"42P21");
pub const ERRCODE_INDETERMINATE_COLLATION: SqlState = make_sqlstate(*b"42P22");
pub const ERRCODE_WRONG_OBJECT_TYPE: SqlState = make_sqlstate(*b"42809");
pub const ERRCODE_GENERATED_ALWAYS: SqlState = make_sqlstate(*b"428C9");
pub const ERRCODE_UNDEFINED_COLUMN: SqlState = make_sqlstate(*b"42703");
pub const ERRCODE_UNDEFINED_CURSOR: SqlState = make_sqlstate(*b"34000");
pub const ERRCODE_UNDEFINED_DATABASE: SqlState = make_sqlstate(*b"3D000");
pub const ERRCODE_UNDEFINED_FUNCTION: SqlState = make_sqlstate(*b"42883");
pub const ERRCODE_UNDEFINED_PSTATEMENT: SqlState = make_sqlstate(*b"26000");
pub const ERRCODE_UNDEFINED_SCHEMA: SqlState = make_sqlstate(*b"3F000");
pub const ERRCODE_UNDEFINED_TABLE: SqlState = make_sqlstate(*b"42P01");
pub const ERRCODE_UNDEFINED_PARAMETER: SqlState = make_sqlstate(*b"42P02");
pub const ERRCODE_UNDEFINED_OBJECT: SqlState = make_sqlstate(*b"42704");
pub const ERRCODE_DUPLICATE_COLUMN: SqlState = make_sqlstate(*b"42701");
pub const ERRCODE_DUPLICATE_CURSOR: SqlState = make_sqlstate(*b"42P03");
pub const ERRCODE_DUPLICATE_DATABASE: SqlState = make_sqlstate(*b"42P04");
pub const ERRCODE_DUPLICATE_FUNCTION: SqlState = make_sqlstate(*b"42723");
pub const ERRCODE_DUPLICATE_PSTATEMENT: SqlState = make_sqlstate(*b"42P05");
pub const ERRCODE_DUPLICATE_SCHEMA: SqlState = make_sqlstate(*b"42P06");
pub const ERRCODE_DUPLICATE_TABLE: SqlState = make_sqlstate(*b"42P07");
pub const ERRCODE_DUPLICATE_ALIAS: SqlState = make_sqlstate(*b"42712");
pub const ERRCODE_DUPLICATE_OBJECT: SqlState = make_sqlstate(*b"42710");
pub const ERRCODE_AMBIGUOUS_COLUMN: SqlState = make_sqlstate(*b"42702");
pub const ERRCODE_AMBIGUOUS_FUNCTION: SqlState = make_sqlstate(*b"42725");
pub const ERRCODE_AMBIGUOUS_PARAMETER: SqlState = make_sqlstate(*b"42P08");
pub const ERRCODE_AMBIGUOUS_ALIAS: SqlState = make_sqlstate(*b"42P09");
pub const ERRCODE_INVALID_COLUMN_REFERENCE: SqlState = make_sqlstate(*b"42P10");
pub const ERRCODE_INVALID_COLUMN_DEFINITION: SqlState = make_sqlstate(*b"42611");
pub const ERRCODE_INVALID_CURSOR_DEFINITION: SqlState = make_sqlstate(*b"42P11");
pub const ERRCODE_INVALID_DATABASE_DEFINITION: SqlState = make_sqlstate(*b"42P12");
pub const ERRCODE_INVALID_FUNCTION_DEFINITION: SqlState = make_sqlstate(*b"42P13");
pub const ERRCODE_INVALID_PSTATEMENT_DEFINITION: SqlState = make_sqlstate(*b"42P14");
pub const ERRCODE_INVALID_SCHEMA_DEFINITION: SqlState = make_sqlstate(*b"42P15");
pub const ERRCODE_INVALID_TABLE_DEFINITION: SqlState = make_sqlstate(*b"42P16");
pub const ERRCODE_INVALID_OBJECT_DEFINITION: SqlState = make_sqlstate(*b"42P17");

// Class 44 - WITH CHECK OPTION Violation
pub const ERRCODE_WITH_CHECK_OPTION_VIOLATION: SqlState = make_sqlstate(*b"44000");

// Class 53 - Insufficient Resources
pub const ERRCODE_INSUFFICIENT_RESOURCES: SqlState = make_sqlstate(*b"53000");
pub const ERRCODE_DISK_FULL: SqlState = make_sqlstate(*b"53100");
pub const ERRCODE_OUT_OF_MEMORY: SqlState = make_sqlstate(*b"53200");
pub const ERRCODE_TOO_MANY_CONNECTIONS: SqlState = make_sqlstate(*b"53300");
pub const ERRCODE_CONFIGURATION_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"53400");

// Class 54 - Program Limit Exceeded
pub const ERRCODE_PROGRAM_LIMIT_EXCEEDED: SqlState = make_sqlstate(*b"54000");
pub const ERRCODE_STATEMENT_TOO_COMPLEX: SqlState = make_sqlstate(*b"54001");
pub const ERRCODE_TOO_MANY_COLUMNS: SqlState = make_sqlstate(*b"54011");
pub const ERRCODE_TOO_MANY_ARGUMENTS: SqlState = make_sqlstate(*b"54023");

// Class 55 - Object Not In Prerequisite State
pub const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: SqlState = make_sqlstate(*b"55000");
pub const ERRCODE_OBJECT_IN_USE: SqlState = make_sqlstate(*b"55006");
pub const ERRCODE_CANT_CHANGE_RUNTIME_PARAM: SqlState = make_sqlstate(*b"55P02");
pub const ERRCODE_LOCK_NOT_AVAILABLE: SqlState = make_sqlstate(*b"55P03");
pub const ERRCODE_UNSAFE_NEW_ENUM_VALUE_USAGE: SqlState = make_sqlstate(*b"55P04");

// Class 57 - Operator Intervention
pub const ERRCODE_OPERATOR_INTERVENTION: SqlState = make_sqlstate(*b"57000");
pub const ERRCODE_QUERY_CANCELED: SqlState = make_sqlstate(*b"57014");
pub const ERRCODE_ADMIN_SHUTDOWN: SqlState = make_sqlstate(*b"57P01");
pub const ERRCODE_CRASH_SHUTDOWN: SqlState = make_sqlstate(*b"57P02");
pub const ERRCODE_CANNOT_CONNECT_NOW: SqlState = make_sqlstate(*b"57P03");
pub const ERRCODE_DATABASE_DROPPED: SqlState = make_sqlstate(*b"57P04");
pub const ERRCODE_IDLE_SESSION_TIMEOUT: SqlState = make_sqlstate(*b"57P05");

// Class 58 - System Error (errors external to PostgreSQL itself)
pub const ERRCODE_SYSTEM_ERROR: SqlState = make_sqlstate(*b"58000");
pub const ERRCODE_IO_ERROR: SqlState = make_sqlstate(*b"58030");
pub const ERRCODE_UNDEFINED_FILE: SqlState = make_sqlstate(*b"58P01");
pub const ERRCODE_DUPLICATE_FILE: SqlState = make_sqlstate(*b"58P02");
pub const ERRCODE_FILE_NAME_TOO_LONG: SqlState = make_sqlstate(*b"58P03");

// Class F0 - Configuration File Error
pub const ERRCODE_CONFIG_FILE_ERROR: SqlState = make_sqlstate(*b"F0000");
pub const ERRCODE_LOCK_FILE_EXISTS: SqlState = make_sqlstate(*b"F0001");

// Class HV - Foreign Data Wrapper Error (SQL/MED)
pub const ERRCODE_FDW_ERROR: SqlState = make_sqlstate(*b"HV000");
pub const ERRCODE_FDW_COLUMN_NAME_NOT_FOUND: SqlState = make_sqlstate(*b"HV005");
pub const ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED: SqlState = make_sqlstate(*b"HV002");
pub const ERRCODE_FDW_FUNCTION_SEQUENCE_ERROR: SqlState = make_sqlstate(*b"HV010");
pub const ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION: SqlState = make_sqlstate(*b"HV021");
pub const ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE: SqlState = make_sqlstate(*b"HV024");
pub const ERRCODE_FDW_INVALID_COLUMN_NAME: SqlState = make_sqlstate(*b"HV007");
pub const ERRCODE_FDW_INVALID_COLUMN_NUMBER: SqlState = make_sqlstate(*b"HV008");
pub const ERRCODE_FDW_INVALID_DATA_TYPE: SqlState = make_sqlstate(*b"HV004");
pub const ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS: SqlState = make_sqlstate(*b"HV006");
pub const ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER: SqlState = make_sqlstate(*b"HV091");
pub const ERRCODE_FDW_INVALID_HANDLE: SqlState = make_sqlstate(*b"HV00B");
pub const ERRCODE_FDW_INVALID_OPTION_INDEX: SqlState = make_sqlstate(*b"HV00C");
pub const ERRCODE_FDW_INVALID_OPTION_NAME: SqlState = make_sqlstate(*b"HV00D");
pub const ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH: SqlState = make_sqlstate(*b"HV090");
pub const ERRCODE_FDW_INVALID_STRING_FORMAT: SqlState = make_sqlstate(*b"HV00A");
pub const ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER: SqlState = make_sqlstate(*b"HV009");
pub const ERRCODE_FDW_TOO_MANY_HANDLES: SqlState = make_sqlstate(*b"HV014");
pub const ERRCODE_FDW_OUT_OF_MEMORY: SqlState = make_sqlstate(*b"HV001");
pub const ERRCODE_FDW_NO_SCHEMAS: SqlState = make_sqlstate(*b"HV00P");
pub const ERRCODE_FDW_OPTION_NAME_NOT_FOUND: SqlState = make_sqlstate(*b"HV00J");
pub const ERRCODE_FDW_REPLY_HANDLE: SqlState = make_sqlstate(*b"HV00K");
pub const ERRCODE_FDW_SCHEMA_NOT_FOUND: SqlState = make_sqlstate(*b"HV00Q");
pub const ERRCODE_FDW_TABLE_NOT_FOUND: SqlState = make_sqlstate(*b"HV00R");
pub const ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION: SqlState = make_sqlstate(*b"HV00L");
pub const ERRCODE_FDW_UNABLE_TO_CREATE_REPLY: SqlState = make_sqlstate(*b"HV00M");
pub const ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION: SqlState = make_sqlstate(*b"HV00N");

// Class P0 - PL/pgSQL Error
pub const ERRCODE_PLPGSQL_ERROR: SqlState = make_sqlstate(*b"P0000");
pub const ERRCODE_RAISE_EXCEPTION: SqlState = make_sqlstate(*b"P0001");
pub const ERRCODE_NO_DATA_FOUND: SqlState = make_sqlstate(*b"P0002");
pub const ERRCODE_TOO_MANY_ROWS: SqlState = make_sqlstate(*b"P0003");
pub const ERRCODE_ASSERT_FAILURE: SqlState = make_sqlstate(*b"P0004");

// Class XX - Internal Error
pub const ERRCODE_INTERNAL_ERROR: SqlState = make_sqlstate(*b"XX000");
pub const ERRCODE_DATA_CORRUPTED: SqlState = make_sqlstate(*b"XX001");
pub const ERRCODE_INDEX_CORRUPTED: SqlState = make_sqlstate(*b"XX002");

/// PQ-protocol error-field identifier (`postgres_ext.h` `PG_DIAG_*`).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrorField(pub i32);

pub const PG_DIAG_SCHEMA_NAME: ErrorField = ErrorField(b's' as i32);
pub const PG_DIAG_TABLE_NAME: ErrorField = ErrorField(b't' as i32);
pub const PG_DIAG_COLUMN_NAME: ErrorField = ErrorField(b'c' as i32);
pub const PG_DIAG_DATATYPE_NAME: ErrorField = ErrorField(b'd' as i32);
pub const PG_DIAG_CONSTRAINT_NAME: ErrorField = ErrorField(b'n' as i32);

pub const fn pg_sixbit(ch: u8) -> i32 {
    ((ch as i32) - (b'0' as i32)) & 0x3f
}

pub const fn pg_unsixbit(value: i32) -> u8 {
    (((value & 0x3f) + (b'0' as i32)) & 0xff) as u8
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstate_round_trips() {
        assert_eq!(unpack_sqlstate(make_sqlstate(*b"XX000")), *b"XX000");
        assert_eq!(unpack_sqlstate(ERRCODE_WARNING), *b"01000");
    }

    #[test]
    fn category_helpers() {
        assert_eq!(
            errcode_to_category(ERRCODE_DIVISION_BY_ZERO),
            ERRCODE_DATA_EXCEPTION
        );
        assert!(errcode_is_category(ERRCODE_DATA_EXCEPTION));
        assert!(!errcode_is_category(ERRCODE_DIVISION_BY_ZERO));
    }
}
