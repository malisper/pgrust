//! ABI vocabulary for the Server Programming Interface (`executor/spi.c`) and
//! the SQL-language function executor (`executor/functions.c`).
//!
//! These `#[repr(C)]` structs / typedefs / constants mirror the C definitions in
//!   * `src/include/executor/spi.h`        (SPITupleTable, SPIPrepareOptions,
//!                                           SPIExecuteOptions, SPIParseOpenOptions,
//!                                           SPIPlanPtr, the SPI_OK_*/SPI_ERROR_*
//!                                           result codes, SPI_OPT_NONATOMIC)
//!   * `src/include/executor/spi_priv.h`   (`_SPI_plan`, `_SPI_connection`,
//!                                           `_SPI_PLAN_MAGIC`)
//!   * `src/include/executor/functions.h`  (`SQLFunctionParseInfo`)
//!
//! Layout-critical fields keep their exact C order/width.  Catalog/executor
//! sub-objects this workspace surfaces elsewhere are reused (`TupleDesc`,
//! `SubTransactionId`, `ParamListInfo`, `ParserSetupHook`, `MemoryContext`,
//! `List`, `Oid`, `slist_head`/`slist_node`); the few executor handles the SPI
//! signatures mention but which are not yet modeled as full `#[repr(C)]` structs
//! (`QueryDesc`, `Portal`) are held as pointer-width opaque handles, which is
//! ABI-identical to the C pointers they stand in for.
//!
//! Surfaced through a dedicated module (referenced as `pgrust_pg_ffi::spi::*`),
//! NOT the crate-root glob, to avoid the ambiguous-glob name-collision trap.

use core::ffi::{c_char, c_int, c_void};

use crate::guc::slist_head;
use crate::heaptuple::{HeapTuple, TupleDesc};
use crate::list::List;
use crate::memory::MemoryContext;
use crate::params::{ParamListInfo, ParserSetupHook};
use crate::xact::SubTransactionId;
use crate::Oid;

/* ===========================================================================
 * Tiny opaque executor handles the SPI/functions signatures mention.
 *
 * `QueryDesc` and `Portal` are not yet modeled as full `#[repr(C)]` structs in
 * this workspace; the SPI signatures only ever pass them by pointer, so a
 * pointer-width opaque handle is ABI-identical.  `RawParseMode` and
 * `FetchDirection` are plain C enums (int-width).
 * =========================================================================== */

/// Opaque `QueryDesc` (from `executor/execdesc.h`).
pub type QueryDesc = c_void;

/// `Portal` — opaque handle to a `PortalData` (`utils/portal.h`).
pub type Portal = *mut c_void;

/// `RawParseMode` (`parser/parser.h`) — C enum, int-width.
pub type RawParseMode = c_int;
pub const RAW_PARSE_DEFAULT: RawParseMode = 0;
pub const RAW_PARSE_TYPE_NAME: RawParseMode = 1;
pub const RAW_PARSE_PLPGSQL_EXPR: RawParseMode = 2;
pub const RAW_PARSE_PLPGSQL_ASSIGN1: RawParseMode = 3;
pub const RAW_PARSE_PLPGSQL_ASSIGN2: RawParseMode = 4;
pub const RAW_PARSE_PLPGSQL_ASSIGN3: RawParseMode = 5;

/// `FetchDirection` (`nodes/parsenodes.h`) — C enum, int-width.
pub type FetchDirection = c_int;
pub const FETCH_FORWARD: FetchDirection = 0;
pub const FETCH_BACKWARD: FetchDirection = 1;
pub const FETCH_ABSOLUTE: FetchDirection = 2;
pub const FETCH_RELATIVE: FetchDirection = 3;

/* ===========================================================================
 * spi.h public types
 * =========================================================================== */

/// `SPITupleTable` (`src/include/executor/spi.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SPITupleTable {
    /* Public members */
    /// tuple descriptor.
    pub tupdesc: TupleDesc,
    /// array of tuples.
    pub vals: *mut HeapTuple,
    /// number of valid tuples.
    pub numvals: u64,

    /* Private members, not intended for external callers */
    /// allocated length of `vals` array.
    pub alloced: u64,
    /// memory context of result table.
    pub tuptabcxt: MemoryContext,
    /// link for internal bookkeeping.
    pub next: crate::guc::slist_node,
    /// subxact in which tuptable was created.
    pub subid: SubTransactionId,
}

/// `SPIPrepareOptions` — optional arguments for `SPI_prepare_extended`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SPIPrepareOptions {
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
    pub parseMode: RawParseMode,
    pub cursorOptions: c_int,
}

/// `SPIExecuteOptions` — optional arguments for `SPI_execute[_plan]_extended`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SPIExecuteOptions {
    pub params: ParamListInfo,
    pub read_only: bool,
    pub allow_nonatomic: bool,
    pub must_return_tuples: bool,
    pub tcount: u64,
    pub dest: *mut c_void,  // DestReceiver *
    pub owner: *mut c_void, // ResourceOwner
}

/// `SPIParseOpenOptions` — optional arguments for `SPI_cursor_parse_open`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SPIParseOpenOptions {
    pub params: ParamListInfo,
    pub cursorOptions: c_int,
    pub read_only: bool,
}

/* ===========================================================================
 * spi_priv.h private types
 * =========================================================================== */

/// `_SPI_PLAN_MAGIC` (`spi_priv.h`).
pub const _SPI_PLAN_MAGIC: c_int = 569278163;

/// `_SPI_plan` (`spi_priv.h`).  `SPIPlanPtr` is `*mut _SPI_plan`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct _SPI_plan {
    /// should equal `_SPI_PLAN_MAGIC`.
    pub magic: c_int,
    /// saved or unsaved plan?
    pub saved: bool,
    /// one-shot plan?
    pub oneshot: bool,
    /// one `CachedPlanSource` per parsetree.
    pub plancache_list: *mut List,
    /// Context containing `_SPI_plan` and data.
    pub plancxt: MemoryContext,
    /// `raw_parser()` mode.
    pub parse_mode: RawParseMode,
    /// Cursor options used for planning.
    pub cursor_options: c_int,
    /// number of plan arguments.
    pub nargs: c_int,
    /// Argument types (NULL if nargs is 0).
    pub argtypes: *mut Oid,
    /// alternative parameter spec method.
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
}

/// `SPIPlanPtr` — plans are opaque structs for standard users of SPI.
pub type SPIPlanPtr = *mut _SPI_plan;

/// `_SPI_connection` (`spi_priv.h`) — per-nesting-level SPI state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct _SPI_connection {
    /* current results */
    /// by Executor.
    pub processed: u64,
    /// tuptable currently being built.
    pub tuptable: *mut SPITupleTable,

    /// subtransaction in which current Executor call was started.
    pub execSubid: SubTransactionId,

    /* resources of this execution context */
    /// list of all live `SPITupleTable`s.
    pub tuptables: slist_head,
    /// procedure context.
    pub procCxt: MemoryContext,
    /// executor context.
    pub execCxt: MemoryContext,
    /// context of `SPI_connect`'s caller.
    pub savedcxt: MemoryContext,
    /// ID of connecting subtransaction.
    pub connectSubid: SubTransactionId,
    /// query environment setup for SPI level (`QueryEnvironment *`).
    pub queryEnv: *mut c_void,

    /* transaction management support */
    /// atomic execution context, does not allow transactions.
    pub atomic: bool,
    /// SPI-managed transaction boundary, skip cleanup.
    pub internal_xact: bool,

    /* saved values of API global variables for previous nesting level */
    pub outer_processed: u64,
    pub outer_tuptable: *mut SPITupleTable,
    pub outer_result: c_int,
}

/* ===========================================================================
 * spi.h result codes  (negative = error, positive = success)
 * =========================================================================== */

pub const SPI_ERROR_CONNECT: c_int = -1;
pub const SPI_ERROR_COPY: c_int = -2;
pub const SPI_ERROR_OPUNKNOWN: c_int = -3;
pub const SPI_ERROR_UNCONNECTED: c_int = -4;
pub const SPI_ERROR_CURSOR: c_int = -5; /* not used anymore */
pub const SPI_ERROR_ARGUMENT: c_int = -6;
pub const SPI_ERROR_PARAM: c_int = -7;
pub const SPI_ERROR_TRANSACTION: c_int = -8;
pub const SPI_ERROR_NOATTRIBUTE: c_int = -9;
pub const SPI_ERROR_NOOUTFUNC: c_int = -10;
pub const SPI_ERROR_TYPUNKNOWN: c_int = -11;
pub const SPI_ERROR_REL_DUPLICATE: c_int = -12;
pub const SPI_ERROR_REL_NOT_FOUND: c_int = -13;

pub const SPI_OK_CONNECT: c_int = 1;
pub const SPI_OK_FINISH: c_int = 2;
pub const SPI_OK_FETCH: c_int = 3;
pub const SPI_OK_UTILITY: c_int = 4;
pub const SPI_OK_SELECT: c_int = 5;
pub const SPI_OK_SELINTO: c_int = 6;
pub const SPI_OK_INSERT: c_int = 7;
pub const SPI_OK_DELETE: c_int = 8;
pub const SPI_OK_UPDATE: c_int = 9;
pub const SPI_OK_CURSOR: c_int = 10;
pub const SPI_OK_INSERT_RETURNING: c_int = 11;
pub const SPI_OK_DELETE_RETURNING: c_int = 12;
pub const SPI_OK_UPDATE_RETURNING: c_int = 13;
pub const SPI_OK_REWRITTEN: c_int = 14;
pub const SPI_OK_REL_REGISTER: c_int = 15;
pub const SPI_OK_REL_UNREGISTER: c_int = 16;
pub const SPI_OK_TD_REGISTER: c_int = 17;
pub const SPI_OK_MERGE: c_int = 18;
pub const SPI_OK_MERGE_RETURNING: c_int = 19;

pub const SPI_OPT_NONATOMIC: c_int = 1 << 0;

/* ===========================================================================
 * functions.h types  (SQL-language function executor)
 * =========================================================================== */

/// `SQLFunctionParseInfo` (`src/include/executor/functions.h`) — data needed by
/// the parser callback hooks to resolve parameter references during parsing of
/// a SQL function's body.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SQLFunctionParseInfo {
    /// function's name.
    pub fname: *mut c_char,
    /// number of input arguments.
    pub nargs: c_int,
    /// resolved types of input arguments.
    pub argtypes: *mut Oid,
    /// names of input arguments; NULL if none (each entry can be NULL).
    pub argnames: *mut *mut c_char,
    /// function's input collation, if known.
    pub collation: Oid,
}

/// `SQLFunctionParseInfoPtr`.
pub type SQLFunctionParseInfoPtr = *mut SQLFunctionParseInfo;

/// `SQLFunctionCache` — opaque to other subsystems.  Defined privately inside
/// `functions.c` (the struct body lives in `backend-executor-functions`); here
/// it is a pointer-width opaque handle so cross-subsystem signatures that pass
/// it by pointer remain ABI-correct.
pub type SQLFunctionCache = c_void;

/// `SQLFunctionCachePtr`.
pub type SQLFunctionCachePtr = *mut SQLFunctionCache;

/// `SQLFunctionHashEntry` — opaque to other subsystems (body in `functions.c`).
pub type SQLFunctionHashEntry = c_void;
