//! Routines to support running PostgreSQL in *bootstrap* mode
//! (`src/backend/bootstrap/bootstrap.c`).
//!
//! Bootstrap mode creates the initial template database. The bootstrap backend
//! does not speak SQL; it reads commands in the BKI bootstrap language, driven
//! by the bootparse/bootscanner front end, which calls into the routines here:
//! [`boot_openrel`], [`DefineAttr`], [`InsertOneTuple`] / [`InsertOneValue`] /
//! [`InsertOneNull`], [`index_register`], and finally [`build_indices`].
//!
//! Process-local globals (`boot_reldesc`, `attrtypes[]`/`numattr`,
//! `values[]`/`Nulls[]`, the cached `pg_type` contents `Typ`/`Ap`, and the
//! registered-index list `ILHead`) are per-backend state (AGENTS.md "Backend
//! global state"): they live in a `thread_local!` cell, mutated by the one
//! bootstrap thread of control. The genuinely external pieces — the relcache,
//! the heap-AM insert, the fmgr type I/O calls, the bootstrap-backend lifecycle
//! and the bootparse front end — are reached through their owners' `-seams`
//! crates, panicking until those owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use backend_utils_error::ereport;
use mcx::Mcx;
use types_tuple::backend_access_common_heaptuple::Datum as TupleDatum;
use types_error::{ErrorLocation, PgError, PgResult};
use types_error::{DEBUG4, ERROR, WARNING};
use types_error::ERRCODE_SYNTAX_ERROR;
use types_guc::guc::{PGC_INTERNAL, PGC_POSTMASTER, PGC_S_ARGV, PGC_S_DYNAMIC_DEFAULT};
use types_nodes::execnodes::IndexInfo;
use types_rel::{Relation, RelationData};
use types_signal::SigHandler;
use types_startup::DispatchOption;
use types_tuple::heaptuple::{FormData_pg_attribute, NameData, ATTRIBUTE_FIXED_PART_SIZE};
use types_tuple::pg_type::FormData_pg_type;

const FILE: &str = "bootstrap.c";

fn loc(lineno: i32, funcname: &str) -> ErrorLocation {
    ErrorLocation::new(FILE, lineno, funcname)
}

/* ----------------
 *		constants from headers
 * ---------------- */

/// `MAXATTR` (`bootstrap/bootstrap.h`) — max attributes in a bootstrapped rel.
pub const MAXATTR: usize = 40;

/// `BOOTCOL_NULL_AUTO` (`bootstrap/bootstrap.h`).
pub const BOOTCOL_NULL_AUTO: i32 = 1;
/// `BOOTCOL_NULL_FORCE_NULL` (`bootstrap/bootstrap.h`).
pub const BOOTCOL_NULL_FORCE_NULL: i32 = 2;
/// `BOOTCOL_NULL_FORCE_NOT_NULL` (`bootstrap/bootstrap.h`).
pub const BOOTCOL_NULL_FORCE_NOT_NULL: i32 = 3;

/// `NAMEDATALEN` (`pg_config_manual.h`).
pub const NAMEDATALEN: usize = 64;

/// `PG_DATA_CHECKSUM_VERSION` (`storage/bufpage.h`).
pub const PG_DATA_CHECKSUM_VERSION: u32 = 1;

/// `InvalidOid`.
const InvalidOid: Oid = 0;
/// `InvalidCompressionMethod` (`access/toast_compression.h`) — `'\0'`.
const InvalidCompressionMethod: i8 = 0;

type Oid = types_core::primitive::Oid;

/* ----------------
 *		type OIDs used in the TypInfo[] table (catalog OIDs, well-known)
 * ---------------- */

const BOOLOID: Oid = 16;
const BYTEAOID: Oid = 17;
const CHAROID: Oid = 18;
const NAMEOID: Oid = 19;
const INT2OID: Oid = 21;
const INT2VECTOROID: Oid = 22;
const INT4OID: Oid = 23;
const REGPROCOID: Oid = 24;
const TEXTOID: Oid = 25;
const OIDOID: Oid = 26;
const TIDOID: Oid = 27;
const XIDOID: Oid = 28;
const CIDOID: Oid = 29;
const OIDVECTOROID: Oid = 30;
const PG_NODE_TREEOID: Oid = 194;
const FLOAT4OID: Oid = 700;
const INT4ARRAYOID: Oid = 1007;
const ACLITEMOID: Oid = 1033;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGROLEOID: Oid = 4096;
const REGNAMESPACEOID: Oid = 4089;

const C_COLLATION_OID: Oid = 950;
const DEFAULT_COLLATION_OID: Oid = 100;

const TYPALIGN_CHAR: i8 = b'c' as i8;
const TYPALIGN_SHORT: i8 = b's' as i8;
const TYPALIGN_INT: i8 = b'i' as i8;
const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;

/* fmgr OIDs of the boot-time type I/O routines (utils/fmgroids.h) */

const F_BOOLIN: Oid = 1242;
const F_BOOLOUT: Oid = 1243;
const F_BYTEAIN: Oid = 1244;
const F_BYTEAOUT: Oid = 31;
const F_CHARIN: Oid = 1245;
const F_CHAROUT: Oid = 33;
const F_INT2IN: Oid = 38;
const F_INT2OUT: Oid = 39;
const F_INT4IN: Oid = 42;
const F_INT4OUT: Oid = 43;
const F_FLOAT4IN: Oid = 200;
const F_FLOAT4OUT: Oid = 201;
const F_NAMEIN: Oid = 34;
const F_NAMEOUT: Oid = 35;
const F_REGCLASSIN: Oid = 2218;
const F_REGCLASSOUT: Oid = 2219;
const F_REGPROCIN: Oid = 44;
const F_REGPROCOUT: Oid = 45;
const F_REGTYPEIN: Oid = 2220;
const F_REGTYPEOUT: Oid = 2221;
const F_REGROLEIN: Oid = 4098;
const F_REGROLEOUT: Oid = 4092;
const F_REGNAMESPACEIN: Oid = 4084;
const F_REGNAMESPACEOUT: Oid = 4085;
const F_TEXTIN: Oid = 46;
const F_TEXTOUT: Oid = 47;
const F_OIDIN: Oid = 1798;
const F_OIDOUT: Oid = 1799;
const F_TIDIN: Oid = 48;
const F_TIDOUT: Oid = 49;
const F_XIDIN: Oid = 50;
const F_XIDOUT: Oid = 51;
const F_CIDIN: Oid = 52;
const F_CIDOUT: Oid = 53;
const F_PG_NODE_TREE_IN: Oid = 195;
const F_PG_NODE_TREE_OUT: Oid = 196;
const F_INT2VECTORIN: Oid = 40;
const F_INT2VECTOROUT: Oid = 41;
const F_OIDVECTORIN: Oid = 54;
const F_OIDVECTOROUT: Oid = 55;
const F_ARRAY_IN: Oid = 750;
const F_ARRAY_OUT: Oid = 751;

/// `struct typinfo` (`bootstrap.c`). `name` is compared against the requested
/// type name exactly as the C `strncmp(…, NAMEDATALEN)`.
#[derive(Clone, Copy)]
pub struct TypInfoEntry {
    pub name: &'static str,
    pub oid: Oid,
    pub elem: Oid,
    pub len: i16,
    pub byval: bool,
    pub align: i8,
    pub storage: i8,
    pub collation: Oid,
    pub inproc: Oid,
    pub outproc: Oid,
}

/// `static const struct typinfo TypInfo[]`.
static TYP_INFO: &[TypInfoEntry] = &[
    TypInfoEntry { name: "bool", oid: BOOLOID, elem: 0, len: 1, byval: true, align: TYPALIGN_CHAR, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_BOOLIN, outproc: F_BOOLOUT },
    TypInfoEntry { name: "bytea", oid: BYTEAOID, elem: 0, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: InvalidOid, inproc: F_BYTEAIN, outproc: F_BYTEAOUT },
    TypInfoEntry { name: "char", oid: CHAROID, elem: 0, len: 1, byval: true, align: TYPALIGN_CHAR, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_CHARIN, outproc: F_CHAROUT },
    TypInfoEntry { name: "int2", oid: INT2OID, elem: 0, len: 2, byval: true, align: TYPALIGN_SHORT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_INT2IN, outproc: F_INT2OUT },
    TypInfoEntry { name: "int4", oid: INT4OID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_INT4IN, outproc: F_INT4OUT },
    TypInfoEntry { name: "float4", oid: FLOAT4OID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_FLOAT4IN, outproc: F_FLOAT4OUT },
    TypInfoEntry { name: "name", oid: NAMEOID, elem: CHAROID, len: NAMEDATALEN as i16, byval: false, align: TYPALIGN_CHAR, storage: TYPSTORAGE_PLAIN, collation: C_COLLATION_OID, inproc: F_NAMEIN, outproc: F_NAMEOUT },
    TypInfoEntry { name: "regclass", oid: REGCLASSOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_REGCLASSIN, outproc: F_REGCLASSOUT },
    TypInfoEntry { name: "regproc", oid: REGPROCOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_REGPROCIN, outproc: F_REGPROCOUT },
    TypInfoEntry { name: "regtype", oid: REGTYPEOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_REGTYPEIN, outproc: F_REGTYPEOUT },
    TypInfoEntry { name: "regrole", oid: REGROLEOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_REGROLEIN, outproc: F_REGROLEOUT },
    TypInfoEntry { name: "regnamespace", oid: REGNAMESPACEOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_REGNAMESPACEIN, outproc: F_REGNAMESPACEOUT },
    TypInfoEntry { name: "text", oid: TEXTOID, elem: 0, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: DEFAULT_COLLATION_OID, inproc: F_TEXTIN, outproc: F_TEXTOUT },
    TypInfoEntry { name: "oid", oid: OIDOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_OIDIN, outproc: F_OIDOUT },
    TypInfoEntry { name: "tid", oid: TIDOID, elem: 0, len: 6, byval: false, align: TYPALIGN_SHORT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_TIDIN, outproc: F_TIDOUT },
    TypInfoEntry { name: "xid", oid: XIDOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_XIDIN, outproc: F_XIDOUT },
    TypInfoEntry { name: "cid", oid: CIDOID, elem: 0, len: 4, byval: true, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_CIDIN, outproc: F_CIDOUT },
    TypInfoEntry { name: "pg_node_tree", oid: PG_NODE_TREEOID, elem: 0, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: DEFAULT_COLLATION_OID, inproc: F_PG_NODE_TREE_IN, outproc: F_PG_NODE_TREE_OUT },
    TypInfoEntry { name: "int2vector", oid: INT2VECTOROID, elem: INT2OID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_INT2VECTORIN, outproc: F_INT2VECTOROUT },
    TypInfoEntry { name: "oidvector", oid: OIDVECTOROID, elem: OIDOID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_PLAIN, collation: InvalidOid, inproc: F_OIDVECTORIN, outproc: F_OIDVECTOROUT },
    TypInfoEntry { name: "_int4", oid: INT4ARRAYOID, elem: INT4OID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: InvalidOid, inproc: F_ARRAY_IN, outproc: F_ARRAY_OUT },
    TypInfoEntry { name: "_text", oid: 1009, elem: TEXTOID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: DEFAULT_COLLATION_OID, inproc: F_ARRAY_IN, outproc: F_ARRAY_OUT },
    TypInfoEntry { name: "_oid", oid: 1028, elem: OIDOID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: InvalidOid, inproc: F_ARRAY_IN, outproc: F_ARRAY_OUT },
    TypInfoEntry { name: "_char", oid: 1002, elem: CHAROID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: InvalidOid, inproc: F_ARRAY_IN, outproc: F_ARRAY_OUT },
    TypInfoEntry { name: "_aclitem", oid: 1034, elem: ACLITEMOID, len: -1, byval: false, align: TYPALIGN_INT, storage: TYPSTORAGE_EXTENDED, collation: InvalidOid, inproc: F_ARRAY_IN, outproc: F_ARRAY_OUT },
];

/// `static const int n_types`.
fn n_types() -> usize {
    TYP_INFO.len()
}

/// `struct typmap` — `{ Oid am_oid; FormData_pg_type am_typ; }`.
#[derive(Clone, Copy)]
pub struct TypMap {
    pub am_oid: Oid,
    pub am_typ: FormData_pg_type,
}

/// `typedef struct _IndexList` — a declared-but-not-yet-built index.
///
/// `IndexInfo` no longer derives `Clone`/`Copy` (it now owns `ExprState` /
/// `Opaque` members), so the list owns its `IndexInfo` by move and the struct
/// is no longer `Clone`. The list lives in process-local bootstrap state, so
/// the carried `IndexInfo` is `'static`.
pub struct IndexList {
    pub il_heap: Oid,
    pub il_ind: Oid,
    pub il_info: IndexInfo<'static>,
}

/* ----------------
 *		process-local global state (bootstrap is single-threaded)
 * ---------------- */

struct BootstrapState {
    /// `Relation boot_reldesc` — the currently-open relation, or `None`
    /// (the C NULL `Relation`). Held over the bootstrap process lifetime.
    boot_reldesc: Option<Relation<'static>>,
    /// `Form_pg_attribute attrtypes[MAXATTR]` — accumulated attribute info;
    /// each slot is `None` until first allocated (`AllocateAttribute`).
    attrtypes: [Option<FormData_pg_attribute>; MAXATTR],
    /// `int numattr` — number of attributes for current relation.
    numattr: i32,
    /// `static List *Typ = NIL` — cached `pg_type` contents (`None` = NIL).
    typ: Option<Vec<TypMap>>,
    /// `static struct typmap *Ap` — index into `typ` of the last match.
    ap: Option<usize>,
    /// `static Datum values[MAXATTR]`. Holds the canonical `Datum<'static>`
    /// (`heap_form_tuple`'s element type) so that by-reference column images
    /// (`text`/`bytea`/`oidvector`/etc.) carry their actual bytes, exactly as
    /// C's `Datum values[]` holds pointers to the palloc'd referents.
    values: [TupleDatum<'static>; MAXATTR],
    /// `static bool Nulls[MAXATTR]`.
    nulls: [bool; MAXATTR],
    /// `static IndexList *ILHead` — the declared-index list. The Vec's *last*
    /// element is the C list head (push-onto-head + head-to-tail walk).
    il_head: Vec<IndexList>,
}

impl BootstrapState {
    fn new() -> Self {
        BootstrapState {
            boot_reldesc: None,
            attrtypes: [None; MAXATTR],
            numattr: 0,
            typ: None,
            ap: None,
            // Canonical `Datum` is not `Copy` (the by-ref arm owns a `PgVec`),
            // so build each slot with `(Datum) 0`.
            values: std::array::from_fn(|_| TupleDatum::null()),
            nulls: [false; MAXATTR],
            il_head: Vec::new(),
        }
    }
}

thread_local! {
    static STATE: RefCell<BootstrapState> = RefCell::new(BootstrapState::new());
}

/* ===========================================================================
 * CheckerModeMain.
 * ========================================================================= */

/// In shared-memory checker mode, all we really want to do is create shared
/// memory and semaphores (already done by `CreateSharedMemoryAndSemaphores`),
/// so there is nothing more to do here.
fn CheckerModeMain() -> ! {
    backend_storage_ipc_ipc_seams::proc_exit::call(0)
}

/* ===========================================================================
 * BootstrapModeMain — the main entry point for running in bootstrap mode.
 * ========================================================================= */

/// The main entry point for running the backend in bootstrap mode.
///
/// `check_only` true: startup is done only far enough to verify the
/// configuration (esp. shared-memory sizing) works up to shared-memory
/// creation. `mcx` is the bootstrap process-lifetime context.
pub fn BootstrapModeMain(mcx: Mcx<'static>, argv: Vec<String>, check_only: bool) -> PgResult<()> {
    let progname = argv[0].clone();
    let mut userDoption: Option<String> = None;
    let mut bootstrap_data_checksum_version: u32 = 0; /* No checksum */

    debug_assert!(!is_under_postmaster());

    backend_utils_init_miscinit_seams::init_standalone_process::call(&argv[0])?;

    /* Set defaults, to be overridden by explicit options below */
    backend_utils_misc_guc_seams::initialize_guc_options::call()?;

    /* an initial --boot or --check should be present */
    debug_assert!(argv.len() > 1 && (argv[1] == "--boot" || argv[1] == "--check"));

    /* argv++; argc-- — drop argv[0], so getopt sees the program-stripped args */
    let mut getopt = Getopt::new(&argv[1..], "B:c:d:D:Fkr:X:-:");

    loop {
        let flag = match getopt.next() {
            Some(f) => f,
            None => break,
        };

        match flag {
            'B' => {
                set_config_option_argv(
                    "shared_buffers",
                    &getopt.optarg_or("BootstrapModeMain: missing argument for -B option")?,
                )?;
            }
            '-' => {
                /*
                 * Error if the user misplaced a special must-be-first option for
                 * dispatching to a subprogram.  parse_dispatch_option() returns
                 * DISPATCH_POSTMASTER if it doesn't find a match, so error for
                 * anything else.
                 */
                let optarg = getopt.optarg_or("BootstrapModeMain: missing argument for long option")?;
                if backend_main_main_seams::parse_dispatch_option::call(&optarg)
                    != DispatchOption::DISPATCH_POSTMASTER
                {
                    return ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!("--{} must be first argument", optarg))
                        .finish(loc(239, "BootstrapModeMain"));
                }

                /* FALLTHROUGH */
                handle_c_option(mcx, flag, &getopt)?;
            }
            'c' => {
                handle_c_option(mcx, flag, &getopt)?;
            }
            'D' => {
                userDoption = Some(getopt.optarg_or("BootstrapModeMain: missing argument for -D option")?);
            }
            'd' => {
                /* Turn on debugging for the bootstrap process. */
                let debugstr = format!(
                    "debug{}",
                    getopt.optarg_or("BootstrapModeMain: missing argument for -d option")?
                );
                backend_utils_misc_guc_seams::set_config_option::call(
                    "log_min_messages",
                    &debugstr,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                )?;
                backend_utils_misc_guc_seams::set_config_option::call(
                    "client_min_messages",
                    &debugstr,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                )?;
            }
            'F' => {
                set_config_option_argv("fsync", "false")?;
            }
            'k' => {
                bootstrap_data_checksum_version = PG_DATA_CHECKSUM_VERSION;
            }
            'r' => {
                set_output_file_name(&getopt.optarg_or("BootstrapModeMain: missing argument for -r option")?);
            }
            'X' => {
                backend_utils_misc_guc_seams::set_config_option::call(
                    "wal_segment_size",
                    &getopt.optarg_or("BootstrapModeMain: missing argument for -X option")?,
                    PGC_INTERNAL,
                    PGC_S_DYNAMIC_DEFAULT,
                )?;
            }
            _ => {
                write_stderr(format!("Try \"{}\" --help\" for more information.\n", progname));
                backend_storage_ipc_ipc_seams::proc_exit::call(1);
            }
        }
    }

    if getopt.argc() != getopt.optind {
        write_stderr(format!("{}: invalid command-line arguments\n", progname));
        backend_storage_ipc_ipc_seams::proc_exit::call(1);
    }

    /* Acquire configuration parameters */
    if !backend_utils_misc_guc_seams::select_config_files::call(userDoption.as_deref(), &progname)? {
        backend_storage_ipc_ipc_seams::proc_exit::call(1);
    }

    /* Validate the DataDir and change into it */
    backend_utils_init_miscinit_seams::check_data_dir::call()?;
    backend_utils_init_miscinit_seams::change_to_data_dir::call()?;

    backend_utils_init_miscinit_seams::create_data_dir_lock_file::call(false)?;

    backend_utils_init_miscinit_seams::set_processing_mode_bootstrap::call();
    backend_utils_init_miscinit_seams::set_ignore_system_indexes::call(true);

    backend_utils_init_postinit_seams::initialize_max_backends::call()?;

    /*
     * Even though bootstrapping runs in single-process mode, initialize
     * postmaster child slots so --check can detect running out of shared
     * memory or other resources if max_connections is set too high.
     */
    backend_postmaster_pmchild_seams::init_postmaster_child_slots::call();

    backend_utils_init_postinit_seams::initialize_fast_path_locks::call();

    backend_storage_ipc_ipci_seams::create_shared_memory_and_semaphores::call();

    /*
     * Estimate number of openable files.  Essential in --check mode too,
     * because on some platforms semaphores count as open files.
     */
    backend_storage_file_fd_seams::set_max_safe_fds::call()?;

    if check_only {
        backend_utils_init_miscinit_seams::set_processing_mode_normal::call();
        CheckerModeMain();
    }

    /* Do backend-like initialization for bootstrap mode */
    backend_storage_lmgr_proc_seams::init_process::call()?;

    backend_utils_init_postinit_seams::base_init::call()?;

    bootstrap_signals();
    backend_access_transam_xlog_seams::boot_strap_xlog::call(bootstrap_data_checksum_version)?;

    /*
     * To ensure src/common/link-canary.c is linked into the backend, call it
     * from somewhere.  Here is as good as anywhere.
     */
    if backend_common_link_canary_seams::pg_link_canary_is_frontend::call() {
        return ereport(ERROR)
            .errmsg_internal("backend is incorrectly linked to frontend functions")
            .finish(loc(371, "BootstrapModeMain"));
    }

    backend_utils_init_postinit_seams::init_postgres_bootstrap::call(mcx)?;

    /* Initialize stuff for bootstrap-file processing */
    STATE.with(|s| {
        let mut st = s.borrow_mut();
        for i in 0..MAXATTR {
            st.attrtypes[i] = None;
            st.nulls[i] = false;
        }
    });

    if backend_bootstrap_bootparse_seams::boot_yylex_init::call() != 0 {
        return ereport(ERROR)
            .errmsg_internal("yylex_init() failed: %m")
            .finish(loc(383, "BootstrapModeMain"));
    }

    /* Process bootstrap input. */
    backend_access_transam_xact_seams::start_transaction_command::call()?;
    backend_bootstrap_bootparse_seams::boot_yyparse::call(mcx)?;
    backend_access_transam_xact_seams::commit_transaction_command::call()?;

    /*
     * We should now know about all mapped relations, so write out the initial
     * relation mapping files.
     */
    backend_utils_cache_relmapper_seams::relation_map_finish_bootstrap::call()?;

    /* Clean up and exit */
    cleanup(mcx)?;
    backend_storage_ipc_ipc_seams::proc_exit::call(0);
}

/// The shared body of the `-c`/`--`/`-` (`case 'c'`) option, including the
/// `--%s`/`-c %s requires a value` error split. `flag` is `'c'` or `'-'`.
fn handle_c_option(mcx: Mcx<'static>, flag: char, getopt: &Getopt) -> PgResult<()> {
    let optarg = getopt.optarg_or("handle_c_option: missing option argument")?;
    let (name, value) = backend_utils_misc_guc_seams::parse_long_option::call(mcx, &optarg)?;
    match value {
        None => {
            if flag == '-' {
                return ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("--{} requires a value", optarg))
                    .finish(loc(254, "BootstrapModeMain"));
            } else {
                return ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("-c {} requires a value", optarg))
                    .finish(loc(259, "BootstrapModeMain"));
            }
        }
        Some(value) => {
            set_config_option_argv(name.as_str(), value.as_str())?;
        }
    }
    Ok(())
}

/// `SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV)`.
fn set_config_option_argv(name: &str, value: &str) -> PgResult<()> {
    backend_utils_misc_guc_seams::set_config_option::call(name, value, PGC_POSTMASTER, PGC_S_ARGV)
}

/// `strlcpy(OutputFileName, optarg, MAXPGPATH)`.
fn set_output_file_name(name: &str) {
    // OutputFileName is a bootstrap-mode global owned by globals.c; the BKI
    // dump path consults it. Backend-local write.
    OUTPUT_FILE_NAME.with(|c| *c.borrow_mut() = name.to_string());
}

thread_local! {
    static OUTPUT_FILE_NAME: RefCell<String> = const { RefCell::new(String::new()) };
}

/// `IsUnderPostmaster` global (a per-backend read).
fn is_under_postmaster() -> bool {
    // bootstrap always runs standalone; the C global is false here.
    false
}

/// `write_stderr(fmt, …)`.
fn write_stderr(message: String) {
    eprint!("{}", message);
}

/* ===========================================================================
 * misc functions
 * ========================================================================= */

/// Set up signal handling for a bootstrap process.
///
/// We don't need any non-default signal handling in bootstrap mode; "curl up
/// and die" is sufficient. Set that explicitly, as documentation if nothing
/// else.
fn bootstrap_signals() {
    debug_assert!(!is_under_postmaster());
    port_pqsignal_seams::pqsignal::call(libc::SIGHUP, SigHandler::Default);
    port_pqsignal_seams::pqsignal::call(libc::SIGINT, SigHandler::Default);
    port_pqsignal_seams::pqsignal::call(libc::SIGTERM, SigHandler::Default);
    port_pqsignal_seams::pqsignal::call(libc::SIGQUIT, SigHandler::Default);
}

/* ===========================================================================
 * MANUAL BACKEND INTERACTIVE INTERFACE COMMANDS
 * ========================================================================= */

/// `boot_openrel` — execute BKI `OPEN` command.
pub fn boot_openrel(mcx: Mcx<'static>, relname: &str) -> PgResult<()> {
    /* if (strlen(relname) >= NAMEDATALEN) relname[NAMEDATALEN-1] = '\0'; */
    let relname: String = if relname.len() >= NAMEDATALEN {
        String::from(&relname[..NAMEDATALEN - 1])
    } else {
        String::from(relname)
    };

    /*
     * pg_type must be filled before any OPEN command is executed, so populate
     * Typ now if we haven't yet.
     */
    if typ_is_nil() {
        populate_typ_list(mcx)?;
    }

    if boot_reldesc_is_open() {
        closerel(mcx, None)?;
    }

    ereport(DEBUG4)
        .errmsg_internal(format!(
            "open relation {}, attrsize {}",
            relname, ATTRIBUTE_FIXED_PART_SIZE as i32
        ))
        .finish(loc(457, "boot_openrel"))?;

    let rel = backend_access_table_table::table_openrv(
        mcx,
        &make_range_var(&relname),
        types_storage::lock::NoLock,
    )?;

    let numattr = relation_get_number_of_attributes(&rel);
    set_numattr(numattr);
    for i in 0..numattr {
        if attrtypes(i as usize).is_none() {
            set_attrtypes(i as usize, Some(AllocateAttribute()));
        }
        /* memmove(attrtypes[i], TupleDescAttr(rd_att, i), ATTRIBUTE_FIXED_PART_SIZE) */
        let at = relation_get_attr(&rel, i);
        set_attrtypes(i as usize, Some(at));

        ereport(DEBUG4)
            .errmsg_internal(format!(
                "create attribute {} name {} len {} num {} type {}",
                i,
                namestr(&at.attname),
                at.attlen,
                at.attnum,
                at.atttypid
            ))
            .finish(loc(473, "boot_openrel"))?;
    }

    set_boot_reldesc(Some(rel));
    Ok(())
}

/// `closerel`.
pub fn closerel(_mcx: Mcx<'static>, relname: Option<&str>) -> PgResult<()> {
    if let Some(relname) = relname {
        if boot_reldesc_is_open() {
            if with_boot_reldesc(|r| r.name() != relname) {
                let open_name = with_boot_reldesc(|r| r.name().to_string());
                return ereport(ERROR)
                    .errmsg_internal(format!("close of {} when {} was expected", relname, open_name))
                    .finish(loc(492, "closerel"));
            }
        } else {
            return ereport(ERROR)
                .errmsg_internal(format!("close of {} before any relation was opened", relname))
                .finish(loc(496, "closerel"));
        }
    }

    if !boot_reldesc_is_open() {
        return ereport(ERROR)
            .errmsg_internal("no open relation to close")
            .finish(loc(501, "closerel"));
    } else {
        let open_name = with_boot_reldesc(|r| r.name().to_string());
        ereport(DEBUG4)
            .errmsg_internal(format!("close relation {}", open_name))
            .finish(loc(504, "closerel"))?;
        let rel = take_boot_reldesc().expect("checked open above");
        rel.close(types_storage::lock::NoLock)?; /* table_close(rel, NoLock) */
    }

    Ok(())
}

/// `DefineAttr` — define a `<field,type>` pair. Called once per field.
pub fn DefineAttr(mcx: Mcx<'static>, name: &str, type_: &str, attnum: i32, nullness: i32) -> PgResult<()> {
    if boot_reldesc_is_open() {
        ereport(WARNING)
            .errmsg_internal("no open relations allowed with CREATE command")
            .finish(loc(528, "DefineAttr"))?;
        closerel(mcx, None)?;
    }

    let attnum = attnum as usize;

    if attrtypes(attnum).is_none() {
        set_attrtypes(attnum, Some(AllocateAttribute()));
    }
    /* MemSet(attrtypes[attnum], 0, ATTRIBUTE_FIXED_PART_SIZE) */
    let mut at = FormData_pg_attribute::default();

    namestrcpy(&mut at.attname, name);
    ereport(DEBUG4)
        .errmsg_internal(format!("column {} {}", namestr(&at.attname), type_))
        .finish(loc(537, "DefineAttr"))?;
    at.attnum = (attnum + 1) as i16;

    let typeoid = gettype(mcx, type_)?;

    if !typ_is_nil() {
        let ap = ap_entry();
        at.atttypid = ap.am_oid;
        at.attlen = ap.am_typ.typlen;
        at.attbyval = ap.am_typ.typbyval;
        at.attalign = ap.am_typ.typalign;
        at.attstorage = ap.am_typ.typstorage;
        at.attcompression = InvalidCompressionMethod;
        at.attcollation = ap.am_typ.typcollation;
        /* if an array type, assume 1-dimensional attribute */
        if ap.am_typ.typelem != InvalidOid && ap.am_typ.typlen < 0 {
            at.attndims = 1;
        } else {
            at.attndims = 0;
        }
    } else {
        let ti = &TYP_INFO[typeoid as usize];
        at.atttypid = ti.oid;
        at.attlen = ti.len;
        at.attbyval = ti.byval;
        at.attalign = ti.align;
        at.attstorage = ti.storage;
        at.attcompression = InvalidCompressionMethod;
        at.attcollation = ti.collation;
        /* if an array type, assume 1-dimensional attribute */
        if ti.elem != InvalidOid && at.attlen < 0 {
            at.attndims = 1;
        } else {
            at.attndims = 0;
        }
    }

    /*
     * If a system catalog column is collation-aware, force it to use C
     * collation, so its behavior is independent of the database's collation.
     * Essential to allow template0 to be cloned with a different collation.
     */
    if oid_is_valid(at.attcollation) {
        at.attcollation = C_COLLATION_OID;
    }

    at.atttypmod = -1;
    at.attislocal = true;

    if nullness == BOOTCOL_NULL_FORCE_NOT_NULL {
        at.attnotnull = true;
    } else if nullness == BOOTCOL_NULL_FORCE_NULL {
        at.attnotnull = false;
    } else {
        debug_assert!(nullness == BOOTCOL_NULL_AUTO);

        /*
         * Mark as "not null" if type is fixed-width and prior columns are
         * likewise fixed-width and not-null.  This corresponds to the case
         * where the column can be accessed directly via C struct declaration.
         */
        if at.attlen > 0 {
            /* check earlier attributes */
            let mut i = 0usize;
            while i < attnum {
                let earlier = attrtypes(i).ok_or_else(|| {
                    PgError::error("DefineAttr: attrtypes[i] is NULL for earlier attribute")
                })?;
                if earlier.attlen <= 0 || !earlier.attnotnull {
                    break;
                }
                i += 1;
            }
            if i == attnum {
                at.attnotnull = true;
            }
        }
    }

    set_attrtypes(attnum, Some(at));
    Ok(())
}

/// `InsertOneTuple` — assemble and insert the current row.
pub fn InsertOneTuple(mcx: Mcx<'static>) -> PgResult<()> {
    let numattr = numattr() as usize;

    ereport(DEBUG4)
        .errmsg_internal(format!("inserting row with {} columns", numattr))
        .finish(loc(635, "InsertOneTuple"))?;

    /*
     * tupDesc = CreateTupleDesc(numattr, attrtypes);
     * tuple = heap_form_tuple(tupDesc, values, Nulls);
     * pfree(tupDesc);
     * simple_heap_insert(boot_reldesc, tuple);
     * heap_freetuple(tuple);
     */
    let mut attr: Vec<FormData_pg_attribute> = Vec::with_capacity(numattr);
    let mut vals: Vec<TupleDatum<'static>> = Vec::with_capacity(numattr);
    let mut nulls: Vec<bool> = Vec::with_capacity(numattr);
    for i in 0..numattr {
        attr.push(attrtypes(i).ok_or_else(|| {
            PgError::error("InsertOneTuple: attrtypes[i] set for an open relation")
        })?);
        vals.push(values(i));
        nulls.push(nulls_(i));
    }
    with_boot_reldesc_rel_res(|rel| {
        backend_access_heap_heapam_seams::insert_one_tuple::call(mcx, rel, &attr, &vals, &nulls)
    })?;

    ereport(DEBUG4)
        .errmsg_internal("row inserted")
        .finish(loc(643, "InsertOneTuple"))?;

    /* Reset null markers for next tuple */
    for i in 0..numattr {
        set_nulls(i, false);
    }

    Ok(())
}

/// `InsertOneValue`.
pub fn InsertOneValue(mcx: Mcx<'static>, value: &str, i: i32) -> PgResult<()> {
    debug_assert!(i >= 0 && (i as usize) < MAXATTR);

    ereport(DEBUG4)
        .errmsg_internal(format!("inserting column {} value \"{}\"", i, value))
        .finish(loc(670, "InsertOneValue"))?;

    let typoid = with_boot_reldesc(|rel| relation_get_attr(rel, i).atttypid);

    let io = boot_get_type_io_data(typoid)?;

    let datum = backend_utils_fmgr_fmgr_seams::oid_input_function_call::call(
        mcx,
        io.typinput,
        value,
        io.typioparam,
        -1,
    )?;
    // C: `values[i] = OidInputFunctionCall(...)`. Store the canonical value
    // verbatim — a by-reference image (`text`/`bytea`/`oidvector`/etc.) keeps
    // its bytes, exactly as C keeps the pointer to the palloc'd referent.
    set_values(i as usize, datum.clone());

    /*
     * We use ereport not elog here so parameters aren't evaluated unless the
     * message is going to be printed, which generally it isn't.
     */
    let out = backend_utils_fmgr_fmgr_seams::oid_output_function_call_datum::call(mcx, io.typoutput, datum)?;
    ereport(DEBUG4)
        .errmsg_internal(format!("inserted -> {}", out.as_str()))
        .finish(loc(685, "InsertOneValue"))?;

    Ok(())
}

/// `InsertOneNull`.
pub fn InsertOneNull(i: i32) -> PgResult<()> {
    ereport(DEBUG4)
        .errmsg_internal(format!("inserting column {} NULL", i))
        .finish(loc(697, "InsertOneNull"))?;
    debug_assert!(i >= 0 && (i as usize) < MAXATTR);

    let attnotnull = with_boot_reldesc(|rel| relation_get_attr(rel, i).attnotnull);
    if attnotnull {
        let (attname, relname) =
            with_boot_reldesc(|rel| (namestr(&relation_get_attr(rel, i).attname), rel.name().to_string()));
        return ereport(ERROR)
            .errmsg_internal(format!(
                "NULL value specified for not-null column \"{}\" of relation \"{}\"",
                attname, relname
            ))
            .finish(loc(700, "InsertOneNull"));
    }
    set_values(i as usize, TupleDatum::null()); /* PointerGetDatum(NULL) */
    set_nulls(i as usize, true);
    Ok(())
}

/// `cleanup`.
fn cleanup(mcx: Mcx<'static>) -> PgResult<()> {
    if boot_reldesc_is_open() {
        closerel(mcx, None)?;
    }
    Ok(())
}

/// `populate_typ_list` — load the `Typ` list by reading `pg_type`.
fn populate_typ_list(mcx: Mcx<'static>) -> PgResult<()> {
    debug_assert!(typ_is_nil());

    let rows = backend_access_heap_heapam_seams::read_pg_type::call(mcx)?;
    let mut list: Vec<TypMap> = Vec::with_capacity(rows.len());
    for (am_oid, am_typ) in rows.iter().copied() {
        list.push(TypMap { am_oid, am_typ });
    }
    set_typ(Some(list));
    Ok(())
}

/// `gettype`.
///
/// NB: this is really ugly; it returns an integer index into `TypInfo[]`, and
/// not an OID at all, until the first reference to a type not known in
/// `TypInfo[]`. At that point it reads and caches `pg_type` in `Typ`, and
/// subsequently returns a real OID (and sets `Ap` to point at the found row).
/// So the caller must check whether `Typ` is still NIL to determine what the
/// return value is!
fn gettype(mcx: Mcx<'static>, type_: &str) -> PgResult<Oid> {
    if !typ_is_nil() {
        if let Some((idx, oid)) = find_in_typ(type_) {
            set_ap(Some(idx));
            return Ok(oid);
        }

        /*
         * The type wasn't known; reload the pg_type contents and check again
         * to handle composite types added since last populating the list.
         */
        set_typ(None); /* list_free_deep(Typ); Typ = NIL; */
        populate_typ_list(mcx)?;

        /*
         * Calling gettype would result in infinite recursion for types missing
         * in pg_type, so just repeat the lookup.
         */
        if let Some((idx, oid)) = find_in_typ(type_) {
            set_ap(Some(idx));
            return Ok(oid);
        }
    } else {
        for i in 0..n_types() {
            if strncmp_str(type_, TYP_INFO[i].name, NAMEDATALEN) {
                return Ok(i as Oid);
            }
        }
        /* Not in TypInfo, so we'd better be able to read pg_type now */
        ereport(DEBUG4)
            .errmsg_internal(format!("external type: {}", type_))
            .finish(loc(817, "gettype"))?;
        populate_typ_list(mcx)?;
        return gettype(mcx, type_);
    }
    ereport(ERROR)
        .errmsg_internal(format!("unrecognized type \"{}\"", type_))
        .finish(loc(821, "gettype"))?;
    /* not reached */
    Ok(0)
}

/// `foreach(lc, Typ)` name match: returns `(index, am_oid)` of the first row
/// whose typname matches `type_` over `NAMEDATALEN`.
fn find_in_typ(type_: &str) -> Option<(usize, Oid)> {
    STATE.with(|s| {
        let st = s.borrow();
        let typ = st.typ.as_ref()?;
        for (idx, app) in typ.iter().enumerate() {
            if strncmp_name(&app.am_typ.typname, type_, NAMEDATALEN) {
                return Some((idx, app.am_oid));
            }
        }
        None
    })
}

/// Result of [`boot_get_type_io_data`]. Defined in the seam crate so the
/// across-cycle `lsyscache.c` caller and this owner share one type.
pub use backend_bootstrap_bootstrap_seams::BootTypeIoData;

/// `boot_get_type_io_data` — obtain type I/O information at bootstrap time.
///
/// Almost the same API as lsyscache.c's `get_type_io_data`, except we only
/// support typinput/typoutput (not the binary I/O routines). Exported so that
/// `array_in`/`array_out` can work during early bootstrap.
pub fn boot_get_type_io_data(typid: Oid) -> PgResult<BootTypeIoData> {
    if !typ_is_nil() {
        /* We have the boot-time contents of pg_type, so use it. */
        let result = STATE.with(|s| -> PgResult<BootTypeIoData> {
            let st = s.borrow();
            let typ = st.typ.as_ref().expect("Typ != NIL checked");
            let mut found: Option<&TypMap> = None;
            for ap in typ.iter() {
                found = Some(ap);
                if ap.am_oid == typid {
                    break;
                }
            }
            let ap = match found {
                Some(ap) if ap.am_oid == typid => ap,
                _ => {
                    /* elog(ERROR) — finish() at ERROR always returns Err. */
                    return Err(ereport(ERROR)
                        .errmsg_internal(format!("type OID {} not found in Typ list", typid))
                        .finish(loc(860, "boot_get_type_io_data"))
                        .unwrap_err());
                }
            };

            /* XXX this logic must match getTypeIOParam() */
            let typioparam = if oid_is_valid(ap.am_typ.typelem) {
                ap.am_typ.typelem
            } else {
                typid
            };

            Ok(BootTypeIoData {
                typlen: ap.am_typ.typlen,
                typbyval: ap.am_typ.typbyval,
                typalign: ap.am_typ.typalign,
                typdelim: ap.am_typ.typdelim,
                typioparam,
                typinput: ap.am_typ.typinput,
                typoutput: ap.am_typ.typoutput,
            })
        })?;
        Ok(result)
    } else {
        /* We don't have pg_type yet, so use the hard-wired TypInfo array. */
        let mut typeindex = 0usize;
        while typeindex < n_types() {
            if TYP_INFO[typeindex].oid == typid {
                break;
            }
            typeindex += 1;
        }
        if typeindex >= n_types() {
            /* elog(ERROR) — finish() at ERROR always returns Err. */
            return Err(ereport(ERROR)
                .errmsg_internal(format!("type OID {} not found in TypInfo", typid))
                .finish(loc(887, "boot_get_type_io_data"))
                .unwrap_err());
        }

        let ti = &TYP_INFO[typeindex];
        /* XXX this logic must match getTypeIOParam() */
        let typioparam = if oid_is_valid(ti.elem) { ti.elem } else { typid };

        Ok(BootTypeIoData {
            typlen: ti.len,
            typbyval: ti.byval,
            typalign: ti.align,
            /* We assume typdelim is ',' for all boot-time types */
            typdelim: b',' as i8,
            typioparam,
            typinput: ti.inproc,
            typoutput: ti.outproc,
        })
    }
}

/// `AllocateAttribute` — `MemoryContextAllocZero(TopMemoryContext,
/// ATTRIBUTE_FIXED_PART_SIZE)` returns a zeroed `Form_pg_attribute`.
fn AllocateAttribute() -> FormData_pg_attribute {
    FormData_pg_attribute::default()
}

/// `index_register` — record an index that has been set up for building later.
///
/// The C copies the `IndexInfo` (`memcpy` of the scalar fields, then
/// `copyObject` of `ii_Expressions`/`ii_Predicate`, `ii_ExpressionsState = NIL`,
/// `ii_PredicateState = NULL`, and asserts no exclusion constraints). In the
/// owned model `IndexInfo` is no longer `Copy`; the caller hands ownership of
/// the just-built node (from `make_index_info`) straight in by value, and the
/// registered-index list owns it directly. The control flow — record into the
/// registered-index list, push onto its head — matches the C.
pub fn index_register(heap: Oid, ind: Oid, index_info: IndexInfo<'static>) {
    /*
     * XXX mao 10/31/92 -- don't gc index reldescs at bootstrap time. The C
     * switches into a no-gc context for the copy; in the owned model the
     * registered list owns its copy directly.
     */
    let il_info = index_info;

    /* newind->il_next = ILHead; ILHead = newind; — push onto list head */
    let newind = IndexList {
        il_heap: heap,
        il_ind: ind,
        il_info,
    };
    il_head_push(newind);
}

/// `build_indices` — fill in all the indexes registered earlier.
pub fn build_indices(mcx: Mcx<'static>) -> PgResult<()> {
    /* for (; ILHead != NULL; ILHead = ILHead->il_next) */
    while let Some(mut entry) = il_head_pop() {
        /* need not bother with locks during bootstrap */
        let heap = backend_access_table_table::table_open(mcx, entry.il_heap, types_storage::lock::NoLock)?;
        let ind = backend_access_index_indexam_seams::index_open::call(mcx, entry.il_ind, types_storage::lock::NoLock)?;

        backend_catalog_index_seams::index_build::call(mcx, &heap, &ind, &mut entry.il_info)?;

        ind.close(types_storage::lock::NoLock)?; /* index_close(ind, NoLock) */
        heap.close(types_storage::lock::NoLock)?; /* table_close(heap, NoLock) */
    }
    Ok(())
}

/* ===========================================================================
 * makeRangeVar(NULL, relname, -1) — a trivial node constructor (makefuncs.c),
 * pure logic, ported in-crate.
 * ========================================================================= */

fn make_range_var(relname: &str) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: None,
        schemaname: None,
        relname: relname.to_string(),
        inh: true, /* makeRangeVar sets inh = true */
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    }
}

/// `RELPERSISTENCE_PERMANENT` ('p') — makeRangeVar's default.
const RELPERSISTENCE_PERMANENT: u8 = b'p';

/// `RelationGetNumberOfAttributes(rel)` — `rel->rd_att->natts`.
fn relation_get_number_of_attributes(rel: &RelationData<'_>) -> i32 {
    rel.rd_att.natts
}

/// `memmove(dst, TupleDescAttr(rel->rd_att, i), ATTRIBUTE_FIXED_PART_SIZE)` —
/// the fixed part of the relation's `i`th attribute (`TupleDescAttr`).
fn relation_get_attr(rel: &RelationData<'_>, i: i32) -> FormData_pg_attribute {
    rel.rd_att.attrs[i as usize]
}

/* ===========================================================================
 * Process-local global accessors (single-threaded bootstrap backend).
 * ========================================================================= */

/// `boot_reldesc != NULL` — whether a relation is currently open. Exposed so
/// the bootparse front end's `Boot_CreateStmt` action can mirror the C check
/// `if (boot_reldesc) { ... closerel(NULL); }`.
pub fn boot_reldesc_is_open() -> bool {
    STATE.with(|s| s.borrow().boot_reldesc.is_some())
}
/// Borrow the open `boot_reldesc` and run `f` on it. Panics if no relation is
/// open — every caller guards first, as the C global deref does.
fn with_boot_reldesc<R>(f: impl FnOnce(&RelationData<'static>) -> R) -> R {
    STATE.with(|s| {
        let st = s.borrow();
        let rel = st
            .boot_reldesc
            .as_ref()
            .expect("bootstrap: boot_reldesc is NULL (no open relation)");
        f(rel)
    })
}
/// As [`with_boot_reldesc`] but hands `f` the full open [`Relation`] handle
/// (not the dereffed `RelationData`), for the `simple_heap_insert` path that
/// needs the relcache-aware handle.
fn with_boot_reldesc_rel_res<R>(
    f: impl FnOnce(&Relation<'static>) -> PgResult<R>,
) -> PgResult<R> {
    STATE.with(|s| {
        let st = s.borrow();
        let rel = st
            .boot_reldesc
            .as_ref()
            .expect("bootstrap: boot_reldesc is NULL (no open relation)");
        f(rel)
    })
}
/// Set the `boot_reldesc` global. Exposed so the bootparse front end's
/// `Boot_CreateStmt` action can store the relation it just created via
/// `heap_create` (C: `boot_reldesc = heap_create(...)`).
pub fn set_boot_reldesc(v: Option<Relation<'static>>) {
    STATE.with(|s| s.borrow_mut().boot_reldesc = v);
}
fn take_boot_reldesc() -> Option<Relation<'static>> {
    STATE.with(|s| s.borrow_mut().boot_reldesc.take())
}

/// `int numattr` — number of attributes accumulated for the current relation.
/// Exposed so the bootparse front end mirrors the grammar's direct use of the
/// `numattr` global (`numattr = 0` in `Boot_CreateStmt`, `++numattr` /
/// `numattr-1` in `boot_column_def`, the `CreateTupleDesc(numattr, attrtypes)`
/// build, and the `num_columns_read != numattr` insert check).
pub fn numattr() -> i32 {
    STATE.with(|s| s.borrow().numattr)
}
/// Set the `numattr` global (the grammar's `numattr = 0` / `++numattr`).
pub fn set_numattr(v: i32) {
    STATE.with(|s| s.borrow_mut().numattr = v);
}

/// `Form_pg_attribute attrtypes[i]` — the accumulated attribute info for column
/// `i`. Exposed so the bootparse front end can read the array to assemble the
/// `CreateTupleDesc(numattr, attrtypes)` descriptor in `Boot_CreateStmt`.
pub fn attrtypes(i: usize) -> Option<FormData_pg_attribute> {
    STATE.with(|s| s.borrow().attrtypes[i])
}
fn set_attrtypes(i: usize, v: Option<FormData_pg_attribute>) {
    STATE.with(|s| s.borrow_mut().attrtypes[i] = v);
}

fn values(i: usize) -> TupleDatum<'static> {
    STATE.with(|s| s.borrow().values[i].clone())
}
fn set_values(i: usize, v: TupleDatum<'static>) {
    STATE.with(|s| s.borrow_mut().values[i] = v);
}

fn nulls_(i: usize) -> bool {
    STATE.with(|s| s.borrow().nulls[i])
}
fn set_nulls(i: usize, v: bool) {
    STATE.with(|s| s.borrow_mut().nulls[i] = v);
}

/// `Typ == NIL`.
fn typ_is_nil() -> bool {
    STATE.with(|s| s.borrow().typ.is_none())
}
fn set_typ(v: Option<Vec<TypMap>>) {
    STATE.with(|s| s.borrow_mut().typ = v);
}

fn set_ap(v: Option<usize>) {
    STATE.with(|s| s.borrow_mut().ap = v);
}
/// `*Ap` — the typmap last matched by [`gettype`] (copied out).
fn ap_entry() -> TypMap {
    STATE.with(|s| {
        let st = s.borrow();
        let idx = st.ap.expect("Ap is set whenever Typ != NIL and a type matched");
        st.typ.as_ref().expect("Typ != NIL")[idx]
    })
}

fn il_head_push(v: IndexList) {
    STATE.with(|s| s.borrow_mut().il_head.push(v));
}
fn il_head_pop() -> Option<IndexList> {
    STATE.with(|s| s.borrow_mut().il_head.pop())
}

/* ===========================================================================
 * Small C-string helpers.
 * ========================================================================= */

/// `OidIsValid(oid)` — `oid != InvalidOid`.
fn oid_is_valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `NameStr(name)` as an owned `String`.
fn namestr(name: &NameData) -> String {
    String::from_utf8_lossy(name.name_str()).into_owned()
}

/// `namestrcpy(&dst, src)` — copy `src` (truncated to `NAMEDATALEN-1`) into a
/// NUL-padded `NameData`.
fn namestrcpy(dst: &mut NameData, src: &str) {
    let bytes = src.as_bytes();
    let n = core::cmp::min(bytes.len(), NAMEDATALEN - 1);
    dst.data = [0; NAMEDATALEN];
    dst.data[..n].copy_from_slice(&bytes[..n]);
}

/// `strncmp(NameStr(name), type, NAMEDATALEN) == 0`.
fn strncmp_name(name: &NameData, type_: &str, n: usize) -> bool {
    strncmp_bytes(name.name_str(), type_.as_bytes(), n)
}

/// `strncmp(type, name, NAMEDATALEN) == 0` for a static-`&str` `TypInfo` name.
fn strncmp_str(type_: &str, name: &str, n: usize) -> bool {
    strncmp_bytes(type_.as_bytes(), name.as_bytes(), n)
}

/// `strncmp(a, b, n) == 0` over NUL-terminated C strings: equal iff they match
/// up to the first NUL or the first `n` bytes, whichever comes first.
fn strncmp_bytes(a: &[u8], b: &[u8], n: usize) -> bool {
    let mut i = 0;
    while i < n {
        let ca = a.get(i).copied().unwrap_or(0);
        let cb = b.get(i).copied().unwrap_or(0);
        if ca != cb {
            return false;
        }
        if ca == 0 {
            return true;
        }
        i += 1;
    }
    true
}

/* ===========================================================================
 * Faithful getopt over the bootstrap optstring.
 *
 * `BootstrapModeMain` parses with libc `getopt(argc, argv, "B:c:d:D:Fkr:X:-:")`.
 * That is pure argument-string logic (no syscall), ported in-crate as a
 * faithful subset covering the forms the bootstrap optstring uses.
 * ========================================================================= */

/// A minimal `getopt` state machine over an owned argv slice.
pub struct Getopt {
    argv: Vec<String>,
    optstring: Vec<u8>,
    /// `optind` — index of the next argv element to scan.
    pub optind: usize,
    /// offset within the current cluster of short options (`-abc`).
    place: usize,
    /// `optarg` — the option's argument, if it took one.
    pub optarg: Option<String>,
}

impl Getopt {
    fn new(argv: &[String], optstring: &str) -> Self {
        Getopt {
            argv: argv.to_vec(),
            optstring: optstring.as_bytes().to_vec(),
            optind: 1, /* getopt starts scanning at argv[1] of the passed argc/argv */
            place: 0,
            optarg: None,
        }
    }

    fn argc(&self) -> usize {
        self.argv.len()
    }

    fn optarg_or(&self, msg: &'static str) -> PgResult<String> {
        self.optarg.clone().ok_or_else(|| PgError::error(msg))
    }

    /// Look up `c` in the optstring; returns `(found, takes_arg)`.
    fn lookup(&self, c: u8) -> (bool, bool) {
        let mut i = 0;
        while i < self.optstring.len() {
            if self.optstring[i] == c {
                let takes_arg = self.optstring.get(i + 1).copied() == Some(b':');
                return (true, takes_arg);
            }
            i += 1;
        }
        (false, false)
    }

    /// `getopt(argc, argv, optstring)` — return the next option char, `None` at
    /// end of options. `'?'` is returned for an unknown option (C `default:`).
    fn next(&mut self) -> Option<char> {
        self.optarg = None;

        if self.place == 0 {
            if self.optind >= self.argv.len() {
                return None;
            }
            let token = &self.argv[self.optind];
            let bytes = token.as_bytes();
            if bytes.is_empty() || bytes[0] != b'-' || bytes.len() == 1 {
                return None;
            }
            if token == "--" {
                self.optind += 1;
                return None;
            }
            self.place = 1;
        }

        let token = self.argv[self.optind].clone();
        let bytes = token.as_bytes();
        let c = bytes[self.place];
        self.place += 1;

        let (found, takes_arg) = self.lookup(c);

        if c == b'-' && self.lookup(b'-').0 {
            /* the '-' option (from "-:") takes the remainder as its argument */
            let rest = String::from(&token[self.place..]);
            self.optarg = Some(rest);
            self.place = 0;
            self.optind += 1;
            return Some('-');
        }

        if !found {
            if self.place >= bytes.len() {
                self.place = 0;
                self.optind += 1;
            }
            return Some('?');
        }

        if takes_arg {
            if self.place < bytes.len() {
                self.optarg = Some(String::from(&token[self.place..]));
                self.place = 0;
                self.optind += 1;
            } else {
                self.optind += 1;
                if self.optind < self.argv.len() {
                    self.optarg = Some(self.argv[self.optind].clone());
                    self.optind += 1;
                } else {
                    self.optarg = None;
                }
                self.place = 0;
            }
        } else if self.place >= bytes.len() {
            self.place = 0;
            self.optind += 1;
        }

        Some(c as char)
    }
}

/* ===========================================================================
 * Seam installation.
 *
 * The BKI front end calls most of bootstrap.c's functions by direct
 * dependency. The one exception is `boot_get_type_io_data`, which
 * `lsyscache.c`'s `get_type_io_data` calls while in bootstrap mode; since
 * bootstrap.c itself depends (via seams) on lsyscache.c, that call crosses a
 * cycle and is wired through `backend-bootstrap-bootstrap-seams`.
 * ========================================================================= */

pub fn init_seams() {
    backend_bootstrap_bootstrap_seams::boot_get_type_io_data::set(boot_get_type_io_data);
}

#[cfg(test)]
mod tests;
