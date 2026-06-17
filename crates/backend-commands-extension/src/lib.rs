#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/extension.c` — CREATE / ALTER / DROP EXTENSION + the
//! extension version-script machinery.
//!
//! F0 ports the parse/analysis half of `extension.c`: the control-file parsing
//! machinery, the version-update-path graph search, the small `pg_extension`
//! catalog-read cores, `RemoveExtensionById`, `CreateExtension`'s parse-side
//! option deconstruction, and the `creating_extension` / `CurrentExtensionObject`
//! backend-global state. Every function is present 1:1 with C in branch order /
//! SQLSTATE / messages / errdetail.
//!
//! The control-file READ goes through `ParseConfigFp` (GUC file parser) over a
//! file slurped by `fd.c`'s `allocate_file_read`; the script-directory scan goes
//! through `fd.c`'s `list_dir`; both are direct calls into their real owners.
//!
//! The catalog-DML / install-orchestration command bodies that do NOT require
//! the script-execution pipeline are now ported here: `get_extension_schema`,
//! `extension_is_trusted`, `convert_requires_to_datum`, `InsertExtensionTuple`,
//! `read_whole_file`, `get_required_extension`, `CreateExtensionInternal` (whose
//! tail mirror-panics into the gated script pipeline), `AlterExtensionNamespace`
//! (ALTER EXTENSION SET SCHEMA — its inward seam is installed), and
//! `extension_file_exists`. `CreateExtension` now drives `CreateExtensionInternal`
//! directly.
//!
//! What remains in [`deferred`] is the genuinely-gated set: the
//! parser/analyzer/planner/executor/utility SCRIPT-EXECUTION pipeline
//! (`execute_extension_script` / `execute_sql_string` / `read_extension_script_file`),
//! `ExecAlterExtensionStmt`/`ApplyExtensionUpdates` (which call it), the
//! SRF / fmgr-`Datum`-arg SQL-callable functions, the syscache-invalidation
//! callback (`get_function_sibling_type`), and two keystone-blocked members:
//! `ExecAlterExtensionContentsStmt`/`…Recurse` (no port/seam for aclchk.c's
//! `recordExtObjInitPriv`/`removeExtObjInitPriv`) and `extension_config_remove`
//! (no value-typed `deconstruct_array` for the by-reference `text[]` squeeze).
//! All behind loud panics (mirror-pg-and-panic). No silent stubs.

use std::cell::Cell;
use std::path::Path;

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_core::{AttrNumber, InvalidOid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR, NOTICE,
};
use types_error::{ERRCODE_INVALID_RECURSION, ERRCODE_UNDEFINED_SCHEMA};

use backend_utils_error::ereport;

use backend_access_common_scankey::ScanKeyInit;
use backend_catalog_indexing::keystone::CatalogTupleDelete;
use backend_utils_cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, EXTENSIONNAME, EXTENSIONOID,
};

use types_cache::SysCacheKey;
use types_catalog::catalog::NAMESPACE_RELATION_ID;
use types_catalog::catalog_dependency::{
    InvalidObjectAddress, ObjectAddress, DEPENDENCY_EXTENSION, DEPENDENCY_NORMAL,
};
use types_catalog::pg_database::DatabaseRelationId;
use types_catalog::catalog_dependency::DEPEND_RELATION_ID;
use types_catalog::pg_extension as cat;
use types_datum::Datum as KeyDatum;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, RowExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_acl::acl::{ACLCHECK_OK, ACL_CREATE};

use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table_seams as table_seams;
use backend_storage_file_fd_seams as fd_seams;
use backend_utils_adt_varlena_seams as varlena_seams;
use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_dependency_seams as dependency_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;
use backend_catalog_pg_shdepend_seams as pg_shdepend_seams;
use backend_catalog_objectaccess_seams as objectaccess_seams;
use backend_catalog_objectaddress_seams as objectaddress_seams;
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_access_transam_xact_seams as xact_seams;

use backend_access_common_heaptuple::{
    heap_copytuple, heap_deform_tuple, heap_form_tuple, heap_modify_tuple,
};
use backend_commands_alter::AlterObjectNamespace_oid;
use types_acl::acl::ACLCHECK_NOT_OWNER;
use types_nodes::parsenodes::{OBJECT_EXTENSION, OBJECT_SCHEMA};
use backend_catalog_catalog::GetNewOidWithIndex;
use backend_catalog_indexing::keystone::{CatalogTupleInsert, CatalogTupleUpdate};
use backend_utils_adt_arrayfuncs::construct::build_name_array;
use backend_utils_adt_name::namein;
use types_tuple::heaptuple::NameData;
use backend_utils_init_miscinit::GetUserId;
use backend_utils_init_small::globals::MyDatabaseId;
use backend_catalog_namespace::{fetch_search_path, get_namespace_oid, isTempNamespace};
use backend_commands_schemacmds::CreateSchemaCommand;
use backend_commands_comment::CreateComments;

pub mod deferred;

/// The C `MAXPGPATH` (the max path length sizing `snprintf` buffers in the
/// filename builders).
pub const MAXPGPATH: usize = 1024;

/// `here(funcname)` — the error-source location attached to emitted NOTICEs.
fn here(funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("../src/backend/commands/extension.c", 0, funcname)
}

// ===========================================================================
// Globally visible state variables (C 78-80)
// ===========================================================================

thread_local! {
    /// `bool creating_extension = false` (C 79): true while a CREATE EXTENSION
    /// or ALTER EXTENSION UPDATE script is executing.
    static CREATING_EXTENSION: Cell<bool> = const { Cell::new(false) };
    /// `Oid CurrentExtensionObject = InvalidOid` (C 80): the OID of the
    /// pg_extension row currently open for insertion.
    static CURRENT_EXTENSION_OBJECT: Cell<Oid> = const { Cell::new(InvalidOid) };
}

/// Read the `creating_extension` backend-global.
pub fn creating_extension() -> bool {
    CREATING_EXTENSION.with(|c| c.get())
}

/// Read the `CurrentExtensionObject` backend-global.
pub fn current_extension_object() -> Oid {
    CURRENT_EXTENSION_OBJECT.with(|c| c.get())
}

/// Set `creating_extension` (used by `CreateExtensionInternal` /
/// `ApplyExtensionUpdates` once they land; exposed for the deferred bodies).
pub fn set_creating_extension(value: bool) {
    CREATING_EXTENSION.with(|c| c.set(value));
}

/// Set `CurrentExtensionObject`.
pub fn set_current_extension_object(value: Oid) {
    CURRENT_EXTENSION_OBJECT.with(|c| c.set(value));
}

// ===========================================================================
// ExtensionControlFile (C 85-104) + new_ExtensionControlFile (C 4003-4018)
// ===========================================================================

/// `typedef struct ExtensionControlFile` — the parsed contents of an extension
/// control file. C `char *` / `List *` fields become `Option<String>` /
/// `Vec<String>`; `name` is always set (by [`new_ExtensionControlFile`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionControlFile {
    /// name of the extension
    pub name: String,
    /// base directory where control and script files are located
    pub basedir: Option<String>,
    /// directory where control file was found
    pub control_dir: Option<String>,
    /// directory for script files
    pub directory: Option<String>,
    /// default install target version, if any
    pub default_version: Option<String>,
    /// string to substitute for `MODULE_PATHNAME`
    pub module_pathname: Option<String>,
    /// comment, if any
    pub comment: Option<String>,
    /// target schema (allowed if `!relocatable`)
    pub schema: Option<String>,
    /// is ALTER EXTENSION SET SCHEMA supported?
    pub relocatable: bool,
    /// must be superuser to install?
    pub superuser: bool,
    /// allow becoming superuser on the fly?
    pub trusted: bool,
    /// encoding of the script file, or -1
    pub encoding: i32,
    /// names of prerequisite extensions
    pub requires: Vec<String>,
    /// names of prerequisite extensions that should not be relocated
    pub no_relocate: Vec<String>,
}

/// `new_ExtensionControlFile` (C 4003-4018) — allocate a control struct with the
/// default field values: `relocatable=false`, `superuser=true`, `trusted=false`,
/// `encoding=-1`; pointer fields initially null.
pub fn new_ExtensionControlFile(extname: &str) -> ExtensionControlFile {
    ExtensionControlFile {
        name: extname.to_string(),
        basedir: None,
        control_dir: None,
        directory: None,
        default_version: None,
        module_pathname: None,
        comment: None,
        schema: None,
        relocatable: false,
        superuser: true,
        trusted: false,
        encoding: -1,
        requires: Vec::new(),
        no_relocate: Vec::new(),
    }
}

// ===========================================================================
// Name / version validity checks (C 359-448)
// ===========================================================================

/// `check_valid_extension_name` (C 359-404). Four checks, in C order, each
/// raising `ERRCODE_INVALID_PARAMETER_VALUE` with the exact message / errdetail:
/// not-empty, no `--`, no leading/trailing `-`, no directory separator.
pub fn check_valid_extension_name(extensionname: &str) -> PgResult<()> {
    let bytes = extensionname.as_bytes();
    let namelen = bytes.len();

    // Disallow empty names.
    if namelen == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension name: \"{extensionname}\""))
            .errdetail("Extension names must not be empty.")
            .into_error());
    }

    // No double dashes, since that would make script filenames ambiguous.
    if extensionname.contains("--") {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension name: \"{extensionname}\""))
            .errdetail("Extension names must not contain \"--\".")
            .into_error());
    }

    // No leading or trailing dash either.
    if bytes[0] == b'-' || bytes[namelen - 1] == b'-' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension name: \"{extensionname}\""))
            .errdetail("Extension names must not begin or end with \"-\".")
            .into_error());
    }

    // No directory separators (sufficient to prevent ".." style attacks).
    if first_dir_separator(extensionname).is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension name: \"{extensionname}\""))
            .errdetail("Extension names must not contain directory separator characters.")
            .into_error());
    }

    Ok(())
}

/// `check_valid_version_name` (C 406-448). Same four checks as
/// [`check_valid_extension_name`] with the version-specific wording.
pub fn check_valid_version_name(versionname: &str) -> PgResult<()> {
    let bytes = versionname.as_bytes();
    let namelen = bytes.len();

    if namelen == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension version name: \"{versionname}\""))
            .errdetail("Version names must not be empty.")
            .into_error());
    }

    if versionname.contains("--") {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension version name: \"{versionname}\""))
            .errdetail("Version names must not contain \"--\".")
            .into_error());
    }

    if bytes[0] == b'-' || bytes[namelen - 1] == b'-' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension version name: \"{versionname}\""))
            .errdetail("Version names must not begin or end with \"-\".")
            .into_error());
    }

    if first_dir_separator(versionname).is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid extension version name: \"{versionname}\""))
            .errdetail("Version names must not contain directory separator characters.")
            .into_error());
    }

    Ok(())
}

/// `first_dir_separator(s)` (`common/path.c`, non-Windows): the byte offset of
/// the first `'/'`, or `None`. A tiny dependency-free path predicate ported
/// in-crate (mirrors the dfmgr/common-path copies), used to reject `..`-style
/// names. The `\` arm covers the Windows separator for completeness.
fn first_dir_separator(s: &str) -> Option<usize> {
    s.bytes().position(|b| b == b'/' || b == b'\\')
}

/// `first_path_var_separator(pathlist)` (`common/path.c`, non-Windows): the byte
/// offset of the first `':'`, or `None`. Ported in-crate (a dependency-free byte
/// scan, like the dfmgr copy).
fn first_path_var_separator(pathlist: &str) -> Option<usize> {
    pathlist.bytes().position(|b| b == b':')
}

// ===========================================================================
// Filename predicates (C 453-467)
// ===========================================================================

/// `is_extension_control_filename` (C 453-459) — true when the last `.`-suffix of
/// `filename` is exactly `.control`.
pub fn is_extension_control_filename(filename: &str) -> bool {
    match filename.rfind('.') {
        Some(dot) => &filename[dot..] == ".control",
        None => false,
    }
}

/// `is_extension_script_filename` (C 461-467) — true when the last `.`-suffix of
/// `filename` is exactly `.sql`.
pub fn is_extension_script_filename(filename: &str) -> bool {
    match filename.rfind('.') {
        Some(dot) => &filename[dot..] == ".sql",
        None => false,
    }
}

// ===========================================================================
// Control-directory + script-file path derivation (C 472-623)
// ===========================================================================

/// `get_share_path(my_exec_path, sharepath)` (`common/path.c`) — the installed
/// share directory, derived from the backend's `my_exec_path` global.
fn share_path() -> String {
    let buf = backend_utils_init_small::globals::my_exec_path();
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let my_exec_path = String::from_utf8_lossy(&buf[..end]).into_owned();
    common_path_seams::get_share_path::call(&my_exec_path)
}

/// `substitute_path_macro(str, macro, value)` (utils/conffiles.c) — substitute a
/// `$macro` appearing at the very start of `s`. A dependency-free string op,
/// ported in-crate 1:1 (the dfmgr copy is the model).
fn substitute_path_macro(s: &str, macro_: &str, value: &str) -> PgResult<String> {
    debug_assert!(macro_.as_bytes().first() == Some(&b'$'));

    if s.as_bytes().first() != Some(&b'$') {
        return Ok(s.to_string());
    }

    let sep = first_dir_separator(s).unwrap_or(s.len());

    if macro_.len() != sep || &s[..sep] != macro_ {
        return Err(PgError::error(format!("invalid macro name in path: {s}"))
            .with_sqlstate(ERRCODE_INVALID_NAME));
    }

    Ok(format!("{value}{}", &s[sep..]))
}

// ---------------------------------------------------------------------------
// `Extension_control_path` GUC backing storage (extension.c:76 `char *`).
//
// C declares `char *Extension_control_path;` as the storage for the
// `extension_control_path` GUC (guc_tables.c). The value is read from this GUC
// slot — not from ControlFile. The GUC machinery sets it to the `$system`
// boot value at startup, so we seed our backing store with the same default.
// ---------------------------------------------------------------------------

thread_local! {
    /// C's `char *Extension_control_path`. A string GUC's storage is never NULL
    /// once booted (the `$system` boot value is non-NULL), hence the seed.
    static EXTENSION_CONTROL_PATH: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

fn extension_control_path_init() {
    EXTENSION_CONTROL_PATH.with(|c| {
        if c.borrow().is_none() {
            *c.borrow_mut() = Some(String::from("$system"));
        }
    });
}

/// The `conf->variable` getter for the `extension_control_path` GUC slot.
fn extension_control_path_get() -> Option<String> {
    extension_control_path_init();
    EXTENSION_CONTROL_PATH.with(|c| c.borrow().clone())
}

/// The `conf->variable` setter for the `extension_control_path` GUC slot.
fn extension_control_path_set(value: Option<String>) {
    EXTENSION_CONTROL_PATH.with(|c| *c.borrow_mut() = value);
}

/// `Extension_control_path` GUC read (the colon-separated control path).
fn extension_control_path() -> String {
    extension_control_path_get().unwrap_or_default()
}

/// `get_extension_control_directories` (C 472-533). When the
/// `Extension_control_path` GUC is empty, the single `$system/extension`
/// directory; otherwise the `:`-separated path with `$system` macro-substituted
/// and every other element suffixed with `/extension`, each canonicalized.
pub fn get_extension_control_directories() -> PgResult<Vec<String>> {
    let sharepath = share_path();
    let system_dir = format!("{sharepath}/extension");

    let ecp_full = extension_control_path();

    let mut paths: Vec<String> = Vec::new();

    if ecp_full.is_empty() {
        paths.push(system_dir);
        return Ok(paths);
    }

    // Walk the path string element by element (mirroring the C pointer walk).
    let mut ecp: &str = &ecp_full;
    loop {
        let len = match first_path_var_separator(ecp) {
            None => ecp.len(),
            Some(i) => i,
        };

        let piece = &ecp[..len];

        // Substitute the path macro if needed or append "extension" suffix if
        // it is a custom extension control path.
        let mangled = if piece == "$system" {
            substitute_path_macro(piece, "$system", &system_dir)?
        } else {
            format!("{piece}/extension")
        };

        // Canonicalize the path based on the OS and add to the list.
        let mangled = common_path_seams::canonicalize_path::call(mangled);
        paths.push(mangled);

        // Break if ecp is empty or move to the next path on ecp.
        if len == ecp.len() {
            break;
        } else {
            ecp = &ecp[len + 1..];
        }
    }

    Ok(paths)
}

/// `find_in_paths(basename, paths)` (C 4028-4059) — search `basename` across the
/// already-macro-expanded `paths`, canonicalizing and requiring each absolute,
/// returning the first existing full path, or `None`.
fn find_in_paths(basename: &str, paths: &[String]) -> PgResult<Option<String>> {
    for path in paths {
        let path = common_path_seams::canonicalize_path::call(path.clone());

        // only absolute paths
        if !common_path_seams::is_absolute_path::call(&path) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_NAME)
                .errmsg(
                    "component in parameter \"extension_control_path\" is not an absolute path",
                )
                .into_error());
        }

        let full = format!("{path}/{basename}");

        if fd_seams::pg_file_exists::call(&full)? {
            return Ok(Some(full));
        }
    }

    Ok(None)
}

/// `find_extension_control_filename` (C 540-564). Build `<name>.control`, search
/// it across [`get_extension_control_directories`], and on success record the
/// directory it was found in into `control.control_dir`. Returns the full
/// filename, or `None`.
pub fn find_extension_control_filename(
    control: &mut ExtensionControlFile,
) -> PgResult<Option<String>> {
    debug_assert!(!control.name.is_empty());

    let basename = format!("{}.control", control.name);

    let paths = get_extension_control_directories()?;
    let result = find_in_paths(&basename, &paths)?;

    if let Some(ref full) = result {
        // p = strrchr(result, '/'); control_dir = pnstrdup(result, p - result);
        let p = full.rfind('/').ok_or_else(|| {
            PgError::error("find_in_paths returns an absolute path containing '/'")
        })?;
        control.control_dir = Some(full[..p].to_string());
    }

    Ok(result)
}

/// `get_extension_script_directory` (C 566-583). The script directory is
/// `control_dir` when `directory` is unset; the `directory` verbatim when it is
/// absolute; else `basedir/directory`.
pub fn get_extension_script_directory(control: &ExtensionControlFile) -> PgResult<String> {
    match &control.directory {
        None => control.control_dir.clone().ok_or_else(|| {
            PgError::error("control_dir is set once a control file has been located")
        }),
        Some(directory) => {
            if common_path_seams::is_absolute_path::call(directory) {
                Ok(directory.clone())
            } else {
                let basedir = control.basedir.as_ref().ok_or_else(|| {
                    PgError::error("basedir is set by parse_extension_control_file")
                })?;
                Ok(format!("{basedir}/{directory}"))
            }
        }
    }
}

/// `get_extension_aux_control_filename` (C 585-601) —
/// `<scriptdir>/<name>--<version>.control`.
pub fn get_extension_aux_control_filename(
    control: &ExtensionControlFile,
    version: &str,
) -> PgResult<String> {
    let scriptdir = get_extension_script_directory(control)?;
    Ok(format!("{scriptdir}/{}--{version}.control", control.name))
}

/// `get_extension_script_filename` (C 603-623) —
/// `<scriptdir>/<name>--<version>.sql`, or
/// `<scriptdir>/<name>--<from_version>--<version>.sql` when `from_version` is
/// given.
pub fn get_extension_script_filename(
    control: &ExtensionControlFile,
    from_version: Option<&str>,
    version: &str,
) -> PgResult<String> {
    let scriptdir = get_extension_script_directory(control)?;
    match from_version {
        Some(from) => Ok(format!(
            "{scriptdir}/{}--{from}--{version}.sql",
            control.name
        )),
        None => Ok(format!("{scriptdir}/{}--{version}.sql", control.name)),
    }
}

// ===========================================================================
// Control-file field dispatch (C 640-865)
// ===========================================================================

/// Read + GUC-parse a control file's `(name, value)` items, or `None` for a
/// missing optional auxiliary file (C: `errno == ENOENT && version`).
///
/// `AllocateFile(filename, "r")` (fd) + `ParseConfigFp(file, …, ERROR, …)`
/// (guc-file) + `FreeFile`. The C parse uses `ERROR` elevel, so a syntax error
/// is thrown there; `allocate_file_read` returning `None` is the ENOENT case.
fn read_control_file_items(
    filename: &str,
    is_aux: bool,
) -> PgResult<Option<Vec<(String, String)>>> {
    let contents = match fd_seams::allocate_file_read::call(filename)? {
        // no complaint for missing auxiliary file (errno == ENOENT && version)
        None => {
            if is_aux {
                return Ok(None);
            }
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not open extension control file \"{filename}\""))
                .into_error());
        }
        Some(bytes) => bytes,
    };

    let mut variables: Vec<backend_utils_misc_guc_file::ConfigVariable> = Vec::new();
    backend_utils_misc_guc_file::ParseConfigFp(
        &contents,
        Path::new(filename),
        backend_utils_misc_guc_file::CONF_FILE_START_DEPTH,
        ERROR,
        &mut variables,
    )?;

    let mut items: Vec<(String, String)> = Vec::with_capacity(variables.len());
    for v in variables {
        let name = v.name.unwrap_or_default();
        let value = v.value.unwrap_or_default();
        items.push((name, value));
    }
    Ok(Some(items))
}

/// `parse_extension_control_file` (C 640-823). Locate the control file (primary
/// if `version == None`, else the optional auxiliary file for that version),
/// read + `ParseConfigFp`-parse it, and run the name/value dispatch over the
/// items, filling `*control`. Auxiliary files are optional (a missing one is the
/// documented early return).
pub fn parse_extension_control_file(
    control: &mut ExtensionControlFile,
    version: Option<&str>,
) -> PgResult<()> {
    // Locate the file to read.  Auxiliary files are optional.
    let filename = if let Some(v) = version {
        get_extension_aux_control_filename(control, v)?
    } else if let Some(dir) = &control.control_dir {
        // If control_dir is already set, use it, else do a path search.
        format!("{dir}/{}.control", control.name)
    } else {
        match find_extension_control_filename(control)? {
            Some(f) => f,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("extension \"{}\" is not available", control.name))
                    .errhint(
                        "The extension must first be installed on the system where PostgreSQL is running.",
                    )
                    .into_error());
            }
        }
    };

    // Assert that the control_dir ends with /extension, then derive basedir.
    let control_dir = control
        .control_dir
        .as_ref()
        .ok_or_else(|| PgError::error("control_dir is set before basedir derivation"))?
        .clone();
    debug_assert!(
        control_dir.ends_with("/extension"),
        "control_dir must end with /extension"
    );
    control.basedir = Some(control_dir[..control_dir.len() - "/extension".len()].to_string());

    // Read + parse the file content (GUC's ParseConfigFp), or take the optional
    // early-return for a missing auxiliary file.
    let items = match read_control_file_items(&filename, version.is_some())? {
        None => return Ok(()),
        Some(items) => items,
    };

    // Convert the ConfigVariable list into ExtensionControlFile entries.
    for (name, value) in &items {
        match name.as_str() {
            "directory" => {
                if version.is_some() {
                    return Err(secondary_file_error(name));
                }
                control.directory = Some(value.clone());
            }
            "default_version" => {
                if version.is_some() {
                    return Err(secondary_file_error(name));
                }
                control.default_version = Some(value.clone());
            }
            "module_pathname" => {
                control.module_pathname = Some(value.clone());
            }
            "comment" => {
                control.comment = Some(value.clone());
            }
            "schema" => {
                control.schema = Some(value.clone());
            }
            "relocatable" => {
                control.relocatable = parse_bool_field(name, value)?;
            }
            "superuser" => {
                control.superuser = parse_bool_field(name, value)?;
            }
            "trusted" => {
                control.trusted = parse_bool_field(name, value)?;
            }
            "encoding" => {
                let enc = common_extra_encnames::pg_valid_server_encoding(value);
                control.encoding = enc;
                if enc < 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("\"{value}\" is not a valid encoding name"))
                        .into_error());
                }
            }
            "requires" => match split_name_list(value)? {
                Some(names) => control.requires = names,
                None => return Err(name_list_error(name)),
            },
            "no_relocate" => match split_name_list(value)? {
                Some(names) => control.no_relocate = names,
                None => return Err(name_list_error(name)),
            },
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "unrecognized parameter \"{name}\" in file \"{filename}\""
                    ))
                    .into_error());
            }
        }
    }

    if control.relocatable && control.schema.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("parameter \"schema\" cannot be specified when \"relocatable\" is true")
            .into_error());
    }

    Ok(())
}

/// `SplitIdentifierString(rawvalue, ',', &namelist)` over the owner varlena
/// seam, returning owned `String`s (the C copies + downcases per identifier
/// rules). `Ok(None)` is the C `false` (syntax error).
fn split_name_list(value: &str) -> PgResult<Option<Vec<String>>> {
    let scratch = mcx::MemoryContext::new("extension split identifier");
    let mcx = scratch.mcx();
    let result = match varlena_seams::split_identifier_string::call(mcx, value, ',')? {
        None => None,
        Some(names) => Some(names.iter().map(|s| s.as_str().to_string()).collect()),
    };
    Ok(result)
}

/// The "cannot be set in a secondary extension control file" error
/// (`directory` / `default_version`, C 716-719 / 726-729).
fn secondary_file_error(name: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "parameter \"{name}\" cannot be set in a secondary extension control file"
        ))
        .into_error()
}

/// The "must be a list of extension names" error (`requires` / `no_relocate`,
/// C 787-790 / 802-805).
fn name_list_error(name: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("parameter \"{name}\" must be a list of extension names"))
        .into_error()
}

/// `parse_bool(value, &field)` with the shared "requires a Boolean value" error
/// (C 747-767). Uses the already-ported pure ASCII [`parse_bool`].
fn parse_bool_field(name: &str, value: &str) -> PgResult<bool> {
    match backend_utils_adt_scalar_seams::parse_bool::call(value) {
        Some(b) => Ok(b),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("parameter \"{name}\" requires a Boolean value"))
            .into_error()),
    }
}

/// `read_extension_control_file` (C 828-839) — build a fresh control struct for
/// `extname` and parse its primary control file.
pub fn read_extension_control_file(extname: &str) -> PgResult<ExtensionControlFile> {
    let mut control = new_ExtensionControlFile(extname);
    parse_extension_control_file(&mut control, None)?;
    Ok(control)
}

/// `read_extension_aux_control_file` (C 847-865) — flat-copy `pcontrol` and
/// overlay the auxiliary (per-version) control file's fields. The original is
/// not modified.
pub fn read_extension_aux_control_file(
    pcontrol: &ExtensionControlFile,
    version: &str,
) -> PgResult<ExtensionControlFile> {
    let mut acontrol = pcontrol.clone();
    parse_extension_control_file(&mut acontrol, Some(version))?;
    Ok(acontrol)
}

// ===========================================================================
// Version-update-path graph (C 1468-1773)
// ===========================================================================

/// `i32::MAX` — the C `INT_MAX` Dijkstra sentinel distance.
const INT_MAX: i32 = i32::MAX;

/// `typedef struct ExtensionVersionInfo` — one vertex of the version-update
/// graph. C links `reachable` / `previous` by raw pointer; the owned port models
/// the graph as an index arena ([`EviList`]): `reachable` / `previous` hold
/// arena indices ([`NO_PREV`] = the C `NULL` predecessor).
#[derive(Clone, Debug)]
pub struct ExtensionVersionInfo {
    /// name of the starting version
    pub name: String,
    /// indices (into the arena) of versions reachable in one step
    pub reachable: Vec<usize>,
    /// does this version have an install script?
    pub installable: bool,
    /// working state for Dijkstra: is distance from start known yet?
    pub distance_known: bool,
    /// current worst-case distance estimate
    pub distance: i32,
    /// current best predecessor (arena index, or [`NO_PREV`])
    pub previous: usize,
}

/// Sentinel for `ExtensionVersionInfo.previous == NULL` in the C struct.
pub const NO_PREV: usize = usize::MAX;

/// The version-graph arena: a `Vec<ExtensionVersionInfo>` standing in for C's
/// `List *evi_list`. Vertices are referenced by their index.
pub type EviList = Vec<ExtensionVersionInfo>;

/// `get_ext_ver_info` (C 1468-1493). Find or create the vertex for
/// `versionname`; return its arena index. A new vertex is initialized for
/// Dijkstra (`distance = INT_MAX`, `distance_known = false`, `previous = NULL`).
pub fn get_ext_ver_info(versionname: &str, evi_list: &mut EviList) -> usize {
    if let Some(idx) = evi_list.iter().position(|evi| evi.name == versionname) {
        return idx;
    }

    evi_list.push(ExtensionVersionInfo {
        name: versionname.to_string(),
        reachable: Vec::new(),
        installable: false,
        distance_known: false,
        distance: INT_MAX,
        previous: NO_PREV,
    });
    evi_list.len() - 1
}

/// `get_nearest_unprocessed_vertex` (C 1501-1521). Return the index of the
/// unprocessed (distance not yet known) vertex with the smallest distance
/// estimate, or `None` if all are processed.
pub fn get_nearest_unprocessed_vertex(evi_list: &EviList) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, evi2) in evi_list.iter().enumerate() {
        // only vertices whose distance is still uncertain are candidates
        if evi2.distance_known {
            continue;
        }
        match best {
            None => best = Some(i),
            Some(b) => {
                if evi_list[b].distance > evi2.distance {
                    best = Some(i);
                }
            }
        }
    }
    best
}

/// `get_ext_ver_list` (C 1529-1583). Build the version-update graph by scanning
/// the extension's script directory and tokenizing each `<name>--[from--]to.sql`
/// filename. Install scripts mark a version installable; update scripts add a
/// `from -> to` edge. A filename with a third `--` is bogus and ignored.
pub fn get_ext_ver_list(control: &ExtensionControlFile) -> PgResult<EviList> {
    let mut evi_list: EviList = Vec::new();
    let extname = control.name.as_str();
    let extnamelen = extname.len();

    let location = get_extension_script_directory(control)?;
    let scratch = mcx::MemoryContext::new("extension script directory scan");
    let entries = fd_seams::list_dir::call(scratch.mcx(), &location, false)?
        .ok_or_else(|| PgError::error("AllocateDir(location) failed for the script directory"))?;

    for de in entries.iter() {
        let d_name = de.name.as_str();

        // must be a .sql file ...
        if !is_extension_script_filename(d_name) {
            continue;
        }

        // ... matching extension name followed by "--" separator (byte-exact,
        // as the C strncmp + d_name[extnamelen]/[extnamelen+1] does).
        let db = d_name.as_bytes();
        if db.len() <= extnamelen + 1
            || &db[..extnamelen] != extname.as_bytes()
            || db[extnamelen] != b'-'
            || db[extnamelen + 1] != b'-'
        {
            continue;
        }

        // extract version name(s) from 'extname--something.sql': vername =
        // d_name + extnamelen + 2 with the trailing ".sql" removed (C truncates
        // at the *last* '.').
        let tail = &d_name[extnamelen + 2..];
        let dot = match tail.rfind('.') {
            Some(d) => d,
            None => continue,
        };
        let vername_full = &tail[..dot];

        // vername2 = strstr(vername, "--")
        match vername_full.find("--") {
            None => {
                // It's an install, not update, script; record its version name.
                let evi = get_ext_ver_info(vername_full, &mut evi_list);
                evi_list[evi].installable = true;
            }
            Some(sep) => {
                let vername = &vername_full[..sep];
                let vername2 = &vername_full[sep + 2..];

                // if there's a third --, it's bogus, ignore it.
                if vername2.contains("--") {
                    continue;
                }

                let evi = get_ext_ver_info(vername, &mut evi_list);
                let evi2 = get_ext_ver_info(vername2, &mut evi_list);
                evi_list[evi].reachable.push(evi2);
            }
        }
    }

    Ok(evi_list)
}

/// `find_update_path` (C 1635-1712). Dijkstra's shortest path from `evi_start`
/// to `evi_target` over the arena. `reject_indirect`: ignore paths through
/// installable versions; `reinitialize`: reset the transient Dijkstra fields
/// first. Returns the list of version names to transition through (the initial
/// version is *not* included), or `None` if unreachable. The strcmp tie-break
/// (C 1685-1698) is preserved exactly.
pub fn find_update_path(
    evi_list: &mut EviList,
    evi_start: usize,
    evi_target: usize,
    reject_indirect: bool,
    reinitialize: bool,
) -> Option<Vec<String>> {
    // Caller error if start == target.
    debug_assert_ne!(evi_start, evi_target);
    // Caller error if reject_indirect and target is installable.
    debug_assert!(!(reject_indirect && evi_list[evi_target].installable));

    if reinitialize {
        for evi in evi_list.iter_mut() {
            evi.distance_known = false;
            evi.distance = INT_MAX;
            evi.previous = NO_PREV;
        }
    }

    evi_list[evi_start].distance = 0;

    while let Some(evi) = get_nearest_unprocessed_vertex(evi_list) {
        if evi_list[evi].distance == INT_MAX {
            break; // all remaining vertices are unreachable
        }
        evi_list[evi].distance_known = true;
        if evi == evi_target {
            break; // found shortest path to target
        }

        // Snapshot the current vertex's reachable / distance / name before
        // mutating the arena, since the body only updates *other* vertices.
        let reachable = evi_list[evi].reachable.clone();
        let evi_distance = evi_list[evi].distance;
        let evi_name = evi_list[evi].name.clone();

        for evi2 in reachable {
            // if reject_indirect, treat installable versions as unreachable
            if reject_indirect && evi_list[evi2].installable {
                continue;
            }
            let newdist = evi_distance + 1;
            if newdist < evi_list[evi2].distance {
                evi_list[evi2].distance = newdist;
                evi_list[evi2].previous = evi;
            } else if newdist == evi_list[evi2].distance
                && evi_list[evi2].previous != NO_PREV
                && evi_name.as_str() < evi_list[evi_list[evi2].previous].name.as_str()
            {
                // Break ties in favor of the version name that comes first
                // according to strcmp().  (Undocumented; for determinism.)
                evi_list[evi2].previous = evi;
            }
        }
    }

    // Return None if target is not reachable from start.
    if !evi_list[evi_target].distance_known {
        return None;
    }

    // Build the update path: lcons walk from target back to start (so the result
    // is start..=target excluding start, in forward order).
    let mut result: Vec<String> = Vec::new();
    let mut evi = evi_target;
    while evi != evi_start {
        result.insert(0, evi_list[evi].name.clone());
        evi = evi_list[evi].previous;
    }

    Some(result)
}

/// `identify_update_path` (C 1592-1618). Build the version graph, then find the
/// shortest update path from `oldVersion` to `newVersion`. Raises the exact "has
/// no update path" error if there is none.
pub fn identify_update_path(
    control: &ExtensionControlFile,
    old_version: &str,
    new_version: &str,
) -> PgResult<Vec<String>> {
    // Extract the version update graph from the script directory.
    let mut evi_list = get_ext_ver_list(control)?;

    // Initialize start and end vertices.
    let evi_start = get_ext_ver_info(old_version, &mut evi_list);
    let evi_target = get_ext_ver_info(new_version, &mut evi_list);

    // Find shortest path.
    let result = find_update_path(&mut evi_list, evi_start, evi_target, false, false);

    match result {
        Some(path) => Ok(path),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "extension \"{}\" has no update path from version \"{old_version}\" to version \"{new_version}\"",
                control.name
            ))
            .into_error()),
    }
}

/// `find_install_path` (C 1728-1773). Given a target version that is not
/// directly installable, find the best installation sequence starting from a
/// directly-installable version. Returns `(Some(start_index), best_path)` for the
/// best start point (shorter path preferred, strcmp tie-break on the start
/// names), or `(None, vec![])` if there is none. An installable target yields
/// itself with an empty path.
pub fn find_install_path(
    evi_list: &mut EviList,
    evi_target: usize,
) -> (Option<usize>, Vec<String>) {
    // If the target is itself installable, start from there with an empty path.
    if evi_list[evi_target].installable {
        return (Some(evi_target), Vec::new());
    }

    let mut evi_start: Option<usize> = None;
    let mut best_path: Vec<String> = Vec::new();

    // Snapshot the installable start candidates (the arena is mutated by the
    // find_update_path Dijkstra passes below, but the set of vertices is fixed).
    let candidates: Vec<usize> = (0..evi_list.len())
        .filter(|&i| evi_list[i].installable)
        .collect();

    for evi1 in candidates {
        // Find shortest path from evi1 to evi_target; no need to consider paths
        // through other installable versions (reject_indirect = true), and
        // reinitialize since the arena was used by a previous pass.
        let path = match find_update_path(evi_list, evi1, evi_target, true, true) {
            Some(p) => p,
            None => continue,
        };

        // Remember best path.
        let better = match evi_start {
            None => true,
            Some(start) => {
                path.len() < best_path.len()
                    || (path.len() == best_path.len()
                        && evi_list[start].name.as_str() < evi_list[evi1].name.as_str())
            }
        };
        if better {
            evi_start = Some(evi1);
            best_path = path;
        }
    }

    (evi_start, best_path)
}

// ===========================================================================
// pg_extension catalog-read cores (C 188-246)
// ===========================================================================

/// `get_extension_oid(extname, missing_ok)` (C 188-204). Given an extension
/// name, look up its OID. Returns `InvalidOid` (with `missing_ok`) or raises the
/// C ereport when the extension does not exist.
pub fn get_extension_oid(extname: &str, missing_ok: bool) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("get_extension_oid");
    // GetSysCacheOid1(EXTENSIONNAME, Anum_pg_extension_oid, CStringGetDatum(extname))
    let result = GetSysCacheOid(
        scratch.mcx(),
        EXTENSIONNAME,
        cat::Anum_pg_extension_oid as AttrNumber,
        SysCacheKey::Str(extname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;

    if !OidIsValid(result) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("extension \"{extname}\" does not exist"))
            .into_error());
    }

    Ok(result)
}

/// `get_extension_name(ext_oid)` (C 210-225). Given an extension OID, look up the
/// name. Returns `None` if no such extension (the C NULL), else a copy of
/// `NameStr(extname)` in `mcx`.
pub fn get_extension_name<'mcx>(mcx: Mcx<'mcx>, ext_oid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let tuple = SearchSysCache1(mcx, EXTENSIONOID, SysCacheKey::Value(KeyDatum::from_oid(ext_oid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };

    // pstrdup(NameStr(((Form_pg_extension) GETSTRUCT(tuple))->extname))
    let (value, isnull) = SysCacheGetAttr(mcx, EXTENSIONOID, &tup, cat::Anum_pg_extension_extname)?;
    if isnull {
        // extname is a non-nullable NAME column; a NULL here is corruption.
        return Err(PgError::error(
            "get_extension_name: unexpected null extname in pg_extension tuple",
        ));
    }
    let bytes: &[u8] = match &value {
        Datum::ByRef(b) => b,
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => {
            return Err(PgError::error("get_extension_name: extname is by-value"))
        }
    };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = core::str::from_utf8(&bytes[..end])
        .map_err(|_| PgError::error("pg_extension extname is not valid UTF-8"))?;
    let result = PgString::from_str_in(s, mcx)?;

    ReleaseSysCache(tup);
    Ok(Some(result))
}

/// `get_extension_schema(ext_oid)` (C 232-246). Given an extension OID, look up
/// the OID of its namespace (`extnamespace`). Returns `InvalidOid` (the C
/// `InvalidOid`) if there is no such extension.
pub fn get_extension_schema(ext_oid: Oid) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("get_extension_schema");
    let mcx = scratch.mcx();

    let tuple = SearchSysCache1(mcx, EXTENSIONOID, SysCacheKey::Value(KeyDatum::from_oid(ext_oid)))?;
    let Some(tup) = tuple else {
        return Ok(InvalidOid);
    };

    // result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
    let (value, isnull) =
        SysCacheGetAttr(mcx, EXTENSIONOID, &tup, cat::Anum_pg_extension_extnamespace)?;
    if isnull {
        return Err(PgError::error(
            "get_extension_schema: unexpected null extnamespace in pg_extension tuple",
        ));
    }
    let result = datum_as_oid(&value)?;

    ReleaseSysCache(tup);
    Ok(result)
}

/// `ObjectAddressSet(addr, class, object)` (`catalog/objectaddress.h`) — the
/// object's address with `objectSubId = 0`.
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// Read a by-value `Datum` as an `Oid` (`DatumGetObjectId`).
fn datum_as_oid(value: &Datum<'_>) -> PgResult<Oid> {
    match value {
        Datum::ByVal(v) => Ok(*v as u32),
        _ => Err(PgError::error("extension.c: expected a by-value Oid datum")),
    }
}

// ===========================================================================
// extension_is_trusted (C 1167-1186)
// ===========================================================================

/// `extension_is_trusted(control)` (C 1167-1186). The trusted + ACL_CREATE-on-
/// database install policy: never trust unless the control file says so, then
/// allow only if the user has CREATE privilege on the current database.
pub fn extension_is_trusted(control: &ExtensionControlFile) -> PgResult<bool> {
    // Never trust unless extension's control file says it's okay.
    if !control.trusted {
        return Ok(false);
    }
    // Allow if user has CREATE privilege on current database.
    let aclresult = aclchk_seams::object_aclcheck::call(
        DatabaseRelationId,
        MyDatabaseId(),
        GetUserId(),
        ACL_CREATE,
    )?;
    Ok(aclresult == ACLCHECK_OK)
}

// ===========================================================================
// convert_requires_to_datum (C 2681-2700)
// ===========================================================================

/// `convert_requires_to_datum(requires)` (C 2681-2700). Build a `name[]` array
/// `Datum` out of the prerequisite-extension names: each name is run through
/// `namein` (yielding a NUL-padded `NAMEDATALEN`-byte block) and packed as a
/// by-reference NAME `Datum`, then `construct_array_builtin(…, NAMEOID)` builds
/// the array.
pub fn convert_requires_to_datum<'mcx>(
    mcx: Mcx<'mcx>,
    requires: &[String],
) -> PgResult<Datum<'mcx>> {
    // DirectFunctionCall1(namein, …): each name is a NUL-padded NAMEDATALEN-byte
    // image. `name` is pass-by-reference, so the legacy word-`Datum` path of
    // `construct_array_builtin` cannot carry the element payloads; the array is
    // built from the raw NAME images directly via `build_name_array` (the
    // value-typed `construct_array_builtin(names, n, NAMEOID)` specialization).
    let names: Vec<NameData> = requires
        .iter()
        .map(|curreq| namein(curreq))
        .collect::<PgResult<Vec<_>>>()?;
    let images: Vec<&[u8]> = names
        .iter()
        .map(|n| {
            // The NameData.data block is NAMEDATALEN c_char; view it as bytes.
            unsafe {
                core::slice::from_raw_parts(n.data.as_ptr() as *const u8, n.data.len())
            }
        })
        .collect();
    let buf = build_name_array(mcx, &images)?;
    Ok(Datum::ByRef(buf))
}

// ===========================================================================
// read_whole_file (C 3939-4001)
// ===========================================================================

/// `read_whole_file(filename, *length)` (C 3939-4001). Slurp a file into memory.
/// The C `stat` + `AllocateFile` + `fread` are folded into `fd.c`'s
/// `allocate_file_read`, which maps ENOENT to `None`; a missing file therefore
/// surfaces the same errcode-for-file-access "could not stat file" ERROR. The
/// `MaxAllocSize` guard is preserved. The script bytes are server-encoding text;
/// they are returned as a (lossy-decoded) `String`, mirroring the C `char *`.
pub fn read_whole_file(filename: &str) -> PgResult<String> {
    let bytes = match fd_seams::allocate_file_read::call(filename)? {
        // stat(filename) < 0 — the C errors "could not stat file".
        None => {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{filename}\""))
                .into_error());
        }
        Some(bytes) => bytes,
    };

    // fst.st_size > (MaxAllocSize - 1)
    if bytes.len() > mcx::MAX_ALLOC_SIZE - 1 {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!("file \"{filename}\" is too large"))
            .into_error());
    }

    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

// ===========================================================================
// extension_file_exists (C 2621-2675)
// ===========================================================================

/// `extension_file_exists(extensionName)` (C 2621-2675). Does a primary control
/// file for `extensionName` exist anywhere on the control path? Scans every
/// directory from [`get_extension_control_directories`]; a missing directory is
/// silently skipped (the C ENOENT case, mapped to `list_dir(..., true) == None`).
pub fn extension_file_exists(extension_name: &str) -> PgResult<bool> {
    let mut result = false;
    let locations = get_extension_control_directories()?;

    let scratch = mcx::MemoryContext::new("extension_file_exists dir scan");
    for location in &locations {
        // missing_ok = true: a missing control directory is the silent ENOENT
        // case (C returns false); any other error is raised by list_dir.
        let entries = match fd_seams::list_dir::call(scratch.mcx(), location, true)? {
            None => continue,
            Some(e) => e,
        };

        for de in entries.iter() {
            let d_name = de.name.as_str();
            if !is_extension_control_filename(d_name) {
                continue;
            }

            // Extract extension name from 'name.control' filename.
            let dot = match d_name.rfind('.') {
                Some(d) => d,
                None => continue,
            };
            let extname = &d_name[..dot];

            // Ignore it if it's an auxiliary control file.
            if extname.contains("--") {
                continue;
            }

            // Done if it matches request.
            if extname == extension_name {
                result = true;
                break;
            }
        }

        if result {
            break;
        }
    }

    Ok(result)
}

// ===========================================================================
// InsertExtensionTuple (C 2192-2271)
// ===========================================================================

/// `InsertExtensionTuple(extName, extOwner, schemaOid, relocatable, extVersion,
/// extConfig, extCondition, requiredExtensions)` (C 2192-2271). Build + insert
/// the `pg_extension` row, then record its owner / schema / required-extension
/// dependencies and fire the post-create hook. Returns the new extension's
/// `ObjectAddress`. The `ObjectAddresses` bookkeeping goes through the
/// dependency-seams (dependency.c is a cycle for this crate).
#[allow(clippy::too_many_arguments)]
pub fn InsertExtensionTuple<'mcx>(
    mcx: Mcx<'mcx>,
    ext_name: &str,
    ext_owner: Oid,
    schema_oid: Oid,
    relocatable: bool,
    ext_version: &str,
    ext_config: Option<Datum<'mcx>>,
    ext_condition: Option<Datum<'mcx>>,
    required_extensions: &[Oid],
) -> PgResult<ObjectAddress> {
    // Build and insert the pg_extension tuple.
    let rel = table_seams::table_open::call(mcx, cat::ExtensionRelationId, RowExclusiveLock)?;

    let natts = cat::Natts_pg_extension as usize;
    let mut values: Vec<Datum<'mcx>> = (0..natts).map(|_| Datum::from_oid(InvalidOid)).collect();
    let mut nulls: Vec<bool> = vec![false; natts];

    let extension_oid =
        GetNewOidWithIndex(&rel, cat::ExtensionOidIndexId, cat::Anum_pg_extension_oid as AttrNumber)?;
    values[cat::Anum_pg_extension_oid as usize - 1] = Datum::from_oid(extension_oid);

    // DirectFunctionCall1(namein, CStringGetDatum(extName)): NUL-padded NAME
    // block carried as a by-reference Datum.
    let name = namein(ext_name)?;
    let name_bytes: Vec<u8> = name.data.iter().map(|&b| b as u8).collect();
    values[cat::Anum_pg_extension_extname as usize - 1] =
        Datum::ByRef(mcx::slice_in(mcx, &name_bytes)?);

    values[cat::Anum_pg_extension_extowner as usize - 1] = Datum::from_oid(ext_owner);
    values[cat::Anum_pg_extension_extnamespace as usize - 1] = Datum::from_oid(schema_oid);
    values[cat::Anum_pg_extension_extrelocatable as usize - 1] = Datum::from_bool(relocatable);

    // CStringGetTextDatum(extVersion): the unified value-typed cstring_to_text.
    values[cat::Anum_pg_extension_extversion as usize - 1] =
        varlena_seams::cstring_to_text_v::call(mcx, ext_version)?;

    match ext_config {
        None => nulls[cat::Anum_pg_extension_extconfig as usize - 1] = true,
        Some(d) => values[cat::Anum_pg_extension_extconfig as usize - 1] = d,
    }
    match ext_condition {
        None => nulls[cat::Anum_pg_extension_extcondition as usize - 1] = true,
        Some(d) => values[cat::Anum_pg_extension_extcondition as usize - 1] = d,
    }

    let mut tuple = heap_form_tuple(mcx, &rel.rd_att, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    // heap_freetuple is implicit (mcx-owned); table_close.
    rel.close(RowExclusiveLock)?;

    // Record dependencies on owner, schema, and prerequisite extensions.
    pg_shdepend_seams::recordDependencyOnOwner::call(
        cat::ExtensionRelationId,
        extension_oid,
        ext_owner,
    )?;

    let mut refobjs = dependency_seams::new_object_addresses::call()?;

    let myself = ObjectAddressSet(cat::ExtensionRelationId, extension_oid);

    let nsp = ObjectAddressSet(NAMESPACE_RELATION_ID, schema_oid);
    dependency_seams::add_exact_object_address::call(nsp, &mut refobjs)?;

    for &reqext in required_extensions {
        let otherext = ObjectAddressSet(cat::ExtensionRelationId, reqext);
        dependency_seams::add_exact_object_address::call(otherext, &mut refobjs)?;
    }

    // Record all of them (this includes duplicate elimination).
    dependency_seams::record_object_address_dependencies::call(myself, &mut refobjs, DEPENDENCY_NORMAL)?;
    dependency_seams::free_object_addresses::call(refobjs)?;

    // Post creation hook for new extension.
    objectaccess_seams::invoke_object_post_create_hook::call(
        cat::ExtensionRelationId,
        extension_oid,
        0,
    )?;

    Ok(myself)
}

// ===========================================================================
// get_required_extension (C 2023-2088)
// ===========================================================================

/// `get_required_extension(reqExtensionName, extensionName, origSchemaName,
/// cascade, parents, is_create)` (C 2023-2088). Get the OID of an extension
/// listed in `requires`, possibly creating it (CASCADE), with cyclic-dependency
/// detection.
pub fn get_required_extension<'mcx>(
    mcx: Mcx<'mcx>,
    req_extension_name: &str,
    extension_name: &str,
    orig_schema_name: Option<&str>,
    cascade: bool,
    parents: &[String],
    is_create: bool,
) -> PgResult<Oid> {
    let mut req_extension_oid = get_extension_oid(req_extension_name, true)?;
    if !OidIsValid(req_extension_oid) {
        if cascade {
            // Check extension name validity before trying to cascade.
            check_valid_extension_name(req_extension_name)?;

            // Check for cyclic dependency between extensions.
            for pname in parents {
                if pname == req_extension_name {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_RECURSION)
                        .errmsg(format!(
                            "cyclic dependency detected between extensions \"{req_extension_name}\" and \"{extension_name}\""
                        ))
                        .into_error());
                }
            }

            ereport(NOTICE)
                .errmsg(format!(
                    "installing required extension \"{req_extension_name}\""
                ))
                .finish(here("get_required_extension"))?;

            // Add current extension to list of parents to pass down.
            let mut cascade_parents = parents.to_vec();
            cascade_parents.push(extension_name.to_string());

            // Create the required extension. We propagate the SCHEMA option if
            // any, and CASCADE, but no other options.
            let addr = CreateExtensionInternal(
                mcx,
                req_extension_name.to_string(),
                orig_schema_name.map(|s| s.to_string()),
                None,
                cascade,
                &cascade_parents,
                is_create,
            )?;

            // Get its newly-assigned OID.
            req_extension_oid = addr.objectId;
        } else {
            let mut report = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "required extension \"{req_extension_name}\" is not installed"
                ));
            if is_create {
                report = report.errhint(
                    "Use CREATE EXTENSION ... CASCADE to install required extensions too.",
                );
            }
            return Err(report.into_error());
        }
    }

    Ok(req_extension_oid)
}

// ===========================================================================
// CreateExtensionInternal (C 1784-2017)
// ===========================================================================

/// `CreateExtensionInternal(extensionName, schemaName, versionName, cascade,
/// parents, is_create)` (C 1784-2017). The CREATE EXTENSION driver: version /
/// schema selection, the CASCADE `requires` loop, the `pg_extension` catalog
/// insert + comment, then the install-script run.
///
/// The install logic up to the script is ported faithfully; the actual script
/// execution (`execute_extension_script`) and any `ApplyExtensionUpdates`
/// continuation drive the gated parser/analyzer/planner/executor/utility script
/// pipeline, so they mirror-panic here.
pub fn CreateExtensionInternal<'mcx>(
    mcx: Mcx<'mcx>,
    extension_name: String,
    schema_name: Option<String>,
    version_name: Option<String>,
    cascade: bool,
    parents: &[String],
    is_create: bool,
) -> PgResult<ObjectAddress> {
    let orig_schema_name = schema_name.clone();
    let mut schema_oid = InvalidOid;
    let extowner = GetUserId();

    // Read the primary control file.
    let pcontrol = read_extension_control_file(&extension_name)?;

    // Determine the version to install.
    let mut version_name: String = match version_name {
        Some(v) => v,
        None => match &pcontrol.default_version {
            Some(v) => v.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("version to install must be specified")
                    .into_error());
            }
        },
    };
    check_valid_version_name(&version_name)?;

    // Figure out which script(s) we need to run to install the desired version.
    let filename = get_extension_script_filename(&pcontrol, None, &version_name)?;
    let update_versions: Vec<String>;
    if fd_seams::pg_file_exists::call(&filename)? {
        // Easy, no extra scripts.
        update_versions = Vec::new();
    } else {
        // Look for best way to install this version.
        let mut evi_list = get_ext_ver_list(&pcontrol)?;

        // Identify the target version.
        let evi_target = get_ext_ver_info(&version_name, &mut evi_list);

        // Identify best path to reach target.
        let (evi_start, best_path) = find_install_path(&mut evi_list, evi_target);

        // Fail if no path ...
        let Some(start) = evi_start else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "extension \"{}\" has no installation script nor update path for version \"{version_name}\"",
                    pcontrol.name
                ))
                .into_error());
        };

        update_versions = best_path;
        // Otherwise, install best starting point and then upgrade.
        version_name = evi_list[start].name.clone();
    }

    // Fetch control parameters for installation target version.
    let control = read_extension_aux_control_file(&pcontrol, &version_name)?;

    // Determine the target schema to install the extension into.
    let mut schema_name: Option<String> = schema_name;
    if let Some(ref sname) = schema_name {
        // If the user is giving us the schema name, it must exist already.
        schema_oid = get_namespace_oid(sname, false)?;
    }

    if let Some(ref control_schema) = control.schema {
        // The extension is not relocatable and the author gave us a schema.
        if let Some(ref sname) = schema_name {
            if control_schema != sname && !cascade {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "extension \"{}\" must be installed in schema \"{control_schema}\"",
                        control.name
                    ))
                    .into_error());
            }
        }

        // Always use the schema from control file for current extension.
        schema_name = Some(control_schema.clone());

        // Find or create the schema in case it does not exist.
        schema_oid = get_namespace_oid(control_schema, true)?;

        if !OidIsValid(schema_oid) {
            // makeNode(CreateSchemaStmt): schemaname set, authrole NULL,
            // schemaElts NIL, if_not_exists false.
            let csstmt = types_nodes::ddlnodes::CreateSchemaStmt {
                schemaname: Some(PgString::from_str_in(control_schema, mcx)?),
                authrole: None,
                schemaElts: mcx::vec_with_capacity_in(mcx, 0)?,
                if_not_exists: false,
            };
            CreateSchemaCommand(mcx, &csstmt, "(generated CREATE SCHEMA command)", -1, -1)?;

            // CreateSchemaCommand includes CommandCounterIncrement, so new
            // schema is now visible.
            schema_oid = get_namespace_oid(control_schema, false)?;
        }
    } else if !OidIsValid(schema_oid) {
        // Neither user nor author specified a schema; use the current default
        // creation namespace (the first explicit entry in the search_path).
        let search_path = fetch_search_path(mcx, false)?;

        if search_path.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_SCHEMA)
                .errmsg("no schema has been selected to create in")
                .into_error());
        }
        schema_oid = search_path[0]; // linitial_oid
        schema_name = match lsyscache_seams::get_namespace_name::call(mcx, schema_oid)? {
            Some(s) => Some(s.as_str().to_string()),
            None => {
                // recently-deleted namespace?
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_SCHEMA)
                    .errmsg("no schema has been selected to create in")
                    .into_error());
            }
        };
    }

    // Make note if a temporary namespace has been accessed in this transaction.
    if isTempNamespace(schema_oid) {
        xact_seams::set_xact_accessed_temp_namespace::call();
    }

    // Look up the prerequisite extensions, install them if necessary, and build
    // lists of their OIDs and the OIDs of their target schemas.
    let mut required_extensions: Vec<Oid> = Vec::new();
    let mut required_schemas: Vec<Oid> = Vec::new();
    for curreq in control.requires.iter() {
        let reqext = get_required_extension(
            mcx,
            curreq,
            &extension_name,
            orig_schema_name.as_deref(),
            cascade,
            parents,
            is_create,
        )?;
        let reqschema = get_extension_schema(reqext)?;
        required_extensions.push(reqext);
        required_schemas.push(reqschema);
    }

    // Insert new tuple into pg_extension, and create dependency entries.
    let address = InsertExtensionTuple(
        mcx,
        &control.name,
        extowner,
        schema_oid,
        control.relocatable,
        &version_name,
        None,
        None,
        &required_extensions,
    )?;
    let extension_oid = address.objectId;

    // Apply any control-file comment on extension.
    if let Some(ref comment) = control.comment {
        CreateComments(mcx, extension_oid, cat::ExtensionRelationId, 0, Some(comment))?;
    }

    // Execute the installation script file.
    // (mirror-pg-and-panic into the gated script-execution pipeline.)
    // These values are computed faithfully but are only consumed past the gated
    // script run (execute_extension_script / ApplyExtensionUpdates), hence the
    // explicit reads here to mark them used.
    let _ = (&required_schemas, &schema_name, &update_versions);
    deferred::execute_extension_script();

    // If additional update scripts have to be executed, apply the updates as
    // though a series of ALTER EXTENSION UPDATE commands were given.
    // (unreachable: execute_extension_script panics; gated on the same pipeline.)
    #[allow(unreachable_code)]
    {
        deferred::ApplyExtensionUpdates();
        Ok(address)
    }
}

// ===========================================================================
// AlterExtensionNamespace (C 3192-3402)
// ===========================================================================

/// `AlterExtensionNamespace(extensionName, newschema, *oldschema)` (C 3192-3402).
/// Execute ALTER EXTENSION ... SET SCHEMA: relocate every member object of the
/// extension into `newschema`, then fix up `pg_extension.extnamespace` and the
/// schema dependency. Returns the extension's `ObjectAddress`; the old schema's
/// OID is returned on the side.
pub fn AlterExtensionNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    extension_name: &str,
    newschema: &str,
) -> PgResult<(ObjectAddress, Oid)> {
    let extension_oid = get_extension_oid(extension_name, false)?;

    let nsp_oid = backend_catalog_namespace::LookupCreationNamespace(mcx, newschema)?;

    // Permission check: must own extension.
    if !aclchk_seams::object_ownercheck::call(cat::ExtensionRelationId, extension_oid, GetUserId())? {
        aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_EXTENSION,
            Some(extension_name.to_string()),
        )?;
    }

    // Permission check: must have creation rights in target namespace.
    let aclresult =
        aclchk_seams::object_aclcheck::call(NAMESPACE_RELATION_ID, nsp_oid, GetUserId(), ACL_CREATE)?;
    if aclresult != ACLCHECK_OK {
        aclchk_seams::aclcheck_error::call(aclresult, OBJECT_SCHEMA, Some(newschema.to_string()))?;
    }

    // If the schema is currently a member of the extension, disallow moving the
    // extension into the schema (would create a dependency loop).
    if pg_depend_seams::getExtensionOfObject::call(NAMESPACE_RELATION_ID, nsp_oid)? == extension_oid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot move extension \"{extension_name}\" into schema \"{newschema}\" because the extension contains the schema"
            ))
            .into_error());
    }

    // Locate the pg_extension tuple.
    let ext_rel = table_seams::table_open::call(mcx, cat::ExtensionRelationId, RowExclusiveLock)?;

    let mut ext_key = ScanKeyData::empty();
    ScanKeyInit(
        &mut ext_key,
        cat::Anum_pg_extension_oid as AttrNumber,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(extension_oid),
    )?;
    let mut ext_scan = genam_seams::systable_beginscan::call(
        &ext_rel,
        cat::ExtensionOidIndexId,
        true,
        None,
        &[ext_key],
    )?;
    let ext_tup = genam_seams::systable_getnext::call(mcx, ext_scan.desc_mut())?;
    let Some(ext_tup) = ext_tup else {
        return Err(PgError::error(format!(
            "could not find tuple for extension {extension_oid}"
        )));
    };

    // Copy tuple so we can modify it below.
    let ext_tup = heap_copytuple(mcx, Some(&ext_tup))?
        .ok_or_else(|| PgError::error("heap_copytuple returned null"))?;
    let cols = heap_deform_tuple(mcx, &ext_tup.tuple, &ext_rel.rd_att, &ext_tup.data)?;
    let ext_namespace = datum_as_oid(&cols[cat::Anum_pg_extension_extnamespace as usize - 1].0)?;
    let ext_relocatable = cols[cat::Anum_pg_extension_extrelocatable as usize - 1].0.as_bool();
    let ext_name_bytes = match &cols[cat::Anum_pg_extension_extname as usize - 1].0 {
        Datum::ByRef(b) => b.as_slice().to_vec(),
        _ => return Err(PgError::error("extname is not by-reference")),
    };
    let extname_str = {
        let end = ext_name_bytes.iter().position(|&b| b == 0).unwrap_or(ext_name_bytes.len());
        String::from_utf8_lossy(&ext_name_bytes[..end]).into_owned()
    };

    ext_scan.end()?;

    // If already in the target schema, silently do nothing.
    if ext_namespace == nsp_oid {
        ext_rel.close(RowExclusiveLock)?;
        return Ok((InvalidObjectAddress, InvalidOid));
    }

    // Check extension is supposed to be relocatable.
    if !ext_relocatable {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("extension \"{extname_str}\" does not support SET SCHEMA"))
            .into_error());
    }

    let mut objs_moved = dependency_seams::new_object_addresses::call()?;

    // Store the OID of the namespace to-be-changed.
    let old_nsp_oid = ext_namespace;

    // Scan pg_depend to find objects that depend directly on the extension, and
    // alter each one's schema.
    let dep_rel = table_seams::table_open::call(mcx, DEPEND_RELATION_ID, AccessShareLock)?;

    let mut dep_key0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut dep_key0,
        types_catalog::catalog_dependency::Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(cat::ExtensionRelationId),
    )?;
    let mut dep_key1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut dep_key1,
        types_catalog::catalog_dependency::Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(extension_oid),
    )?;
    let mut dep_scan = genam_seams::systable_beginscan::call(
        &dep_rel,
        types_catalog::catalog_dependency::DependReferenceIndexId,
        true,
        None,
        &[dep_key0, dep_key1],
    )?;

    loop {
        let dep_tup = match genam_seams::systable_getnext::call(mcx, dep_scan.desc_mut())? {
            Some(t) => t,
            None => break,
        };
        let dcols = heap_deform_tuple(mcx, &dep_tup.tuple, &dep_rel.rd_att, &dep_tup.data)?;
        let dep_classid = datum_as_oid(&dcols[types_catalog::catalog_dependency::Anum_pg_depend_classid as usize - 1].0)?;
        let dep_objid = datum_as_oid(&dcols[types_catalog::catalog_dependency::Anum_pg_depend_objid as usize - 1].0)?;
        let dep_objsubid = dcols[types_catalog::catalog_dependency::Anum_pg_depend_objsubid as usize - 1].0.as_i32();
        let deptype: i8 = dcols[types_catalog::catalog_dependency::Anum_pg_depend_deptype as usize - 1].0.as_i32() as i8;

        // If a dependent extension has a no_relocate request for this extension,
        // disallow SET SCHEMA.
        if deptype == DEPENDENCY_NORMAL.as_char() && dep_classid == cat::ExtensionRelationId {
            let depextname = match get_extension_name(mcx, dep_objid)? {
                Some(n) => n.as_str().to_string(),
                None => String::new(),
            };
            let dcontrol = read_extension_control_file(&depextname)?;
            for nrextname in dcontrol.no_relocate.iter() {
                if nrextname == &extname_str {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!(
                            "cannot SET SCHEMA of extension \"{extname_str}\" because other extensions prevent it"
                        ))
                        .errdetail(format!(
                            "Extension \"{depextname}\" requests no relocation of extension \"{extname_str}\"."
                        ))
                        .into_error());
                }
            }
        }

        // Otherwise, ignore non-membership dependencies.
        if deptype != DEPENDENCY_EXTENSION.as_char() {
            continue;
        }

        if dep_objsubid != 0 {
            return Err(PgError::error(
                "extension should not have a sub-object dependency",
            ));
        }

        // Relocate the object.
        let dep_old_nsp_oid = AlterObjectNamespace_oid(mcx, dep_classid, dep_objid, nsp_oid, &mut objs_moved)?;

        // If not all the objects had the same old namespace, complain.
        if dep_old_nsp_oid != InvalidOid && dep_old_nsp_oid != old_nsp_oid {
            let dep = ObjectAddress {
                classId: dep_classid,
                objectId: dep_objid,
                objectSubId: dep_objsubid,
            };
            let objdesc = objectaddress_seams::get_object_description::call(mcx, &dep, false)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let oldnspname = lsyscache_seams::get_namespace_name::call(mcx, old_nsp_oid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("extension \"{extname_str}\" does not support SET SCHEMA"))
                .errdetail(format!(
                    "{objdesc} is not in the extension's schema \"{oldnspname}\""
                ))
                .into_error());
        }
    }

    dep_scan.end()?;
    dep_rel.close(AccessShareLock)?;

    // Now adjust pg_extension.extnamespace via heap_modify_tuple + update.
    let natts = cat::Natts_pg_extension as usize;
    let mut repl_val: Vec<Datum<'mcx>> = (0..natts).map(|_| Datum::from_oid(InvalidOid)).collect();
    let mut repl_null = vec![false; natts];
    let mut repl_repl = vec![false; natts];
    repl_val[cat::Anum_pg_extension_extnamespace as usize - 1] = Datum::from_oid(nsp_oid);
    repl_repl[cat::Anum_pg_extension_extnamespace as usize - 1] = true;
    let _ = &mut repl_null;
    let mut new_tup = heap_modify_tuple(mcx, &ext_tup, &ext_rel.rd_att, &repl_val, &repl_null, &repl_repl)
        .map_err(|e| PgError::error(format!("heap_modify_tuple failed: {e:?}")))?;
    new_tup.tuple.t_self = ext_tup.tuple.t_self;

    CatalogTupleUpdate(mcx, &ext_rel, ext_tup.tuple.t_self, &mut new_tup)?;

    ext_rel.close(RowExclusiveLock)?;

    // Update dependency to point to the new schema.
    if pg_depend_seams::changeDependencyFor::call(
        mcx,
        cat::ExtensionRelationId,
        extension_oid,
        NAMESPACE_RELATION_ID,
        old_nsp_oid,
        nsp_oid,
    )? != 1
    {
        return Err(PgError::error(format!(
            "could not change schema dependency for extension {extname_str}"
        )));
    }

    objectaccess_seams::invoke_object_post_alter_hook::call(
        cat::ExtensionRelationId,
        extension_oid,
        0,
    )?;

    let ext_addr = ObjectAddressSet(cat::ExtensionRelationId, extension_oid);

    let _ = objs_moved; // owned ObjectAddresses; freed at scope end (C frees implicitly).
    Ok((ext_addr, old_nsp_oid))
}

/// Seam wrapper for [`AlterExtensionNamespace`]: the inward seam carries no
/// `mcx`, so this runs the body in a private scratch context. `want_oldschema`
/// mirrors the C `oldschema != NULL` out-parameter; when false the returned old
/// schema OID is suppressed to `InvalidOid` (the C does not write `*oldschema`).
fn AlterExtensionNamespace_seam(
    extension_name: &str,
    newschema: &str,
    want_oldschema: bool,
) -> PgResult<(ObjectAddress, Oid)> {
    let scratch = mcx::MemoryContext::new("AlterExtensionNamespace");
    let (addr, oldschema) = AlterExtensionNamespace(scratch.mcx(), extension_name, newschema)?;
    Ok((addr, if want_oldschema { oldschema } else { InvalidOid }))
}

// ===========================================================================
// RemoveExtensionById (C 2280-2322)
// ===========================================================================

/// `RemoveExtensionById(extId)` (C 2280-2322). The per-class
/// `OCLASS_EXTENSION` drop handler dependency.c's `doDeletion` invokes for a
/// `pg_extension` object. All it does is remove the pg_extension tuple itself;
/// everything else is handled by the dependency infrastructure.
pub fn RemoveExtensionById(extId: Oid) -> PgResult<()> {
    let scratch = mcx::MemoryContext::new("RemoveExtensionById");
    let mcx = scratch.mcx();

    // Disallow deletion of the extension currently open for insertion, else
    // recordDependencyOnCurrentExtension() could create dangling pg_depend rows.
    if extId == current_extension_object() {
        let name = match get_extension_name(mcx, extId)? {
            Some(n) => n.as_str().to_string(),
            None => String::new(),
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot drop extension \"{name}\" because it is being modified"
            ))
            .into_error());
    }

    let rel = table_seams::table_open::call(mcx, cat::ExtensionRelationId, RowExclusiveLock)?;

    // ScanKeyInit(&entry[0], Anum_pg_extension_oid, BTEqualStrategyNumber,
    //             F_OIDEQ, ObjectIdGetDatum(extId));
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        cat::Anum_pg_extension_oid as AttrNumber,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        Datum::from_oid(extId),
    )?;
    let keys = [key];

    let mut scan =
        genam_seams::systable_beginscan::call(&rel, cat::ExtensionOidIndexId, true, None, &keys)?;
    let tuple = genam_seams::systable_getnext::call(mcx, scan.desc_mut())?;

    // We assume that there can be at most one matching tuple.
    if let Some(tup) = &tuple {
        CatalogTupleDelete(mcx, &rel, tup.tuple.t_self)?;
    }

    scan.end()?;
    rel.close(RowExclusiveLock)?;
    Ok(())
}

// ===========================================================================
// CreateExtension (parse-side) (C 2094-2176)
// ===========================================================================

/// `CreateExtension(pstate, stmt)` (C 2094-2176). Parse-side of CREATE EXTENSION:
/// name validity, duplicate-name / IF NOT EXISTS handling, nested-create guard,
/// and option deconstruction, then `CreateExtensionInternal` (deferred — the
/// executor/SPI-driven install body is out of F0 scope).
pub fn CreateExtension<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &types_nodes::parsestmt::ParseState<'_>,
    stmt: &types_nodes::ddlnodes::CreateExtensionStmt<'_>,
) -> PgResult<ObjectAddress> {
    let extname = stmt
        .extname
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");

    // Check extension name validity before any filesystem access.
    check_valid_extension_name(extname)?;

    // Check for duplicate extension name (a friendlier message + IF NOT EXISTS
    // support; the unique index is the backstop against races).
    if OidIsValid(get_extension_oid(extname, true)?) {
        if stmt.if_not_exists {
            ereport(NOTICE)
                .errcode(types_error::ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("extension \"{extname}\" already exists, skipping"))
                .finish(here("CreateExtension"))?;
            return Ok(InvalidObjectAddress);
        } else {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("extension \"{extname}\" already exists"))
                .into_error());
        }
    }

    // We use global variables to track the extension being created, so we can
    // create only one extension at the same time.
    if creating_extension() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("nested CREATE EXTENSION is not supported")
            .into_error());
    }

    // Deconstruct the statement option list.
    let mut d_schema = false;
    let mut d_new_version = false;
    let mut d_cascade = false;
    let mut schema_name: Option<String> = None;
    let mut version_name: Option<String> = None;
    let mut cascade = false;

    for opt in stmt.options.iter() {
        let defel = as_defelem(opt);
        let defname = defel.defname.as_deref().unwrap_or("");
        if defname == "schema" {
            if d_schema {
                return error_conflicting_def_elem(defel, pstate);
            }
            d_schema = true;
            schema_name = Some(def_get_string(defel)?);
        } else if defname == "new_version" {
            if d_new_version {
                return error_conflicting_def_elem(defel, pstate);
            }
            d_new_version = true;
            version_name = Some(def_get_string(defel)?);
        } else if defname == "cascade" {
            if d_cascade {
                return error_conflicting_def_elem(defel, pstate);
            }
            d_cascade = true;
            cascade = def_get_boolean(defel)?;
        } else {
            return Err(PgError::error(format!("unrecognized option: {defname}")));
        }
    }

    // Call CreateExtensionInternal to do the real work.
    CreateExtensionInternal(
        mcx,
        extname.to_string(),
        schema_name,
        version_name,
        cascade,
        &[],
        true,
    )
}

// ---------------------------------------------------------------------------
// DefElem readers over the raw-parser node tree (define.c's defGetString /
// defGetBoolean / errorConflictingDefElem, on the ddlnodes::DefElem model the
// CreateExtensionStmt.options carry). Ported in-crate (1:1 with C) because the
// shared define.c crate operates on the parsenodes::DefElem model.
// ---------------------------------------------------------------------------

/// Borrow an option `Node` as its inner `DefElem` (`castNode(DefElem, lfirst)`).
fn as_defelem<'a, 'mcx>(
    node: &'a types_nodes::nodes::NodePtr<'mcx>,
) -> &'a types_nodes::ddlnodes::DefElem<'mcx> {
    match &**node {
        types_nodes::nodes::Node::DefElem(d) => d,
        _ => panic!("extension.c: CREATE EXTENSION option is not a DefElem node"),
    }
}

/// `defGetString(def)` (define.c:34-62) for the string-valued options CREATE
/// EXTENSION accepts (schema / new_version are `T_String` value nodes).
fn def_get_string(def: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<String> {
    let arg = def.arg.as_deref().ok_or_else(|| {
        ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "{} requires a parameter",
                def.defname.as_deref().unwrap_or("")
            ))
            .into_error()
    })?;
    match arg {
        types_nodes::nodes::Node::Integer(i) => Ok(i.ival.to_string()),
        types_nodes::nodes::Node::Boolean(b) => {
            Ok(if b.boolval { "true" } else { "false" }.to_string())
        }
        types_nodes::nodes::Node::String(s) => Ok(s.sval.as_str().to_string()),
        _ => Err(PgError::error(format!(
            "unrecognized node type: {}",
            arg.node_tag().0
        ))),
    }
}

/// `defGetBoolean(def)` (define.c:93-143) for the `cascade` option.
fn def_get_boolean(def: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<bool> {
    // If no parameter value given, assume "true" is meant.
    let Some(arg) = def.arg.as_deref() else {
        return Ok(true);
    };

    match arg {
        types_nodes::nodes::Node::Integer(i) => match i.ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        },
        types_nodes::nodes::Node::String(s) => {
            let sval = s.sval.as_str();
            if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
                return Ok(true);
            }
            if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
                return Ok(false);
            }
        }
        _ => {}
    }

    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!(
            "{} requires a Boolean value",
            def.defname.as_deref().unwrap_or("")
        ))
        .into_error())
}

/// `errorConflictingDefElem(defel, pstate)` (define.c) — "conflicting or
/// redundant options" at the option's parse location.
fn error_conflicting_def_elem(
    def: &types_nodes::ddlnodes::DefElem<'_>,
    _pstate: &types_nodes::parsestmt::ParseState<'_>,
) -> PgResult<ObjectAddress> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(def.location)
        .into_error())
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every inward seam this unit owns
/// (`backend-commands-extension-seams`).
pub fn init_seams() {
    use backend_commands_extension_seams as s;

    // `extension_control_path` GUC slot (`char *Extension_control_path`,
    // extension.c:76) — install its `conf->variable` get/set accessors over
    // this unit's backing store.
    backend_utils_misc_guc_tables::vars::Extension_control_path.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: extension_control_path_get,
            set: extension_control_path_set,
        },
    );

    s::creating_extension::set(creating_extension);
    s::current_extension_object::set(current_extension_object);
    s::get_extension_name::set(get_extension_name);
    s::get_extension_oid::set(get_extension_oid);
    s::RemoveExtensionById::set(RemoveExtensionById);
    s::AlterExtensionNamespace::set(AlterExtensionNamespace_seam);
}
