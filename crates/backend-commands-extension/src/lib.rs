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
//! The executor- / SPI- / catalog-DML command bodies (`CreateExtensionInternal`,
//! `InsertExtensionTuple`, `ExecAlterExtensionStmt`, the SRFs, etc.) are out of
//! F0 scope: they live in [`deferred`] behind loud panics (mirror-pg-and-panic)
//! until the executor / SPI subsystems are wired. No silent stubs.

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

use backend_utils_error::ereport;

use backend_access_common_scankey::ScanKeyInit;
use backend_catalog_indexing::keystone::CatalogTupleDelete;
use backend_utils_cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, EXTENSIONNAME, EXTENSIONOID,
};

use types_cache::SysCacheKey;
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_catalog::pg_extension as cat;
use types_datum::Datum as KeyDatum;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table_seams as table_seams;
use backend_storage_file_fd_seams as fd_seams;
use backend_utils_adt_varlena_seams as varlena_seams;

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

/// `Extension_control_path` GUC read (the colon-separated control path).
fn extension_control_path() -> String {
    let accessors = backend_utils_misc_guc_tables::vars::Extension_control_path.get();
    (accessors.get)().unwrap_or_default()
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

    // Call CreateExtensionInternal to do the real work (deferred: it drives the
    // syscache/namespace/ACL lookups, schema-create + script-execution
    // pipeline, and the pg_extension catalog insert).
    let _ = (mcx, schema_name, version_name, cascade);
    deferred::CreateExtensionInternal()
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
    s::creating_extension::set(creating_extension);
    s::current_extension_object::set(current_extension_object);
    s::get_extension_name::set(get_extension_name);
    s::get_extension_oid::set(get_extension_oid);
    s::RemoveExtensionById::set(RemoveExtensionById);
}
