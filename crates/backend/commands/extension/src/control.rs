// Control-file location + parsing (extension.c:453-865, 3939-4059). Cold DDL
// path: owned std strings per the guc_file/pgtz precedent.
use std::cell::RefCell;

use elog::ereport;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_NAME, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR,
};

use crate::{first_dir_separator, is_extension_control_filename};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionControlFile {
    pub name: String,
    pub basedir: Option<String>,
    pub control_dir: Option<String>,
    pub directory: Option<String>,
    pub default_version: Option<String>,
    pub module_pathname: Option<String>,
    pub comment: Option<String>,
    pub schema: Option<String>,
    pub relocatable: bool,
    pub superuser: bool,
    pub trusted: bool,
    pub encoding: i32,
    pub requires: Vec<String>,
    pub no_relocate: Vec<String>,
}

pub fn new_ExtensionControlFile(extname: &str) -> ExtensionControlFile {
    ExtensionControlFile {
        name: extname.to_string(),
        superuser: true,
        encoding: -1,
        ..Default::default()
    }
}

thread_local! {
    // C `char *Extension_control_path` (extension.c:76), the GUC backing store.
    static EXTENSION_CONTROL_PATH: RefCell<String> = const { RefCell::new(String::new()) };
}

fn extension_control_path_get() -> Option<String> {
    Some(EXTENSION_CONTROL_PATH.with(|c| c.borrow().clone()))
}

fn extension_control_path_set(v: Option<String>) {
    EXTENSION_CONTROL_PATH.with(|c| *c.borrow_mut() = v.unwrap_or_default());
}

pub(crate) fn install_extension_control_path_guc() {
    guc_tables::vars::Extension_control_path.install(guc_tables::GucVarAccessors {
        get: extension_control_path_get,
        set: extension_control_path_set,
    });
}

// DIVERGENCE from get_share_path(my_exec_path): PGRUST_PGSHAREDIR
// (runtime, then build) takes precedence (pgtz/tzparser precedent;
// harness override).
fn share_path() -> String {
    if let Ok(dir) = std::env::var("PGRUST_PGSHAREDIR") {
        return dir;
    }
    if let Some(share) = option_env!("PGRUST_PGSHAREDIR") {
        return String::from(share);
    }
    let exec = init_small::globals::my_exec_path();
    let len = exec.iter().position(|&b| b == 0).unwrap_or(exec.len());
    pg_path::get_share_path(&String::from_utf8_lossy(&exec[..len]))
}

// substitute_path_macro (utils/conffiles.c), $macro at the start of `s` only.
fn substitute_path_macro(s: &str, macro_: &str, value: &str) -> PgResult<String> {
    debug_assert!(macro_.starts_with('$'));
    if !s.starts_with('$') {
        return Ok(s.to_string());
    }
    let sep = first_dir_separator(s).unwrap_or(s.len());
    if &s[..sep] != macro_ {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_NAME)
            .errmsg(format!("invalid macro name in path: {s}"))
            .into_error()
            .into());
    }
    Ok(format!("{value}{}", &s[sep..]))
}

// DIVERGENCE: contrib extension files are staged beside the binary by
// main_main's build.rs (no `make install` exists); the staged dir precedes
// the system dir so this repo's vendored scripts win over whatever C install
// PGRUST_PGSHAREDIR points at (that env is set for tzdata, not extensions).
fn staged_extension_dir() -> Option<String> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?.join("share/extension");
    dir.is_dir().then(|| dir.to_string_lossy().into_owned())
}

pub(crate) fn get_extension_control_directories() -> PgResult<Vec<String>> {
    let system_dir = format!("{}/extension", share_path());
    let ecp = EXTENSION_CONTROL_PATH.with(|c| c.borrow().clone());

    let mut paths = Vec::new();
    paths.extend(staged_extension_dir());

    if ecp.is_empty() {
        paths.push(system_dir);
        return Ok(paths);
    }

    for piece in ecp.split(':') {
        let mangled = if piece == "$system" {
            substitute_path_macro(piece, "$system", &system_dir)?
        } else {
            format!("{piece}/extension")
        };
        paths.push(pg_path::canonicalize_path(&mangled));
    }
    Ok(paths)
}

// find_in_paths (extension.c:4029): absolute-only search of basename in paths.
fn find_in_paths(basename: &str, paths: &[String]) -> PgResult<Option<String>> {
    for path in paths {
        let path = pg_path::canonicalize_path(path);
        if !pg_path::is_absolute_path(&path) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_NAME)
                .errmsg(
                    "component in parameter \"extension_control_path\" is not an absolute path",
                )
                .into_error()
                .into());
        }
        let full = format!("{path}/{basename}");
        if std::fs::metadata(&full).is_ok() {
            return Ok(Some(full));
        }
    }
    Ok(None)
}

fn find_extension_control_filename(control: &mut ExtensionControlFile) -> PgResult<Option<String>> {
    let basename = format!("{}.control", control.name);
    let paths = get_extension_control_directories()?;
    let result = find_in_paths(&basename, &paths)?;
    if let Some(full) = &result {
        let p = full.rfind('/').expect("absolute path has a separator");
        control.control_dir = Some(full[..p].to_string());
    }
    Ok(result)
}

pub(crate) fn get_extension_script_directory(control: &ExtensionControlFile) -> String {
    let Some(directory) = &control.directory else {
        return control.control_dir.clone().expect("control_dir set by control-file read");
    };
    if pg_path::is_absolute_path(directory) {
        return directory.clone();
    }
    let basedir = control.basedir.as_ref().expect("basedir set by control-file read");
    format!("{basedir}/{directory}")
}

fn get_extension_aux_control_filename(control: &ExtensionControlFile, version: &str) -> String {
    format!("{}/{}--{version}.control", get_extension_script_directory(control), control.name)
}

pub(crate) fn get_extension_script_filename(
    control: &ExtensionControlFile,
    from_version: Option<&str>,
    version: &str,
) -> String {
    let scriptdir = get_extension_script_directory(control);
    match from_version {
        Some(from) => format!("{scriptdir}/{}--{from}--{version}.sql", control.name),
        None => format!("{scriptdir}/{}--{version}.sql", control.name),
    }
}

fn parse_extension_control_file(
    control: &mut ExtensionControlFile,
    version: Option<&str>,
) -> PgResult<()> {
    let filename = if let Some(version) = version {
        Some(get_extension_aux_control_filename(control, version))
    } else if let Some(dir) = &control.control_dir {
        Some(format!("{dir}/{}.control", control.name))
    } else {
        find_extension_control_filename(control)?
    };

    let Some(filename) = filename else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("extension \"{}\" is not available", control.name))
            .errhint(
                "The extension must first be installed on the system where PostgreSQL is running.",
            )
            .into_error()
            .into());
    };

    let control_dir = control.control_dir.as_ref().expect("control_dir set above");
    debug_assert!(control_dir.ends_with("/extension"));
    control.basedir = Some(control_dir[..control_dir.len() - "/extension".len()].to_string());

    let contents = match std::fs::read(&filename) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && version.is_some() => {
            return Ok(());
        }
        Err(e) => {
            let mut b = ereport(ERROR);
            if let Some(errno) = e.raw_os_error() {
                b = b.with_saved_errno(errno).errcode_for_file_access();
            }
            return Err(b
                .errmsg(format!("could not open extension control file \"{filename}\": %m"))
                .into_error()
                .into());
        }
    };

    let mut items: Vec<guc_file::ConfigVariable> = Vec::new();
    guc_file::ParseConfigFp(
        &contents,
        std::path::Path::new(&filename),
        guc_file::CONF_FILE_START_DEPTH,
        ERROR,
        &mut items,
    )?;

    for item in &items {
        let name = item.name.as_deref().unwrap_or("");
        let value = item.value.as_deref().unwrap_or("");
        match name {
            "directory" => {
                secondary_check(name, version)?;
                control.directory = Some(value.to_string());
            }
            "default_version" => {
                secondary_check(name, version)?;
                control.default_version = Some(value.to_string());
            }
            "module_pathname" => control.module_pathname = Some(value.to_string()),
            "comment" => control.comment = Some(value.to_string()),
            "schema" => control.schema = Some(value.to_string()),
            "relocatable" => control.relocatable = parse_bool_param(name, value)?,
            "superuser" => control.superuser = parse_bool_param(name, value)?,
            "trusted" => control.trusted = parse_bool_param(name, value)?,
            "encoding" => {
                control.encoding = mbutils::pg_valid_server_encoding(value);
                if control.encoding < 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("\"{value}\" is not a valid encoding name"))
                        .into_error()
                        .into());
                }
            }
            "requires" => control.requires = split_names_param(name, value)?,
            "no_relocate" => control.no_relocate = split_names_param(name, value)?,
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized parameter \"{name}\" in file \"{filename}\""))
                    .into_error()
                    .into());
            }
        }
    }

    if control.relocatable && control.schema.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("parameter \"schema\" cannot be specified when \"relocatable\" is true")
            .into_error()
            .into());
    }
    Ok(())
}

fn secondary_check(name: &str, version: Option<&str>) -> PgResult<()> {
    if version.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "parameter \"{name}\" cannot be set in a secondary extension control file"
            ))
            .into_error()
            .into());
    }
    Ok(())
}

fn parse_bool_param(name: &str, value: &str) -> PgResult<bool> {
    adt_bool::parse_bool(value).ok_or_else(|| {
        Box::new(
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("parameter \"{name}\" requires a Boolean value"))
                .into_error(),
        )
    })
}

fn split_names_param(name: &str, value: &str) -> PgResult<Vec<String>> {
    let scratch = mcx::MemoryContext::new("extension control name list");
    match varlena::split_identifier_string(
        scratch.mcx(),
        value,
        b',',
        mbutils::GetDatabaseEncoding(),
    )? {
        Some(names) => Ok(names),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("parameter \"{name}\" must be a list of extension names"))
            .into_error()
            .into()),
    }
}

pub fn read_extension_control_file(extname: &str) -> PgResult<ExtensionControlFile> {
    let mut control = new_ExtensionControlFile(extname);
    parse_extension_control_file(&mut control, None)?;
    Ok(control)
}

pub(crate) fn read_extension_aux_control_file(
    pcontrol: &ExtensionControlFile,
    version: &str,
) -> PgResult<ExtensionControlFile> {
    let mut acontrol = pcontrol.clone();
    parse_extension_control_file(&mut acontrol, Some(version))?;
    Ok(acontrol)
}

// read_whole_file (extension.c:3939), Unix arms only.
pub(crate) fn read_whole_file(filename: &str) -> PgResult<Vec<u8>> {
    const MAX_ALLOC_SIZE: u64 = 0x3fffffff;
    let meta = std::fs::metadata(filename).map_err(|e| {
        let mut b = ereport(ERROR);
        if let Some(errno) = e.raw_os_error() {
            b = b.with_saved_errno(errno).errcode_for_file_access();
        }
        b.errmsg(format!("could not stat file \"{filename}\": %m")).into_error()
    })?;
    if meta.len() > MAX_ALLOC_SIZE - 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!("file \"{filename}\" is too large"))
            .into_error()
            .into());
    }
    std::fs::read(filename).map_err(|e| {
        let mut b = ereport(ERROR);
        if let Some(errno) = e.raw_os_error() {
            b = b.with_saved_errno(errno).errcode_for_file_access();
        }
        b.errmsg(format!("could not open file \"{filename}\" for reading: %m"))
            .into_error()
            .into()
    })
}

// Shared directory-scan shape of pg_available_extensions /
// pg_available_extension_versions / extension_file_exists: yield each primary
// control-file name once, path-search order.
pub(crate) fn for_each_primary_control_name(
    mut f: impl FnMut(&str, &str) -> PgResult<bool>,
) -> PgResult<()> {
    let locations = get_extension_control_directories()?;
    let mut found: Vec<String> = Vec::new();
    for location in &locations {
        let entries = match std::fs::read_dir(location) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                let mut b = ereport(ERROR);
                if let Some(errno) = e.raw_os_error() {
                    b = b.with_saved_errno(errno).errcode_for_file_access();
                }
                return Err(b
                    .errmsg(format!("could not open directory \"{location}\": %m"))
                    .into_error()
                    .into());
            }
        };
        for de in entries {
            let de = de.map_err(|e| {
                let mut b = ereport(ERROR);
                if let Some(errno) = e.raw_os_error() {
                    b = b.with_saved_errno(errno).errcode_for_file_access();
                }
                b.errmsg(format!("could not read directory \"{location}\": %m")).into_error()
            })?;
            let fname = de.file_name();
            let Some(fname) = fname.to_str() else { continue };
            if !is_extension_control_filename(fname) {
                continue;
            }
            let extname = &fname[..fname.rfind('.').expect("suffix-checked")];
            if extname.contains("--") {
                continue;
            }
            if found.iter().any(|n| n == extname) {
                continue;
            }
            found.push(extname.to_string());
            if !f(extname, location)? {
                return Ok(());
            }
        }
    }
    Ok(())
}

pub fn extension_file_exists(extension_name: &str) -> PgResult<bool> {
    let mut result = false;
    for_each_primary_control_name(|extname, _location| {
        if extname == extension_name {
            result = true;
            return Ok(false);
        }
        Ok(true)
    })?;
    Ok(result)
}

// parse_extension_control_file with a pre-seeded control_dir, for the
// directory-scan SRFs.
pub(crate) fn parse_control_in_dir(
    extname: &str,
    location: &str,
) -> PgResult<ExtensionControlFile> {
    let mut control = new_ExtensionControlFile(extname);
    control.control_dir = Some(location.to_string());
    parse_extension_control_file(&mut control, None)?;
    Ok(control)
}
