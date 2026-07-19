// extension.c — CREATE/ALTER/DROP EXTENSION + control-file/version-script
// machinery. Loud: ALTER EXTENSION ADD/DROP + SET SCHEMA, extension_config_dump
// / config_remove, get_function_sibling_type, pg_get_loaded_modules.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid, EXTENSION_RELATION_ID};
use types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};

use cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, EXTENSIONNAME,
    EXTENSIONOID,
};
use datum::Datum;
use elog::ereport;

pub mod alter;
pub mod contents;
pub mod control;
pub mod create;
pub mod funcs;
pub mod graph;
pub mod script;

pub use alter::{AlterExtensionNamespace, ExecAlterExtensionStmt};
pub use contents::ExecAlterExtensionContentsStmt;
pub use control::{extension_file_exists, read_extension_control_file, ExtensionControlFile};
pub use create::{CreateExtension, InsertExtensionTuple, RemoveExtensionById};
pub use pg_depend::{creating_extension, CurrentExtensionObject};

pub const ExtensionRelationId: Oid = EXTENSION_RELATION_ID;
pub const ExtensionOidIndexId: Oid = 3080;
pub const ExtensionNameIndexId: Oid = 3081;

pub const Anum_pg_extension_oid: i32 = 1;
pub const Anum_pg_extension_extname: i32 = 2;
pub const Anum_pg_extension_extowner: i32 = 3;
pub const Anum_pg_extension_extnamespace: i32 = 4;
pub const Anum_pg_extension_extrelocatable: i32 = 5;
pub const Anum_pg_extension_extversion: i32 = 6;
pub const Anum_pg_extension_extconfig: i32 = 7;
pub const Anum_pg_extension_extcondition: i32 = 8;
pub const Natts_pg_extension: usize = 8;

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: extension.c {what}")
}

pub fn get_extension_oid(extname: &str, missing_ok: bool) -> PgResult<Oid> {
    let result = GetSysCacheOid(
        EXTENSIONNAME,
        Anum_pg_extension_oid,
        SysCacheKey::Str(extname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if result == InvalidOid && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("extension \"{extname}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(result)
}

pub fn get_extension_name<'mcx>(mcx: Mcx<'mcx>, ext_oid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let Some(tuple) =
        SearchSysCache1(EXTENSIONOID, SysCacheKey::Value(Datum::from_oid(ext_oid)))?
    else {
        return Ok(None);
    };
    let (d, isnull) = SysCacheGetAttr(EXTENSIONOID, &tuple, Anum_pg_extension_extname)?;
    debug_assert!(!isnull);
    let p = d.as_usize() as *const u8;
    // SAFETY: extname is a NameData attr of a live syscache tuple — 64
    // NUL-padded bytes.
    let bytes = unsafe { core::slice::from_raw_parts(p, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    let s = core::str::from_utf8(&bytes[..len]).expect("extname is server-encoding text");
    let name = PgString::from_str_in(s, mcx)?;
    ReleaseSysCache(tuple);
    Ok(Some(name))
}

pub fn get_extension_schema(ext_oid: Oid) -> PgResult<Oid> {
    let Some(tuple) =
        SearchSysCache1(EXTENSIONOID, SysCacheKey::Value(Datum::from_oid(ext_oid)))?
    else {
        return Ok(InvalidOid);
    };
    let (d, isnull) = SysCacheGetAttr(EXTENSIONOID, &tuple, Anum_pg_extension_extnamespace)?;
    debug_assert!(!isnull);
    let result = d.as_oid();
    ReleaseSysCache(tuple);
    Ok(result)
}

fn invalid_name(kind_msg: String, detail: &'static str) -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(kind_msg)
            .errdetail(detail)
            .into_error(),
    )
}

pub fn check_valid_extension_name(extensionname: &str) -> PgResult<()> {
    let bytes = extensionname.as_bytes();
    if bytes.is_empty() {
        return Err(invalid_name(
            format!("invalid extension name: \"{extensionname}\""),
            "Extension names must not be empty.",
        )
        .into());
    }
    if extensionname.contains("--") {
        return Err(invalid_name(
            format!("invalid extension name: \"{extensionname}\""),
            "Extension names must not contain \"--\".",
        )
        .into());
    }
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(invalid_name(
            format!("invalid extension name: \"{extensionname}\""),
            "Extension names must not begin or end with \"-\".",
        )
        .into());
    }
    if first_dir_separator(extensionname).is_some() {
        return Err(invalid_name(
            format!("invalid extension name: \"{extensionname}\""),
            "Extension names must not contain directory separator characters.",
        )
        .into());
    }
    Ok(())
}

pub fn check_valid_version_name(versionname: &str) -> PgResult<()> {
    let bytes = versionname.as_bytes();
    if bytes.is_empty() {
        return Err(invalid_name(
            format!("invalid extension version name: \"{versionname}\""),
            "Version names must not be empty.",
        )
        .into());
    }
    if versionname.contains("--") {
        return Err(invalid_name(
            format!("invalid extension version name: \"{versionname}\""),
            "Version names must not contain \"--\".",
        )
        .into());
    }
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(invalid_name(
            format!("invalid extension version name: \"{versionname}\""),
            "Version names must not begin or end with \"-\".",
        )
        .into());
    }
    if first_dir_separator(versionname).is_some() {
        return Err(invalid_name(
            format!("invalid extension version name: \"{versionname}\""),
            "Version names must not contain directory separator characters.",
        )
        .into());
    }
    Ok(())
}

// first_dir_separator (common/path.c, non-Windows).
pub(crate) fn first_dir_separator(s: &str) -> Option<usize> {
    s.bytes().position(|b| b == b'/')
}

pub fn is_extension_control_filename(filename: &str) -> bool {
    matches!(filename.rfind('.'), Some(dot) if &filename[dot..] == ".control")
}

pub fn is_extension_script_filename(filename: &str) -> bool {
    matches!(filename.rfind('.'), Some(dot) if &filename[dot..] == ".sql")
}

pub fn init_seams() {
    control::install_extension_control_path_guc();
    extension_seams::pg_available_extensions::set(funcs::fc_pg_available_extensions);
    extension_seams::pg_available_extension_versions::set(funcs::fc_pg_available_extension_versions);
    extension_seams::pg_extension_update_paths::set(funcs::fc_pg_extension_update_paths);
    extension_seams::get_extension_name::set(|ext_oid| {
        let cx = mcx::MemoryContext::new_bump("get_extension_name");
        let out = match get_extension_name(cx.mcx(), ext_oid) {
            Ok(name) => Ok(name.map(|s| s.as_str().to_owned())),
            Err(e) => Err(e),
        };
        out
    });
}
