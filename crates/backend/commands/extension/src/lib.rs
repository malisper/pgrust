// extension.c — CREATE/ALTER/DROP EXTENSION + control-file/version-script
// machinery. Loud: ALTER EXTENSION ADD/DROP + SET SCHEMA, extension_config_dump
// / config_remove, get_function_sibling_type, pg_get_loaded_modules.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid, EXTENSION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
};

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

// elog(ERROR, "could not find tuple for extension %u") (extension.c:2860,
// 3057, 3252, 3604): a catchable XX000, not a backend abort.
#[cold]
#[inline(never)]
pub(crate) fn extension_tuple_not_found(oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("could not find tuple for extension {oid}")))
}

// carve-ratifications.md §11 (tcop's non_utf8_query_error): text C would run or spell.
#[cold]
#[inline(never)]
pub(crate) fn non_utf8_text_error() -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "query strings with non-ASCII characters are not supported yet in databases \
                 with encoding \"{}\"",
                mbutils::GetDatabaseEncodingName()
            ),
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_hint("Use a database with encoding \"UTF8\"."),
    )
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
    // SQL_ASCII keeps bind-parameter bytes as-is; C hands them to every %s.
    let name = PgString::from_str_in(&String::from_utf8_lossy(&bytes[..len]), mcx)?;
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

// errmsg("invalid ... name: \"%s\"") on the name bytes: C's exact bytes go on the wire.
fn invalid_name(what: &str, name: &[u8], detail: &'static str) -> Box<PgError> {
    let mut msg = format!("invalid {what}: \"").into_bytes();
    msg.extend_from_slice(name);
    msg.push(b'"');
    Box::new(
        PgError::error_raw_message(msg)
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail(detail),
    )
}

pub fn check_valid_extension_name(extensionname: &str) -> PgResult<()> {
    check_valid_extension_name_bytes(extensionname.as_bytes())
}

// check_valid_extension_name (extension.c:360-397) is bytewise.
pub(crate) fn check_valid_extension_name_bytes(bytes: &[u8]) -> PgResult<()> {
    if bytes.is_empty() {
        return Err(invalid_name(
            "extension name",
            bytes,
            "Extension names must not be empty.",
        )
        .into());
    }
    if bytes.windows(2).any(|w| w == b"--") {
        return Err(invalid_name(
            "extension name",
            bytes,
            "Extension names must not contain \"--\".",
        )
        .into());
    }
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(invalid_name(
            "extension name",
            bytes,
            "Extension names must not begin or end with \"-\".",
        )
        .into());
    }
    if first_dir_separator(bytes).is_some() {
        return Err(invalid_name(
            "extension name",
            bytes,
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
            "extension version name",
            bytes,
            "Version names must not be empty.",
        )
        .into());
    }
    if versionname.contains("--") {
        return Err(invalid_name(
            "extension version name",
            bytes,
            "Version names must not contain \"--\".",
        )
        .into());
    }
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return Err(invalid_name(
            "extension version name",
            bytes,
            "Version names must not begin or end with \"-\".",
        )
        .into());
    }
    if first_dir_separator(bytes).is_some() {
        return Err(invalid_name(
            "extension version name",
            bytes,
            "Version names must not contain directory separator characters.",
        )
        .into());
    }
    Ok(())
}

// first_dir_separator (common/path.c, non-Windows).
pub(crate) fn first_dir_separator(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'/')
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
    extension_seams::pg_extension_config_dump::set(funcs::fc_pg_extension_config_dump);
    extension_seams::pg_get_loaded_modules::set(funcs::fc_pg_get_loaded_modules);
    extension_seams::get_extension_name::set(|ext_oid| {
        let cx = mcx::MemoryContext::new_bump("get_extension_name");
        let out = match get_extension_name(cx.mcx(), ext_oid) {
            Ok(name) => Ok(name.map(|s| s.as_str().to_owned())),
            Err(e) => Err(e),
        };
        out
    });
}

#[cfg(test)]
mod elog_error_tests {
    use super::*;

    #[test]
    fn missing_extension_tuple_is_a_catchable_xx000() {
        let e = extension_tuple_not_found(16389);
        assert_eq!(e.message(), "could not find tuple for extension 16389");
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), ERROR);
    }

    // extension.c:360-397 on a SQL_ASCII bind parameter's raw bytes.
    #[test]
    fn extension_name_checks_run_on_the_raw_bytes() {
        assert!(check_valid_extension_name_bytes(b"caf\xe9").is_ok());
        let e = check_valid_extension_name_bytes(b"caf\xe9--x").unwrap_err();
        assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(e.message(), "invalid extension name: \"caf\u{FFFD}--x\"");
        assert_eq!(
            e.message_raw.as_deref(),
            Some(&b"invalid extension name: \"caf\xe9--x\""[..])
        );
        assert_eq!(e.detail(), Some("Extension names must not contain \"--\"."));
        let e = check_valid_extension_name_bytes(b"a/\xe9").unwrap_err();
        assert_eq!(
            e.detail(),
            Some("Extension names must not contain directory separator characters.")
        );
        let e = check_valid_extension_name("").unwrap_err();
        assert_eq!(e.detail(), Some("Extension names must not be empty."));
    }
}
