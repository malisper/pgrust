use elog::write_stderr;
use miscinit::make_absolute_path;
use types_error::PgResult;
use types_guc::{PGC_POSTMASTER, PGC_S_OVERRIDE};

use crate::process_config::pg_timezone_abbrev_initialize;

pub const CONFIG_FILENAME: &str = "postgresql.conf";
const HBA_FILENAME: &str = "pg_hba.conf";
const IDENT_FILENAME: &str = "pg_ident.conf";

fn nonempty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.is_empty())
}

// SelectConfigFiles (guc.c:1784). Ok(false) = C's "return false" (caller
// exits with status 2).
pub fn SelectConfigFiles(user_d_option: Option<&str>, progname: &str) -> PgResult<bool> {
    let configdir = user_d_option
        .map(str::to_owned)
        .or_else(|| std::env::var("PGDATA").ok())
        .map(|d| make_absolute_path(&d));

    if let Some(dir) = configdir.as_deref() {
        if let Err(e) = std::fs::metadata(dir) {
            write_stderr(&format!("{progname}: could not access directory \"{dir}\": {e}\n"));
            if e.kind() == std::io::ErrorKind::NotFound {
                write_stderr(
                    "Run initdb or pg_basebackup to initialize a PostgreSQL data directory.\n",
                );
            }
            return Ok(false);
        }
    }

    let fname = match nonempty(guc_tables::vars::ConfigFileName.read()) {
        Some(explicit) => make_absolute_path(&explicit),
        None => match configdir.as_deref() {
            Some(dir) => format!("{dir}/{CONFIG_FILENAME}"),
            None => {
                write_stderr(&format!(
                    "{progname} does not know where to find the server configuration file.\n\
                     You must specify the --config-file or -D invocation option or set the PGDATA environment variable.\n"
                ));
                return Ok(false);
            }
        },
    };
    crate::SetConfigOption("config_file", Some(&fname), PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    let config_file_name = guc_tables::vars::ConfigFileName.read().unwrap_or_default();
    if let Err(e) = std::fs::metadata(&config_file_name) {
        write_stderr(&format!(
            "{progname}: could not access the server configuration file \"{config_file_name}\": {e}\n"
        ));
        return Ok(false);
    }

    // First pass: only data_directory is retained until DataDir is known
    // (process_config_file_internal owns that arm).
    guc_file::ProcessConfigFile(PGC_POSTMASTER)?;

    match nonempty(guc_tables::vars::data_directory.read()) {
        Some(dd) => miscinit::SetDataDir(&dd),
        None => match configdir.as_deref() {
            Some(dir) => miscinit::SetDataDir(dir),
            None => {
                write_stderr(&format!(
                    "{progname} does not know where to find the database system data.\n\
                     This can be specified as \"data_directory\" in \"{config_file_name}\", \
                     or by the -D invocation option, or by the PGDATA environment variable.\n"
                ));
                return Ok(false);
            }
        },
    }
    let data_dir = init_small::globals::DataDir()
        .expect("SelectConfigFiles: SetDataDir published DataDir")
        .to_string();
    crate::SetConfigOption("data_directory", Some(&data_dir), PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    // Second pass picks up postgresql.auto.conf now that DataDir is known.
    guc_file::ProcessConfigFile(PGC_POSTMASTER)?;

    pg_timezone_abbrev_initialize()?;

    let hba = match nonempty(guc_tables::vars::HbaFileName.read()) {
        Some(explicit) => make_absolute_path(&explicit),
        None => match configdir.as_deref() {
            Some(dir) => format!("{dir}/{HBA_FILENAME}"),
            None => {
                write_stderr(&format!(
                    "{progname} does not know where to find the \"hba\" configuration file.\n\
                     This can be specified as \"hba_file\" in \"{config_file_name}\", \
                     or by the -D invocation option, or by the PGDATA environment variable.\n"
                ));
                return Ok(false);
            }
        },
    };
    crate::SetConfigOption("hba_file", Some(&hba), PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    let ident = match nonempty(guc_tables::vars::IdentFileName.read()) {
        Some(explicit) => make_absolute_path(&explicit),
        None => match configdir.as_deref() {
            Some(dir) => format!("{dir}/{IDENT_FILENAME}"),
            None => {
                write_stderr(&format!(
                    "{progname} does not know where to find the \"ident\" configuration file.\n\
                     This can be specified as \"ident_file\" in \"{config_file_name}\", \
                     or by the -D invocation option, or by the PGDATA environment variable.\n"
                ));
                return Ok(false);
            }
        },
    };
    crate::SetConfigOption("ident_file", Some(&ident), PGC_POSTMASTER, PGC_S_OVERRIDE)?;

    Ok(true)
}
