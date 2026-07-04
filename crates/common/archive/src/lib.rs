//! src/common/archive.c

#![allow(non_snake_case)]

use types_error::PgResult;

// make_native_path (port/path.c) is a Windows-only backslash rewrite; identity here.
pub fn BuildRestoreCommand(
    restore_command: &str,
    xlogpath: &str,
    xlogfname: &str,
    last_restart_point_fname: &str,
) -> PgResult<String> {
    percentrepl::replace_percent_placeholders(
        restore_command,
        "restore_command",
        &[
            ('f', Some(xlogfname)),
            ('r', Some(last_restart_point_fname)),
            ('p', Some(xlogpath)),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_all_three_placeholders() {
        let cmd = BuildRestoreCommand(
            "cp %p /archive/%f (restart %r)",
            "pg_wal/RECOVERYXLOG",
            "000000010000000000000001",
            "000000010000000000000000",
        )
        .unwrap();
        assert_eq!(
            cmd,
            "cp pg_wal/RECOVERYXLOG /archive/000000010000000000000001 (restart 000000010000000000000000)"
        );
    }

    #[test]
    fn unsupported_placeholder_errors() {
        assert!(BuildRestoreCommand("cp %z", "p", "f", "r").is_err());
    }
}
