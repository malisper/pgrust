//! backup_label / tablespace_map readers (xlogrecovery.c). The fscanf-based
//! C parsers are matched field-for-field; format deviations are FATAL like C.

use elog::{elog, ereport};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgResult, DEBUG1, FATAL};

use crate::{data_path, loc, InvalidXLogRecPtr, BACKUP_LABEL_FILE, TABLESPACE_MAP};

pub(crate) struct BackupLabel {
    pub checkpoint_loc: XLogRecPtr,
    pub backup_label_tli: TimeLineID,
    pub backup_end_required: bool,
    pub backup_from_standby: bool,
    pub redo_start_lsn: XLogRecPtr,
    pub redo_start_tli: TimeLineID,
}

fn invalid_data<T>(file: &str, func: &'static str) -> PgResult<T> {
    ereport(FATAL)
        .errmsg(format!("invalid data in file \"{file}\""))
        .finish(loc(func))?;
    unreachable!()
}

// "START WAL LOCATION: %X/%X (file %08X<rest>)\n" — the leading TLI of the
// segment name is the %08X; C also captures the full segment name.
fn parse_lsn_pair(s: &str) -> Option<(u64, &str)> {
    let (hi, rest) = s.split_once('/')?;
    let end = rest
        .find(|c: char| !c.is_ascii_hexdigit())
        .unwrap_or(rest.len());
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(&rest[..end], 16).ok()?;
    Some(((hi << 32) | lo, &rest[end..]))
}

pub(crate) fn read_backup_label() -> PgResult<Option<BackupLabel>> {
    let path = data_path(BACKUP_LABEL_FILE);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            { ereport(FATAL)
                .errmsg(format!("could not read file \"{BACKUP_LABEL_FILE}\": {e}"))
                .finish(loc("read_backup_label"))?; unreachable!() }
        }
    };
    let mut out = BackupLabel {
        checkpoint_loc: InvalidXLogRecPtr,
        backup_label_tli: 0,
        backup_end_required: false,
        backup_from_standby: false,
        redo_start_lsn: InvalidXLogRecPtr,
        redo_start_tli: 0,
    };
    let mut lines = content.lines();

    let l1 = lines.next().unwrap_or("");
    let Some(rest) = l1.strip_prefix("START WAL LOCATION: ") else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    let Some((lsn, tail)) = parse_lsn_pair(rest) else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    let Some(fname) = tail.trim_start().strip_prefix("(file ") else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    if fname.len() < 8 {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    }
    let Ok(tli_from_walseg) = u32::from_str_radix(&fname[..8], 16) else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    out.redo_start_lsn = lsn;
    out.redo_start_tli = tli_from_walseg;
    out.backup_label_tli = tli_from_walseg;

    let l2 = lines.next().unwrap_or("");
    let Some(rest) = l2.strip_prefix("CHECKPOINT LOCATION: ") else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    let Some((cp, _)) = parse_lsn_pair(rest) else {
        return invalid_data(BACKUP_LABEL_FILE, "read_backup_label");
    };
    out.checkpoint_loc = cp;

    for line in lines {
        if let Some(v) = line.strip_prefix("BACKUP METHOD: ") {
            if v.trim() == "streamed" {
                out.backup_end_required = true;
            }
        } else if let Some(v) = line.strip_prefix("BACKUP FROM: ") {
            if v.trim() == "standby" {
                out.backup_from_standby = true;
            }
        } else if let Some(v) = line.strip_prefix("START TIMELINE: ") {
            if let Ok(tli_from_file) = v.trim().parse::<u32>() {
                if tli_from_walseg != tli_from_file {
                    { ereport(FATAL)
                        .errmsg(format!("invalid data in file \"{BACKUP_LABEL_FILE}\""))
                        .errdetail(format!(
                            "Timeline ID parsed is {tli_from_file}, but expected {tli_from_walseg}."
                        ))
                        .finish(loc("read_backup_label"))?; unreachable!() }
                }
                let _ = elog(
                    DEBUG1,
                    format!("backup timeline {tli_from_file} in file \"{BACKUP_LABEL_FILE}\""),
                );
            }
        } else if line.starts_with("INCREMENTAL FROM LSN: ") {
            ereport(FATAL)
                .errmsg("this is an incremental backup, not a data directory")
                .errhint("Use pg_combinebackup to reconstruct a valid data directory.")
                .finish(loc("read_backup_label"))?;
        unreachable!()
        }
    }
    Ok(Some(out))
}

pub(crate) struct TablespaceInfo {
    pub oid: u32,
    pub path: String,
}

pub(crate) fn read_tablespace_map() -> PgResult<Option<Vec<TablespaceInfo>>> {
    let path = data_path(TABLESPACE_MAP);
    let content = match std::fs::read(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            { ereport(FATAL)
                .errmsg(format!("could not read file \"{TABLESPACE_MAP}\": {e}"))
                .finish(loc("read_tablespace_map"))?; unreachable!() }
        }
    };

    let mut tablespaces = Vec::new();
    let mut buf = Vec::new();
    let mut was_backslash = false;
    for &ch in &content {
        if !was_backslash && (ch == b'\n' || ch == b'\r') {
            if buf.is_empty() {
                continue;
            }
            let line = std::mem::take(&mut buf);
            let Some(sp) = line.iter().position(|&b| b == b' ') else {
                return invalid_data(TABLESPACE_MAP, "read_tablespace_map");
            };
            if sp < 1 || sp >= line.len() - 1 {
                return invalid_data(TABLESPACE_MAP, "read_tablespace_map");
            }
            let oid_str = std::str::from_utf8(&line[..sp]).unwrap_or("");
            let Ok(oid) = oid_str.parse::<u32>() else {
                return invalid_data(TABLESPACE_MAP, "read_tablespace_map");
            };
            tablespaces.push(TablespaceInfo {
                oid,
                path: String::from_utf8_lossy(&line[sp + 1..]).into_owned(),
            });
        } else if !was_backslash && ch == b'\\' {
            was_backslash = true;
        } else {
            buf.push(ch);
            was_backslash = false;
        }
    }
    if !buf.is_empty() || was_backslash {
        return invalid_data(TABLESPACE_MAP, "read_tablespace_map");
    }
    Ok(Some(tablespaces))
}
