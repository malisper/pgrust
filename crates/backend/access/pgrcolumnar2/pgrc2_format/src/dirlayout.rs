//! Directory layout + file naming (spec §12; ruling O-7): parts are real
//! files in a per-table directory, a SIBLING of where the relation's main
//! fork would live (tablespace semantics preserved). Lifecycle wiring
//! (pendingDeletes at CREATE/DROP/abort) is M3-H's; the names and parsers
//! are frozen here. Pure string functions — no I/O in this crate.

/// The per-table directory name beside the main-fork path.
pub fn table_dir_name(relfilenumber: u64) -> String {
    format!("pgrc2_{relfilenumber}")
}

/// `part-<part_no>.pgrc2`
pub fn part_file_name(part_no: u32) -> String {
    format!("part-{part_no}.pgrc2")
}

/// `manifest-<gen>.pgrc2m`
pub fn manifest_file_name(gen: u64) -> String {
    format!("manifest-{gen}.pgrc2m")
}

/// The commit pointer (spec §13.2).
pub const CURRENT_FILE_NAME: &str = "CURRENT";
/// The commit pointer's rename source.
pub const CURRENT_TMP_FILE_NAME: &str = "CURRENT.tmp";

/// `part-<part_no>-<tag>-<gen>.pgrc2s`
pub fn sidecar_file_name(part_no: u32, kind: crate::sidecar::SidecarKind, gen: u64) -> String {
    format!("part-{part_no}-{}-{gen}.pgrc2s", kind.tag())
}

/// `tmp-<fxid>-<seq>.pgrc2t` — writer scratch; abort/crash cleanup scans
/// this prefix (spec §13.3 dead-band discipline).
pub fn temp_file_name(fxid: u64, seq: u32) -> String {
    format!("tmp-{fxid}-{seq}.pgrc2t")
}

/// Parse `part-<n>.pgrc2` → part_no.
pub fn parse_part_file_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("part-")?.strip_suffix(".pgrc2")?;
    parse_decimal_u64(rest).and_then(|v| u32::try_from(v).ok())
}

/// Parse `manifest-<gen>.pgrc2m` → gen.
pub fn parse_manifest_file_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("manifest-")?.strip_suffix(".pgrc2m")?;
    parse_decimal_u64(rest)
}

/// Parse `part-<n>-<tag>-<gen>.pgrc2s` → (part_no, kind, gen).
pub fn parse_sidecar_file_name(name: &str) -> Option<(u32, crate::sidecar::SidecarKind, u64)> {
    let rest = name.strip_prefix("part-")?.strip_suffix(".pgrc2s")?;
    let mut it = rest.split('-');
    let part_no = parse_decimal_u64(it.next()?).and_then(|v| u32::try_from(v).ok())?;
    let kind = match it.next()? {
        "dv" => crate::sidecar::SidecarKind::Dv,
        "pcache" => crate::sidecar::SidecarKind::PredicateCache,
        "memo" => crate::sidecar::SidecarKind::Memo,
        "trgm" => crate::sidecar::SidecarKind::Trgm,
        "stats" => crate::sidecar::SidecarKind::Stats,
        _ => return None,
    };
    let gen = parse_decimal_u64(it.next()?)?;
    if it.next().is_some() {
        return None;
    }
    Some((part_no, kind, gen))
}

/// Is this a writer temp file (cleanup-scan predicate)?
pub fn is_temp_file_name(name: &str) -> bool {
    name.starts_with("tmp-") && name.ends_with(".pgrc2t")
}

/// Strict decimal parse: no signs, no leading zeros (except "0" itself) —
/// names are canonical, so parse(format(x)) == x and nothing else parses.
fn parse_decimal_u64(s: &str) -> Option<u64> {
    if s.is_empty() || (s.len() > 1 && s.starts_with('0')) {
        return None;
    }
    if !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}
