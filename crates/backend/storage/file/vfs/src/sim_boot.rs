//! Sim-boot asset composition — the provider-selection seam's mechanism half
//! (dst-p3-scheduler §9.3; COMPOSE FINDING 1 resolution, option (a)).
//!
//! Under `--cfg pgrust_sim`, `ActiveVfs = SimVfs` for the WHOLE binary (the
//! P1 §1.2 zero-cost law: one compile-time provider per cfg, no runtime
//! knob). A whole-server boot therefore needs the sim namespace to be
//! SELF-CONTAINED: this module pre-populates SimVfs with the read-only,
//! host-born assets bootstrap reads through the vfs — the initdb'd datadir
//! snapshot plus the share trees (timezone, timezonesets, tsearch) named by
//! the checked-in manifest `crates/backend/storage/file/vfs/src/asset-manifest.toml` — before any secondary
//! thread performs a vfs op and before any fault plan installs. The ingest
//! precedes the schedule by construction and emits no events.
//!
//! Determinism accounting (§9.3 item 3): the ingest digests every entry —
//! `(root_tag, root-relative path, kind, len, content_fnv)` tuples, sorted —
//! into `asset_hash`, reported on the `SIM-ASSETS` boot line. Two runs on
//! one snapshot print the same line; host drift (a tzdata update, a
//! re-minted datadir) changes the hash and fails identity comparison loudly
//! as ASSET-DRIFT, never as a silent schedule divergence. Root-RELATIVE
//! paths keep the hash a content identity, independent of where the harness
//! scratch directory happens to live; the synthetic ancestor directories the
//! ingest mints to scaffold each root's absolute path are namespace plumbing
//! and are deliberately NOT digest-covered.
//!
//! Host reads here use std::fs (this IS the declared host boundary — the
//! one place the sim world is seeded from; cp -RL semantics: symlinks are
//! dereferenced, entries walked in sorted order). The vfs-EXCLUDED
//! guc_file/conffiles host stdio reads ride the same snapshot directory, so
//! their content is digest-covered too.

use std::ffi::CString;
use std::path::{Component, Path, PathBuf};

use crate::sim::SimVfs;

const MANIFEST: &str = include_str!("asset-manifest.toml");
const MANIFEST_VERSION: u32 = 1;

/// Symlink-dereferencing walks refuse to recurse forever (cp -RL trap).
const MAX_WALK_DEPTH: usize = 40;

// ---------------------------------------------------------------------------
// FNV-1a 64 — the digest primitive. Deliberately dependency-free and stable;
// this is a drift detector, not a cryptographic commitment.
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

struct Manifest {
    share_roots: Vec<String>,
}

/// Minimal parser for the checked-in manifest: `version = N` plus one
/// `roots = [ "..." , ... ]` array. The manifest and this parser grow in
/// lockstep; anything unrecognized outside comments/section headers errors.
fn parse_manifest(text: &str) -> Result<Manifest, String> {
    let mut version: Option<u32> = None;
    let mut share_roots: Vec<String> = Vec::new();
    let mut in_roots = false;
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if in_roots {
            if line.starts_with(']') {
                in_roots = false;
                continue;
            }
            let item = line.trim_end_matches(',').trim();
            let Some(name) = item.strip_prefix('"').and_then(|s| s.strip_suffix('"')) else {
                return Err(format!("manifest line {}: expected \"root\" entry", lineno + 1));
            };
            if name.is_empty() || name.starts_with('/') || name.contains("..") {
                return Err(format!("manifest line {}: invalid root {name:?}", lineno + 1));
            }
            share_roots.push(name.to_string());
            continue;
        }
        if let Some(v) = line.strip_prefix("version") {
            let v = v.trim_start().strip_prefix('=').map(str::trim).unwrap_or("");
            version = Some(
                v.parse::<u32>()
                    .map_err(|_| format!("manifest line {}: bad version", lineno + 1))?,
            );
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            continue; // section header; roots key carries the meaning
        }
        if let Some(rest) = line.strip_prefix("roots") {
            let rest = rest.trim_start();
            if rest.starts_with("= [") {
                in_roots = true;
                continue;
            }
            return Err(format!("manifest line {}: expected roots = [", lineno + 1));
        }
        return Err(format!("manifest line {}: unrecognized: {line:?}", lineno + 1));
    }
    if version != Some(MANIFEST_VERSION) {
        return Err(format!(
            "manifest version {version:?} != supported {MANIFEST_VERSION}"
        ));
    }
    if in_roots {
        return Err("manifest: unterminated roots array".into());
    }
    Ok(Manifest { share_roots })
}

// ---------------------------------------------------------------------------
// Ingest walk
// ---------------------------------------------------------------------------

/// One digest tuple: (root tag, root-relative path, kind, len, content fnv).
struct EntryRecord {
    root_tag: &'static str,
    rel: String,
    kind: u8, // b'd' | b'f'
    len: u64,
    content: u64,
}

#[derive(Default)]
struct Tally {
    files: u64,
    dirs: u64,
    bytes: u64,
    records: Vec<EntryRecord>,
}

/// Lexical normalization mirroring the sim namespace's `norm_path` (relative
/// resolution aside: inputs here are absolute) — used for under-root
/// comparisons, never for host IO.
fn lex_norm(p: &str) -> PathBuf {
    let mut out = PathBuf::from("/");
    for comp in Path::new(p).components() {
        match comp {
            Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(name) => out.push(name),
        }
    }
    out
}

fn cpath(p: &str) -> Result<CString, String> {
    CString::new(p).map_err(|_| format!("path contains NUL: {p:?}"))
}

fn host_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o7777
}

/// mkdir -p the (already-normalized) ancestors of `root` in the sim
/// namespace. Scaffolding only: mode 0755, not digest-covered.
fn ingest_scaffold(root_abs: &str) -> Result<(), String> {
    let norm = lex_norm(root_abs);
    let mut prefix = PathBuf::from("/");
    let Some(parent) = norm.parent() else {
        return Ok(()); // root "/" needs no scaffold
    };
    for comp in parent.components() {
        if let Component::Normal(name) = comp {
            prefix.push(name);
            let c = cpath(prefix.to_str().expect("normalized utf8 path"))?;
            SimVfs::ingest_dir(&c, 0o755)
                .map_err(|e| format!("scaffold mkdir {}: errno {e}", prefix.display()))?;
        }
    }
    Ok(())
}

/// Import one host tree into the sim namespace at the SAME (lexically
/// normalized) path. Sorted-order walk, symlinks dereferenced (the cp -RL
/// shape from the wasm/pack lanes). Records every entry into the digest.
fn ingest_tree(
    host_root: &Path,
    sim_root: &str,
    root_tag: &'static str,
    tally: &mut Tally,
) -> Result<(), String> {
    ingest_scaffold(sim_root)?;
    let meta = std::fs::metadata(host_root)
        .map_err(|e| format!("{root_tag} root {}: {e}", host_root.display()))?;
    if !meta.is_dir() {
        return Err(format!("{root_tag} root {} is not a directory", host_root.display()));
    }
    let root_c = cpath(sim_root)?;
    SimVfs::ingest_dir(&root_c, host_mode(&meta))
        .map_err(|e| format!("ingest mkdir {sim_root}: errno {e}"))?;
    tally.dirs += 1;
    tally.records.push(EntryRecord {
        root_tag,
        rel: String::new(),
        kind: b'd',
        len: 0,
        content: 0,
    });
    walk(host_root, sim_root, "", root_tag, tally, 0)
}

fn walk(
    host_dir: &Path,
    sim_root: &str,
    rel_dir: &str,
    root_tag: &'static str,
    tally: &mut Tally,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_WALK_DEPTH {
        return Err(format!(
            "{root_tag}: walk depth > {MAX_WALK_DEPTH} at {} (symlink cycle?)",
            host_dir.display()
        ));
    }
    let mut entries: Vec<_> = std::fs::read_dir(host_dir)
        .map_err(|e| format!("read_dir {}: {e}", host_dir.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("read_dir {}: {e}", host_dir.display()))?;
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e
            .file_name()
            .into_string()
            .map_err(|n| format!("non-UTF-8 entry {n:?} under {}", host_dir.display()))?;
        let host_path = e.path();
        let rel = if rel_dir.is_empty() { name.clone() } else { format!("{rel_dir}/{name}") };
        let sim_path = format!(
            "{}/{name}",
            if sim_root == "/" { "" } else { sim_root }
        );
        // std::fs::metadata follows symlinks: cp -RL.
        let meta = std::fs::metadata(&host_path)
            .map_err(|e| format!("stat {}: {e}", host_path.display()))?;
        if meta.is_dir() {
            let c = cpath(&sim_path)?;
            SimVfs::ingest_dir(&c, host_mode(&meta))
                .map_err(|err| format!("ingest mkdir {sim_path}: errno {err}"))?;
            tally.dirs += 1;
            tally.records.push(EntryRecord {
                root_tag,
                rel: rel.clone(),
                kind: b'd',
                len: 0,
                content: 0,
            });
            walk(&host_path, &sim_path, &rel, root_tag, tally, depth + 1)?;
        } else if meta.is_file() {
            let data = std::fs::read(&host_path)
                .map_err(|e| format!("read {}: {e}", host_path.display()))?;
            let c = cpath(&sim_path)?;
            SimVfs::ingest_file(&c, &data, host_mode(&meta))
                .map_err(|err| format!("ingest file {sim_path}: errno {err}"))?;
            tally.files += 1;
            tally.bytes += data.len() as u64;
            tally.records.push(EntryRecord {
                root_tag,
                rel,
                kind: b'f',
                len: data.len() as u64,
                content: fnv1a(FNV_OFFSET, &data),
            });
        } else {
            return Err(format!(
                "{}: unsupported file type (socket/fifo/device) — not a snapshotable asset",
                host_path.display()
            ));
        }
    }
    Ok(())
}

fn asset_hash(records: &mut Vec<EntryRecord>) -> u64 {
    records.sort_by(|a, b| (a.root_tag, &a.rel).cmp(&(b.root_tag, &b.rel)));
    let mut h = FNV_OFFSET;
    for r in records.iter() {
        h = fnv1a(h, r.root_tag.as_bytes());
        h = fnv1a(h, &[0]);
        h = fnv1a(h, r.rel.as_bytes());
        h = fnv1a(h, &[0, r.kind]);
        h = fnv1a(h, &r.len.to_le_bytes());
        h = fnv1a(h, &r.content.to_le_bytes());
    }
    h
}

// ---------------------------------------------------------------------------
// The boot entry
// ---------------------------------------------------------------------------

/// Compose the whole-server sim namespace (§9.3): install the process-shared
/// universe, ingest the datadir snapshot at its host absolute path (which
/// becomes the sim boot cwd), then the manifest's share roots from
/// PGRUST_PGSHAREDIR (plus PGRUST_TZDIR when it points outside the share
/// dir). Returns the `SIM-ASSETS` report line for the boot log.
///
/// Call exactly once, before the first vfs access to any of these trees and
/// before any secondary thread performs a vfs op — the postmaster calls it
/// before SelectConfigFiles (conf-file GUC validation already scans the
/// timezone tree through the vfs).
pub fn compose_boot_namespace(datadir_abs: &str) -> Result<String, String> {
    if !datadir_abs.starts_with('/') {
        return Err(format!("datadir must be absolute, got {datadir_abs:?}"));
    }
    let manifest = parse_manifest(MANIFEST)?;

    SimVfs::install_process_universe();
    let mut tally = Tally::default();

    // 1. The world root: the initdb'd datadir snapshot.
    ingest_tree(Path::new(datadir_abs), datadir_abs, "datadir", &mut tally)?;
    SimVfs::set_boot_cwd(&lex_norm(datadir_abs));

    // 2. Share asset roots (manifest-declared, harness-located).
    let share = std::env::var("PGRUST_PGSHAREDIR").map_err(|_| {
        "PGRUST_PGSHAREDIR not set: a whole-server sim boot needs the share \
         asset trees (crates/backend/storage/file/vfs/src/asset-manifest.toml [share] roots)"
            .to_string()
    })?;
    if !share.starts_with('/') {
        return Err(format!("PGRUST_PGSHAREDIR must be absolute, got {share:?}"));
    }
    let mut roots = 1u64; // datadir
    for root in &manifest.share_roots {
        let host = format!("{share}/{root}");
        if !Path::new(&host).exists() {
            // Not every install ships every share root (tsearch_data may be
            // absent on minimal snapshots); absence is part of the world
            // identity via the missing digest records, not an error.
            continue;
        }
        ingest_tree(Path::new(&host), &host, share_tag(root), &mut tally)?;
        roots += 1;
    }

    // 3. PGRUST_TZDIR outside the share dir (harness override shape).
    if let Ok(tz) = std::env::var("PGRUST_TZDIR") {
        if tz.starts_with('/')
            && !lex_norm(&tz).starts_with(lex_norm(&share))
            && Path::new(&tz).exists()
        {
            ingest_tree(Path::new(&tz), &tz, "tzdir", &mut tally)?;
            roots += 1;
        }
    }

    let hash = asset_hash(&mut tally.records);
    Ok(format!(
        "SIM-ASSETS roots={roots} dirs={} files={} bytes={} asset_hash={hash:#018x}",
        tally.dirs, tally.files, tally.bytes
    ))
}

/// Stable per-root digest tags for the manifest share roots.
fn share_tag(root: &str) -> &'static str {
    match root {
        "timezone" => "share/timezone",
        "timezonesets" => "share/timezonesets",
        "tsearch_data" => "share/tsearch_data",
        _ => "share/other",
    }
}
