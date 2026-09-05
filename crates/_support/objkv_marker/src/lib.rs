//! The one local file saying where this cluster's truth lives. It matters
//! *when* it can be read: before any catalog, the only moment "how is pg_class
//! stored?" is answerable without asking pg_class.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

pub const FILENAME: &str = "objkv_catalogs";

/// The `relam` a nailed catalog carries in bucket mode. Not any pg_am oid:
/// that is per-database, while `formrdesc` has one answer for all of them.
pub const NAILED_AM: u32 = 9050;

/// Nothing but its presence matters; the bucket holds everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Marker;

static MARKER: OnceLock<Option<Marker>> = OnceLock::new();

/// Set when this process performed the flip, having booted before it. The file
/// is read once per process, so such a server goes on writing catalog rows to
/// files the bucket has superseded, with nothing saying so. Rebuilding its
/// view is not small, so writes are refused until a restart instead.
static FLIPPED_MID_LIFE: AtomicBool = AtomicBool::new(false);

pub fn note_flip() {
    FLIPPED_MID_LIFE.store(true, Ordering::Relaxed);
}

/// The flip did not happen after all: the marker write failed, so the local
/// catalogs are still the truth and writing them is fine again.
pub fn clear_flip() {
    FLIPPED_MID_LIFE.store(false, Ordering::Relaxed);
}

pub fn flipped_needs_restart() -> bool {
    // The flag alone, deliberately. `catalogs_in_bucket()` reads the marker
    // file once and caches it: if the flip's own `note_flip` is the first
    // access, the cache holds the marker that was just written, the gate reads
    // "already in bucket mode", and this process goes on writing catalog rows
    // to local files the bucket has replaced. Only a process that booted
    // before the flip ever sets the flag, so it is the whole question.
    FLIPPED_MID_LIFE.load(Ordering::Relaxed)
}

/// Reads the marker now, so a later flip cannot change what this process
/// thinks it booted with. Called before the marker file is written.
pub fn prime() {
    let _ = marker();
}

pub fn marker() -> Option<Marker> {
    *MARKER.get_or_init(|| {
        let dir = ::init_small::globals::DataDir()?;
        let text = std::fs::read_to_string(std::path::Path::new(&dir).join(FILENAME)).ok()?;
        parse(&text)
    })
}

pub fn catalogs_in_bucket() -> bool {
    marker().is_some()
}

pub fn catalog_am() -> u32 {
    if catalogs_in_bucket() { NAILED_AM } else { 0 }
}

pub fn body() -> String {
    "v1\ncatalogs=bucket\n".to_string()
}

/// Anything unrecognised reads as "no marker": a file we cannot understand is
/// not permission to read catalogs elsewhere.
fn parse(text: &str) -> Option<Marker> {
    let mut lines = text.lines();
    if lines.next()?.trim() != "v1" {
        return None;
    }
    let mut in_bucket = false;
    for line in lines {
        let (k, v) = line.split_once('=')?;
        match k.trim() {
            "catalogs" => in_bucket = v.trim() == "bucket",
            _ => return None,
        }
    }
    if !in_bucket {
        return None;
    }
    Some(Marker)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        assert_eq!(parse(&body()), Some(Marker));
    }

    #[test]
    fn anything_unrecognised_reads_as_no_marker() {
        for bad in [
            "",
            "v2\ncatalogs=bucket\n",
            "v1\ncatalogs=disk\n",
            "v1\ncatalogs=bucket\noid_high=7\n",
            "v1\nsomething=else\n",
        ] {
            assert_eq!(parse(bad), None, "{bad:?} must not parse");
        }
    }
}
