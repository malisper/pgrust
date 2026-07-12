//! Session-local Part cache: relation OID -> parsed Part (footer, granule
//! directory, block zone maps, blooms — the whole mmap'd part), reused
//! across statements (docs/design/cbstore-part-cache.md).
//!
//! STALENESS RULE (the design note's §3, kept in sync verbatim): a cached
//! parsed footer for relation R may be reused by a statement iff
//!   (a) no relcache invalidation for R has been delivered since the entry
//!       was built (the sinval-driven callback below drops the entry when
//!       one is), AND
//!   (b) the validity probe at lookup observes, in order: the relation's
//!       main-fork path unchanged; the part's column count unchanged;
//!       seg0's (st_dev, st_ino, st_size) equal to the values captured
//!       immediately BEFORE the cached footer was read+CRC-validated; and
//!       the header's footer_off word (read from the live shared mapping)
//!       equal to the offset the cached footer was parsed from.
//! On any mismatch the entry is discarded and the footer is re-read,
//! re-parsed and CRC-revalidated from the current header pointer.
//! Reuse under (a)+(b) is snapshot-safe in both directions:
//!   lower bound — footer publish is fsync-ordered before the appending
//!   transaction's commit, so every row group whose xmin any statement
//!   snapshot can see is contained in the footer the current header points
//!   at; a matching probe proves that footer IS the cached one, hence no
//!   snapshot-visible row group is missing;
//!   upper bound — the footer is a snapshot-agnostic superset: row groups
//!   newer than the snapshot are excluded per-RG at scan time by the
//!   xmin-vs-snapshot visibility gate (rg_visible / rg_wholly_visible),
//!   never answered from cached metadata.

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::rc::Rc;
use std::sync::OnceLock;

use ::datum::Datum;
use ::types_core::{InvalidOid, Oid};
use ::types_error::PgResult;

use crate::format::get_u64;
use crate::reader::Part;

struct Entry {
    part: Rc<Part>,
    path: String,
    ncols: usize,
    dev: u64,
    ino: u64,
    len: u64,
    footer_off: u64,
}

thread_local! {
    static CACHE: RefCell<Option<HashMap<Oid, Entry>>> = const { RefCell::new(None) };
}

// Was the hidden GUC `cbstore_part_cache` on the old branch; re-homed to an
// env off-switch (default ON) — the lane-v2 line adds no SQL GUCs (see
// costsize::gucs' cbstore knob note). `PGRUST_CBSTORE_PART_CACHE=0`/`off`
// disables (byte-identical A/B gate).
fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_PART_CACHE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

fn debug_log() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARTCACHE_DEBUG").is_ok_and(|v| v == "1"))
}

fn invalidate_callback(_arg: Datum, relid: Oid) {
    CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            let before = map.len();
            if relid == InvalidOid {
                map.clear();
            } else {
                map.remove(&relid);
            }
            if debug_log() && map.len() != before {
                eprintln!("PARTCACHE|inval relid={relid}");
            }
        }
    });
}

// Callback registration precedes map install: a registration failure must
// not leave a map that caches without invalidation.
fn init() -> PgResult<()> {
    inval::invalidate::CacheRegisterRelcacheCallback(invalidate_callback, Datum::null())?;
    CACHE.with(|cell| *cell.borrow_mut() = Some(HashMap::new()));
    Ok(())
}

fn probe(e: &Entry, path: &str, ncols: usize) -> bool {
    if e.path != path || e.ncols != ncols {
        return false;
    }
    let Ok(md) = std::fs::metadata(path) else { return false };
    if (md.dev(), md.ino(), md.len()) != (e.dev, e.ino, e.len) {
        return false;
    }
    // len matched, so the header page is inside the live mapping.
    get_u64(e.part.bytes(), 16) == e.footer_off
}

/// Cache-or-open the relation's Part. None: no committed footer yet
/// (deliberately uncached — the next COPY changes everything anyway).
pub fn cached_part(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Rc<Part>>> {
    let ncols = crate::writer::coltypes_of(rel)?.len();
    let path = crate::rel_main_path(rel);
    if !enabled() {
        return Ok(Part::open(&path, ncols)?.map(Rc::new));
    }
    if CACHE.with(|cell| cell.borrow().is_none()) {
        init()?;
    }
    let relid = rel.rd_id;
    let hit = CACHE.with(|cell| {
        let b = cell.borrow();
        let map = b.as_ref().expect("part cache initialized");
        map.get(&relid).filter(|e| probe(e, &path, ncols)).map(|e| Rc::clone(&e.part))
    });
    if let Some(part) = hit {
        if debug_log() {
            eprintln!("PARTCACHE|hit relid={relid}");
        }
        return Ok(Some(part));
    }
    // Stat BEFORE the read: a publish racing the build makes the parsed
    // footer newer than the probe values, so the next lookup rebuilds
    // (spurious miss, never a stale hit).
    let md = std::fs::metadata(&path).ok();
    let Some(part) = Part::open(&path, ncols)? else {
        CACHE.with(|cell| {
            if let Some(map) = cell.borrow_mut().as_mut() {
                map.remove(&relid);
            }
        });
        return Ok(None);
    };
    let part = Rc::new(part);
    if let Some(md) = md {
        if debug_log() {
            eprintln!("PARTCACHE|fill relid={relid}");
        }
        CACHE.with(|cell| {
            if let Some(map) = cell.borrow_mut().as_mut() {
                map.insert(
                    relid,
                    Entry {
                        part: Rc::clone(&part),
                        path,
                        ncols,
                        dev: md.dev(),
                        ino: md.ino(),
                        len: md.len(),
                        footer_off: part.footer_off,
                    },
                );
            }
        });
    }
    Ok(Some(part))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_for(path: &str, ncols: usize) -> Entry {
        let md = std::fs::metadata(path).unwrap();
        let part = Part::open(path, ncols).unwrap().unwrap();
        Entry {
            footer_off: part.footer_off,
            part: Rc::new(part),
            path: path.to_string(),
            ncols,
            dev: md.dev(),
            ino: md.ino(),
            len: md.len(),
        }
    }

    #[test]
    fn probe_matches_then_detects_republish_growth_and_recreate() {
        let path = crate::reader::test_part(
            &format!("partcache-probe-{}", std::process::id()),
            &[1000, 2000],
            3,
        );
        let e = entry_for(&path, 3);
        assert!(probe(&e, &path, 3));
        assert!(!probe(&e, &path, 4));
        assert!(!probe(&e, "/nonexistent", 3));

        // Append growth without publish: len probe fails.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0u8; 64]);
        std::fs::write(&path, &bytes).unwrap();
        assert!(!probe(&e, &path, 3));

        // Unlink + recreate byte-identical: inode probe fails.
        let orig = {
            std::fs::remove_file(&path).unwrap();
            bytes.truncate(bytes.len() - 64);
            std::fs::write(&path, &bytes).unwrap();
            entry_for(&path, 3)
        };
        assert!(probe(&orig, &path, 3));
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert!(!probe(&orig, &path, 3));
        std::fs::remove_file(&path).unwrap();
    }
}
