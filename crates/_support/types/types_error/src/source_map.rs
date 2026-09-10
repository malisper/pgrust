//! Rust source path -> C source basename for the wire F field.
//!
//! C's ereport/elog capture `__FILE__` at the report site and the protocol
//! carries the basename as the F error field; clients pin it (ruby-pg expects
//! `parse_relation.c` for an undefined table, pgcli expects `scan.l` for a
//! syntax error). Ported sites that name their C file/line explicitly are
//! untouched; every other site is captured by `#[track_caller]` as a Rust path
//! (`crates/backend/parser/parse_relation/src/lib.rs`), which must never reach
//! the wire. Each crate is the port of a specific C unit, so the crate
//! directory maps to a C basename: `<crate>.c` unless the generated
//! `OVERRIDES` table (from CATALOG.tsv, `scripts/error-source-map.py`) says
//! otherwise. Rust line numbers have no C counterpart, so a mapped location
//! carries line 0 — the repo-wide "unknown, greppable" convention — rather
//! than a plausible-looking wrong number.

use alloc::string::String;

#[path = "source_map_table.rs"]
mod table;

/// Whether a captured filename is a Rust source path (needs mapping).
#[inline]
pub fn is_rust_source_path(filename: &str) -> bool {
    filename.ends_with(".rs")
}

/// The crate directory component of a Rust source path: the component
/// before the last `/src/` (a `_seams` shim reports as its host crate). A
/// path without `/src/` falls back to its parent directory, then to the
/// file stem.
pub fn crate_dir_of(path: &str) -> &str {
    let path = path.trim_end_matches('/');
    let dir = match path.rfind("/src/") {
        Some(pos) => {
            let head = &path[..pos];
            head.rsplit(['/', '\\']).next().unwrap_or(head)
        }
        None => {
            let mut parts = path.rsplit(['/', '\\']);
            let file = parts.next().unwrap_or(path);
            match parts.next() {
                Some(parent) if !parent.is_empty() => parent,
                _ => file.strip_suffix(".rs").unwrap_or(file),
            }
        }
    };
    dir.strip_suffix("_seams").unwrap_or(dir)
}

/// C source basename for a crate directory name.
pub fn c_basename_for_crate(crate_dir: &str) -> String {
    match table::OVERRIDES.binary_search_by(|(k, _)| (*k).cmp(crate_dir)) {
        Ok(i) => String::from(table::OVERRIDES[i].1),
        Err(_) => alloc::format!("{crate_dir}.c"),
    }
}

/// Map a Rust source path to the C basename of the unit it ports. Returns
/// `None` for anything that is not a Rust path (already a C name).
pub fn c_basename_for_rust_path(path: &str) -> Option<String> {
    if !is_rust_source_path(path) {
        return None;
    }
    Some(c_basename_for_crate(crate_dir_of(path)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_sorted_unique_and_non_default() {
        for w in table::OVERRIDES.windows(2) {
            assert!(w[0].0 < w[1].0, "OVERRIDES not sorted/unique at {:?}", w);
        }
        for (k, v) in table::OVERRIDES {
            assert_ne!(*v, alloc::format!("{k}.c"), "redundant override for {k}");
            assert!(!k.is_empty() && !v.is_empty());
        }
    }

    #[test]
    fn crate_dir_extraction() {
        assert_eq!(
            crate_dir_of("crates/backend/parser/parse_relation/src/lib.rs"),
            "parse_relation"
        );
        assert_eq!(
            crate_dir_of("crates/backend/access/nbtree/nbtree/src/insert.rs"),
            "nbtree"
        );
        assert_eq!(
            crate_dir_of("/abs/path/crates/backend/tcop/postgres/src/lib.rs"),
            "postgres"
        );
        assert_eq!(
            crate_dir_of("crates/backend/parser/parse_relation_seams/src/lib.rs"),
            "parse_relation"
        );
        assert_eq!(
            crate_dir_of("crates/backend/utils/adt/int/src/builtins/mod.rs"),
            "int"
        );
        assert_eq!(crate_dir_of("foo/bar.rs"), "foo");
        assert_eq!(crate_dir_of("bar.rs"), "bar");
    }

    #[test]
    fn rust_paths_map_to_c_basenames() {
        assert_eq!(
            c_basename_for_rust_path("crates/backend/parser/parse_relation/src/lib.rs").as_deref(),
            Some("parse_relation.c")
        );
        assert_eq!(
            c_basename_for_rust_path("crates/backend/parser/gram_core/src/parse.rs").as_deref(),
            Some("gram.y")
        );
        assert_eq!(
            c_basename_for_rust_path("crates/backend/executor/nodemodifytable/src/lib.rs")
                .as_deref(),
            Some("nodeModifyTable.c")
        );
        assert_eq!(
            c_basename_for_rust_path("crates/backend/utils/adt/int/src/lib.rs").as_deref(),
            Some("int.c")
        );
        // Unknown crates still never leak a path.
        let mapped = c_basename_for_rust_path("crates/x/y/brand_new_crate/src/lib.rs").unwrap();
        assert_eq!(mapped, "brand_new_crate.c");
        assert!(!mapped.contains('/'));
        // C names pass through untouched.
        assert_eq!(c_basename_for_rust_path("postgres.c"), None);
        assert_eq!(c_basename_for_rust_path("scan.l"), None);
    }
}
