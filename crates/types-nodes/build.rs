//! Central node-tree code generator for `types-nodes` — the idiomatic-Rust
//! analogue of the *central* half of PostgreSQL's
//! `src/backend/nodes/gen_node_support.pl`.
//!
//! `gen_node_support.pl` does two jobs: (1) per-struct copy/equal/out/read
//! support — in this port that is the `#[derive(PgNode)]` proc-macro
//! (`backend-nodes-macros`) — and (2) the *central* artifacts: the single
//! `NodeTag` enumeration and the tag-discriminated dispatch over every node
//! type. THIS generator does job (2): it assembles the unified `Node<'mcx>`
//! enum plus the central `node_tag` / `copy_node_in` / `equal_node` dispatch
//! that matches each variant and delegates to that variant's per-struct
//! `PgNodeCopy`/`PgNodeEqual` impl.
//!
//! The owned-tree model re-homes ALL allocation onto `mcx`: copy is fallible
//! and threads a TARGET context (`copy_node_in(&self, dst) -> PgResult<..>` —
//! the analogue of `copyObject` into `CurrentMemoryContext`), while `equal_node`
//! stays infallible/lifetime-agnostic.
//!
//! # Inputs
//!
//! 1. `nodetags.h` — the authoritative `T_*` tag list (the *same* source
//!    `gen_node_support.pl` drives from). Parsed into a tag -> numeric-value
//!    map; the numeric value is the canonical `NodeTag` discriminant and also
//!    fixes ABI variant order. Located in the sibling `pgrust` PostgreSQL 18.3
//!    checkout.
//! 2. `nodes.list` (next to this build.rs) — the data-driven variant->struct
//!    mapping table over the CONVERTED leaf node families. Growing the enum is
//!    purely appending lines here.
//!
//! # Output
//!
//! `$OUT_DIR/node_tree.rs`, `include!`d by `src/node_tree.rs`. It defines
//! `pub enum Node<'mcx>`, `impl Node { pub fn node_tag(&self) -> NodeTag }`,
//! `fn copy_node_in(&self, Mcx) -> PgResult<Node>` and
//! `fn equal_node(&self, &Node) -> bool`.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|| panic!("build.rs main: CARGO_MANIFEST_DIR is not set")),
    );

    // ---- locate + parse nodetags.h --------------------------------------
    let nodetags_path = find_nodetags_h(&manifest_dir);
    println!("cargo:rerun-if-changed={}", nodetags_path.display());
    let nodetags_src = fs::read_to_string(&nodetags_path)
        .unwrap_or_else(|e| panic!("cannot read nodetags.h at {}: {e}", nodetags_path.display()));
    let tag_values = parse_nodetags(&nodetags_src);
    assert!(
        !tag_values.is_empty(),
        "parsed zero T_* tags from {}",
        nodetags_path.display()
    );

    // ---- read the data-driven variant->struct mapping list --------------
    let list_path = manifest_dir.join("nodes.list");
    println!("cargo:rerun-if-changed={}", list_path.display());
    let list_src = fs::read_to_string(&list_path)
        .unwrap_or_else(|e| panic!("cannot read nodes.list at {}: {e}", list_path.display()));
    let mappings = parse_nodes_list(&list_src);
    assert!(!mappings.is_empty(), "nodes.list contains no mappings");

    // ---- resolve each mapping against nodetags.h, sort by ABI tag order -
    let mut variants: Vec<Variant> = mappings
        .into_iter()
        .map(|m| {
            let value = *tag_values.get(&m.tag).unwrap_or_else(|| {
                panic!(
                    "nodes.list tag `{}` is not present in nodetags.h ({})",
                    m.tag,
                    nodetags_path.display()
                )
            });
            let variant_ident = m
                .tag
                .strip_prefix("T_")
                .unwrap_or_else(|| panic!("tag `{}` does not start with T_", m.tag))
                .to_string();
            Variant {
                variant_ident,
                struct_path: m.struct_path,
                has_lifetime: m.has_lifetime,
                tag: m.tag,
                value,
            }
        })
        .collect();

    // ABI ordering: variant order follows nodetags.h numeric tag value.
    variants.sort_by_key(|v| v.value);

    // ---- emit the generated module --------------------------------------
    let code = emit(&variants);
    let out_path = PathBuf::from(
        env::var_os("OUT_DIR").unwrap_or_else(|| panic!("build.rs main: OUT_DIR is not set")),
    )
    .join("node_tree.rs");
    fs::write(&out_path, code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", out_path.display()));
}

/// A single resolved enum variant.
struct Variant {
    /// Variant name in `enum Node` (the tag with `T_` stripped).
    variant_ident: String,
    /// Fully-qualified path to the wrapped struct.
    struct_path: String,
    /// Whether the wrapped struct is `<'mcx>`-parameterized.
    has_lifetime: bool,
    /// The original `T_*` tag (for the doc reference).
    tag: String,
    /// Numeric NodeTag value from nodetags.h (discriminant + sort key).
    value: u32,
}

/// One line of the data-driven mapping list.
struct Mapping {
    tag: String,
    struct_path: String,
    has_lifetime: bool,
}

/// Locate `nodes/nodetags.h`. Honor an explicit `PGRUST_NODETAGS_H` override,
/// then try a `build-rust` symlink chain (matching src-idiomatic), then the
/// sibling `pgrust` PostgreSQL 18.3 checkout that this fabled worktree is paired
/// with.
fn find_nodetags_h(manifest_dir: &Path) -> PathBuf {
    if let Some(p) = env::var_os("PGRUST_NODETAGS_H") {
        let p = PathBuf::from(p);
        if p.exists() {
            return p;
        }
        panic!(
            "PGRUST_NODETAGS_H is set to {} but that file does not exist",
            p.display()
        );
    }

    // Relative locations to try, walking up from the crate manifest dir. The
    // first is the src-idiomatic `build-rust` symlink convention; the rest reach
    // the sibling `pgrust` checkout (`.../work/pgrust/postgres-18.3/...`).
    const RELS: &[&str] = &[
        "build-rust/src/include/nodes/nodetags.h",
        "pgrust/postgres-18.3/src/backend/nodes/nodetags.h",
        "../pgrust/postgres-18.3/src/backend/nodes/nodetags.h",
    ];
    let mut dir = Some(manifest_dir);
    while let Some(d) = dir {
        for rel in RELS {
            let candidate = d.join(rel);
            if candidate.exists() {
                return candidate;
            }
        }
        dir = d.parent();
    }
    panic!(
        "could not locate nodetags.h (tried {RELS:?}) walking up from {}; \
         set PGRUST_NODETAGS_H to point at it",
        manifest_dir.display()
    );
}

/// Parse `T_Name = N,` lines from nodetags.h into a tag -> value map. Mirrors
/// the way `gen_node_support.pl` itself treats this enumeration as the single
/// source of truth for tags. Lenient about whitespace + the trailing comma.
fn parse_nodetags(src: &str) -> BTreeMap<String, u32> {
    let mut map = BTreeMap::new();
    for raw in src.lines() {
        let line = raw.trim();
        if !line.starts_with("T_") {
            continue;
        }
        let Some((name, rest)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() || !name.starts_with("T_") {
            continue;
        }
        let value_str = rest.trim().trim_end_matches(',').trim();
        let value: u32 = value_str
            .parse()
            .unwrap_or_else(|_| panic!("could not parse NodeTag value from line: `{raw}`"));
        if let Some(prev) = map.insert(name.to_string(), value) {
            panic!("duplicate tag `{name}` in nodetags.h (values {prev} and {value})");
        }
    }
    map
}

/// Parse the data-driven `nodes.list`: `T_tag  struct::Path  (mcx|-)` per
/// non-comment, non-blank line.
fn parse_nodes_list(src: &str) -> Vec<Mapping> {
    let mut out = Vec::new();
    for raw in src.lines() {
        let line = match raw.split_once('#') {
            Some((before, _)) => before,
            None => raw,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let tag = parts
            .next()
            .unwrap_or_else(|| panic!("malformed nodes.list line: `{raw}`"))
            .to_string();
        let struct_path = parts
            .next()
            .unwrap_or_else(|| panic!("nodes.list line missing struct path: `{raw}`"))
            .to_string();
        let lifetime = parts
            .next()
            .unwrap_or_else(|| panic!("nodes.list line missing lifetime column (mcx|-): `{raw}`"));
        let has_lifetime = match lifetime {
            "mcx" => true,
            "-" => false,
            other => panic!("nodes.list lifetime column must be `mcx` or `-`, got `{other}`: `{raw}`"),
        };
        assert!(
            parts.next().is_none(),
            "nodes.list line has extra columns: `{raw}`"
        );
        out.push(Mapping {
            tag,
            struct_path,
            has_lifetime,
        });
    }
    out
}

/// Emit the generated `node_tree.rs` source text.
fn emit(variants: &[Variant]) -> String {
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs from nodetags.h + nodes.list.\n\
         // DO NOT EDIT. Regenerated on every build.\n\n",
    );

    // ---- the unified Node enum (ABI tag order) --------------------------
    s.push_str(
        "/// The unified, tag-discriminated leaf node tree — the idiomatic\n\
         /// owned-tree analogue of C's `NodeTag`-tagged `Node *`, re-homed onto\n\
         /// `mcx`. One variant per CONVERTED leaf node family, in `nodetags.h`\n\
         /// numeric-tag order (ABI ordering). A PARTIAL enum over the converted\n\
         /// subset (it grows as more node families convert). The `'mcx` lifetime\n\
         /// is the allocator lifetime of the context the node tree lives in.\n\
         /// Generated.\n",
    );
    s.push_str("#[derive(Debug)]\n#[non_exhaustive]\npub enum Node<'mcx> {\n");
    for v in variants {
        s.push_str(&format!(
            "    /// `{tag}` = {val}\n    {ident}({path}{lt}),\n",
            tag = v.tag,
            val = v.value,
            ident = v.variant_ident,
            path = v.struct_path,
            lt = if v.has_lifetime { "<'mcx>" } else { "" },
        ));
    }
    s.push_str("}\n\n");

    // Bind the `'mcx` lifetime even when no variant uses it (a future all-
    // lifetime-free subset), so the enum + impl stay valid at any list shape.
    s.push_str("impl<'mcx> Node<'mcx> {\n");

    // ---- node_tag() -----------------------------------------------------
    s.push_str(
        "    /// `nodeTag(node)` — the `NodeTag` discriminant of this node,\n    \
         /// matching the value in `nodetags.h`. Reconciles with the existing\n    \
         /// [`crate::nodes::NodeTag`] data value (same numeric tag).\n    \
         pub fn node_tag(&self) -> crate::nodes::NodeTag {\n        \
         match self {\n",
    );
    for v in variants {
        s.push_str(&format!(
            "            Node::{ident}(_) => crate::nodes::NodeTag({val}),\n",
            ident = v.variant_ident,
            val = v.value,
        ));
    }
    s.push_str("        }\n    }\n\n");

    // ---- copy_node_in dispatch ------------------------------------------
    s.push_str(
        "    /// Central deep-copy dispatch — the fallible owned-tree analogue of\n    \
         /// `copyObjectImpl` (`copyfuncs.c`). Matches the variant and delegates\n    \
         /// to that struct's per-struct `PgNodeCopy` impl (generated by\n    \
         /// `#[derive(PgNode)]`, or hand-written for the special-cased\n    \
         /// `List`/`Bitmapset`), re-homing the copy onto the TARGET context\n    \
         /// `dst`. Fallible: a charged allocation can OOM. Generated.\n    \
         pub fn copy_node_in<'dst>(\n        &self,\n        dst: mcx::Mcx<'dst>,\n    \
         ) -> types_error::PgResult<Node<'dst>> {\n        \
         use backend_nodes_node_support::PgNodeCopy;\n        match self {\n",
    );
    for v in variants {
        s.push_str(&format!(
            "            Node::{ident}(n) => Ok(Node::{ident}(n.copy_node_in(dst)?)),\n",
            ident = v.variant_ident,
        ));
    }
    s.push_str("        }\n    }\n\n");

    // ---- equal_node dispatch --------------------------------------------
    s.push_str(
        "    /// Central structural-equality dispatch — the analogue of `equal()`\n    \
         /// (`equalfuncs.c`). Two nodes with different tags are never equal;\n    \
         /// same-tag nodes delegate to that struct's per-struct `PgNodeEqual`\n    \
         /// impl. Infallible + lifetime-agnostic. Generated.\n    \
         pub fn equal_node(&self, other: &Node<'_>) -> bool {\n        \
         use backend_nodes_node_support::PgNodeEqual;\n        match (self, other) {\n",
    );
    for v in variants {
        s.push_str(&format!(
            "            (Node::{ident}(x), Node::{ident}(y)) => x.equal_node(y),\n",
            ident = v.variant_ident,
        ));
    }
    // Different-tag pairs: never equal. The wildcard is only reachable when the
    // enum has >1 variant; suppress the unreachable-pattern lint for the 1-tag
    // edge case so the generator stays valid at any list size.
    s.push_str("            #[allow(unreachable_patterns)]\n            _ => false,\n");
    s.push_str("        }\n    }\n");

    s.push_str("}\n");

    s
}
