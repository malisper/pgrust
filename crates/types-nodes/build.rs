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
                out_read: m.out_read,
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

/// How a variant participates in the central `out_node` / `read_node`
/// serialization dispatch (the OUT/READ stage). The copy/equal dispatch is
/// uniform across every variant, but OUT/READ is opt-in per struct (PostgreSQL
/// generates support only for nodes NOT in `@no_read`/`special_read_write`), so
/// the central dispatch cannot blindly call `n.out_node(buf)` for every variant
/// — that would fail to compile for the ones whose struct did not opt in. Each
/// variant declares its kind in the optional 4th column of `nodes.list`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OutReadKind {
    /// The struct derives (or hand-writes a framed) `PgNodeOut` + `PgNodeRead`
    /// via `#[derive(PgNode)] #[pg_node(out_read)]`. The central dispatch
    /// delegates straight to those impls: OUT calls `n.out_node(buf)`; READ peeks
    /// the `{LABEL` opener and calls `<Struct>::read_node(cur, dst)`.
    Derived,
    /// The struct supplies a hand-written `PgNodeOut`/`PgNodeRead` pair (C's
    /// `pg_node_attr(custom_read_write)` / `special_read_write`, e.g. the value
    /// nodes, `Bitmapset`, `List`). Until that hand-written pair + the per-label
    /// read dispatch are wired (a later stage), the central dispatch routes such
    /// a variant to a panicking `out_node_custom`/`read_node_custom` hook — the
    /// sanctioned "not yet ported" seam, NEVER a fabricated value. This is the
    /// DEFAULT when the kind column is omitted.
    Custom,
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
    /// How this variant routes through the OUT/READ central dispatch.
    out_read: OutReadKind,
}

/// One line of the data-driven mapping list.
struct Mapping {
    tag: String,
    struct_path: String,
    has_lifetime: bool,
    out_read: OutReadKind,
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
        // Optional 4th column: the OUT/READ dispatch kind. Defaults to `custom`
        // (the panicking hook) so a row that has not yet wired a serializer
        // compiles without inventing one.
        let out_read = match parts.next() {
            None | Some("custom") => OutReadKind::Custom,
            Some("derived") => OutReadKind::Derived,
            Some(other) => panic!(
                "nodes.list out_read column must be `derived` or `custom`, got `{other}`: `{raw}`"
            ),
        };
        assert!(
            parts.next().is_none(),
            "nodes.list line has extra columns: `{raw}`"
        );
        out.push(Mapping {
            tag,
            struct_path,
            has_lifetime,
            out_read,
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
    s.push_str("        }\n    }\n\n");

    // ---- out_node dispatch ----------------------------------------------
    // The central serialization dispatch — the analogue of `_outNode`
    // (outfuncs.c), which switches on `nodeTag(obj)` and calls the per-type
    // `_out#Tag`. A `derived` variant has a `PgNodeOut` impl (generated by the
    // derive, or a framed hand-written one), so the dispatch forwards straight to
    // `n.out_node(buf)`. A `custom` variant has NO generic framed serializer yet
    // (it is `special_read_write` / not yet wired), so it routes to the
    // panicking `out_node_custom` hook — the sanctioned "not yet ported" seam,
    // never a fabricated token. `buf` is a plain scratch `String` (the
    // `StringInfo` analogue); OUT never charges `mcx` and never fails. Generated.
    s.push_str(
        "    /// `_outNode` (outfuncs.c) — central serialization dispatch. Appends\n    \
         /// this node's textual token form to `buf`. A `derived` variant\n    \
         /// delegates to its `PgNodeOut` impl; a `custom`/special-read-write\n    \
         /// variant routes to the panicking `out_node_custom` hook until its\n    \
         /// hand-written serializer is wired. Generated.\n    \
         #[allow(unused_variables)]\n    \
         pub fn out_node(\n        &self,\n        \
         buf: &mut backend_nodes_node_support::alloc_reexport::string::String,\n    \
         ) {\n        \
         #[allow(unused_imports)]\n        \
         use backend_nodes_node_support::PgNodeOut;\n        match self {\n",
    );
    for v in variants {
        match v.out_read {
            OutReadKind::Derived => s.push_str(&format!(
                "            Node::{ident}(n) => n.out_node(buf),\n",
                ident = v.variant_ident,
            )),
            OutReadKind::Custom => s.push_str(&format!(
                "            Node::{ident}(n) => out_node_custom({tag:?}, buf),\n",
                ident = v.variant_ident,
                tag = v.tag,
            )),
        }
    }
    s.push_str("        }\n    }\n\n");

    // ---- read_node dispatch (LABEL-keyed) -------------------------------
    // The central deserialization dispatch — the owned-tree fusion of `nodeRead`
    // (read.c) with `parseNodeString` (readfuncs.c). It peeks the leading token:
    //
    //   * a `{` opens a framed node `{LABEL ...}`; the LABEL chooses the
    //     per-struct reader (parseNodeString's MATCH chain). A `derived` variant
    //     is read by `<Struct>::read_node(cur, dst)`; a `custom` framed variant
    //     and every bare value-node / list token (`(`, an Integer/Float/...
    //     literal) route to the panicking `read_node_custom` hook keyed on the
    //     label/kind, until the hand-written readers + value-token dispatch are
    //     wired (a later stage). This NEVER fabricates a node.
    //
    // Like `copy_node_in`, READ rebuilds a tree, so it threads the TARGET context
    // `dst` and is fallible. Generated.
    s.push_str(
        "    /// `nodeRead` / `parseNodeString` — central deserialization dispatch.\n    \
         /// Peeks the leading `{LABEL` (or a bare value token) and routes to the\n    \
         /// matching per-struct reader, re-homing the rebuilt tree onto `dst`.\n    \
         /// A `derived` variant delegates to `<Struct>::read_node`; a `custom` or\n    \
         /// bare value/list token routes to the panicking `read_node_custom` hook\n    \
         /// until its hand-written reader is wired. Generated.\n    \
         #[allow(unused_variables, unreachable_code)]\n    \
         pub fn read_node<'dst>(\n        \
         cur: &mut backend_nodes_node_support::ReadCursor<'_>,\n        \
         dst: mcx::Mcx<'dst>,\n    \
         ) -> types_error::PgResult<Node<'dst>> {\n        \
         #[allow(unused_imports)]\n        \
         use backend_nodes_node_support::PgNodeRead;\n        \
         let __peek = cur.peek_token()\n            \
         .expect(\"read_node: unexpected end of node token stream\");\n        \
         if __peek.text == \"{\" {\n            \
         // A framed `{LABEL ...}` node: peek past the brace to the LABEL.\n            \
         let __save = cur.save();\n            \
         cur.skip_token(); // the `{`\n            \
         let __label_tok = cur.expect_token();\n            \
         let __label = __label_tok.text;\n            \
         cur.restore(__save); // rewind so the per-struct reader sees `{LABEL`\n            \
         return match __label {\n",
    );
    for v in variants {
        let ty = if v.has_lifetime {
            format!("{}<'dst>", v.struct_path)
        } else {
            v.struct_path.clone()
        };
        let label = v.tag.strip_prefix("T_").unwrap_or(&v.tag).to_ascii_uppercase();
        match v.out_read {
            OutReadKind::Derived => s.push_str(&format!(
                "                {label:?} => Ok(Node::{ident}(\n                    \
                 <{ty} as PgNodeRead>::read_node(cur, dst)?)),\n",
                label = label,
                ident = v.variant_ident,
                ty = ty,
            )),
            OutReadKind::Custom => s.push_str(&format!(
                "                {label:?} => read_node_custom({label:?}, cur, dst),\n",
                label = label,
            )),
        }
    }
    s.push_str(
        "                other => ::core::panic!(\n                    \
         \"read_node: unrecognized node label {:?}\", other),\n            \
         };\n        }\n        \
         // A bare (non-`{`-framed) token: a value-node literal\n        \
         // (Integer/Float/Boolean/String/BitString) or a `(`-opened List. These\n        \
         // need the hand-written value-token dispatch (the analogue of\n        \
         // `nodeRead`'s `nodeTokenType` arms), not yet wired — route to the\n        \
         // panicking hook rather than fabricate a node.\n        \
         read_node_custom(\"<bare-value-token>\", cur, dst)\n    }\n",
    );

    s.push_str("}\n\n");

    // ---- out_node_custom / read_node_custom hooks -----------------------
    // The "not yet ported" seams for `special_read_write` / not-yet-wired
    // variants (every variant in the current converted subset: the value nodes,
    // `Bitmapset`, `List` — all `pg_node_attr(special_read_write)` in C, with
    // hand-written `_out`/`_read` routines). They panic loudly with the offending
    // label, exactly mirroring an unported callee: a real serialization round
    // over the converted subset is not reachable until a later stage wires the
    // hand-written readers/writers. They are NEVER silently stubbed.
    s.push_str(
        "/// Panicking OUT hook for a `special_read_write` / not-yet-wired variant\n\
         /// (the value nodes / `Bitmapset` / `List`): their hand-written\n\
         /// serializers are wired in a later stage. Mirrors calling into an\n\
         /// unported routine. Generated.\n\
         #[allow(unused_variables)]\n\
         fn out_node_custom(\n    label: &str,\n    \
         buf: &mut backend_nodes_node_support::alloc_reexport::string::String,\n) {\n    \
         ::core::panic!(\n        \
         \"out_node: special-read-write node `{}` needs a hand-written serializer; \
         none wired yet (K1 phase-3 OUT/READ stage is infra-only)\",\n        label)\n}\n\n",
    );
    s.push_str(
        "/// Panicking READ hook for a `special_read_write` / not-yet-wired variant\n\
         /// (and the bare value/list token forms). Mirrors calling into an\n\
         /// unported routine. Generated.\n\
         #[allow(unused_variables)]\n\
         fn read_node_custom<'dst>(\n    label: &str,\n    \
         cur: &mut backend_nodes_node_support::ReadCursor<'_>,\n    \
         dst: mcx::Mcx<'dst>,\n) -> types_error::PgResult<Node<'dst>> {\n    \
         ::core::panic!(\n        \
         \"read_node: special-read-write node `{}` needs a hand-written reader; \
         none wired yet (K1 phase-3 OUT/READ stage is infra-only)\",\n        label)\n}\n\n",
    );

    // ---- node_to_string / parse_node_string free fns --------------------
    // The top-level entry points: `nodeToString` (outfuncs.c) builds a fresh
    // scratch buffer and serializes; `stringToNode` (read.c) wraps a `ReadCursor`
    // over the text and reads one node, re-homing it onto `dst`. These mirror the
    // copy-side `copy_node_in` entry shape (fallible, threads the target context
    // on the rebuild side). Generated.
    s.push_str(
        "/// `nodeToString(obj)` (outfuncs.c) — serialize a node to its textual\n\
         /// token form. Builds a fresh scratch buffer (the `StringInfo` analogue)\n\
         /// and appends the node's tokens. Infallible. Generated.\n\
         pub fn node_to_string(node: &Node<'_>) -> backend_nodes_node_support::alloc_reexport::string::String {\n    \
         let mut buf = backend_nodes_node_support::alloc_reexport::string::String::new();\n    \
         node.out_node(&mut buf);\n    buf\n}\n\n",
    );
    s.push_str(
        "/// `stringToNode(str)` (read.c) — parse one node from its textual token\n\
         /// form, re-homing the rebuilt tree onto the target context `dst`\n\
         /// (C's `stringToNode` allocates against `CurrentMemoryContext`; here the\n\
         /// destination is explicit). Fallible: the rebuild can OOM. Generated.\n\
         pub fn parse_node_string<'dst>(\n    s: &str,\n    \
         dst: mcx::Mcx<'dst>,\n) -> types_error::PgResult<Node<'dst>> {\n    \
         let mut cur = backend_nodes_node_support::ReadCursor::new(s);\n    \
         Node::read_node(&mut cur, dst)\n}\n",
    );

    s
}
