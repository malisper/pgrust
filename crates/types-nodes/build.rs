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

    // ---- ADDITIVE: ntag consts + enum accessors over the hand-written enum --
    // Node-opaque migration Phase 1 (docs/proposals/node-opaque-migration.md §3
    // P1 + §5). This is 100% additive: it generates a `ntag` module of `T_*`
    // consts and the full `as_/as_*_mut/expect_/is_/into_` accessor set, emitted
    // as ENUM MATCHES over the CURRENT hand-written `enum Node`/`enum Expr`. The
    // enum, `tag()`, `clone_in()`, and all consumers stay byte-identical; this
    // establishes the stable accessor API that Phase 2 migrates onto.
    let nodes_rs = manifest_dir.join("src/nodes.rs");
    let primnodes_rs = manifest_dir.join("src/primnodes.rs");
    println!("cargo:rerun-if-changed={}", nodes_rs.display());
    println!("cargo:rerun-if-changed={}", primnodes_rs.display());
    let nodes_src = fs::read_to_string(&nodes_rs)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", nodes_rs.display()));
    let primnodes_src = fs::read_to_string(&primnodes_rs)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", primnodes_rs.display()));

    let node_enum = parse_node_enum(&nodes_src);
    assert!(!node_enum.is_empty(), "parsed zero Node enum variants");
    let expr_enum = parse_expr_enum(&primnodes_src);
    assert!(!expr_enum.is_empty(), "parsed zero Expr enum variants");
    let existing = parse_existing_node_methods(&nodes_src);
    // Hand-written `impl Expr` method names in primnodes.rs (the special
    // aliases and `_mut`/`expect_*` accessors that don't fit the uniform
    // generated shape). The Expr accessor generator skips these so it never
    // collides with them. (Macro-emitted `pub fn $is` text yields the literal
    // name `$is`, which can never collide — harmless.)
    let existing_expr = parse_existing_node_methods(&primnodes_src);

    let (ntag_code, acc_code, expr_tag_code) =
        emit_node_accessors(&node_enum, &expr_enum, &tag_values, &existing);
    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR").unwrap_or_else(|| panic!("build.rs main: OUT_DIR is not set")),
    );
    // The `ntag` module is module-scoped; the accessor methods are impl-scoped.
    // Two files so each can be `include!`d at the right scope.
    let ntag_path = out_dir.join("node_ntag.rs");
    fs::write(&ntag_path, ntag_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", ntag_path.display()));
    let acc_path = out_dir.join("node_accessors.rs");
    fs::write(&acc_path, acc_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", acc_path.display()));
    // The `expr_tag()` method + `etag` module — the Expr-side mirror of
    // `node_tag()`/`ntag`. The `impl Expr { expr_tag }` part is impl-scoped; the
    // `etag` module is file-scoped. One file, `include!`d at file scope in
    // primnodes.rs (the `impl Expr` block carries its own braces).
    let expr_tag_path = out_dir.join("expr_tag.rs");
    fs::write(&expr_tag_path, expr_tag_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", expr_tag_path.display()));

    // ---- ADDITIVE: the FULL Expr accessor surface (node-opaque P3) ----------
    // The Expr-side mirror of `emit_node_accessors`'s `impl Node` block: emit
    // `is_/as_/as_*_mut/expect_/into_/expect_into_` for EVERY `Expr` variant,
    // as enum matches over the hand-written `enum Expr`. Generated into its own
    // `impl Expr` block, `include!`d in primnodes.rs. Names already hand-written
    // in primnodes.rs are skipped (`existing_expr`), so this is 100% additive.
    let expr_acc_code = emit_expr_accessors(&expr_enum, &existing_expr);
    let expr_acc_path = out_dir.join("expr_accessors.rs");
    fs::write(&expr_acc_path, expr_acc_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", expr_acc_path.display()));

    // ---- ADDITIVE: Node `mk_<snake_variant>` constructors (node-opaque P3 s2) -
    // For every Node-direct and Expr-family variant emit an associated
    // constructor `Node::mk_<snake_variant>(mcx, payload) -> Node`. The `mcx`
    // arg is bound `_mcx` (unused for now): at the future opaque flip ONLY these
    // bodies change (to allocate the opaque representation), not the call sites.
    // For now each body just builds the existing enum variant. 100% additive.
    let ctor_code = emit_constructors(&node_enum, &expr_enum, &existing);
    let ctor_path = out_dir.join("node_constructors.rs");
    fs::write(&ctor_path, ctor_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", ctor_path.display()));

    // ---- ADDITIVE: the full `NodePayload` impl set (node-opaque P3 CODEGEN) ----
    // The pre-flip codegen generator: emit, for every flip-target `Node` variant,
    // the `#[repr(transparent)]` `NodePayload_<V>` adapter + its
    // `impl NodePayload<'mcx>` (the `node_tag`/`clone_in_dyn`/`equal_dyn` vtable),
    // plus a `single_lifetime_guard!` build-time witness (soundness gate (a),
    // §4.2a) per adapter. Written into a STANDALONE module that is gated behind
    // the off-by-default `node_payload_codegen` cargo feature, so the live
    // `Node` enum / its representation are byte-untouched in the normal build.
    // Validated to compile via `cargo build -p types-nodes --features
    // node_payload_codegen`. At the atomic flip this module's bodies (and the
    // `mk_*` ctors) become the live representation; until then it is dead,
    // additive, behavior-preserving substrate. See
    // docs/proposals/node-opaque-migration.md §6.5 step 1.
    let payload_code = emit_node_payload_impls(&node_enum, &tag_values);
    let payload_path = out_dir.join("node_payload_impls.rs");
    fs::write(&payload_path, payload_code)
        .unwrap_or_else(|e| panic!("cannot write {}: {e}", payload_path.display()));
}

/// Emit the full `NodePayload` adapter + impl set for every flip-target `Node`
/// variant into `$OUT_DIR/node_payload_impls.rs`. This is the pre-flip codegen
/// generator (docs/proposals/node-opaque-migration.md §6.5 step 1): it produces
/// the `#[repr(transparent)]` per-variant adapter, its `impl NodePayload<'mcx>`
/// (the `node_tag`/`clone_in_dyn`/`equal_dyn` vtable the opaque representation
/// dispatches through), and a `single_lifetime_guard!` build-time soundness
/// witness (§4.2a) per adapter.
///
/// The module is `include!`d ONLY behind the off-by-default `node_payload_codegen`
/// cargo feature, so the live `Node` enum is byte-untouched in the normal build
/// (`cargo build --bin postgres`). The generator SOURCES the variant table from
/// the hand-written enum (the flip target), exactly like the accessor/constructor
/// generators, NOT from the 7-entry `nodes.list` (which drives the separate,
/// partial `node_tree::Node`).
///
/// Per-variant bodies, mirroring the hand-written `Node::clone_in` (nodes.rs):
/// - `node_tag` — the `nodetags.h` value (compile-time constant).
/// - `clone_in_dyn` — deep-copy into `mcx`, re-homing the subtree, then rebuild
///   the live `Node` via the matching generated `mk_*` constructor. The special
///   `List`/`IntList` (carried as `PgVec`) and `Expr` (nested sub-enum) variants
///   get their exact enum-body clone logic; every other variant delegates to its
///   payload's `clone_in(mcx)`.
/// - `equal_dyn` — tag-gate then route to the installable node-equality seam
///   (`crate::opaque_node::node_equal_seam`), the project's sanctioned
///   "install-or-panic-loudly" pattern (mirroring `out_node_custom`/
///   `read_node_custom`): the real per-payload `equal()` comparators live in the
///   higher `backend-nodes-equalfuncs` crate (which deps `types-nodes`, so the
///   call cannot be made in-crate without a cycle); the seam is installed by that
///   crate at the flip. NEVER a fabricated `true`/`false`.
fn emit_node_payload_impls(
    node: &[EnumVariant],
    tag_values: &BTreeMap<String, u32>,
) -> String {
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P3 CODEGEN).\n\
         // DO NOT EDIT. Regenerated on every build. Gated behind the off-by-default\n\
         // `node_payload_codegen` feature — the live `Node` enum is byte-untouched\n\
         // in the normal build. This is the pre-flip `NodePayload` impl set: at the\n\
         // atomic flip these adapters + bodies become the live representation.\n\
         //\n\
         // Each variant gets: a `#[repr(transparent)]` `NodePayload_<V>` adapter\n\
         // over its payload, an `impl NodePayload<'mcx>` (node_tag/clone_in_dyn/\n\
         // equal_dyn), and a `single_lifetime_guard!` build-time soundness witness\n\
         // (proposal §4.2a).\n\
         use crate::nodes::{Node, NodeTag, NodePtr};\n\
         use crate::opaque_node::{NodePayload, node_equal_seam};\n\
         use mcx::{Mcx, PgVec};\n\
         use types_error::PgResult;\n\n",
    );

    let mut count = 0usize;
    for v in node {
        // The `Expr` routing arm is a real flip-target variant (its payload is the
        // nested `Expr` sub-enum); it gets an adapter like any other. The special
        // clone logic is handled below.
        let key = format!("T_{}", v.ident);
        // The `Expr` routing arm has no single `T_Expr` tag — it dispatches per
        // inner Expr leaf — so its `node_tag()` forwards to `Expr::expr_tag()`.
        // Every other variant resolves to its constant `nodetags.h` value.
        let node_tag_body = if v.ident == "Expr" {
            "self.0.expr_tag()".to_string()
        } else {
            let val = *tag_values.get(&key).unwrap_or_else(|| {
                panic!(
                    "node-opaque CODEGEN: enum variant `{}` has no `{}` in nodetags.h",
                    v.ident, key
                )
            });
            format!("NodeTag({val})")
        };

        // Classify the payload shape from the parsed enum text (the single source
        // of truth, exactly like the accessor/constructor generators).
        let payload = v.payload.trim();
        let has_lifetime = payload.contains("<'mcx>");
        // The adapter's stored payload type and the adapter struct's own generic
        // header. A lifetime-free payload (`crate::value::Integer`, `Expr`) still
        // needs the adapter to carry `'mcx` (so it satisfies `NodePayload<'mcx>`),
        // bound through a `PhantomData<Mcx<'mcx>>` field.
        let adapter = format!("NodePayload_{}", v.ident);
        let snake = to_snake_case(&v.ident);

        // ---- the `#[repr(transparent)]` adapter struct ----------------------
        // `repr(transparent)` is mandatory for the tag-keyed downcast (§1.3): the
        // payload must sit at the adapter's address. A lifetime-free payload needs
        // a zero-sized `PhantomData<Mcx<'mcx>>` to bind `'mcx`; since that breaks
        // single-field `transparent`, those adapters store the payload as the sole
        // non-zero-sized field with the ZST phantom (transparent permits one
        // non-ZST field + any number of ZST fields).
        if has_lifetime {
            s.push_str(&format!(
                "/// `#[repr(transparent)]` adapter over `{payload}` (tag `{tag}`).\n\
                 #[repr(transparent)]\n\
                 pub struct {adapter}<'mcx>(pub {payload});\n",
                payload = payload,
                tag = key,
                adapter = adapter,
            ));
        } else {
            s.push_str(&format!(
                "/// `#[repr(transparent)]` adapter over `{payload}` (tag `{tag}`). The\n\
                 /// payload is lifetime-free, so `'mcx` is bound through a ZST phantom\n\
                 /// (transparent permits one non-ZST field plus ZST fields).\n\
                 #[repr(transparent)]\n\
                 pub struct {adapter}<'mcx>(\n    pub {payload},\n    \
                 pub core::marker::PhantomData<Mcx<'mcx>>,\n);\n",
                payload = payload,
                tag = key,
                adapter = adapter,
            ));
        }

        // ---- single-lifetime soundness witness (§4.2a) ----------------------
        // `single_lifetime_guard!` emits a fixed-named `__single_lifetime_witness`
        // fn; wrap each per-adapter invocation in its own `const _: () = { .. }`
        // block so the names don't collide across the ~251 adapters.
        s.push_str(&format!(
            "const _: () = {{ crate::single_lifetime_guard!({adapter}<'mcx>); }};\n",
            adapter = adapter,
        ));

        // ---- the `impl NodePayload` ----------------------------------------
        s.push_str(&format!(
            "impl<'mcx> NodePayload<'mcx> for {adapter}<'mcx> {{\n",
            adapter = adapter,
        ));
        // node_tag: the compile-time constant from nodetags.h (or, for the
        // `Expr` routing arm, a forward to the inner `Expr::expr_tag()`).
        s.push_str(&format!(
            "    #[inline]\n    fn node_tag(&self) -> NodeTag {{ {body} }}\n",
            body = node_tag_body,
        ));

        // plan_base / plan_base_mut: for the 41 plan-bearing variants, override
        // the trait default to return the nested `&self.0.<...>.plan` field (the
        // vtable form of the hand-written `Node::plan_head` upcast, §1.1). Every
        // other adapter inherits the `None` default.
        if let Some(path) = plan_field_path(&v.ident) {
            s.push_str(&format!(
                "    #[inline]\n    \
                 fn plan_base(&self) -> Option<&crate::nodeindexscan::Plan<'mcx>> {{ Some(&self.0.{path}) }}\n    \
                 #[inline]\n    \
                 fn plan_base_mut(&mut self) -> Option<&mut crate::nodeindexscan::Plan<'mcx>> {{ Some(&mut self.0.{path}) }}\n",
                path = path,
            ));
        }

        // clone_in_dyn: mirror the hand-written `Node::clone_in` arm for this
        // variant, then rebuild the live `Node` via the generated `mk_*` ctor.
        s.push_str(&clone_in_dyn_body(&v.ident, payload, &snake));

        // equal_dyn: tag-gate then route to the installable equality seam (the
        // real per-payload comparators live in the higher equalfuncs crate).
        s.push_str(
            "    #[inline]\n    \
             fn equal_dyn(&self, other: &dyn NodePayload<'mcx>) -> bool {\n        \
             if other.node_tag() != self.node_tag() { return false; }\n        \
             // SAFETY: tags match -> the tag<->adapter bijection (§1.3) makes\n        \
             // `other` a `Self`; `repr(transparent)` gives the payload the same\n        \
             // address as `__payload_ptr()`. Route the two same-typed payloads to\n        \
             // the installable node-equality seam (the real `equal()` comparators\n        \
             // live in the higher `backend-nodes-equalfuncs` crate; installed at\n        \
             // the flip). Loud-until-installed, never a fabricated answer.\n        \
             let other_self = unsafe { &*(other.__payload_ptr() as *const Self) };\n        \
             node_equal_seam(self.node_tag(), self.__payload_ptr(), other_self.__payload_ptr())\n    }\n",
        );
        s.push_str("}\n\n");
        count += 1;
    }

    s.push_str(&format!(
        "/// Number of flip-target `Node` variants for which a `NodePayload` adapter\n\
         /// + impl was generated. A compile-time census of the codegen coverage.\n\
         pub const GENERATED_PAYLOAD_ADAPTER_COUNT: usize = {count};\n",
        count = count,
    ));

    s
}

/// The nested field path to the embedded `Plan` base for a plan-bearing `Node`
/// variant (the `&self.0.<path>` the generated `plan_base` returns), mirroring
/// the hand-written `Node::plan_head` match arms (`&a.plan`, `&m.join.plan`,
/// `&t.scan.plan`, `&s.sort.plan`). `None` for a non-plan variant (it inherits
/// the trait's `None` default). This table is the authoritative plan-upcast map
/// and must stay in sync with the plan node structs.
fn plan_field_path(ident: &str) -> Option<&'static str> {
    let p = match ident {
        // direct `.plan` field
        "Append" | "ModifyTable" | "Material" | "Gather" | "GatherMerge" | "MergeAppend"
        | "BitmapAnd" | "BitmapOr" | "RecursiveUnion" | "Group" | "ProjectSet" | "Result"
        | "SetOp" | "Memoize" | "Limit" | "Unique" | "Sort" | "Agg" | "WindowAgg" | "Hash" => {
            "plan"
        }
        // `.join.plan` (Join-derived)
        "MergeJoin" | "NestLoop" | "HashJoin" => "join.plan",
        // `.scan.plan` (Scan-derived)
        "IndexScan" | "IndexOnlyScan" | "BitmapIndexScan" | "BitmapHeapScan" | "TableFuncScan"
        | "FunctionScan" | "ValuesScan" | "CteScan" | "NamedTuplestoreScan" | "TidRangeScan"
        | "SampleScan" | "TidScan" | "WorkTableScan" | "SeqScan" | "SubqueryScan"
        | "ForeignScan" | "CustomScan" => "scan.plan",
        // `.sort.plan` (IncrementalSort embeds a Sort)
        "IncrementalSort" => "sort.plan",
        _ => return None,
    };
    Some(p)
}

/// Emit the `clone_in_dyn` method body for one variant, mirroring the exact arm
/// in the hand-written `Node::clone_in` (nodes.rs): special `List`/`IntList`
/// (carried as `PgVec`) and `Expr` (nested sub-enum) bodies; otherwise delegate
/// to the payload's `clone_in(mcx)` and rebuild via the generated `mk_*` ctor.
fn clone_in_dyn_body(ident: &str, _payload: &str, snake: &str) -> String {
    match ident {
        // `_copyList` (T_List): equal length, element-wise `clone_in`, re-boxed.
        "List" => String::from(
            "    fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {\n        \
             let mut out: PgVec<'b, NodePtr<'b>> =\n            \
             mcx::vec_with_capacity_in(mcx, self.0.len())?;\n        \
             for item in self.0.iter() {\n            \
             let cloned = item.clone_in(mcx)?;\n            \
             out.push(mcx::alloc_in(mcx, cloned)?);\n        }\n        \
             Node::mk_list(mcx, out)\n    }\n",
        ),
        // T_IntList: bare-int list, copied verbatim into a fresh context vec.
        "IntList" => String::from(
            "    fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {\n        \
             let mut out: PgVec<'b, i32> = mcx::vec_with_capacity_in(mcx, self.0.len())?;\n        \
             out.extend(self.0.iter().copied());\n        \
             Node::mk_int_list(mcx, out)\n    }\n",
        ),
        // `Expr`: the lifetime-free expression sub-enum's own deep `clone_in`
        // (`.clone()` PANICS on context-allocated Aggref/SubLink children).
        "Expr" => String::from(
            "    fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {\n        \
             Node::mk_expr(mcx, self.0.clone_in(mcx)?)\n    }\n",
        ),
        // Every other variant: per-struct `clone_in` (the analogue of `copyObject`\n
        // into the target context), rebuilt via the generated `mk_<snake>` ctor.
        _ => format!(
            "    fn clone_in_dyn<'b>(&self, mcx: Mcx<'b>) -> PgResult<Node<'b>> {{\n        \
             Node::mk_{snake}(mcx, self.0.clone_in(mcx)?)\n    }}\n",
            snake = snake,
        ),
    }
}

/// Convert a CamelCase enum-variant ident into `snake_case` for the `mk_*`
/// constructor name. Inserts `_` before each uppercase letter that follows a
/// lowercase letter, a digit, or another uppercase letter that is itself
/// followed by a lowercase letter (so `GatherMerge` -> `gather_merge`,
/// `WindowFunc` -> `window_func`, `JsonValueExpr` -> `json_value_expr`, and an
/// acronym run like `CTECycleClause` -> `cte_cycle_clause`).
fn to_snake_case(ident: &str) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::with_capacity(ident.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase());
            if prev.is_ascii_lowercase()
                || prev.is_ascii_digit()
                || (prev.is_ascii_uppercase() && next_lower)
            {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// Emit the `mk_<snake_variant>` constructor set into
/// `$OUT_DIR/node_constructors.rs`, `include!`d as an `impl<'mcx> Node<'mcx>`
/// block in nodes.rs. For each Node-direct variant the body builds
/// `Node::<V>(payload)`; for each Expr-family variant the body builds
/// `Node::Expr(Expr::<V>(payload))`. The `mcx` parameter is bound `_mcx` and
/// unused — at the future opaque flip ONLY these bodies change, not call sites.
/// 100% additive: skips any `mk_*` name already hand-written.
fn emit_constructors(
    node: &[EnumVariant],
    expr: &[EnumVariant],
    existing: &std::collections::BTreeSet<String>,
) -> String {
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P3 stage 2).\n\
         // DO NOT EDIT. Regenerated on every build. 100% ADDITIVE: per-variant\n\
         // `mk_<snake_variant>` associated constructors in their own\n\
         // `impl<'mcx> Node<'mcx>` block. Each body builds the existing enum\n\
         // variant; `mcx` is bound `_mcx` (unused) — at the opaque flip ONLY\n\
         // these bodies change, not the call sites.\n\
         impl<'mcx> crate::nodes::Node<'mcx> {\n",
    );

    let mut emitted: std::collections::BTreeSet<String> = existing.clone();
    let mut seen_ctor: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    // The `Node::Expr` routing arm: a whole-`Expr`-value constructor. Unlike the
    // per-leaf `mk_<exprvariant>` builders below, this takes an already-built
    // `Expr` and wraps it. At the opaque flip ONLY this body changes (it boxes a
    // single `NodePayload_Expr` adapter over `Expr`), not the call sites.
    {
        let name = "mk_expr".to_string();
        if seen_ctor.insert(name.clone()) {
            emitted.insert(name);
            s.push_str(
                "    /// Wrap an already-built `Expr` value as a `Node::Expr`. `mcx` is\n    \
                 /// unused for now; at the opaque flip this body allocates the opaque rep.\n    \
                 #[inline]\n    \
                 pub fn mk_expr(_mcx: mcx::Mcx<'mcx>, payload: crate::primnodes::Expr) -> types_error::PgResult<crate::nodes::Node<'mcx>> {\n        \
                 Ok(crate::nodes::Node::Expr(payload))\n    }\n",
            );
        }
    }

    // Node-direct variants first (they win collisions with Expr-routed names).
    for v in node {
        if v.ident == "Expr" {
            continue; // the routing arm; not a constructible leaf.
        }
        let name = format!("mk_{}", to_snake_case(&v.ident));
        if emitted.contains(&name) || !seen_ctor.insert(name.clone()) {
            continue;
        }
        let payload_ty = &v.payload;
        s.push_str(&format!(
            "    /// `makeNode({ident})` — build a `{ident}` node. `mcx` is unused\n    \
             /// for now; at the opaque flip this body allocates the opaque rep.\n    \
             #[inline]\n    \
             pub fn mk_{snake}(_mcx: mcx::Mcx<'mcx>, payload: {payload_ty}) -> types_error::PgResult<crate::nodes::Node<'mcx>> {{\n        \
             Ok(crate::nodes::Node::{ident}(payload))\n    }}\n",
            ident = v.ident,
            snake = to_snake_case(&v.ident),
        ));
    }

    // Expr-family variants, routed through `Node::Expr(Expr::<V>(..))`.
    for v in expr {
        let name = format!("mk_{}", to_snake_case(&v.ident));
        if emitted.contains(&name) || !seen_ctor.insert(name.clone()) {
            continue;
        }
        let payload_ty = format!("crate::primnodes::{}", v.payload);
        s.push_str(&format!(
            "    /// `makeNode({ident})` — build a `{ident}` expression node, routed\n    \
             /// through `Node::Expr`. `mcx` is unused for now; at the opaque flip\n    \
             /// this body allocates the opaque rep.\n    \
             #[inline]\n    \
             pub fn mk_{snake}(_mcx: mcx::Mcx<'mcx>, payload: {payload_ty}) -> types_error::PgResult<crate::nodes::Node<'mcx>> {{\n        \
             Ok(crate::nodes::Node::Expr(crate::primnodes::Expr::{ident}(payload))\n)    }}\n",
            ident = v.ident,
            snake = to_snake_case(&v.ident),
        ));
        let _ = &emitted;
    }

    s.push_str("}\n");
    let _ = &mut emitted;
    s
}

/// One variant of a parsed Rust enum: `Ident(<payload type text>)`.
struct EnumVariant {
    /// Variant name as it appears in the enum.
    ident: String,
    /// The full payload type text inside the parens, e.g.
    /// `crate::nodeindexscan::TidScan<'mcx>` or `OpExpr` or
    /// `PgVec<'mcx, NodePtr<'mcx>>`.
    payload: String,
}

/// Parse `pub enum Node<'mcx> { Ident(Payload), ... }` from `src/nodes.rs` into
/// (ident, payload-type-text) pairs. Mirrors `parse_nodes_list` in spirit; the
/// single source of truth is the hand-written enum body. Skips doc/line/block
/// comments and the routing `Expr` arm's children (handled separately).
fn parse_node_enum(src: &str) -> Vec<EnumVariant> {
    parse_rust_enum_body(src, "pub enum Node<'mcx> {")
}

/// Parse `pub enum Expr { Ident(Payload), ... }` from `src/primnodes.rs`.
fn parse_expr_enum(src: &str) -> Vec<EnumVariant> {
    parse_rust_enum_body(src, "pub enum Expr {")
}

/// Shared one-variant-per-line enum-body parser. Finds the opening header line,
/// then reads single-line `Ident(Payload),` entries up to the matching closing
/// `}` at column 0. Tolerant of doc-comments and `// ...` / `/* ... */` lines.
fn parse_rust_enum_body(src: &str, header: &str) -> Vec<EnumVariant> {
    let start = src
        .find(header)
        .unwrap_or_else(|| panic!("could not find enum header `{header}`"));
    // Body begins after the header line's newline.
    let after = &src[start + header.len()..];
    let mut out = Vec::new();
    for raw in after.lines() {
        let line = raw.trim();
        // The enum body terminates at the first `}` flush at the line start.
        if raw.starts_with('}') {
            break;
        }
        if line.is_empty()
            || line.starts_with("//")
            || line.starts_with("/*")
            || line.starts_with('*')
        {
            continue;
        }
        // Expect `Ident(Payload),` — Payload may itself contain balanced parens
        // / angle brackets; take everything between the FIRST `(` and the LAST
        // `)` before the trailing comma.
        let Some(open) = line.find('(') else {
            continue;
        };
        let ident = line[..open].trim();
        if ident.is_empty() || !ident.chars().next().unwrap().is_ascii_alphabetic() && !ident.starts_with('_') {
            continue;
        }
        if !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        let rest = &line[open + 1..];
        let Some(close) = rest.rfind(')') else {
            continue;
        };
        let payload = rest[..close].trim().to_string();
        out.push(EnumVariant {
            ident: ident.to_string(),
            payload,
        });
    }
    out
}

/// Collect the set of `pub fn <name>(` method names already defined in
/// `src/nodes.rs` (hand-written accessors such as `as_var`, `as_table_func`),
/// so the generator skips emitting a colliding name. Reconcile, don't collide.
fn parse_existing_node_methods(src: &str) -> std::collections::BTreeSet<String> {
    let mut set = std::collections::BTreeSet::new();
    for raw in src.lines() {
        let line = raw.trim();
        if let Some(rest) = line.strip_prefix("pub fn ") {
            if let Some(paren) = rest.find('(') {
                let name = rest[..paren].trim();
                if !name.is_empty() {
                    set.insert(name.to_string());
                }
            }
        }
    }
    set
}

/// Emit the additive `ntag` module + the enum-match accessor set into
/// `$OUT_DIR/node_accessors.rs`, `include!`d inside `impl<'mcx> Node<'mcx>` (and
/// the `ntag` module at file scope). `tag_values` is the nodetags.h tag->value
/// map; every Node/Expr variant ident resolves through `T_<ident>` (verified
/// 1:1 against the existing `tag()` match at port time).
fn emit_node_accessors(
    node: &[EnumVariant],
    expr: &[EnumVariant],
    tag_values: &BTreeMap<String, u32>,
    existing: &std::collections::BTreeSet<String>,
) -> (String, String, String) {
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P1).\n\
         // DO NOT EDIT. Regenerated on every build. 100% ADDITIVE: `ntag` consts\n\
         // (module scope) for every Node/Expr variant.\n\n",
    );

    // ---- ntag module: T_<Variant> consts for ALL Node + Expr variants -------
    // The O(1) jump-table enabler Phase 2 migrates onto. Sourced from nodetags.h
    // keyed by `T_<variant_ident>` (1:1 with the existing `tag()`/`expr_tag()`
    // match values, verified at port time). The `Expr` routing arm of `Node` is
    // not a real node and has no tag, so it is skipped.
    s.push_str(
        "/// `T_*` NodeTag constants for every `Node`/`Expr` variant — the\n\
         /// O(1) tag-keyed dispatch surface for the node-opaque migration. Each\n\
         /// const equals the `nodetags.h` value (and the existing `tag()` arm).\n\
         /// Generated, additive.\n\
         pub mod ntag {\n    \
         use crate::nodes::NodeTag;\n",
    );
    let mut seen_tags: BTreeMap<String, u32> = BTreeMap::new();
    let mut ntag_count = 0usize;
    for v in node.iter().chain(expr.iter()) {
        if v.ident == "Expr" {
            continue;
        }
        let key = format!("T_{}", v.ident);
        let Some(&val) = tag_values.get(&key) else {
            panic!(
                "node-opaque P1: enum variant `{}` has no `{}` in nodetags.h",
                v.ident, key
            );
        };
        match seen_tags.insert(key.clone(), val) {
            Some(prev) => assert_eq!(
                prev, val,
                "node-opaque P1: tag `{key}` resolves to two values ({prev}, {val})"
            ),
            None => {
                s.push_str(&format!(
                    "    /// `{key}` = {val}\n    pub const {key}: NodeTag = NodeTag({val});\n",
                ));
                ntag_count += 1;
            }
        }
    }
    s.push_str("}\n\n");

    // The generator itself asserts tag uniqueness above (distinct idents may
    // legitimately share a tag — DistinctExpr / NullIfExpr alias OpExpr's
    // payload but have their OWN tags, so no clash).
    let _ = ntag_count;
    let ntag_code = s;

    // ---- accessor set, emitted inside `impl<'mcx> Node<'mcx>` ---------------
    // For each Node-direct variant: as_<v>/as_<v>_mut/expect_<v>/is_<v>/into_<v>
    // matching `Node::<V>(..)`. For each Expr-family variant: the same set
    // routed through `Node::Expr(Expr::<V>(..))`. Node-direct wins on a name
    // collision; any name already hand-written in nodes.rs is skipped.
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P1).\n\
         // DO NOT EDIT. Regenerated on every build. 100% ADDITIVE accessor set\n\
         // in its own `impl<'mcx> Node<'mcx>` block. Each method is an enum match\n\
         // against the CURRENT enum — additive, behavior-preserving.\n\
         impl<'mcx> crate::nodes::Node<'mcx> {\n",
    );

    let mut emitted_names: std::collections::BTreeSet<String> = existing.clone();
    let mut method_count = 0usize;

    // Node-direct variants first (they win collisions with Expr-routed names).
    for v in node {
        if v.ident == "Expr" {
            continue; // the routing arm; its own as_expr is hand-written.
        }
        let lc = v.ident.to_ascii_lowercase();
        let pat = format!("Node::{}(x)", v.ident);
        let ty = ret_ty(&v.payload);
        method_count += emit_accessor_block(
            &mut s,
            &lc,
            &pat,
            &ty,
            &v.payload,
            &v.ident,
            &mut emitted_names,
        );
    }

    // Expr-family variants, routed through `Node::Expr(Expr::<V>(..))`.
    for v in expr {
        let lc = v.ident.to_ascii_lowercase();
        let pat = format!("Node::Expr(crate::primnodes::Expr::{}(x))", v.ident);
        // Expr payloads are lifetime-free; qualify under crate::primnodes.
        let ty = format!("crate::primnodes::{}", v.payload);
        method_count += emit_accessor_block(
            &mut s,
            &lc,
            &pat,
            &ty,
            &format!("crate::primnodes::{}", v.payload),
            &v.ident,
            &mut emitted_names,
        );
    }

    s.push_str("}\n");
    let _ = method_count;
    let acc_code = s;

    // ---- expr_tag(): the Expr-side mirror of node_tag() --------------------
    // Dispatch an Expr VALUE (already known to be Expr-family) to its NodeTag.
    // The dual-homed caveat is harmless here: Expr variants share a tag with
    // raw-grammar Node twins, but we are matching the Expr enum, so the tag is
    // unambiguous within Expr. Plus an `etag` module re-exporting the per-Expr
    // `T_*` consts so consumers can write `match e.expr_tag() { etag::T_Var => .. }`.
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P3).\n\
         // DO NOT EDIT. Regenerated on every build. 100% ADDITIVE: `expr_tag()`\n\
         // (the Expr-side mirror of `Node::node_tag()`) + the `etag` module.\n\n",
    );
    // `etag` module: re-export the `ntag` `T_*` consts for every Expr variant so
    // an Expr-tag dispatch reads `etag::T_Var`, matching `e.expr_tag()`.
    s.push_str(
        "/// `T_*` NodeTag constants for every `Expr` variant — the O(1)\n\
         /// tag-keyed dispatch surface mirroring [`crate::nodes::ntag`] but\n\
         /// scoped to the Expr family, for `match e.expr_tag() { etag::T_Var => .. }`.\n\
         /// Each is re-exported from `ntag` (same numeric tag). Generated, additive.\n\
         pub mod etag {\n",
    );
    {
        let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for v in expr {
            let key = format!("T_{}", v.ident);
            if !seen.insert(key.clone()) {
                continue;
            }
            s.push_str(&format!(
                "    /// `{key}` — re-exported from [`crate::nodes::ntag`].\n    \
                 pub use crate::nodes::ntag::{key};\n",
            ));
        }
    }
    s.push_str("}\n\n");

    // `impl Expr { expr_tag }` — exhaustive value match to the NodeTag.
    s.push_str(
        "impl Expr {\n    \
         /// `nodeTag(expr)` — the [`crate::nodes::NodeTag`] discriminant of this\n    \
         /// `Expr` value, matching `nodetags.h` (and the `T_*` arm a consumer\n    \
         /// migrates onto via [`etag`]). The Expr-side mirror of\n    \
         /// [`crate::nodes::Node::node_tag`]. Generated, additive.\n    \
         #[inline]\n    \
         pub fn expr_tag(&self) -> crate::nodes::NodeTag {\n        \
         match self {\n",
    );
    for v in expr {
        let key = format!("T_{}", v.ident);
        let Some(&val) = tag_values.get(&key) else {
            panic!(
                "node-opaque P3: Expr variant `{}` has no `{}` in nodetags.h",
                v.ident, key
            );
        };
        s.push_str(&format!(
            "            Expr::{ident}(_) => crate::nodes::NodeTag({val}),\n",
            ident = v.ident,
        ));
    }
    s.push_str("        }\n    }\n}\n");
    let expr_tag_code = s;

    (ntag_code, acc_code, expr_tag_code)
}

/// Normalize a Node payload type-text into the accessor return type, leaving
/// fully-qualified `crate::...` paths and `PgVec<...>` as-is (they already name
/// the borrowed type). Returns the borrow target type text.
fn ret_ty(payload: &str) -> String {
    payload.to_string()
}

/// Emit the five accessors for one variant, skipping any whose name already
/// exists. Returns the number of methods actually emitted.
fn emit_accessor_block(
    s: &mut String,
    lc: &str,
    pat: &str,
    ret: &str,
    owned: &str,
    ident: &str,
    emitted: &mut std::collections::BTreeSet<String>,
) -> usize {
    // The bound name in the pattern is `x`; the `is_` test wants a wildcard so
    // it never binds (avoids unused-binding lints).
    let pat_mut = pat.to_string();
    let pat_wild = pat.replacen("(x)", "(_)", 1);
    let mut n = 0;
    let methods: [(String, String); 6] = [
        (
            format!("as_{lc}"),
            format!(
                "    /// `castNode({ident}, node)` (borrow) — `Some` iff this node is `{ident}`.\n    \
                 pub fn as_{lc}(&self) -> Option<&{ret}> {{\n        \
                 match self {{ {pat} => Some(x), _ => None }}\n    }}\n",
            ),
        ),
        (
            format!("as_{lc}_mut"),
            format!(
                "    /// `castNode({ident}, node)` (mutable borrow).\n    \
                 pub fn as_{lc}_mut(&mut self) -> Option<&mut {ret}> {{\n        \
                 match self {{ {pat_mut} => Some(x), _ => None }}\n    }}\n",
            ),
        ),
        (
            format!("expect_{lc}"),
            format!(
                "    /// `castNode({ident}, node)` (borrow, asserting) — panics if not `{ident}`.\n    \
                 pub fn expect_{lc}(&self) -> &{ret} {{\n        \
                 match self {{ {pat} => x, _ => ::core::panic!(\"expect_{lc}: not a {ident} node\") }}\n    }}\n",
            ),
        ),
        (
            format!("expect_{lc}_mut"),
            format!(
                "    /// `castNode({ident}, node)` (mutable borrow, asserting) — panics if not `{ident}`.\n    \
                 pub fn expect_{lc}_mut(&mut self) -> &mut {ret} {{\n        \
                 match self {{ {pat_mut} => x, _ => ::core::panic!(\"expect_{lc}_mut: not a {ident} node\") }}\n    }}\n",
            ),
        ),
        (
            format!("is_{lc}"),
            format!(
                "    /// `IsA(node, {ident})`.\n    \
                 pub fn is_{lc}(&self) -> bool {{\n        \
                 matches!(self, {pat_wild})\n    }}\n",
            ),
        ),
        (
            format!("into_{lc}"),
            format!(
                "    /// Consume into the `{ident}` payload, or `None` if not `{ident}`.\n    \
                 pub fn into_{lc}(self) -> Option<{owned}> {{\n        \
                 match self {{ {pat} => Some(x), _ => None }}\n    }}\n",
            ),
        ),
    ];
    for (name, body) in methods {
        if emitted.contains(&name) {
            continue;
        }
        emitted.insert(name);
        s.push_str(&body);
        n += 1;
    }
    n
}

/// Emit the FULL Expr accessor surface into `$OUT_DIR/expr_accessors.rs`,
/// `include!`d as an `impl Expr` block in primnodes.rs. For every `Expr`
/// variant this generates, matching `Expr::<V>(..)`:
///
/// - `is_<v>(&self) -> bool`            — `IsA(node, V)`
/// - `as_<v>(&self) -> Option<&T>`      — `castNode(V, node)` borrow
/// - `as_<v>_mut(&mut self) -> Option<&mut T>`
/// - `expect_<v>(&self) -> &T`          — asserting borrow (C `castNode` elogs)
/// - `into_<v>(self) -> Option<T>`      — fallible owned take
/// - `expect_into_<v>(self) -> T`       — asserting owned take (C reuses storage)
///
/// `T` is the variant's payload type (read from the enum, so payload-type
/// aliases like `DistinctExpr(OpExpr)` / `SubPlan(SubPlanExpr)` get the correct
/// return type). The `existing` set holds method names already hand-written in
/// primnodes.rs; those are skipped so the block stays purely additive.
fn emit_expr_accessors(
    expr: &[EnumVariant],
    existing: &std::collections::BTreeSet<String>,
) -> String {
    let mut s = String::new();
    s.push_str(
        "// @generated by types-nodes/build.rs (node-opaque migration P3).\n\
         // DO NOT EDIT. Regenerated on every build. 100% ADDITIVE: the full\n\
         // `is_/as_/as_*_mut/expect_/into_/expect_into_` accessor set for every\n\
         // `Expr` variant, as enum matches over the CURRENT hand-written enum.\n\
         // Names hand-written in primnodes.rs are skipped (reconcile, don't\n\
         // collide). This is the Expr-side mirror of `impl Node`'s accessors.\n\
         impl Expr {\n",
    );

    let mut emitted: std::collections::BTreeSet<String> = existing.clone();
    let mut count = 0usize;
    for v in expr {
        let lc = v.ident.to_ascii_lowercase();
        let ident = &v.ident;
        let ty = &v.payload; // payload type text, e.g. `OpExpr`, `SubPlanExpr`
        let pat = format!("Expr::{ident}(x)");
        let pat_wild = format!("Expr::{ident}(_)");
        let methods: [(String, String); 7] = [
            (
                format!("is_{lc}"),
                format!(
                    "    /// `IsA(node, {ident})`.\n    #[inline]\n    \
                     pub fn is_{lc}(&self) -> bool {{ matches!(self, {pat_wild}) }}\n",
                ),
            ),
            (
                format!("as_{lc}"),
                format!(
                    "    /// `castNode({ident}, node)` (borrow) — `Some` iff this is `Expr::{ident}`.\n    #[inline]\n    \
                     pub fn as_{lc}(&self) -> Option<&{ty}> {{\n        \
                     match self {{ {pat} => Some(x), _ => None }}\n    }}\n",
                ),
            ),
            (
                format!("as_{lc}_mut"),
                format!(
                    "    /// `castNode({ident}, node)` (mutable borrow).\n    #[inline]\n    \
                     pub fn as_{lc}_mut(&mut self) -> Option<&mut {ty}> {{\n        \
                     match self {{ {pat} => Some(x), _ => None }}\n    }}\n",
                ),
            ),
            (
                format!("expect_{lc}"),
                format!(
                    "    /// `castNode({ident}, node)` (borrow, asserting) — panics if not `Expr::{ident}`.\n    #[inline]\n    \
                     pub fn expect_{lc}(&self) -> &{ty} {{\n        \
                     match self {{ {pat} => x, _ => ::core::panic!(\"Expr::expect_{lc}: not a {ident}\") }}\n    }}\n",
                ),
            ),
            (
                format!("expect_{lc}_mut"),
                format!(
                    "    /// `castNode({ident}, node)` (mutable borrow, asserting) — panics if not `Expr::{ident}`.\n    #[inline]\n    \
                     pub fn expect_{lc}_mut(&mut self) -> &mut {ty} {{\n        \
                     match self {{ {pat} => x, _ => ::core::panic!(\"Expr::expect_{lc}_mut: not a {ident}\") }}\n    }}\n",
                ),
            ),
            (
                format!("into_{lc}"),
                format!(
                    "    /// Consume into the `{ident}` payload, or `None` if not `Expr::{ident}`.\n    #[inline]\n    \
                     pub fn into_{lc}(self) -> Option<{ty}> {{\n        \
                     match self {{ {pat} => Some(x), _ => None }}\n    }}\n",
                ),
            ),
            (
                format!("expect_into_{lc}"),
                format!(
                    "    /// Consume into the `{ident}` payload (C reuses the node's storage in\n    \
                     /// place); panics on a wrong tag (a caller bug).\n    #[inline]\n    \
                     pub fn expect_into_{lc}(self) -> {ty} {{\n        \
                     match self {{ {pat} => x, _ => ::core::panic!(\"Expr::expect_into_{lc}: not a {ident}\") }}\n    }}\n",
                ),
            ),
        ];
        for (name, body) in methods {
            if emitted.contains(&name) {
                continue;
            }
            emitted.insert(name);
            s.push_str(&body);
            count += 1;
        }
    }
    s.push_str("}\n");
    let _ = count;
    s
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
