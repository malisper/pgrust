//! Seam declarations for the `backend-nodes-copyfuncs` unit
//! (`nodes/copyfuncs.c`).
//!
//! `copyfuncs.c` provides one public entry point, `copyObject(obj)`: a deep copy
//! of an arbitrary `Node` tree. The owned-tree port re-homes the copy onto a
//! TARGET memory context (`copyObject` allocates into `CurrentMemoryContext`;
//! here the destination context is threaded explicitly), and the copy is
//! fallible (a charged allocation can OOM — the C `ereport(ERROR)`).
//!
//! The owning `backend-nodes-copyfuncs` crate installs this from its
//! `init_seams()`; until then a call panics loudly. The `Node` identity is the
//! central enum generated into `types-nodes` ([`nodes::node_tree::Node`]).

seam_core::seam!(
    /// `copyObject(n)` (copyfuncs.c) — deep-copy an arbitrary `Node` tree into
    /// the destination context `dst`, returning a freshly allocated,
    /// structurally equal tree. Fallible: copying allocates against `dst`.
    pub fn copy_object<'dst>(
        dst: mcx::Mcx<'dst>,
        n: &nodes::node_tree::Node<'_>,
    ) -> types_error::PgResult<nodes::node_tree::Node<'dst>>
);
