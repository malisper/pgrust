//! The rows `deserialize_deflist` (commands/tsearchcmds.c) produces: a
//! `List` of `DefElem` nodes. `buildDefItem` infers each value node's kind
//! (`T_Integer`/`T_Float`/`T_Boolean`/`T_String`), which the dict init methods'
//! `defGetBoolean`/`defGetInt32`/... switch on. The owned model carries the
//! list as these typed rows (the value rendered to text plus its inferred node
//! kind) rather than a node-tree pointer.

use mcx::PgString;

/// The `buildDefItem`-inferred node kind of a deserialized option value, so the
/// consumer can rebuild a `DefElem` arg of the right `nodeTag` (e.g.
/// `casesensitive = 1` must reach `defGetBoolean` as a `T_Integer`, not a
/// `T_String`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefElemValKind {
    Integer,
    Float,
    Boolean,
    String,
}

/// One `DefElem` as built by `deserialize_deflist`: `defname` plus the value
/// rendered to text and the `buildDefItem`-inferred node kind.
#[derive(Debug)]
pub struct DefElemString<'mcx> {
    pub defname: PgString<'mcx>,
    pub arg: PgString<'mcx>,
    pub kind: DefElemValKind,
}
