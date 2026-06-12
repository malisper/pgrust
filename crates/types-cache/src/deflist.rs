//! The rows `deserialize_deflist` (commands/tsearchcmds.c) produces: a
//! `List` of `DefElem` nodes whose `arg` is always a `String` node. The
//! owned model carries the list as these typed rows rather than a node-tree
//! pointer.

use mcx::PgString;

/// One `DefElem` as built by `deserialize_deflist`: `defname` plus the
/// `String`-node argument.
#[derive(Debug)]
pub struct DefElemString<'mcx> {
    pub defname: PgString<'mcx>,
    pub arg: PgString<'mcx>,
}
