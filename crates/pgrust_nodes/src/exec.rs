use pgrust_core::ItemPointerData;

/// Executor-local binding for system Vars like `tableoid`.
///
/// PostgreSQL resolves these against dedicated scan/outer/inner slots rather
/// than against projected user-column layouts. pgrust does not mirror that
/// slot/opcode machinery exactly yet, so upper executor nodes carry the active
/// base-relation bindings explicitly and expression evaluation consults them
/// when evaluating a system Var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemVarBinding {
    pub varno: usize,
    pub table_oid: u32,
    pub tid: Option<ItemPointerData>,
    pub xmin: Option<u32>,
    pub cmin: Option<u32>,
    pub xmax: Option<u32>,
}
