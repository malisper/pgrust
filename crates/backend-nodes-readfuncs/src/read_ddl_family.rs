//! `_read<Type>` readers for the raw-grammar DDL statement family. Each reader
//! reads its fields in the exact order the OUT side wrote them. `try_read`
//! returns `Some(result)` iff this family owns `label`.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::nodes::Node;

/// Dispatch the DDL LABELs this module owns.
pub(crate) fn try_read<'mcx>(_mcx: Mcx<'mcx>, _label: &[u8]) -> Option<PgResult<Node<'mcx>>> {
    None
}
