pub use pgrust_catalog_store::rowcodec::*;

use crate::backend::catalog::CatalogError;
use crate::backend::executor::RelationDesc;
use crate::include::nodes::datum::Value;

// :HACK: `HeapTuple` is still a root storage/access type. Keep only this
// root bridge here while the portable row codecs live in `pgrust_catalog_store`.
pub(crate) fn decode_catalog_tuple_values(
    desc: &RelationDesc,
    tuple: &crate::include::access::htup::HeapTuple,
) -> Result<Vec<Value>, CatalogError> {
    let raw = tuple
        .deform(&desc.attribute_descs())
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    pgrust_catalog_store::rowcodec::decode_catalog_tuple_values_from_raw(desc, &raw)
}
