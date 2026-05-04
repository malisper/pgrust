// :HACK: root compatibility shim while GiST tuple codecs live in `pgrust_access`.
use pgrust_access::gist::tuple as access_tuple;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::itup::IndexTupleData;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
        AccessError::Io(message) => CatalogError::Io(message),
        AccessError::UniqueViolation(message) => CatalogError::UniqueViolation(message),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub(crate) fn encode_key_payload(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<Vec<u8>, CatalogError> {
    catalog_result(access_tuple::encode_key_payload(
        desc,
        values,
        &RootAccessServices,
    ))
}

pub(crate) fn decode_key_payload(
    desc: &RelationDesc,
    payload: &[u8],
) -> Result<Vec<Value>, CatalogError> {
    catalog_result(access_tuple::decode_key_payload(
        desc,
        payload,
        &RootAccessServices,
    ))
}

pub(crate) fn decode_tuple_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    catalog_result(access_tuple::decode_tuple_values(
        desc,
        tuple,
        &RootAccessServices,
    ))
}

pub(crate) fn make_leaf_tuple(
    desc: &RelationDesc,
    values: &[Value],
    heap_tid: ItemPointerData,
) -> Result<IndexTupleData, CatalogError> {
    catalog_result(access_tuple::make_leaf_tuple(
        desc,
        values,
        heap_tid,
        &RootAccessServices,
    ))
}

pub(crate) fn make_downlink_tuple(
    desc: &RelationDesc,
    values: &[Value],
    child_block: u32,
) -> Result<IndexTupleData, CatalogError> {
    catalog_result(access_tuple::make_downlink_tuple(
        desc,
        values,
        child_block,
        &RootAccessServices,
    ))
}

pub(crate) fn tuple_storage_size(
    desc: &RelationDesc,
    values: &[Value],
) -> Result<usize, CatalogError> {
    catalog_result(access_tuple::tuple_storage_size(
        desc,
        values,
        &RootAccessServices,
    ))
}
