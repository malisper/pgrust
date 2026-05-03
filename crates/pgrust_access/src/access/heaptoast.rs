use crate::access::htup::HeapTuple;
use crate::access::htup::SIZEOF_HEAP_TUPLE_HEADER;
use crate::access::itemptr::ItemPointerData;
use crate::access::toast_compression::ToastCompressionId;
use crate::varatt::VARHDRSZ;
use crate::varatt::{VarattExternal, decode_ondisk_toast_pointer, encode_ondisk_toast_pointer};
use crate::{AccessError, AccessResult};
use pgrust_nodes::Value;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{SqlType, SqlTypeKind};
use pgrust_storage::page::bufpage::{ITEM_ID_SIZE, MAXALIGN, SIZE_OF_PAGE_HEADER_DATA};
use pgrust_storage::smgr::BLCKSZ;

pub const TOAST_TUPLES_PER_PAGE: usize = 4;
pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
pub const EXTERN_TUPLES_PER_PAGE: usize = 4;

pub const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> usize {
    let item_space = SIZE_OF_PAGE_HEADER_DATA + tuples_per_page * ITEM_ID_SIZE;
    let maxaligned = (item_space + (MAXALIGN - 1)) & !(MAXALIGN - 1);
    let available = BLCKSZ - maxaligned;
    available / tuples_per_page
}

pub const TOAST_TUPLE_THRESHOLD: usize = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE);
pub const TOAST_TUPLE_TARGET: usize = TOAST_TUPLE_THRESHOLD;
pub const TOAST_TUPLE_TARGET_MAIN: usize = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE_MAIN);

pub const EXTERN_TUPLE_MAX_SIZE: usize = maximum_bytes_per_tuple(EXTERN_TUPLES_PER_PAGE);
pub const TOAST_MAX_CHUNK_SIZE: usize = EXTERN_TUPLE_MAX_SIZE
    - ((SIZEOF_HEAP_TUPLE_HEADER + (MAXALIGN - 1)) & !(MAXALIGN - 1))
    - std::mem::size_of::<u32>()
    - std::mem::size_of::<i32>()
    - VARHDRSZ;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredToastValue {
    pub pointer: VarattExternal,
    pub chunk_tids: Vec<ItemPointerData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalToastValueInput {
    pub data: Vec<u8>,
    pub rawsize: i32,
    pub compression_id: ToastCompressionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastChunk {
    pub id: u32,
    pub seq: i32,
    pub data: Vec<u8>,
}

pub fn encoded_pointer_bytes(pointer: VarattExternal) -> Vec<u8> {
    encode_ondisk_toast_pointer(pointer).to_vec()
}

pub fn toast_relation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            pgrust_catalog_data::desc::column_desc(
                "chunk_id",
                SqlType::new(SqlTypeKind::Oid),
                false,
            ),
            pgrust_catalog_data::desc::column_desc(
                "chunk_seq",
                SqlType::new(SqlTypeKind::Int4),
                false,
            ),
            pgrust_catalog_data::desc::column_desc(
                "chunk_data",
                SqlType::new(SqlTypeKind::Bytea),
                false,
            ),
        ],
    }
}

pub fn toast_chunk_row_values(value_id: u32, chunk_seq: i32, chunk: &[u8]) -> Vec<Value> {
    vec![
        Value::Int64(i64::from(value_id)),
        Value::Int32(chunk_seq),
        Value::Bytea(chunk.to_vec()),
    ]
}

fn read_i32_field(bytes: &[u8], field: &'static str) -> AccessResult<i32> {
    let raw = bytes
        .try_into()
        .map_err(|_| AccessError::Scalar(format!("toast {field} must be exactly 4 bytes")))?;
    Ok(i32::from_le_bytes(raw))
}

fn read_u32_field(bytes: &[u8], field: &'static str) -> AccessResult<u32> {
    let raw = bytes
        .try_into()
        .map_err(|_| AccessError::Scalar(format!("toast {field} must be exactly 4 bytes")))?;
    Ok(u32::from_le_bytes(raw))
}

pub fn toast_chunk_id_from_values(values: &[Option<&[u8]>]) -> AccessResult<Option<u32>> {
    values
        .first()
        .and_then(|value| *value)
        .map(|bytes| read_u32_field(bytes, "chunk_id"))
        .transpose()
}

pub fn toast_chunk_from_values(values: &[Option<&[u8]>]) -> AccessResult<Option<ToastChunk>> {
    let Some(id) = toast_chunk_id_from_values(values)? else {
        return Ok(None);
    };
    let Some(seq_bytes) = values.get(1).and_then(|value| *value) else {
        return Ok(None);
    };
    let seq = read_i32_field(seq_bytes, "chunk_seq")?;
    let data = values
        .get(2)
        .and_then(|value| *value)
        .ok_or(AccessError::Corrupt("toast chunk missing data"))?
        .to_vec();
    Ok(Some(ToastChunk { id, seq, data }))
}

pub fn extract_external_pointers(
    desc: &RelationDesc,
    tuple: &HeapTuple,
) -> AccessResult<Vec<VarattExternal>> {
    let attr_descs = desc.attribute_descs();
    let raw = tuple
        .deform(&attr_descs)
        .map_err(|err| AccessError::Scalar(format!("heap tuple deform failed: {err:?}")))?;
    Ok(raw
        .into_iter()
        .flatten()
        .filter_map(decode_ondisk_toast_pointer)
        .collect())
}
