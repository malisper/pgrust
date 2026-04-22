use crate::backend::catalog::CatalogError;
use crate::backend::executor::value_io::{decode_value, encode_value};
use crate::include::access::brin_internal::{BrinDesc, BrinMemTuple, BrinTupleBytes};
use crate::include::access::brin_tuple::{
    BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_OFFSET_MASK, BRIN_PLACEHOLDER_MASK, BrinTuple,
    brin_header_size_with_bitmap, brin_null_bitmap_len, brin_tuple_data_offset,
    brin_tuple_has_nulls, brin_tuple_is_empty_range, brin_tuple_is_placeholder,
};
use crate::include::access::htup::{HeapTuple, TupleValue};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{ColumnDesc, RelationDesc};
use crate::include::varatt::{is_compressed_inline_datum, is_ondisk_toast_pointer};

fn disk_column(column: &ColumnDesc, suffix: usize) -> ColumnDesc {
    let mut column = column.clone();
    column.name = format!("{}_{}", column.name, suffix);
    column.storage.nullable = true;
    column
}

pub(crate) fn brin_build_desc(index_desc: &RelationDesc) -> BrinDesc {
    let info = index_desc
        .columns
        .iter()
        .map(|_| crate::include::access::brin_internal::BrinOpcInfo {
            nstored: 2,
            regular_nulls: true,
        })
        .collect::<Vec<_>>();
    BrinDesc {
        tupdesc: index_desc.clone(),
        total_stored: info.iter().map(|info| info.nstored).sum(),
        info,
    }
}

pub(crate) fn brin_disk_tupdesc(desc: &BrinDesc) -> RelationDesc {
    let mut columns = Vec::with_capacity(desc.total_stored);
    for (column_index, column) in desc.tupdesc.columns.iter().enumerate() {
        let info = &desc.info[column_index];
        for stored in 0..info.nstored {
            columns.push(disk_column(column, stored + 1));
        }
    }
    RelationDesc { columns }
}

fn encode_disk_tuple_value(column: &ColumnDesc, value: &Value) -> Result<TupleValue, CatalogError> {
    encode_value(column, value).map_err(|err| CatalogError::Io(format!("{err:?}")))
}

fn reversed_null_bitmap_bits(tuple: &BrinMemTuple) -> Vec<u8> {
    let natts = tuple.columns.len();
    let mut bitmap = vec![0u8; brin_null_bitmap_len(natts)];
    for (attno, column) in tuple.columns.iter().enumerate() {
        if column.all_nulls {
            bitmap[attno / 8] |= 1 << (attno % 8);
        }
        if column.has_nulls {
            let bit = natts + attno;
            bitmap[bit / 8] |= 1 << (bit % 8);
        }
    }
    bitmap
}

pub(crate) fn brin_form_tuple(
    desc: &BrinDesc,
    tuple: &BrinMemTuple,
) -> Result<BrinTupleBytes, CatalogError> {
    let disk_desc = brin_disk_tupdesc(desc);
    let mut values = Vec::with_capacity(desc.total_stored);
    for (column_index, column) in tuple.columns.iter().enumerate() {
        let disk_column = &disk_desc.columns[column_index * desc.info[column_index].nstored];
        let info = &desc.info[column_index];
        if column.all_nulls {
            values.extend((0..info.nstored).map(|_| TupleValue::Null));
            continue;
        }
        if column.values.len() != info.nstored {
            return Err(CatalogError::Corrupt("BRIN value count mismatch"));
        }
        for value in &column.values {
            values.push(encode_disk_tuple_value(disk_column, value)?);
        }
    }

    let any_nulls = tuple.columns.iter().any(|column| column.all_nulls || column.has_nulls);
    let heap_tuple = HeapTuple::from_values(&disk_desc.attribute_descs(), &values)
        .map_err(|err| CatalogError::Io(format!("{err:?}")))?;
    let header_size = brin_header_size_with_bitmap(tuple.columns.len(), any_nulls);
    if header_size > usize::from(BRIN_OFFSET_MASK) {
        return Err(CatalogError::Io(format!(
            "BRIN tuple header offset {} exceeds PostgreSQL bt_info capacity",
            header_size
        )));
    }

    let total_len =
        crate::backend::storage::page::bufpage::max_align(BrinTuple::SIZE + if any_nulls {
            brin_null_bitmap_len(tuple.columns.len())
        } else {
            0
        } + heap_tuple.data.len());
    let mut bytes = vec![0u8; total_len];
    bytes[0..4].copy_from_slice(&tuple.blkno.to_le_bytes());

    let mut bt_info = header_size as u8;
    if any_nulls {
        bt_info |= BRIN_NULLS_MASK;
        let bitmap = reversed_null_bitmap_bits(tuple);
        bytes[BrinTuple::SIZE..BrinTuple::SIZE + bitmap.len()].copy_from_slice(&bitmap);
    }
    if tuple.placeholder {
        bt_info |= BRIN_PLACEHOLDER_MASK;
    }
    if tuple.empty_range {
        bt_info |= BRIN_EMPTY_RANGE_MASK;
    }
    bytes[4] = bt_info;
    bytes[header_size..header_size + heap_tuple.data.len()].copy_from_slice(&heap_tuple.data);

    Ok(BrinTupleBytes {
        header: BrinTuple {
            bt_blkno: tuple.blkno,
            bt_info,
        },
        bytes,
    })
}

pub(crate) fn brin_form_placeholder_tuple(
    desc: &BrinDesc,
    blkno: u32,
) -> Result<BrinTupleBytes, CatalogError> {
    let tuple = BrinMemTuple::placeholder(desc, blkno);
    brin_form_tuple(desc, &tuple)
}

fn bitmap_bit(bitmap: &[u8], bit: usize) -> bool {
    bitmap
        .get(bit / 8)
        .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
}

fn disk_nulls_for_tuple(desc: &BrinDesc, tuple: &BrinMemTuple) -> Vec<bool> {
    let mut nulls = Vec::with_capacity(desc.total_stored);
    for (column_index, column) in tuple.columns.iter().enumerate() {
        let count = desc.info[column_index].nstored;
        if column.all_nulls {
            nulls.extend((0..count).map(|_| true));
        } else {
            nulls.extend((0..count).map(|_| false));
        }
    }
    nulls
}

fn parse_disk_values<'a>(
    disk_desc: &'a RelationDesc,
    data: &'a [u8],
    nulls: &[bool],
) -> Result<Vec<Option<&'a [u8]>>, CatalogError> {
    let attr_descs = disk_desc.attribute_descs();
    let mut values = Vec::with_capacity(attr_descs.len());
    let mut raw_offset = 0usize;
    for (index, attr) in attr_descs.iter().enumerate() {
        if nulls.get(index).copied().unwrap_or(false) {
            values.push(None);
            continue;
        }
        match attr.attlen {
            len if len > 0 => {
                raw_offset = attr.attalign.align_offset(raw_offset);
                let end = raw_offset + len as usize;
                let datum = data
                    .get(raw_offset..end)
                    .ok_or(CatalogError::Corrupt("truncated BRIN fixed-width datum"))?;
                values.push(Some(datum));
                raw_offset = end;
            }
            -1 => {
                let slice = data
                    .get(raw_offset..)
                    .ok_or(CatalogError::Corrupt("truncated BRIN varlena datum"))?;
                if is_ondisk_toast_pointer(slice) {
                    let end = raw_offset + crate::include::varatt::TOAST_POINTER_SIZE;
                    let datum = data
                        .get(raw_offset..end)
                        .ok_or(CatalogError::Corrupt("truncated BRIN toast pointer"))?;
                    values.push(Some(datum));
                    raw_offset = end;
                } else if slice.first().is_some_and(|byte| byte & 0x01 != 0) {
                    let total_len = usize::from(slice[0] >> 1);
                    let end = raw_offset + total_len;
                    let datum = data
                        .get(raw_offset + 1..end)
                        .ok_or(CatalogError::Corrupt("truncated BRIN short varlena datum"))?;
                    values.push(Some(datum));
                    raw_offset = end;
                } else {
                    raw_offset = attr.attalign.align_offset(raw_offset);
                    let header = data
                        .get(raw_offset..raw_offset + 4)
                        .ok_or(CatalogError::Corrupt("truncated BRIN varlena header"))?;
                    let total_len = (u32::from_le_bytes(header.try_into().unwrap()) >> 2) as usize;
                    let end = raw_offset + total_len;
                    let whole = data
                        .get(raw_offset..end)
                        .ok_or(CatalogError::Corrupt("truncated BRIN varlena datum"))?;
                    if is_compressed_inline_datum(whole) {
                        values.push(Some(whole));
                    } else {
                        values.push(Some(&whole[4..]));
                    }
                    raw_offset = end;
                }
            }
            -2 => {
                let start = raw_offset;
                while data
                    .get(raw_offset)
                    .copied()
                    .ok_or(CatalogError::Corrupt("unterminated BRIN cstring datum"))?
                    != 0
                {
                    raw_offset += 1;
                }
                let datum = &data[start..raw_offset];
                values.push(Some(datum));
                raw_offset += 1;
            }
            other => {
                return Err(CatalogError::Io(format!(
                    "unsupported BRIN attribute length {}",
                    other
                )));
            }
        }
    }
    Ok(values)
}

pub(crate) fn brin_deform_tuple(
    desc: &BrinDesc,
    tuple_bytes: &[u8],
) -> Result<BrinMemTuple, CatalogError> {
    if tuple_bytes.len() < BrinTuple::SIZE {
        return Err(CatalogError::Corrupt("truncated BRIN tuple"));
    }
    let blkno = u32::from_le_bytes(tuple_bytes[0..4].try_into().unwrap());
    let bt_info = tuple_bytes[4];
    let natts = desc.tupdesc.columns.len();
    let has_nulls = brin_tuple_has_nulls(bt_info);
    let header_size = brin_tuple_data_offset(bt_info);
    if header_size < BrinTuple::SIZE || header_size > tuple_bytes.len() {
        return Err(CatalogError::Corrupt("invalid BRIN tuple data offset"));
    }
    let null_bitmap = if has_nulls {
        let len = brin_null_bitmap_len(natts);
        tuple_bytes
            .get(BrinTuple::SIZE..BrinTuple::SIZE + len)
            .ok_or(CatalogError::Corrupt("truncated BRIN null bitmap"))?
    } else {
        &[][..]
    };

    let mut memtuple = BrinMemTuple::new(desc, blkno);
    memtuple.placeholder = brin_tuple_is_placeholder(bt_info);
    memtuple.empty_range = brin_tuple_is_empty_range(bt_info);

    for attno in 0..natts {
        memtuple.columns[attno].all_nulls = has_nulls && bitmap_bit(null_bitmap, attno);
        memtuple.columns[attno].has_nulls = has_nulls && bitmap_bit(null_bitmap, natts + attno);
    }

    let disk_desc = brin_disk_tupdesc(desc);
    let disk_nulls = disk_nulls_for_tuple(desc, &memtuple);
    let raw_values = parse_disk_values(&disk_desc, &tuple_bytes[header_size..], &disk_nulls)?;

    let mut stored_index = 0usize;
    for attno in 0..natts {
        let count = desc.info[attno].nstored;
        if memtuple.columns[attno].all_nulls {
            stored_index += count;
            continue;
        }
        for value_index in 0..count {
            let value = decode_value(&disk_desc.columns[stored_index], raw_values[stored_index])
                .map_err(|err| CatalogError::Io(format!("{err:?}")))?;
            memtuple.columns[attno].values[value_index] = value;
            stored_index += 1;
        }
    }

    Ok(memtuple)
}

pub(crate) fn brin_tuple_bytes_equal(left: &[u8], right: &[u8]) -> bool {
    left == right
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::brin_tuple::{
        BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_PLACEHOLDER_MASK,
    };

    fn one_int4_desc() -> BrinDesc {
        brin_build_desc(&RelationDesc {
            columns: vec![column_desc("a", SqlType::new(SqlTypeKind::Int4), true)],
        })
    }

    #[test]
    fn forms_fixed_width_minmax_tuple_with_pg_header_bits() {
        let desc = one_int4_desc();
        let mut memtuple = BrinMemTuple::new(&desc, 128);
        memtuple.empty_range = false;
        memtuple.columns[0].all_nulls = false;
        memtuple.columns[0].values = vec![Value::Int32(10), Value::Int32(20)];

        let tuple = brin_form_tuple(&desc, &memtuple).unwrap();

        assert_eq!(tuple.header.bt_blkno, 128);
        assert_eq!(tuple.header.bt_info, 8);
        assert_eq!(&tuple.bytes[0..5], &[128, 0, 0, 0, 8]);
        assert_eq!(tuple.bytes.len(), 16);
        assert_eq!(&tuple.bytes[8..12], &10i32.to_le_bytes());
        assert_eq!(&tuple.bytes[12..16], &20i32.to_le_bytes());
    }

    #[test]
    fn forms_placeholder_tuple_with_double_null_bitmap() {
        let desc = one_int4_desc();
        let tuple = brin_form_placeholder_tuple(&desc, 64).unwrap();

        assert_eq!(tuple.bytes.len(), 8);
        assert_eq!(&tuple.bytes[0..4], &64u32.to_le_bytes());
        assert_eq!(
            tuple.header.bt_info,
            8 | BRIN_NULLS_MASK | BRIN_PLACEHOLDER_MASK | BRIN_EMPTY_RANGE_MASK
        );
        assert_eq!(tuple.bytes[5], 0x01);
        assert_eq!(tuple.bytes[6], 0);
        assert_eq!(tuple.bytes[7], 0);
    }

    #[test]
    fn null_bitmap_uses_pg_bit_ordering() {
        let desc = brin_build_desc(&RelationDesc {
            columns: vec![
                column_desc("a", SqlType::new(SqlTypeKind::Int4), true),
                column_desc("b", SqlType::new(SqlTypeKind::Int4), true),
            ],
        });
        let mut memtuple = BrinMemTuple::new(&desc, 1);
        memtuple.empty_range = false;
        memtuple.columns[0].all_nulls = true;
        memtuple.columns[1].all_nulls = false;
        memtuple.columns[1].has_nulls = true;
        memtuple.columns[1].values = vec![Value::Int32(1), Value::Int32(2)];

        let tuple = brin_form_tuple(&desc, &memtuple).unwrap();

        assert_eq!(tuple.bytes[5], 0b1001);
    }

    #[test]
    fn deforms_roundtrip_tuple() {
        let desc = one_int4_desc();
        let mut memtuple = BrinMemTuple::new(&desc, 512);
        memtuple.empty_range = false;
        memtuple.columns[0].all_nulls = false;
        memtuple.columns[0].has_nulls = true;
        memtuple.columns[0].values = vec![Value::Int32(-7), Value::Int32(42)];

        let tuple = brin_form_tuple(&desc, &memtuple).unwrap();
        let deformed = brin_deform_tuple(&desc, &tuple.bytes).unwrap();

        assert_eq!(deformed, memtuple);
    }
}
