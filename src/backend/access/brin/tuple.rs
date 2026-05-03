// :HACK: root compatibility shim while BRIN tuple codecs live in `pgrust_access`.
use pgrust_access::brin::tuple as access_tuple;
use pgrust_access::{AccessError, AccessResult};

use crate::backend::access::RootAccessServices;
use crate::backend::catalog::CatalogError;
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::access::brin_internal::{BrinDesc, BrinMemTuple, BrinTupleBytes};
use crate::include::nodes::primnodes::RelationDesc;

fn catalog_error(error: AccessError) -> CatalogError {
    match error {
        AccessError::Corrupt(message) => CatalogError::Corrupt(message),
        AccessError::Interrupted(reason) => CatalogError::Interrupted(reason),
        AccessError::Scalar(message) | AccessError::Unsupported(message) => {
            CatalogError::Io(message)
        }
    }
}

fn catalog_result<T>(result: AccessResult<T>) -> Result<T, CatalogError> {
    result.map_err(catalog_error)
}

pub(crate) fn brin_opfamily_is_minmax_multi(opfamily_oid: Option<u32>) -> bool {
    access_tuple::brin_opfamily_is_minmax_multi(opfamily_oid)
}

pub(crate) fn brin_build_desc(index_desc: &RelationDesc) -> BrinDesc {
    access_tuple::brin_build_desc(index_desc)
}

pub(crate) fn brin_build_desc_with_meta(
    index_desc: &RelationDesc,
    index_meta: Option<&IndexRelCacheEntry>,
) -> BrinDesc {
    access_tuple::brin_build_desc_with_meta(index_desc, index_meta)
}

pub(crate) fn brin_disk_tupdesc(desc: &BrinDesc) -> RelationDesc {
    access_tuple::brin_disk_tupdesc(desc)
}

pub(crate) fn brin_form_tuple(
    desc: &BrinDesc,
    tuple: &BrinMemTuple,
) -> Result<BrinTupleBytes, CatalogError> {
    catalog_result(access_tuple::brin_form_tuple(
        desc,
        tuple,
        &RootAccessServices,
    ))
}

pub(crate) fn brin_form_placeholder_tuple(
    desc: &BrinDesc,
    blkno: u32,
) -> Result<BrinTupleBytes, CatalogError> {
    catalog_result(access_tuple::brin_form_placeholder_tuple(
        desc,
        blkno,
        &RootAccessServices,
    ))
}

pub(crate) fn brin_deform_tuple(
    desc: &BrinDesc,
    tuple_bytes: &[u8],
) -> Result<BrinMemTuple, CatalogError> {
    catalog_result(access_tuple::brin_deform_tuple(
        desc,
        tuple_bytes,
        &RootAccessServices,
    ))
}

pub(crate) fn brin_tuple_bytes_equal(left: &[u8], right: &[u8]) -> bool {
    access_tuple::brin_tuple_bytes_equal(left, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::brin_tuple::{
        BRIN_EMPTY_RANGE_MASK, BRIN_NULLS_MASK, BRIN_PLACEHOLDER_MASK,
    };
    use crate::include::nodes::datum::Value;

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

    #[test]
    fn forms_tuple_with_aligned_bitmap_padding() {
        let columns = (0..20)
            .map(|index| column_desc(&format!("c{index}"), SqlType::new(SqlTypeKind::Int2), true))
            .collect();
        let desc = brin_build_desc(&RelationDesc { columns });
        let mut memtuple = BrinMemTuple::new(&desc, 96);
        memtuple.empty_range = false;
        for (index, column) in memtuple.columns.iter_mut().enumerate().skip(1) {
            column.all_nulls = false;
            column.values = vec![Value::Int16(index as i16), Value::Int16((index * 2) as i16)];
        }

        let tuple = brin_form_tuple(&desc, &memtuple).unwrap();
        let deformed = brin_deform_tuple(&desc, &tuple.bytes).unwrap();

        assert_eq!(deformed, memtuple);
    }
}
