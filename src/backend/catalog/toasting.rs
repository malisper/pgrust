use crate::backend::executor::RelationDesc;
use crate::backend::storage::page::bufpage::MAXALIGN;
use crate::include::access::heaptoast::TOAST_TUPLE_THRESHOLD;
use crate::include::access::htup::{AttributeStorage, SIZEOF_HEAP_TUPLE_HEADER};
pub use crate::include::catalog::toasting::{
    PG_TOAST_NAMESPACE, toast_index_name, toast_relation_name,
};

fn bitmap_len(natts: usize) -> usize {
    natts.div_ceil(8)
}

fn type_maximum_size(column: &crate::backend::executor::ColumnDesc) -> Option<usize> {
    let sql_type = column.sql_type;
    if sql_type.is_array {
        return None;
    }
    match sql_type.kind {
        crate::backend::parser::SqlTypeKind::Name => Some(64 + crate::include::varatt::VARHDRSZ),
        crate::backend::parser::SqlTypeKind::InternalChar => Some(2),
        crate::backend::parser::SqlTypeKind::Timestamp => {
            Some(64 + crate::include::varatt::VARHDRSZ)
        }
        crate::backend::parser::SqlTypeKind::Varchar
        | crate::backend::parser::SqlTypeKind::Char => sql_type
            .char_len()
            .map(|len| len as usize + crate::include::varatt::VARHDRSZ),
        crate::backend::parser::SqlTypeKind::Bit | crate::backend::parser::SqlTypeKind::VarBit => {
            sql_type
                .bit_len()
                .map(|len| (len as usize).div_ceil(8) + crate::include::varatt::VARHDRSZ)
        }
        crate::backend::parser::SqlTypeKind::Bool
        | crate::backend::parser::SqlTypeKind::Int2
        | crate::backend::parser::SqlTypeKind::Int4
        | crate::backend::parser::SqlTypeKind::Int8
        | crate::backend::parser::SqlTypeKind::Oid
        | crate::backend::parser::SqlTypeKind::Float4
        | crate::backend::parser::SqlTypeKind::Float8 => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::Int2Vector
        | crate::backend::parser::SqlTypeKind::OidVector
        | crate::backend::parser::SqlTypeKind::Bytea
        | crate::backend::parser::SqlTypeKind::Numeric
        | crate::backend::parser::SqlTypeKind::Json
        | crate::backend::parser::SqlTypeKind::Jsonb
        | crate::backend::parser::SqlTypeKind::JsonPath
        | crate::backend::parser::SqlTypeKind::Text
        | crate::backend::parser::SqlTypeKind::PgNodeTree => None,
    }
}

pub fn relation_needs_toast_table(desc: &RelationDesc) -> bool {
    let mut data_length = 0usize;
    let mut maxlength_unknown = false;
    let mut has_toastable_attrs = false;

    for column in &desc.columns {
        let storage = &column.storage;
        data_length = storage.attalign.align_offset(data_length);
        if storage.attlen > 0 {
            data_length += storage.attlen as usize;
        } else {
            match type_maximum_size(column) {
                Some(maxlen) => data_length += maxlen,
                None => maxlength_unknown = true,
            }
            if storage.attstorage != AttributeStorage::Plain {
                has_toastable_attrs = true;
            }
        }
    }

    if !has_toastable_attrs {
        return false;
    }
    if maxlength_unknown {
        return true;
    }

    let tuple_length =
        ((SIZEOF_HEAP_TUPLE_HEADER + bitmap_len(desc.columns.len()) + (MAXALIGN - 1))
            & !(MAXALIGN - 1))
            + ((data_length + (MAXALIGN - 1)) & !(MAXALIGN - 1));
    tuple_length > TOAST_TUPLE_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn unlimited_text_column_needs_toast() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "payload",
                SqlType::new(SqlTypeKind::Text),
                false,
            )],
        };
        assert!(relation_needs_toast_table(&desc));
    }

    #[test]
    fn bounded_varchar_does_not_need_toast() {
        let desc = RelationDesc {
            columns: vec![column_desc(
                "payload",
                SqlType::with_char_len(SqlTypeKind::Varchar, 20),
                false,
            )],
        };
        assert!(!relation_needs_toast_table(&desc));
    }
}
