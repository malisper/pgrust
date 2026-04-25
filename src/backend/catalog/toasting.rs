use crate::backend::catalog::catalog::{
    Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, column_desc,
};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::storage::page::bufpage::MAXALIGN;
use crate::include::access::heaptoast::TOAST_TUPLE_THRESHOLD;
use crate::include::access::htup::SIZEOF_HEAP_TUPLE_HEADER;
use crate::include::access::htup::{AttributeCompression, AttributeStorage};
pub use crate::include::catalog::toasting::{
    PG_TOAST_NAMESPACE, toast_index_name, toast_relation_name,
};
use crate::include::catalog::{
    BTREE_AM_OID, DEPENDENCY_INTERNAL, INT4_BTREE_OPCLASS_OID, OID_BTREE_OPCLASS_OID,
    PG_CLASS_RELATION_OID, PgDependRow,
};
use crate::include::nodes::parsenodes::IndexColumnDef;

#[derive(Debug, Clone, PartialEq)]
pub struct ToastCatalogChanges {
    pub parent_name: String,
    pub old_parent: CatalogEntry,
    pub new_parent: CatalogEntry,
    pub toast_name: String,
    pub toast_entry: CatalogEntry,
    pub index_name: String,
    pub index_entry: CatalogEntry,
}

fn bitmap_len(natts: usize) -> usize {
    natts.div_ceil(8)
}

fn type_maximum_size(column: &crate::backend::executor::ColumnDesc) -> Option<usize> {
    let sql_type = column.sql_type;
    if sql_type.is_array {
        return None;
    }
    if sql_type.is_range() {
        return None;
    }
    if sql_type.is_multirange() {
        return None;
    }
    match sql_type.kind {
        crate::backend::parser::SqlTypeKind::AnyArray
        | crate::backend::parser::SqlTypeKind::AnyElement
        | crate::backend::parser::SqlTypeKind::AnyRange
        | crate::backend::parser::SqlTypeKind::AnyMultirange
        | crate::backend::parser::SqlTypeKind::AnyCompatible
        | crate::backend::parser::SqlTypeKind::AnyCompatibleArray
        | crate::backend::parser::SqlTypeKind::AnyCompatibleRange
        | crate::backend::parser::SqlTypeKind::AnyCompatibleMultirange
        | crate::backend::parser::SqlTypeKind::AnyEnum => None,
        crate::backend::parser::SqlTypeKind::Record
        | crate::backend::parser::SqlTypeKind::Composite => None,
        crate::backend::parser::SqlTypeKind::Internal => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::Void => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::Trigger => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::FdwHandler => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::Name => Some(64 + crate::include::varatt::VARHDRSZ),
        crate::backend::parser::SqlTypeKind::InternalChar => Some(2),
        crate::backend::parser::SqlTypeKind::Date
        | crate::backend::parser::SqlTypeKind::Time
        | crate::backend::parser::SqlTypeKind::TimeTz
        | crate::backend::parser::SqlTypeKind::Timestamp
        | crate::backend::parser::SqlTypeKind::TimestampTz
        | crate::backend::parser::SqlTypeKind::Uuid => Some(column.storage.attlen as usize),
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
        | crate::backend::parser::SqlTypeKind::Enum
        | crate::backend::parser::SqlTypeKind::Int2
        | crate::backend::parser::SqlTypeKind::Int4
        | crate::backend::parser::SqlTypeKind::Int8
        | crate::backend::parser::SqlTypeKind::Money
        | crate::backend::parser::SqlTypeKind::Oid
        | crate::backend::parser::SqlTypeKind::RegProc
        | crate::backend::parser::SqlTypeKind::RegClass
        | crate::backend::parser::SqlTypeKind::RegType
        | crate::backend::parser::SqlTypeKind::RegRole
        | crate::backend::parser::SqlTypeKind::RegNamespace
        | crate::backend::parser::SqlTypeKind::RegOper
        | crate::backend::parser::SqlTypeKind::RegOperator
        | crate::backend::parser::SqlTypeKind::RegProcedure
        | crate::backend::parser::SqlTypeKind::RegCollation
        | crate::backend::parser::SqlTypeKind::Xid
        | crate::backend::parser::SqlTypeKind::PgLsn
        | crate::backend::parser::SqlTypeKind::MacAddr
        | crate::backend::parser::SqlTypeKind::MacAddr8
        | crate::backend::parser::SqlTypeKind::Point
        | crate::backend::parser::SqlTypeKind::Lseg
        | crate::backend::parser::SqlTypeKind::Line
        | crate::backend::parser::SqlTypeKind::Box
        | crate::backend::parser::SqlTypeKind::Circle
        | crate::backend::parser::SqlTypeKind::Float4
        | crate::backend::parser::SqlTypeKind::Float8 => Some(column.storage.attlen as usize),
        crate::backend::parser::SqlTypeKind::Int2Vector
        | crate::backend::parser::SqlTypeKind::Tid
        | crate::backend::parser::SqlTypeKind::OidVector
        | crate::backend::parser::SqlTypeKind::Bytea
        | crate::backend::parser::SqlTypeKind::Inet
        | crate::backend::parser::SqlTypeKind::Cidr
        | crate::backend::parser::SqlTypeKind::Interval
        | crate::backend::parser::SqlTypeKind::Path
        | crate::backend::parser::SqlTypeKind::Polygon
        | crate::backend::parser::SqlTypeKind::Numeric
        | crate::backend::parser::SqlTypeKind::Json
        | crate::backend::parser::SqlTypeKind::Jsonb
        | crate::backend::parser::SqlTypeKind::JsonPath
        | crate::backend::parser::SqlTypeKind::Xml
        | crate::backend::parser::SqlTypeKind::Text
        | crate::backend::parser::SqlTypeKind::PgNodeTree
        | crate::backend::parser::SqlTypeKind::TsVector
        | crate::backend::parser::SqlTypeKind::TsQuery => None,
        crate::backend::parser::SqlTypeKind::Range
        | crate::backend::parser::SqlTypeKind::Int4Range
        | crate::backend::parser::SqlTypeKind::Int8Range
        | crate::backend::parser::SqlTypeKind::NumericRange
        | crate::backend::parser::SqlTypeKind::DateRange
        | crate::backend::parser::SqlTypeKind::TimestampRange
        | crate::backend::parser::SqlTypeKind::TimestampTzRange => {
            unreachable!("range handled above")
        }
        crate::backend::parser::SqlTypeKind::Multirange => {
            unreachable!("multirange handled above")
        }
        crate::backend::parser::SqlTypeKind::RegConfig
        | crate::backend::parser::SqlTypeKind::RegDictionary => {
            Some(column.storage.attlen as usize)
        }
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

pub(crate) fn toast_relation_desc() -> RelationDesc {
    let mut chunk_id = column_desc("chunk_id", SqlType::new(SqlTypeKind::Oid), false);
    chunk_id.storage.attstorage = AttributeStorage::Plain;
    chunk_id.storage.attcompression = AttributeCompression::Default;

    let mut chunk_seq = column_desc("chunk_seq", SqlType::new(SqlTypeKind::Int4), false);
    chunk_seq.storage.attstorage = AttributeStorage::Plain;
    chunk_seq.storage.attcompression = AttributeCompression::Default;

    let mut chunk_data = column_desc("chunk_data", SqlType::new(SqlTypeKind::Bytea), false);
    chunk_data.storage.attstorage = AttributeStorage::Plain;
    chunk_data.storage.attcompression = AttributeCompression::Default;

    RelationDesc {
        columns: vec![chunk_id, chunk_seq, chunk_data],
    }
}

pub fn new_relation_create_toast_table(
    catalog: &mut Catalog,
    relation_oid: u32,
    toast_namespace_name: &str,
    toast_namespace_oid: u32,
) -> Result<Option<ToastCatalogChanges>, CatalogError> {
    let Some((parent_name, parent)) = catalog
        .entries()
        .find(|(_, entry)| entry.relation_oid == relation_oid)
        .map(|(name, entry)| (name.to_string(), entry.clone()))
    else {
        return Err(CatalogError::UnknownTable(relation_oid.to_string()));
    };

    if parent.relkind != 'r'
        || parent.reltoastrelid != 0
        || !relation_needs_toast_table(&parent.desc)
    {
        return Ok(None);
    }

    let toast_name = format!(
        "{toast_namespace_name}.{}",
        toast_relation_name(relation_oid)
    );
    let toast_entry = catalog.create_table_with_relkind(
        toast_name.clone(),
        toast_relation_desc(),
        toast_namespace_oid,
        parent.rel.db_oid,
        parent.relpersistence,
        't',
        parent.owner_oid,
    )?;
    catalog.add_depend_row(PgDependRow {
        classid: PG_CLASS_RELATION_OID,
        objid: toast_entry.relation_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_INTERNAL,
    });

    let index_name = format!("{toast_namespace_name}.{}", toast_index_name(relation_oid));
    let toast_index = catalog.create_index_for_relation_with_options(
        index_name.clone(),
        toast_entry.relation_oid,
        true,
        &[
            IndexColumnDef::from("chunk_id"),
            IndexColumnDef::from("chunk_seq"),
        ],
        &CatalogIndexBuildOptions {
            am_oid: BTREE_AM_OID,
            indclass: vec![OID_BTREE_OPCLASS_OID, INT4_BTREE_OPCLASS_OID],
            indcollation: vec![0, 0],
            indoption: vec![0, 0],
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            brin_options: None,
            gin_options: None,
            hash_options: None,
        },
    )?;
    let (_index_name, _old_index, index_entry) =
        catalog.set_index_ready_valid(toast_index.relation_oid, true, true)?;
    let (_updated_name, old_parent, new_parent) =
        catalog.set_relation_toast_relid(relation_oid, toast_entry.relation_oid)?;

    Ok(Some(ToastCatalogChanges {
        parent_name,
        old_parent,
        new_parent,
        toast_name,
        toast_entry,
        index_name,
        index_entry,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::Catalog;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::catalog::{
        DEPENDENCY_INTERNAL, PG_CLASS_RELATION_OID, PG_TOAST_NAMESPACE_OID,
    };

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

    #[test]
    fn new_relation_create_toast_table_creates_heap_and_index() {
        let mut catalog = Catalog::default();
        let table = catalog
            .create_table(
                "docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let changes = new_relation_create_toast_table(
            &mut catalog,
            table.relation_oid,
            PG_TOAST_NAMESPACE,
            PG_TOAST_NAMESPACE_OID,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            changes.new_parent.reltoastrelid,
            changes.toast_entry.relation_oid
        );
        assert_eq!(changes.toast_entry.relkind, 't');
        assert_eq!(changes.toast_entry.namespace_oid, PG_TOAST_NAMESPACE_OID);
        assert_eq!(
            changes.index_entry.index_meta.as_ref().map(|meta| (
                meta.indkey.clone(),
                meta.indisunique,
                meta.indisready,
                meta.indisvalid
            )),
            Some((vec![1, 2], true, true, true))
        );
        assert!(catalog.depend_rows().iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == changes.toast_entry.relation_oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.deptype == DEPENDENCY_INTERNAL
        }));
    }

    #[test]
    fn new_relation_create_toast_table_uses_supplied_namespace() {
        let mut catalog = Catalog::default();
        let table = catalog
            .create_table_with_options(
                "pg_temp_1.docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
                0x7000_0001,
                0x7000_0001,
                't',
                crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            )
            .unwrap();

        let changes = new_relation_create_toast_table(
            &mut catalog,
            table.relation_oid,
            "pg_toast_temp_1",
            0x7800_0001,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            changes.new_parent.reltoastrelid,
            changes.toast_entry.relation_oid
        );
        assert_eq!(changes.toast_entry.relkind, 't');
        assert_eq!(changes.toast_entry.namespace_oid, 0x7800_0001);
        assert!(changes.toast_name.starts_with("pg_toast_temp_1."));
    }
}
