use crate::catalog::{Catalog, CatalogEntry, CatalogError, CatalogIndexBuildOptions, column_desc};
pub use pgrust_catalog_data::toasting::{
    PG_TOAST_NAMESPACE, toast_index_name, toast_relation_name,
};
use pgrust_catalog_data::{
    BTREE_AM_OID, DEPENDENCY_INTERNAL, INT4_BTREE_OPCLASS_OID, OID_BTREE_OPCLASS_OID,
    PG_CLASS_RELATION_OID, PgDependRow,
};
use pgrust_core::{AttributeCompression, AttributeStorage};
use pgrust_nodes::parsenodes::IndexColumnDef;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{SqlType, SqlTypeKind};

pub const MAXALIGN: usize = 8;
pub const SIZEOF_HEAP_TUPLE_HEADER: usize = 23;
pub const VARHDRSZ: usize = 4;
const BLCKSZ: usize = 8192;
const ITEM_ID_SIZE: usize = 4;
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;

const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> usize {
    let item_space = SIZE_OF_PAGE_HEADER_DATA + tuples_per_page * ITEM_ID_SIZE;
    let maxaligned = (item_space + (MAXALIGN - 1)) & !(MAXALIGN - 1);
    let available = BLCKSZ - maxaligned;
    available / tuples_per_page
}

pub const TOAST_TUPLE_THRESHOLD: usize = maximum_bytes_per_tuple(4);

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

fn type_maximum_size(column: &pgrust_nodes::primnodes::ColumnDesc) -> Option<usize> {
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
        pgrust_nodes::SqlTypeKind::AnyArray
        | pgrust_nodes::SqlTypeKind::AnyElement
        | pgrust_nodes::SqlTypeKind::AnyRange
        | pgrust_nodes::SqlTypeKind::AnyMultirange
        | pgrust_nodes::SqlTypeKind::AnyCompatible
        | pgrust_nodes::SqlTypeKind::AnyCompatibleArray
        | pgrust_nodes::SqlTypeKind::AnyCompatibleRange
        | pgrust_nodes::SqlTypeKind::AnyCompatibleMultirange
        | pgrust_nodes::SqlTypeKind::AnyEnum => None,
        pgrust_nodes::SqlTypeKind::Record | pgrust_nodes::SqlTypeKind::Composite => None,
        pgrust_nodes::SqlTypeKind::Internal => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Shell => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Cstring => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Void => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Trigger | pgrust_nodes::SqlTypeKind::EventTrigger => {
            Some(column.storage.attlen as usize)
        }
        pgrust_nodes::SqlTypeKind::FdwHandler => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Name => Some(64 + VARHDRSZ),
        pgrust_nodes::SqlTypeKind::InternalChar => Some(2),
        pgrust_nodes::SqlTypeKind::Date
        | pgrust_nodes::SqlTypeKind::Time
        | pgrust_nodes::SqlTypeKind::TimeTz
        | pgrust_nodes::SqlTypeKind::Timestamp
        | pgrust_nodes::SqlTypeKind::TimestampTz
        | pgrust_nodes::SqlTypeKind::Uuid => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Varchar | pgrust_nodes::SqlTypeKind::Char => {
            sql_type.char_len().map(|len| len as usize + VARHDRSZ)
        }
        pgrust_nodes::SqlTypeKind::Bit | pgrust_nodes::SqlTypeKind::VarBit => sql_type
            .bit_len()
            .map(|len| (len as usize).div_ceil(8) + VARHDRSZ),
        pgrust_nodes::SqlTypeKind::Bool
        | pgrust_nodes::SqlTypeKind::Enum
        | pgrust_nodes::SqlTypeKind::Int2
        | pgrust_nodes::SqlTypeKind::Int4
        | pgrust_nodes::SqlTypeKind::Int8
        | pgrust_nodes::SqlTypeKind::Money
        | pgrust_nodes::SqlTypeKind::Oid
        | pgrust_nodes::SqlTypeKind::RegProc
        | pgrust_nodes::SqlTypeKind::RegClass
        | pgrust_nodes::SqlTypeKind::RegType
        | pgrust_nodes::SqlTypeKind::RegRole
        | pgrust_nodes::SqlTypeKind::RegNamespace
        | pgrust_nodes::SqlTypeKind::RegOper
        | pgrust_nodes::SqlTypeKind::RegOperator
        | pgrust_nodes::SqlTypeKind::RegProcedure
        | pgrust_nodes::SqlTypeKind::RegCollation
        | pgrust_nodes::SqlTypeKind::Xid
        | pgrust_nodes::SqlTypeKind::PgLsn
        | pgrust_nodes::SqlTypeKind::MacAddr
        | pgrust_nodes::SqlTypeKind::MacAddr8
        | pgrust_nodes::SqlTypeKind::Point
        | pgrust_nodes::SqlTypeKind::Lseg
        | pgrust_nodes::SqlTypeKind::Line
        | pgrust_nodes::SqlTypeKind::Box
        | pgrust_nodes::SqlTypeKind::Circle
        | pgrust_nodes::SqlTypeKind::Float4
        | pgrust_nodes::SqlTypeKind::Float8 => Some(column.storage.attlen as usize),
        pgrust_nodes::SqlTypeKind::Int2Vector
        | pgrust_nodes::SqlTypeKind::Tid
        | pgrust_nodes::SqlTypeKind::OidVector
        | pgrust_nodes::SqlTypeKind::Bytea
        | pgrust_nodes::SqlTypeKind::Inet
        | pgrust_nodes::SqlTypeKind::Cidr
        | pgrust_nodes::SqlTypeKind::Interval
        | pgrust_nodes::SqlTypeKind::Path
        | pgrust_nodes::SqlTypeKind::Polygon
        | pgrust_nodes::SqlTypeKind::Numeric
        | pgrust_nodes::SqlTypeKind::Json
        | pgrust_nodes::SqlTypeKind::Jsonb
        | pgrust_nodes::SqlTypeKind::JsonPath
        | pgrust_nodes::SqlTypeKind::Xml
        | pgrust_nodes::SqlTypeKind::Text
        | pgrust_nodes::SqlTypeKind::PgNodeTree
        | pgrust_nodes::SqlTypeKind::TsVector
        | pgrust_nodes::SqlTypeKind::TsQuery => None,
        pgrust_nodes::SqlTypeKind::Range
        | pgrust_nodes::SqlTypeKind::Int4Range
        | pgrust_nodes::SqlTypeKind::Int8Range
        | pgrust_nodes::SqlTypeKind::NumericRange
        | pgrust_nodes::SqlTypeKind::DateRange
        | pgrust_nodes::SqlTypeKind::TimestampRange
        | pgrust_nodes::SqlTypeKind::TimestampTzRange => {
            unreachable!("range handled above")
        }
        pgrust_nodes::SqlTypeKind::Multirange => {
            unreachable!("multirange handled above")
        }
        pgrust_nodes::SqlTypeKind::RegConfig | pgrust_nodes::SqlTypeKind::RegDictionary => {
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

pub fn toast_relation_desc() -> RelationDesc {
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
            indclass_options: vec![Vec::new(), Vec::new()],
            indcollation: vec![0, 0],
            indoption: vec![0, 0],
            reloptions: None,
            indnullsnotdistinct: false,
            indisexclusion: false,
            indimmediate: true,
            btree_options: None,
            brin_options: None,
            gist_options: None,
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
