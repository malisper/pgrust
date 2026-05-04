use serde::{Deserialize, Serialize};

use pgrust_core::{PgPartitionedTableRow, RelFileLocator};

use crate::access::{BrinOptions, BtreeOptions, GinOptions, GistOptions, HashOptions};
use crate::partition::LoweredPartitionSpec;
use crate::primnodes::Expr;
use crate::primnodes::RelationDesc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexAmOpEntry {
    pub strategy: i16,
    pub purpose: char,
    pub lefttype: u32,
    pub righttype: u32,
    pub operator_oid: u32,
    pub operator_proc_oid: u32,
    pub sortfamily_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexAmProcEntry {
    pub procnum: i16,
    pub lefttype: u32,
    pub righttype: u32,
    pub proc_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRelCacheEntry {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub am_oid: u32,
    pub am_handler_oid: Option<u32>,
    pub indkey: Vec<i16>,
    pub indclass: Vec<u32>,
    #[serde(default)]
    pub indclass_options: Vec<Vec<(String, String)>>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub opfamily_oids: Vec<u32>,
    pub opcintype_oids: Vec<u32>,
    pub opckeytype_oids: Vec<u32>,
    pub amop_entries: Vec<Vec<IndexAmOpEntry>>,
    pub amproc_entries: Vec<Vec<IndexAmProcEntry>>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
    #[serde(skip)]
    pub rd_indexprs: Option<Vec<Expr>>,
    #[serde(skip)]
    pub rd_indpred: Option<Option<Expr>>,
    pub btree_options: Option<BtreeOptions>,
    pub brin_options: Option<BrinOptions>,
    #[serde(default)]
    pub gist_options: Option<GistOptions>,
    pub gin_options: Option<GinOptions>,
    pub hash_options: Option<HashOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelCacheEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub of_type_oid: u32,
    pub row_type_oid: u32,
    pub array_type_oid: u32,
    pub reltoastrelid: u32,
    pub relhasindex: bool,
    pub relpersistence: char,
    pub relkind: char,
    pub relispartition: bool,
    pub relispopulated: bool,
    pub relpartbound: Option<String>,
    pub relhastriggers: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub desc: RelationDesc,
    pub partitioned_table: Option<PgPartitionedTableRow>,
    pub partition_spec: Option<LoweredPartitionSpec>,
    pub index: Option<IndexRelCacheEntry>,
}
