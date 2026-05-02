use crate::backend::catalog::CatalogError;
use crate::backend::utils::cache::relcache::{IndexRelCacheEntry, index_amproc_oid};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::spgist::{
    SPGIST_CHOOSE_PROC, SPGIST_CONFIG_PROC, SPGIST_INNER_CONSISTENT_PROC,
    SPGIST_LEAF_CONSISTENT_PROC, SPGIST_PICKSPLIT_PROC,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

use super::support::{self, SpgistConfigResult};

#[derive(Debug, Clone)]
pub(crate) struct SpgistColumnState {
    pub(crate) config_proc: u32,
    pub(crate) choose_proc: u32,
    pub(crate) picksplit_proc: u32,
    pub(crate) inner_consistent_proc: u32,
    pub(crate) leaf_consistent_proc: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct SpgistState {
    pub(crate) columns: Vec<SpgistColumnState>,
}

impl SpgistState {
    pub(crate) fn new(
        desc: &RelationDesc,
        index_meta: &IndexRelCacheEntry,
    ) -> Result<Self, CatalogError> {
        let key_count = usize::try_from(index_meta.indnkeyatts)
            .unwrap_or_default()
            .min(desc.columns.len());
        if key_count == 0 {
            return Err(CatalogError::Corrupt("SP-GiST key column state missing"));
        }
        let mut columns = Vec::with_capacity(key_count);
        for column_index in 0..key_count {
            let config_proc = index_amproc_oid(index_meta, desc, column_index, SPGIST_CONFIG_PROC)
                .ok_or(CatalogError::Corrupt("missing SP-GiST config support proc"))?;
            let choose_proc = index_amproc_oid(index_meta, desc, column_index, SPGIST_CHOOSE_PROC)
                .ok_or(CatalogError::Corrupt("missing SP-GiST choose support proc"))?;
            let picksplit_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_PICKSPLIT_PROC).ok_or(
                    CatalogError::Corrupt("missing SP-GiST picksplit support proc"),
                )?;
            let inner_consistent_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_INNER_CONSISTENT_PROC)
                    .ok_or(CatalogError::Corrupt(
                        "missing SP-GiST inner consistent support proc",
                    ))?;
            let leaf_consistent_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_LEAF_CONSISTENT_PROC)
                    .ok_or(CatalogError::Corrupt(
                        "missing SP-GiST leaf consistent support proc",
                    ))?;
            let config = support::config(config_proc)?;
            if !config.can_return_data {
                return Err(CatalogError::Corrupt(
                    "SP-GiST box opclass must return index data",
                ));
            }
            columns.push(SpgistColumnState {
                config_proc,
                choose_proc,
                picksplit_proc,
                inner_consistent_proc,
                leaf_consistent_proc,
            });
        }
        Ok(Self { columns })
    }

    pub(crate) fn config(&self, column_index: usize) -> Result<SpgistConfigResult, CatalogError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(CatalogError::Corrupt("SP-GiST column state missing"))?;
        support::config(column.config_proc)
    }

    pub(crate) fn choose(
        &self,
        column_index: usize,
        centroid: &Value,
        leaf: &Value,
    ) -> Result<u8, CatalogError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(CatalogError::Corrupt("SP-GiST column state missing"))?;
        support::choose(column.choose_proc, centroid, leaf)
    }

    pub(crate) fn picksplit(
        &self,
        column_index: usize,
        values: &[Value],
    ) -> Result<Option<(crate::include::nodes::datum::GeoBox, Vec<u8>)>, CatalogError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(CatalogError::Corrupt("SP-GiST column state missing"))?;
        support::picksplit(column.picksplit_proc, values)
    }

    pub(crate) fn leaf_consistent(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
    ) -> Result<bool, CatalogError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(CatalogError::Corrupt("spgist scan key attno out of range"))?;
        if matches!(key.argument, Value::Null) {
            return match key.strategy {
                0 => Ok(matches!(tuple_value, Value::Null)),
                1 => Ok(!matches!(tuple_value, Value::Null)),
                _ => Ok(false),
            };
        }
        let column = self
            .columns
            .get(attno)
            .ok_or(CatalogError::Corrupt("SP-GiST column state missing"))?;
        support::leaf_consistent(
            column.leaf_consistent_proc,
            key.strategy,
            tuple_value,
            &key.argument,
        )
    }

    pub(crate) fn order_distance(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
    ) -> Result<Option<f64>, CatalogError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(CatalogError::Corrupt("spgist order-by attno out of range"))?;
        let column = self
            .columns
            .get(attno)
            .ok_or(CatalogError::Corrupt("SP-GiST column state missing"))?;
        let _ = support::inner_consistent(column.inner_consistent_proc, tuple_value, &[])?;
        support::order_distance(column.leaf_consistent_proc, tuple_value, &key.argument)
    }
}
