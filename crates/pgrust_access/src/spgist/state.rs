use pgrust_catalog_data::{
    ANYELEMENTOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID, BIT_TYPE_OID, CIDR_TYPE_OID,
    INET_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, VARBIT_TYPE_OID,
    builtin_range_spec_by_multirange_oid, builtin_range_spec_by_oid, sql_type_oid,
};
use pgrust_nodes::datum::{GeoBox, Value};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::relcache::IndexRelCacheEntry;

use crate::access::scankey::ScanKeyData;
use crate::access::spgist::{
    SPGIST_CHOOSE_PROC, SPGIST_CONFIG_PROC, SPGIST_INNER_CONSISTENT_PROC,
    SPGIST_LEAF_CONSISTENT_PROC, SPGIST_PICKSPLIT_PROC,
};
use crate::{AccessError, AccessScalarServices};

use super::support::{self, SpgistConfigResult};

#[derive(Debug, Clone)]
pub(crate) struct SpgistColumnState {
    pub(crate) config_proc: u32,
    pub(crate) choose_proc: u32,
    pub(crate) picksplit_proc: u32,
    pub(crate) inner_consistent_proc: u32,
    pub(crate) leaf_consistent_proc: u32,
}

#[derive(Clone)]
pub(crate) struct SpgistState<'a> {
    pub(crate) columns: Vec<SpgistColumnState>,
    scalar: &'a dyn AccessScalarServices,
}

impl<'a> SpgistState<'a> {
    pub(crate) fn new(
        desc: &RelationDesc,
        index_meta: &IndexRelCacheEntry,
        scalar: &'a dyn AccessScalarServices,
    ) -> Result<Self, AccessError> {
        let key_count = usize::try_from(index_meta.indnkeyatts)
            .unwrap_or_default()
            .min(desc.columns.len());
        if key_count == 0 {
            return Err(AccessError::Corrupt("SP-GiST key column state missing"));
        }
        let mut columns = Vec::with_capacity(key_count);
        for column_index in 0..key_count {
            let config_proc = index_amproc_oid(index_meta, desc, column_index, SPGIST_CONFIG_PROC)
                .ok_or(AccessError::Corrupt("missing SP-GiST config support proc"))?;
            let choose_proc = index_amproc_oid(index_meta, desc, column_index, SPGIST_CHOOSE_PROC)
                .ok_or(AccessError::Corrupt("missing SP-GiST choose support proc"))?;
            let picksplit_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_PICKSPLIT_PROC).ok_or(
                    AccessError::Corrupt("missing SP-GiST picksplit support proc"),
                )?;
            let inner_consistent_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_INNER_CONSISTENT_PROC)
                    .ok_or(AccessError::Corrupt(
                        "missing SP-GiST inner consistent support proc",
                    ))?;
            let leaf_consistent_proc =
                index_amproc_oid(index_meta, desc, column_index, SPGIST_LEAF_CONSISTENT_PROC)
                    .ok_or(AccessError::Corrupt(
                        "missing SP-GiST leaf consistent support proc",
                    ))?;
            let config = support::config(config_proc)?;
            if !config.can_return_data {
                return Err(AccessError::Corrupt(
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
        Ok(Self { columns, scalar })
    }

    pub(crate) fn config(&self, column_index: usize) -> Result<SpgistConfigResult, AccessError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(AccessError::Corrupt("SP-GiST column state missing"))?;
        support::config(column.config_proc)
    }

    pub(crate) fn choose(
        &self,
        column_index: usize,
        centroid: &Value,
        leaf: &Value,
    ) -> Result<u8, AccessError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(AccessError::Corrupt("SP-GiST column state missing"))?;
        support::choose(column.choose_proc, centroid, leaf)
    }

    pub(crate) fn picksplit(
        &self,
        column_index: usize,
        values: &[Value],
    ) -> Result<Option<(GeoBox, Vec<u8>)>, AccessError> {
        let column = self
            .columns
            .get(column_index)
            .ok_or(AccessError::Corrupt("SP-GiST column state missing"))?;
        support::picksplit(column.picksplit_proc, values)
    }

    pub(crate) fn leaf_consistent(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
    ) -> Result<bool, AccessError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(AccessError::Corrupt("spgist scan key attno out of range"))?;
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
            .ok_or(AccessError::Corrupt("SP-GiST column state missing"))?;
        support::leaf_consistent(
            column.leaf_consistent_proc,
            key.strategy,
            tuple_value,
            &key.argument,
            self.scalar,
        )
    }

    pub(crate) fn order_distance(
        &self,
        tuple_values: &[Value],
        key: &ScanKeyData,
    ) -> Result<Option<f64>, AccessError> {
        let attno = key.attribute_number.saturating_sub(1) as usize;
        let tuple_value = tuple_values
            .get(attno)
            .ok_or(AccessError::Corrupt("spgist order-by attno out of range"))?;
        let column = self
            .columns
            .get(attno)
            .ok_or(AccessError::Corrupt("SP-GiST column state missing"))?;
        let _ = support::inner_consistent(column.inner_consistent_proc, tuple_value, &[])?;
        support::order_distance(
            column.leaf_consistent_proc,
            tuple_value,
            &key.argument,
            self.scalar,
        )
    }
}

fn index_indexed_operator_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opcintype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .filter(|oid| {
            !matches!(
                *oid,
                pgrust_catalog_data::ANYOID
                    | pgrust_catalog_data::ANYARRAYOID
                    | pgrust_catalog_data::ANYRANGEOID
                    | pgrust_catalog_data::ANYMULTIRANGEOID
            )
        })
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_indexed_operand_type_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
) -> Option<u32> {
    index
        .opckeytype_oids
        .get(column_index)
        .copied()
        .filter(|oid| *oid != 0)
        .or_else(|| {
            desc.columns
                .get(column_index)
                .map(|column| sql_type_oid(column.sql_type))
        })
}

fn index_type_match_score(
    entry_lefttype: u32,
    entry_righttype: u32,
    left_type_oid: Option<u32>,
    right_type_oid: Option<u32>,
) -> Option<u8> {
    fn same_index_type_family(entry_type: u32, actual_type: u32) -> bool {
        matches!(
            (entry_type, actual_type),
            (INET_TYPE_OID | CIDR_TYPE_OID, INET_TYPE_OID | CIDR_TYPE_OID)
                | (
                    BIT_TYPE_OID | VARBIT_TYPE_OID,
                    BIT_TYPE_OID | VARBIT_TYPE_OID
                )
                | (
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID,
                    TIMESTAMP_TYPE_OID | TIMESTAMPTZ_TYPE_OID
                )
        )
    }

    fn component_score(entry_type: u32, actual_type: Option<u32>) -> Option<u8> {
        match actual_type {
            None => Some(0),
            Some(actual) if entry_type == actual => Some(4),
            Some(actual) if same_index_type_family(entry_type, actual) => Some(3),
            Some(_) if entry_type == ANYOID => Some(1),
            Some(actual)
                if entry_type == ANYRANGEOID && builtin_range_spec_by_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(actual)
                if entry_type == ANYMULTIRANGEOID
                    && builtin_range_spec_by_multirange_oid(actual).is_some() =>
            {
                Some(2)
            }
            Some(_) if entry_type == ANYELEMENTOID => Some(1),
            Some(_) => None,
        }
    }

    Some(
        component_score(entry_lefttype, left_type_oid)?
            + component_score(entry_righttype, right_type_oid)?,
    )
}

fn index_amproc_oid(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    procnum: i16,
) -> Option<u32> {
    let operand_type_oid = index_indexed_operand_type_oid(index, desc, column_index);
    let operator_type_oid = index_indexed_operator_type_oid(index, desc, column_index);
    let mut best: Option<(u8, u32)> = None;
    for entry in index.amproc_entries.get(column_index)?.iter() {
        if entry.procnum != procnum {
            continue;
        }
        let operand_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operand_type_oid,
            operand_type_oid,
        );
        let operator_score = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            operator_type_oid,
            operator_type_oid,
        );
        let Some(score) = operand_score.or(operator_score) else {
            continue;
        };
        if best.is_none_or(|(best_score, _)| score > best_score) {
            best = Some((score, entry.proc_oid));
        }
    }
    best.map(|(_, proc_oid)| proc_oid)
}
