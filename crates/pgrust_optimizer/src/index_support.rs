use pgrust_catalog_data::{
    ANYELEMENTOID, ANYMULTIRANGEOID, ANYOID, ANYRANGEOID, BIT_TYPE_OID, CIDR_TYPE_OID,
    INET_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID, VARBIT_TYPE_OID,
    builtin_range_spec_by_multirange_oid, builtin_range_spec_by_oid,
    builtin_scalar_function_for_proc_oid, sql_type_oid,
};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::relcache::{IndexAmOpEntry, IndexRelCacheEntry};

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
                ANYOID | pgrust_catalog_data::ANYARRAYOID | ANYRANGEOID | ANYMULTIRANGEOID
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

pub fn index_amproc_oid(
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

pub fn index_amop_strategy_for_operator(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('s'),
        |entry| entry.operator_oid == operator_oid,
    )
}

pub fn index_amop_ordering_strategy_for_operator(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('o'),
        |entry| entry.operator_oid == operator_oid,
    )
    .map(normalize_ordering_strategy)
}

pub fn index_amop_strategy_for_proc(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_proc_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('s'),
        |entry| proc_oids_match(entry.operator_proc_oid, operator_proc_oid),
    )
}

pub fn index_amop_ordering_strategy_for_proc(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    operator_proc_oid: u32,
    right_type_oid: Option<u32>,
) -> Option<u16> {
    index_amop_strategy_matching(
        index,
        desc,
        column_index,
        right_type_oid,
        Some('o'),
        |entry| proc_oids_match(entry.operator_proc_oid, operator_proc_oid),
    )
    .map(normalize_ordering_strategy)
}

fn index_amop_strategy_matching(
    index: &IndexRelCacheEntry,
    desc: &RelationDesc,
    column_index: usize,
    right_type_oid: Option<u32>,
    purpose: Option<char>,
    predicate: impl Fn(&IndexAmOpEntry) -> bool,
) -> Option<u16> {
    let left_type_oid = index_indexed_operator_type_oid(index, desc, column_index);
    let mut best: Option<(u8, i16)> = None;
    for entry in index.amop_entries.get(column_index)?.iter() {
        if purpose.is_some_and(|purpose| entry.purpose != purpose) || !predicate(entry) {
            continue;
        }
        let Some(score) = index_type_match_score(
            entry.lefttype,
            entry.righttype,
            left_type_oid,
            right_type_oid,
        ) else {
            continue;
        };
        if best.is_none_or(|(best_score, _)| score > best_score) {
            best = Some((score, entry.strategy));
        }
    }
    best.and_then(|(_, strategy)| u16::try_from(strategy).ok())
}

fn proc_oids_match(left: u32, right: u32) -> bool {
    left == right
        || builtin_scalar_function_for_proc_oid(left)
            .zip(builtin_scalar_function_for_proc_oid(right))
            .is_some_and(|(left, right)| left == right)
}

fn normalize_ordering_strategy(strategy: u16) -> u16 {
    if strategy == 15 { 1 } else { strategy }
}
