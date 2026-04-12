use crate::include::access::nbtree::{
    BT_EQUAL_STRATEGY_NUMBER, BT_GREATER_EQUAL_STRATEGY_NUMBER, BT_GREATER_STRATEGY_NUMBER,
    BT_LESS_EQUAL_STRATEGY_NUMBER, BT_LESS_STRATEGY_NUMBER,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtPreprocessedKey {
    pub attribute_number: i16,
    pub strategy: u16,
    pub argument: Value,
}

impl BtPreprocessedKey {
    pub fn matches_strategy_number(strategy: u16) -> bool {
        matches!(
            strategy,
            BT_LESS_STRATEGY_NUMBER
                | BT_LESS_EQUAL_STRATEGY_NUMBER
                | BT_EQUAL_STRATEGY_NUMBER
                | BT_GREATER_EQUAL_STRATEGY_NUMBER
                | BT_GREATER_STRATEGY_NUMBER
        )
    }
}

pub fn preprocess_scan_keys(keys: &[ScanKeyData]) -> Vec<BtPreprocessedKey> {
    let mut out: Vec<_> = keys
        .iter()
        .filter(|key| BtPreprocessedKey::matches_strategy_number(key.strategy))
        .map(|key| BtPreprocessedKey {
            attribute_number: key.attribute_number,
            strategy: key.strategy,
            argument: key.argument.clone(),
        })
        .collect();
    out.sort_by_key(|key| (key.attribute_number, key.strategy));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preprocess_filters_and_orders_keys() {
        let keys = vec![
            ScanKeyData {
                attribute_number: 2,
                strategy: 99,
                argument: Value::Int32(2),
            },
            ScanKeyData {
                attribute_number: 1,
                strategy: BT_EQUAL_STRATEGY_NUMBER,
                argument: Value::Int32(1),
            },
            ScanKeyData {
                attribute_number: 2,
                strategy: BT_GREATER_STRATEGY_NUMBER,
                argument: Value::Int32(2),
            },
        ];
        let preprocessed = preprocess_scan_keys(&keys);
        assert_eq!(preprocessed.len(), 2);
        assert_eq!(preprocessed[0].attribute_number, 1);
    }
}
