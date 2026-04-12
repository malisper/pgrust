use crate::include::nodes::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanKeyData {
    pub attribute_number: i16,
    pub strategy: u16,
    pub argument: Value,
}
