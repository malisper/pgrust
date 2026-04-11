#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelCacheEntry<K, V> {
    pub key: K,
    pub value: V,
}
