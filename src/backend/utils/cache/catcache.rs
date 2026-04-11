#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatCacheEntry<K, V> {
    pub key: K,
    pub value: V,
}
