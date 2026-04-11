#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogNamespace {
    PgCatalog,
    Public,
    Temp,
}
