#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SysCacheKind {
    NamespaceByName,
    NamespaceByOid,
    ClassByName,
    ClassByOid,
    TypeByName,
    TypeByOid,
}
