//! Publication vocabulary (`catalog/pg_publication.h`), trimmed to the items
//! the logical-replication protocol consumes.

/// `PublishGencolsType` (`catalog/pg_publication.h`): how generated columns
/// are handled in a publication when there is no column list. The values are
/// the catalog characters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PublishGencolsType {
    /// `PUBLISH_GENCOLS_NONE = 'n'` — generated columns are published only
    /// when present in a publication column list.
    None = b'n',
    /// `PUBLISH_GENCOLS_STORED = 's'` — stored generated columns are
    /// published even when there is no column list.
    Stored = b's',
}
