use ::types_core::{AttrNumber, Oid, INDEX_MAX_KEYS};

// rd_fkeylist element; the cached slice is shared read-only (clone to mutate).
#[derive(Clone, Debug)]
pub struct ForeignKeyCacheInfo {
    pub conoid: Oid,
    pub conrelid: Oid,
    pub confrelid: Oid,
    pub nkeys: i32,
    pub conenforced: bool,
    pub conkey: [AttrNumber; INDEX_MAX_KEYS as usize],
    pub confkey: [AttrNumber; INDEX_MAX_KEYS as usize],
    pub conpfeqop: [Oid; INDEX_MAX_KEYS as usize],
}
