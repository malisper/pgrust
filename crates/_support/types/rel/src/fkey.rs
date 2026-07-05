use ::types_core::{AttrNumber, Oid, INDEX_MAX_KEYS};

// ForeignKeyCacheInfo (utils/rel.h): rd_fkeylist element. Cached slice is
// shared read-only; callers needing a mutable copy clone the element.
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
