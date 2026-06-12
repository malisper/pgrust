//! Parse-node vocabulary (`nodes/primnodes.h` / `nodes/parsenodes.h`),
//! trimmed to the items ports currently consume.

use alloc::string::String;

/// `RangeVar` (`nodes/primnodes.h`) — a range variable, possibly qualified.
///
/// Trimmed: the `alias` (table alias node) and node-tag fields are not yet
/// carried; ports so far only read/write the name parts, `inh`, and
/// `relpersistence`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeVar {
    /// the catalog (database) name, or None
    pub catalogname: Option<String>,
    /// the schema name, or None
    pub schemaname: Option<String>,
    /// the relation/sequence name
    pub relname: Option<String>,
    /// expand rel by inheritance? recursively act on children?
    pub inh: bool,
    /// see `RELPERSISTENCE_*` in `pg_class.h`; the C `char` carried as `u8`.
    pub relpersistence: u8,
    /// token location, or -1 if unknown
    pub location: i32,
}

/// `makeRangeVar(schemaname, relname, location)` (`nodes/makefuncs.c`): a
/// permanent, inheritance-enabled RangeVar. Lives with the type, as the
/// canonical constructor.
pub fn makeRangeVar(
    schemaname: Option<String>,
    relname: Option<String>,
    location: i32,
) -> RangeVar {
    RangeVar {
        catalogname: None,
        schemaname,
        relname,
        inh: true,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location,
    }
}

/// `ObjectType` (`nodes/parsenodes.h`). Values mirror the C enum order.
pub type ObjectType = u32;

/// `OBJECT_SCHEMA`
pub const OBJECT_SCHEMA: ObjectType = 36;
/// `OBJECT_TABLE`
pub const OBJECT_TABLE: ObjectType = 41;

/// `DropBehavior` (`nodes/parsenodes.h`).
pub type DropBehavior = u32;

/// `DROP_RESTRICT` — drop fails if any dependent objects.
pub const DROP_RESTRICT: DropBehavior = 0;
/// `DROP_CASCADE` — remove dependent objects too.
pub const DROP_CASCADE: DropBehavior = 1;
