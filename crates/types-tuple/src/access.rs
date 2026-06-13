//! Access-layer relation vocabulary: relkind/relpersistence bytes
//! (`catalog/pg_class.h`) and `RangeVar` (`nodes/primnodes.h`). Lock modes
//! live in `types-storage` (`storage/lockdefs.h`); the ephemeral-named-
//! relation types live in `types-nodes` (`utils/queryenvironment.h`), next
//! to the `Tuplestorestate` payload they carry.

/* ----------------------------------------------------------------
 * catalog/pg_class.h: relkind / relpersistence vocabulary
 * ---------------------------------------------------------------- */

pub const RELKIND_RELATION: u8 = b'r';
pub const RELKIND_INDEX: u8 = b'i';
pub const RELKIND_SEQUENCE: u8 = b'S';
pub const RELKIND_TOASTVALUE: u8 = b't';
pub const RELKIND_VIEW: u8 = b'v';
pub const RELKIND_MATVIEW: u8 = b'm';
pub const RELKIND_COMPOSITE_TYPE: u8 = b'c';
pub const RELKIND_FOREIGN_TABLE: u8 = b'f';
pub const RELKIND_PARTITIONED_TABLE: u8 = b'p';
pub const RELKIND_PARTITIONED_INDEX: u8 = b'I';

pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
pub const RELPERSISTENCE_TEMP: u8 = b't';

/* `pg_class.relreplident` values (`catalog/pg_class.h`). */
pub const REPLICA_IDENTITY_DEFAULT: u8 = b'd';
pub const REPLICA_IDENTITY_NOTHING: u8 = b'n';
pub const REPLICA_IDENTITY_FULL: u8 = b'f';
pub const REPLICA_IDENTITY_INDEX: u8 = b'i';

/* `pg_attribute.attgenerated` values (`catalog/pg_attribute.h`). */
pub const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
pub const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

/* ----------------------------------------------------------------
 * nodes/primnodes.h: RangeVar
 * ---------------------------------------------------------------- */

/// `RangeVar` (`nodes/primnodes.h`) — a qualified relation name as written in
/// a query, trimmed to the fields the relation-open paths consume. The C
/// `char *` name fields become owned strings (`relname` is never NULL in a
/// well-formed parse node; `catalogname`/`schemaname` may be).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RangeVar {
    /// the catalog (database) name, or `None`
    pub catalogname: Option<alloc::string::String>,
    /// the schema name, or `None`
    pub schemaname: Option<alloc::string::String>,
    /// the relation/sequence name
    pub relname: alloc::string::String,
    /// expand rel by inheritance? recursively act on children?
    pub inh: bool,
    /// see `RELPERSISTENCE_*`
    pub relpersistence: u8,
    /// token location, or -1 if unknown
    pub location: i32,
}
