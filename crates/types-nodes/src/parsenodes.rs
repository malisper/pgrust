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

/// `ObjectType` (`nodes/parsenodes.h`). Discriminants mirror the C enum
/// order (implicit values 0..).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ObjectType {
    AccessMethod = 0,
    Aggregate = 1,
    Amop = 2,
    Amproc = 3,
    /// type's attribute, when distinct from column
    Attribute = 4,
    Cast = 5,
    Column = 6,
    Collation = 7,
    Conversion = 8,
    Database = 9,
    Default = 10,
    Defacl = 11,
    Domain = 12,
    Domconstraint = 13,
    EventTrigger = 14,
    Extension = 15,
    Fdw = 16,
    ForeignServer = 17,
    ForeignTable = 18,
    Function = 19,
    Index = 20,
    Language = 21,
    Largeobject = 22,
    Matview = 23,
    Opclass = 24,
    Operator = 25,
    Opfamily = 26,
    ParameterAcl = 27,
    Policy = 28,
    Procedure = 29,
    Publication = 30,
    PublicationNamespace = 31,
    PublicationRel = 32,
    Role = 33,
    Routine = 34,
    Rule = 35,
    Schema = 36,
    Sequence = 37,
    Subscription = 38,
    StatisticExt = 39,
    Tabconstraint = 40,
    Table = 41,
    Tablespace = 42,
    Transform = 43,
    Trigger = 44,
    TsConfiguration = 45,
    TsDictionary = 46,
    TsParser = 47,
    TsTemplate = 48,
    Type = 49,
    UserMapping = 50,
    View = 51,
}

pub use ObjectType::{
    AccessMethod as OBJECT_ACCESS_METHOD, Aggregate as OBJECT_AGGREGATE, Amop as OBJECT_AMOP,
    Amproc as OBJECT_AMPROC, Attribute as OBJECT_ATTRIBUTE, Cast as OBJECT_CAST,
    Collation as OBJECT_COLLATION, Column as OBJECT_COLUMN, Conversion as OBJECT_CONVERSION,
    Database as OBJECT_DATABASE, Default as OBJECT_DEFAULT, Defacl as OBJECT_DEFACL,
    Domain as OBJECT_DOMAIN, Domconstraint as OBJECT_DOMCONSTRAINT,
    EventTrigger as OBJECT_EVENT_TRIGGER, Extension as OBJECT_EXTENSION, Fdw as OBJECT_FDW,
    ForeignServer as OBJECT_FOREIGN_SERVER, ForeignTable as OBJECT_FOREIGN_TABLE,
    Function as OBJECT_FUNCTION, Index as OBJECT_INDEX, Language as OBJECT_LANGUAGE,
    Largeobject as OBJECT_LARGEOBJECT, Matview as OBJECT_MATVIEW, Opclass as OBJECT_OPCLASS,
    Operator as OBJECT_OPERATOR, Opfamily as OBJECT_OPFAMILY, ParameterAcl as OBJECT_PARAMETER_ACL,
    Policy as OBJECT_POLICY, Procedure as OBJECT_PROCEDURE, Publication as OBJECT_PUBLICATION,
    PublicationNamespace as OBJECT_PUBLICATION_NAMESPACE, PublicationRel as OBJECT_PUBLICATION_REL,
    Role as OBJECT_ROLE, Routine as OBJECT_ROUTINE, Rule as OBJECT_RULE, Schema as OBJECT_SCHEMA,
    Sequence as OBJECT_SEQUENCE, StatisticExt as OBJECT_STATISTIC_EXT,
    Subscription as OBJECT_SUBSCRIPTION, Tabconstraint as OBJECT_TABCONSTRAINT,
    Table as OBJECT_TABLE, Tablespace as OBJECT_TABLESPACE, Transform as OBJECT_TRANSFORM,
    Trigger as OBJECT_TRIGGER, TsConfiguration as OBJECT_TSCONFIGURATION,
    TsDictionary as OBJECT_TSDICTIONARY, TsParser as OBJECT_TSPARSER,
    TsTemplate as OBJECT_TSTEMPLATE, Type as OBJECT_TYPE, UserMapping as OBJECT_USER_MAPPING,
    View as OBJECT_VIEW,
};

/// `DropBehavior` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DropBehavior {
    /// `DROP_RESTRICT` — drop fails if any dependent objects.
    Restrict = 0,
    /// `DROP_CASCADE` — remove dependent objects too.
    Cascade = 1,
}

pub use DropBehavior::{Cascade as DROP_CASCADE, Restrict as DROP_RESTRICT};
