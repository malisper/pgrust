#![allow(dead_code, unused_imports, unused_variables)]

mod compat;
pub mod error;
#[path = "backend/mod.rs"]
mod expr_backend;
pub mod services;
pub mod varatt;

pub use compat::include::access::htup::{HeapTuple, TupleValue};
pub use error::{ExprError, ExprResult, RegexError};
pub use expr_backend::executor::*;
pub use expr_backend::utils::misc::guc_datetime::{
    DateOrder, DateStyleFormat, DateTimeConfig, IntervalStyle, default_datetime_config,
};
pub use expr_backend::utils::misc::guc_xml::{
    XmlBinaryFormat, XmlConfig, XmlOptionSetting, format_xmlbinary, format_xmloption,
    parse_xmlbinary, parse_xmloption,
};
pub use expr_backend::{access, executor, libpq, tsearch, utils};
pub use services::{
    BoundRelation, DomainConstraintLookup, DomainConstraintLookupKind, DomainLookup,
    ExprCatalogLookup, ExprServices, clear_services, current_services, with_expr_services,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteaOutputFormat {
    Hex,
    Escape,
}
