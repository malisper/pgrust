#![allow(dead_code, unused_imports, unused_variables)]

#[path = "backend/mod.rs"]
pub mod expr_backend;
pub mod backend {
    pub use crate::expr_backend::*;
}
pub mod compat;
pub mod error;
pub mod services;
pub mod varatt;

pub use error::{ExprError, ExprResult, RegexError};
pub use expr_backend::executor::*;
pub use expr_backend::utils::misc::guc_datetime::{
    DateOrder, DateStyleFormat, DateTimeConfig, IntervalStyle, default_datetime_config,
};
pub use expr_backend::utils::misc::guc_xml::{
    XmlBinaryFormat, XmlConfig, XmlOptionSetting, format_xmlbinary, format_xmloption,
    parse_xmlbinary, parse_xmloption,
};
pub use services::{
    BoundRelation, DomainConstraintLookup, DomainConstraintLookupKind, DomainLookup,
    ExprCatalogLookup, ExprServices, clear_services, current_services, with_expr_services,
};
