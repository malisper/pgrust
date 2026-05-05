use std::cell::RefCell;
use std::collections::BTreeMap;

use pgrust_catalog_data::*;
use pgrust_core::CompactString;
use pgrust_nodes::datum::{IndirectVarlenaValue, RecordDescriptor, Value};
use pgrust_nodes::parsenodes::{Query, SqlType};
use pgrust_nodes::primnodes::RelationDesc;

use crate::error::{ExprError, ExprResult};
use crate::expr_backend::utils::misc::guc_datetime::DateTimeConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRelation {
    pub relation_oid: u32,
    pub oid: Option<u32>,
    pub name: String,
    pub relkind: char,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainConstraintLookupKind {
    Check,
    NotNull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainConstraintLookup {
    pub name: String,
    pub kind: DomainConstraintLookupKind,
    pub expr: Option<String>,
    pub enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainLookup {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub sql_type: SqlType,
    pub not_null: bool,
    pub check: Option<String>,
    pub constraints: Vec<DomainConstraintLookup>,
}

pub trait ExprCatalogLookup: Send + Sync {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        None
    }

    fn lookup_relation_by_oid(&self, _relation_oid: u32) -> Option<BoundRelation> {
        None
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.lookup_relation_by_oid(relation_oid)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        bootstrap_pg_class_rows()
            .into_iter()
            .find(|row| row.oid == relation_oid)
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        bootstrap_pg_authid_rows()
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        bootstrap_pg_namespace_rows().to_vec()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        self.namespace_rows().into_iter().find(|row| row.oid == oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        bootstrap_pg_proc_rows_by_name(name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        bootstrap_pg_proc_row_by_oid(oid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        let normalized = normalize_catalog_lookup_name(name);
        bootstrap_pg_operator_rows().into_iter().find(|row| {
            row.oprname.eq_ignore_ascii_case(normalized)
                && row.oprleft == left_type_oid
                && row.oprright == right_type_oid
        })
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        bootstrap_pg_operator_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        bootstrap_pg_operator_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        bootstrap_pg_collation_rows().to_vec()
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        bootstrap_pg_ts_config_rows().to_vec()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        bootstrap_pg_ts_dict_rows().to_vec()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        bootstrap_pg_ts_config_map_rows()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        builtin_type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.type_rows().into_iter().find(|row| row.oid == oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        let normalized = normalize_catalog_lookup_name(name);
        self.type_rows()
            .into_iter()
            .find(|row| row.typname.eq_ignore_ascii_case(normalized))
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        Some(sql_type_oid(sql_type))
    }

    fn domain_by_type_oid(&self, _domain_oid: u32) -> Option<DomainLookup> {
        None
    }

    fn enum_label_oid(&self, _type_oid: u32, _label: &str) -> Option<u32> {
        None
    }

    fn enum_label(&self, _type_oid: u32, _label_oid: u32) -> Option<String> {
        None
    }

    fn enum_label_by_oid(&self, label_oid: u32) -> Option<String> {
        bootstrap_pg_enum_rows()
            .into_iter()
            .find(|row| row.oid == label_oid)
            .map(|row| row.enumlabel)
    }

    fn enum_label_is_committed(&self, _type_oid: u32, _label_oid: u32) -> bool {
        true
    }

    fn domain_allowed_enum_label_oids(&self, _domain_oid: u32) -> Option<Vec<u32>> {
        None
    }

    fn domain_check_name(&self, _domain_oid: u32) -> Option<String> {
        None
    }
}

pub trait ExprServices: Sync {
    fn datetime_config(&self) -> DateTimeConfig {
        DateTimeConfig::default()
    }

    fn push_warning(&self, _message: String) {}

    fn register_anonymous_record_descriptor(&self, _descriptor: &RecordDescriptor) {}

    fn lookup_anonymous_record_descriptor(&self, _typmod: i32) -> RecordDescriptor {
        RecordDescriptor::anonymous(Vec::new(), _typmod)
    }

    fn indirect_varlena_to_value(
        &self,
        indirect: &IndirectVarlenaValue,
    ) -> ExprResult<Option<Value>> {
        let _ = indirect;
        Ok(None)
    }

    fn stored_view_query_for_rule(&self, _rewrite_oid: u32) -> Option<Query> {
        None
    }
}

struct DefaultServices;

impl ExprServices for DefaultServices {}

static DEFAULT_SERVICES: DefaultServices = DefaultServices;

thread_local! {
    static SERVICE_STACK: RefCell<Vec<&'static dyn ExprServices>> = const { RefCell::new(Vec::new()) };
}

pub fn with_expr_services<T>(services: &'static dyn ExprServices, f: impl FnOnce() -> T) -> T {
    SERVICE_STACK.with(|stack| stack.borrow_mut().push(services));
    let result = f();
    SERVICE_STACK.with(|stack| {
        let popped = stack.borrow_mut().pop();
        debug_assert!(popped.is_some());
    });
    result
}

pub fn clear_services() {
    SERVICE_STACK.with(|stack| stack.borrow_mut().clear());
}

pub fn current_services() -> &'static dyn ExprServices {
    SERVICE_STACK.with(|stack| stack.borrow().last().copied().unwrap_or(&DEFAULT_SERVICES))
}

pub fn normalize_catalog_lookup_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

pub fn anonymous_record_descriptor(typmod: i32) -> RecordDescriptor {
    current_services().lookup_anonymous_record_descriptor(typmod)
}

pub fn register_record_descriptor(descriptor: &RecordDescriptor) {
    current_services().register_anonymous_record_descriptor(descriptor);
}
