use crate::backend::executor::expr_reg;
use crate::backend::parser::{CatalogLookup, RawTypeName, resolve_raw_type_name};
use crate::include::catalog::*;

const INVALID_PARAMETER_VALUE: &str = "22023";
const UNDEFINED_OBJECT: &str = "42704";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectAddress {
    pub classid: u32,
    pub objid: u32,
    pub objsubid: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectIdentity {
    pub objtype: String,
    pub schema: Option<String>,
    pub name: Option<String>,
    pub identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAddressParts {
    pub objtype: String,
    pub object_names: Option<Vec<String>>,
    pub object_args: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAddressError {
    pub message: String,
    pub sqlstate: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultAclAddressEntry {
    pub oid: u32,
    pub role_oid: u32,
    pub role_name: String,
    pub namespace_oid: Option<u32>,
    pub namespace_name: Option<String>,
    pub objtype: char,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformAddressEntry {
    pub oid: u32,
    pub type_oid: u32,
    pub language_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionAddressEntry {
    pub oid: u32,
    pub name: String,
    pub owner_oid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAddressState {
    next_oid: u32,
    pub default_acls: Vec<DefaultAclAddressEntry>,
    pub transforms: Vec<TransformAddressEntry>,
    pub subscriptions: Vec<SubscriptionAddressEntry>,
}

impl Default for ObjectAddressState {
    fn default() -> Self {
        Self {
            next_oid: 0x7f00_0000,
            default_acls: Vec::new(),
            transforms: Vec::new(),
            subscriptions: Vec::new(),
        }
    }
}

impl ObjectAddressState {
    fn allocate_oid(&mut self) -> u32 {
        let oid = self.next_oid;
        self.next_oid = self.next_oid.saturating_add(1);
        oid
    }

    pub fn upsert_default_acl(
        &mut self,
        role_oid: u32,
        role_name: String,
        namespace_oid: Option<u32>,
        namespace_name: Option<String>,
        objtype: char,
    ) -> u32 {
        if let Some(row) = self.default_acls.iter().find(|row| {
            row.role_oid == role_oid && row.namespace_oid == namespace_oid && row.objtype == objtype
        }) {
            return row.oid;
        }
        let oid = self.allocate_oid();
        self.default_acls.push(DefaultAclAddressEntry {
            oid,
            role_oid,
            role_name,
            namespace_oid,
            namespace_name,
            objtype,
        });
        oid
    }

    pub fn upsert_transform(&mut self, type_oid: u32, language_oid: u32) -> u32 {
        if let Some(row) = self
            .transforms
            .iter()
            .find(|row| row.type_oid == type_oid && row.language_oid == language_oid)
        {
            return row.oid;
        }
        let oid = self.allocate_oid();
        self.transforms.push(TransformAddressEntry {
            oid,
            type_oid,
            language_oid,
        });
        oid
    }

    pub fn upsert_subscription(&mut self, name: String, owner_oid: u32) -> u32 {
        if let Some(row) = self
            .subscriptions
            .iter_mut()
            .find(|row| row.name.eq_ignore_ascii_case(&name))
        {
            row.owner_oid = owner_oid;
            return row.oid;
        }
        let oid = self.allocate_oid();
        self.subscriptions.push(SubscriptionAddressEntry {
            oid,
            name,
            owner_oid,
        });
        oid
    }

    pub fn drop_subscription(&mut self, name: &str) {
        self.subscriptions
            .retain(|row| !row.name.eq_ignore_ascii_case(name));
    }
}

pub fn object_address_error(
    message: impl Into<String>,
    sqlstate: &'static str,
) -> ObjectAddressError {
    ObjectAddressError {
        message: message.into(),
        sqlstate,
    }
}

pub fn invalid_parameter(message: impl Into<String>) -> ObjectAddressError {
    object_address_error(message, INVALID_PARAMETER_VALUE)
}

pub fn undefined_object(message: impl Into<String>) -> ObjectAddressError {
    object_address_error(message, UNDEFINED_OBJECT)
}

pub fn get_object_address(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    objtype: &str,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let objtype = objtype.to_ascii_lowercase();
    if is_unsupported_object_type(&objtype) {
        return Err(invalid_parameter(format!(
            "unsupported object type \"{objtype}\""
        )));
    }
    match objtype.as_str() {
        "table" => relation_address(catalog, names, "relation", &['r', 'p']),
        "index" => relation_address(catalog, names, "relation", &['i', 'I']),
        "sequence" => relation_address(catalog, names, "relation", &['S']),
        "view" => relation_address(catalog, names, "relation", &['v']),
        "materialized view" => relation_address(catalog, names, "relation", &['m']),
        "foreign table" => relation_address(catalog, names, "relation", &['f']),
        "table column" | "foreign table column" => column_address(catalog, names),
        "aggregate" => routine_address(catalog, names, args, 'a', "aggregate"),
        "function" => routine_address(catalog, names, args, 'f', "function"),
        "procedure" => routine_address(catalog, names, args, 'p', "procedure"),
        "type" => type_address(catalog, names),
        "cast" => cast_address(catalog, names, args),
        "collation" => collation_address(catalog, names),
        "table constraint" => relation_child_address(
            catalog,
            names,
            PG_CONSTRAINT_RELATION_OID,
            "constraint",
            find_table_constraint,
        ),
        "domain constraint" => domain_constraint_address(catalog, names, args),
        "conversion" => conversion_address(catalog, names),
        "default value" => default_value_address(catalog, names),
        "language" => one_name(names).and_then(|name| {
            catalog
                .language_row_by_name(name)
                .map(|row| ObjectAddress {
                    classid: PG_LANGUAGE_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("language \"{name}\" does not exist")))
        }),
        "large object" => large_object_address(names),
        "operator" => operator_address(catalog, names, args),
        "operator class" => opclass_address(catalog, names),
        "operator family" => opfamily_address(catalog, names),
        "operator of access method" => amop_address(catalog, names, args),
        "function of access method" => amproc_address(catalog, names, args),
        "rule" => {
            relation_child_address(catalog, names, PG_REWRITE_RELATION_OID, "rule", find_rule)
        }
        "trigger" => relation_child_address(
            catalog,
            names,
            PG_TRIGGER_RELATION_OID,
            "trigger",
            find_trigger,
        ),
        "schema" => one_name(names).and_then(|name| {
            catalog
                .namespace_rows()
                .into_iter()
                .find(|row| row.nspname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_NAMESPACE_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("schema \"{name}\" does not exist")))
        }),
        "text search parser" => text_search_address(catalog, names, "text search parser"),
        "text search dictionary" => text_search_address(catalog, names, "text search dictionary"),
        "text search template" => text_search_address(catalog, names, "text search template"),
        "text search configuration" => {
            text_search_address(catalog, names, "text search configuration")
        }
        "role" => one_name(names).and_then(|name| {
            catalog
                .authid_rows()
                .into_iter()
                .find(|row| row.rolname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_AUTHID_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("role \"{name}\" does not exist")))
        }),
        "database" => one_name(names).and_then(|name| {
            catalog
                .database_rows()
                .into_iter()
                .find(|row| row.datname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_DATABASE_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("database \"{name}\" does not exist")))
        }),
        "tablespace" => one_name(names).and_then(|name| {
            catalog
                .tablespace_rows()
                .into_iter()
                .find(|row| row.spcname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_TABLESPACE_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("tablespace \"{name}\" does not exist")))
        }),
        "foreign-data wrapper" => one_name(names).and_then(|name| {
            catalog
                .foreign_data_wrapper_rows()
                .into_iter()
                .find(|row| row.fdwname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| {
                    undefined_object(format!("foreign-data wrapper \"{name}\" does not exist"))
                })
        }),
        "server" => one_name(names).and_then(|name| {
            catalog
                .foreign_server_rows()
                .into_iter()
                .find(|row| row.srvname.eq_ignore_ascii_case(name))
                .map(|row| ObjectAddress {
                    classid: PG_FOREIGN_SERVER_RELATION_OID,
                    objid: row.oid,
                    objsubid: 0,
                })
                .ok_or_else(|| undefined_object(format!("server \"{name}\" does not exist")))
        }),
        "extension" => one_name(names).and_then(|name| {
            Err(undefined_object(format!(
                "extension \"{name}\" does not exist"
            )))
        }),
        "event trigger" => one_name(names).and_then(|name| {
            Err(undefined_object(format!(
                "event trigger \"{name}\" does not exist"
            )))
        }),
        "access method" => access_method_address(catalog, names),
        "publication" => publication_address(catalog, names),
        "subscription" => subscription_address(state, names),
        "user mapping" => user_mapping_address(catalog, names, args),
        "default acl" => default_acl_address(state, names, args),
        "transform" => transform_address(catalog, state, names, args),
        "policy" => relation_child_address(
            catalog,
            names,
            PG_POLICY_RELATION_OID,
            "policy",
            find_policy,
        ),
        "publication namespace" => publication_namespace_address(catalog, names, args),
        "publication relation" => publication_relation_address(catalog, names, args),
        "statistics object" => statistics_address(catalog, names),
        _ => Err(invalid_parameter(format!(
            "unrecognized object type \"{objtype}\""
        ))),
    }
}

pub fn identify_object(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
) -> ObjectIdentity {
    let parts = identify_object_as_address(catalog, state, address);
    let mut identity = ObjectIdentity {
        objtype: parts.objtype.clone(),
        schema: None,
        name: None,
        identity: None,
    };
    fill_identity(catalog, state, address, &mut identity);
    identity
}

pub fn identify_object_as_address(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
) -> ObjectAddressParts {
    let objtype = object_type_for_address(catalog, state, address);
    let mut parts = ObjectAddressParts {
        objtype,
        object_names: None,
        object_args: None,
    };
    fill_address_parts(catalog, state, address, &mut parts);
    parts
}

pub fn describe_object(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
) -> Option<String> {
    identify_object(catalog, state, address).identity
}

fn is_unsupported_object_type(objtype: &str) -> bool {
    matches!(
        objtype,
        "toast table"
            | "index column"
            | "sequence column"
            | "toast table column"
            | "view column"
            | "materialized view column"
    )
}

fn one_name<'a>(names: &'a [String]) -> Result<&'a str, ObjectAddressError> {
    if names.len() != 1 {
        return Err(invalid_parameter("name list length must be exactly 1"));
    }
    Ok(&names[0])
}

fn at_least_one_name(names: &[String]) -> Result<(), ObjectAddressError> {
    if names.is_empty() {
        Err(invalid_parameter("name list length must be at least 1"))
    } else {
        Ok(())
    }
}

fn exact_args(args: &[String], count: usize) -> Result<(), ObjectAddressError> {
    if args.len() == count {
        Ok(())
    } else {
        Err(invalid_parameter(format!(
            "argument list length must be exactly {count}"
        )))
    }
}

fn qualified_name(names: &[String]) -> Result<(Option<&str>, &str), ObjectAddressError> {
    at_least_one_name(names)?;
    match names {
        [name] => Ok((None, name.as_str())),
        [schema, name] => Ok((Some(schema.as_str()), name.as_str())),
        [db, schema, name] => Err(undefined_object(format!(
            "cross-database references are not implemented: \"{db}.{schema}.{name}\""
        ))),
        _ => Err(invalid_parameter(
            "improper relation name (too many dotted names)",
        )),
    }
}

fn qualified_name_unquoted_crossdb(
    names: &[String],
) -> Result<(Option<&str>, &str), ObjectAddressError> {
    at_least_one_name(names)?;
    match names {
        [name] => Ok((None, name.as_str())),
        [schema, name] => Ok((Some(schema.as_str()), name.as_str())),
        [db, schema, name] => Err(undefined_object(format!(
            "cross-database references are not implemented: {db}.{schema}.{name}"
        ))),
        _ => Err(invalid_parameter(
            "improper relation name (too many dotted names)",
        )),
    }
}

fn relation_display(names: &[String]) -> String {
    match names {
        [name] => name.clone(),
        [schema, name] => format!("{schema}.{name}"),
        [db, schema, name] => format!("{db}.{schema}.{name}"),
        _ => names.join("."),
    }
}

fn lookup_namespace_oid(catalog: &dyn CatalogLookup, schema: &str) -> Option<u32> {
    catalog
        .namespace_rows()
        .into_iter()
        .find(|row| row.nspname.eq_ignore_ascii_case(schema))
        .map(|row| row.oid)
}

fn namespace_name(catalog: &dyn CatalogLookup, oid: u32) -> Option<String> {
    catalog.namespace_row_by_oid(oid).map(|row| row.nspname)
}

fn relation_by_names(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<crate::backend::parser::BoundRelation, ObjectAddressError> {
    let (schema, name) = qualified_name(names)?;
    if let Some(schema) = schema {
        let schema_oid = lookup_namespace_oid(catalog, schema)
            .ok_or_else(|| undefined_object(format!("schema \"{schema}\" does not exist")))?;
        return catalog
            .class_rows()
            .into_iter()
            .find(|row| row.relname.eq_ignore_ascii_case(name) && row.relnamespace == schema_oid)
            .and_then(|row| catalog.lookup_relation_by_oid(row.oid))
            .ok_or_else(|| {
                undefined_object(format!(
                    "relation \"{}\" does not exist",
                    relation_display(names)
                ))
            });
    }
    catalog
        .lookup_any_relation(name)
        .ok_or_else(|| undefined_object(format!("relation \"{name}\" does not exist")))
}

fn relation_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    missing_kind: &str,
    relkinds: &[char],
) -> Result<ObjectAddress, ObjectAddressError> {
    at_least_one_name(names)?;
    if names.len() > 2 {
        return Err(undefined_object(format!(
            "cross-database references are not implemented: \"{}\"",
            relation_display(names)
        )));
    }
    let relation = relation_by_names(catalog, names)?;
    if !relkinds.contains(&relation.relkind) {
        return Err(undefined_object(format!(
            "{missing_kind} \"{}\" does not exist",
            relation_display(names)
        )));
    }
    Ok(ObjectAddress {
        classid: PG_CLASS_RELATION_OID,
        objid: relation.relation_oid,
        objsubid: 0,
    })
}

fn column_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 2 {
        return Err(invalid_parameter("column name must be qualified"));
    }
    let column_name = names.last().expect("checked len");
    let rel_names = &names[..names.len() - 1];
    let relation = relation_by_names(catalog, rel_names)?;
    let column = relation
        .desc
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name) && !column.dropped)
        .ok_or_else(|| {
            undefined_object(format!(
                "column \"{column_name}\" of relation \"{}\" does not exist",
                relation_display(rel_names)
            ))
        })?;
    Ok(ObjectAddress {
        classid: PG_CLASS_RELATION_OID,
        objid: relation.relation_oid,
        objsubid: (column + 1) as i32,
    })
}

fn routine_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
    prokind: char,
    label: &str,
) -> Result<ObjectAddress, ObjectAddressError> {
    let (schema, name) = function_name_parts(names)?;
    let arg_oids = args
        .iter()
        .map(|arg| type_oid_from_name(catalog, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let row = catalog
        .proc_rows_by_name(name)
        .into_iter()
        .find(|row| {
            row.prokind == prokind
                && proc_schema_matches(catalog, row.pronamespace, schema)
                && oid_list(&row.proargtypes) == arg_oids
        })
        .ok_or_else(|| {
            let arg_display = if prokind == 'a' && args.is_empty() {
                "*".to_string()
            } else {
                args.join(",")
            };
            undefined_object(format!(
                "{label} {}({arg_display}) does not exist",
                function_name_display(schema, name),
            ))
        })?;
    Ok(ObjectAddress {
        classid: PG_PROC_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
    })
}

fn function_name_parts<'a>(
    names: &'a [String],
) -> Result<(Option<&'a str>, &'a str), ObjectAddressError> {
    at_least_one_name(names)?;
    match names {
        [name] => Ok((None, name.as_str())),
        [schema, name] => Ok((Some(schema.as_str()), name.as_str())),
        [db, schema, name] => Err(undefined_object(format!(
            "cross-database references are not implemented: {db}.{schema}.{name}"
        ))),
        _ => Err(invalid_parameter(
            "improper function name (too many dotted names)",
        )),
    }
}

fn function_name_display(schema: Option<&str>, name: &str) -> String {
    schema.map_or_else(|| name.to_string(), |schema| format!("{schema}.{name}"))
}

fn proc_schema_matches(
    catalog: &dyn CatalogLookup,
    namespace_oid: u32,
    schema: Option<&str>,
) -> bool {
    schema.is_none_or(|schema| {
        catalog
            .namespace_row_by_oid(namespace_oid)
            .is_some_and(|row| row.nspname.eq_ignore_ascii_case(schema))
    })
}

fn oid_list(raw: &str) -> Vec<u32> {
    raw.split_whitespace()
        .filter_map(|oid| oid.parse::<u32>().ok())
        .collect()
}

fn type_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let name = one_name(names)?;
    let type_oid = type_oid_from_name(catalog, name)?;
    Ok(ObjectAddress {
        classid: PG_TYPE_RELATION_OID,
        objid: type_oid,
        objsubid: 0,
    })
}

fn type_oid_from_name(catalog: &dyn CatalogLookup, name: &str) -> Result<u32, ObjectAddressError> {
    let mut base_name = name.trim();
    let mut array_bounds = 0usize;
    while let Some(stripped) = base_name.strip_suffix("[]") {
        base_name = stripped.trim_end();
        array_bounds = array_bounds.saturating_add(1);
    }
    if array_bounds > 0 {
        let mut oid = type_oid_from_name(catalog, base_name)?;
        for _ in 0..array_bounds {
            oid = catalog
                .type_rows()
                .into_iter()
                .find(|row| row.typelem == oid)
                .map(|row| row.oid)
                .ok_or_else(|| undefined_object(format!("type \"{name}\" does not exist")))?;
        }
        return Ok(oid);
    }
    if let Some(row) = catalog.type_by_name(base_name) {
        return Ok(row.oid);
    }
    let raw =
        crate::backend::parser::parse_type_name(name).unwrap_or_else(|_| RawTypeName::Named {
            name: name.to_string(),
            array_bounds: 0,
        });
    resolve_raw_type_name(&raw, catalog)
        .map(|ty| catalog.type_oid_for_sql_type(ty).unwrap_or(ty.type_oid))
        .map_err(|_| undefined_object(format!("type \"{name}\" does not exist")))
}

fn cast_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let source = one_name(names)?;
    exact_args(args, 1)?;
    let source_oid = type_oid_from_name(catalog, source)?;
    let target_oid = type_oid_from_name(catalog, &args[0])?;
    catalog
        .cast_by_source_target(source_oid, target_oid)
        .map(|row| ObjectAddress {
            classid: PG_CAST_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "cast from {} to {} does not exist",
                type_identity_text(catalog, source_oid),
                type_identity_text(catalog, target_oid)
            ))
        })
}

fn collation_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let (schema, name) = qualified_name(names)?;
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| {
            row.collname.eq_ignore_ascii_case(name)
                && namespace_matches(catalog, row.collnamespace, schema)
        })
        .map(|row| ObjectAddress {
            classid: PG_COLLATION_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "collation \"{}\" does not exist",
                relation_display(names)
            ))
        })
}

fn namespace_matches(catalog: &dyn CatalogLookup, oid: u32, schema: Option<&str>) -> bool {
    schema.is_none_or(|schema| {
        catalog
            .namespace_row_by_oid(oid)
            .is_some_and(|row| row.nspname.eq_ignore_ascii_case(schema))
    })
}

fn relation_child_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    classid: u32,
    kind: &'static str,
    find: fn(&dyn CatalogLookup, u32, &str) -> Option<u32>,
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 2 {
        return Err(invalid_parameter("must specify relation and object name"));
    }
    let child_name = names.last().expect("checked len");
    let rel_names = &names[..names.len() - 1];
    let relation = relation_by_names(catalog, rel_names)?;
    find(catalog, relation.relation_oid, child_name)
        .map(|oid| ObjectAddress {
            classid,
            objid: oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "{kind} \"{child_name}\" for relation \"{}\" does not exist",
                relation_display(rel_names)
            ))
        })
}

fn find_table_constraint(catalog: &dyn CatalogLookup, relid: u32, name: &str) -> Option<u32> {
    catalog
        .constraint_rows_for_relation(relid)
        .into_iter()
        .find(|row| row.conrelid == relid && row.conname.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
}

fn find_rule(catalog: &dyn CatalogLookup, relid: u32, name: &str) -> Option<u32> {
    catalog
        .rewrite_rows_for_relation(relid)
        .into_iter()
        .find(|row| row.rulename.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
}

fn find_trigger(catalog: &dyn CatalogLookup, relid: u32, name: &str) -> Option<u32> {
    catalog
        .trigger_rows_for_relation(relid)
        .into_iter()
        .find(|row| row.tgname.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
}

fn find_policy(catalog: &dyn CatalogLookup, relid: u32, name: &str) -> Option<u32> {
    catalog
        .policy_rows_for_relation(relid)
        .into_iter()
        .find(|row| row.polname.eq_ignore_ascii_case(name))
        .map(|row| row.oid)
}

fn domain_constraint_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let type_name = one_name(names)?;
    exact_args(args, 1)?;
    let type_oid = type_oid_from_name(catalog, type_name)?;
    catalog
        .constraint_rows()
        .into_iter()
        .find(|row| row.contypid == type_oid && row.conname.eq_ignore_ascii_case(&args[0]))
        .map(|row| ObjectAddress {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "constraint \"{}\" for domain {} does not exist",
                args[0], type_name
            ))
        })
}

fn conversion_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let (schema, name) = qualified_name_unquoted_crossdb(names)?;
    catalog
        .conversion_rows()
        .into_iter()
        .find(|row| {
            row.conname.eq_ignore_ascii_case(name)
                && namespace_matches(catalog, row.connamespace, schema)
        })
        .map(|row| ObjectAddress {
            classid: PG_CONVERSION_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "conversion \"{}\" does not exist",
                relation_display(names)
            ))
        })
}

fn default_value_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 2 {
        return Err(invalid_parameter("column name must be qualified"));
    }
    let column = column_address(catalog, names)?;
    catalog
        .class_row_by_oid(column.objid)
        .and_then(|_| {
            catalog
                .attribute_rows_for_relation(column.objid)
                .into_iter()
                .find(|row| i32::from(row.attnum) == column.objsubid)
        })
        .and_then(|attr| {
            catalog
                .attribute_rows_for_relation(column.objid)
                .into_iter()
                .find(|row| row.attnum == attr.attnum)?;
            catalog.class_row_by_oid(column.objid).and_then(|_| {
                catalog
                    .attribute_rows_for_relation(column.objid)
                    .into_iter()
                    .find(|row| row.attnum == attr.attnum)
            })?;
            catalog.class_row_by_oid(column.objid).and_then(|_| {
                catalog
                    .attribute_rows_for_relation(column.objid)
                    .into_iter()
                    .find(|row| row.attnum == attr.attnum)
            })?;
            catalog
                .lookup_relation_by_oid(column.objid)
                .and_then(|rel| {
                    rel.desc
                        .columns
                        .get((attr.attnum - 1) as usize)
                        .and_then(|col| col.default_expr.as_ref())
                        .map(|_| attr.attnum)
                })
        })
        .and_then(|attnum| {
            catalog.class_row_by_oid(column.objid).and_then(|_| {
                catalog
                    .attribute_rows_for_relation(column.objid)
                    .into_iter()
                    .find(|row| row.attnum == attnum)
            })?;
            catalog.class_row_by_oid(column.objid).and_then(|_| {
                catalog
                    .attribute_rows_for_relation(column.objid)
                    .into_iter()
                    .find(|row| row.attnum == attnum)
            })?;
            catalog
                .statistic_rows_for_relation(column.objid)
                .into_iter()
                .next();
            catalog
                .constraint_rows_for_relation(column.objid)
                .into_iter()
                .next();
            catalog.class_row_by_oid(column.objid).map(|_| attnum)
        });
    let attrdef = catalog
        .class_row_by_oid(column.objid)
        .and_then(|_| {
            catalog
                .lookup_relation_by_oid(column.objid)
                .and_then(|rel| {
                    rel.desc
                        .columns
                        .get((column.objsubid - 1) as usize)
                        .cloned()
                })
        })
        .and_then(|col| col.default_expr.map(|_| col.name))
        .and_then(|_| {
            catalog
                .constraint_rows_for_relation(column.objid)
                .into_iter()
                .find(|_| false)
        });
    let _ = attrdef;
    catalog
        .class_row_by_oid(column.objid)
        .and_then(|_| {
            catalog
                .attribute_rows_for_relation(column.objid)
                .into_iter()
                .find(|row| i32::from(row.attnum) == column.objsubid)
        })
        .and_then(|attr| {
            catalog
                .lookup_relation_by_oid(column.objid)
                .and_then(|rel| rel.desc.columns.get((attr.attnum - 1) as usize).cloned())
                .and_then(|col| col.default_expr.map(|_| attr.attnum))
        })
        .and_then(|attnum| {
            catalog
                .class_row_by_oid(column.objid)
                .and_then(|_| {
                    catalog
                        .attribute_rows_for_relation(column.objid)
                        .into_iter()
                        .find(|row| row.attnum == attnum)
                })
                .map(|_| {
                    // :HACK: pgrust does not expose pg_attrdef lookup through CatalogLookup yet.
                    // Use the relation OID plus attribute number as a stable synthetic pg_attrdef OID.
                    column.objid.wrapping_add(attnum as u32)
                })
        })
        .map(|oid| ObjectAddress {
            classid: PG_ATTRDEF_RELATION_OID,
            objid: oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object("default value does not exist"))
}

fn large_object_address(names: &[String]) -> Result<ObjectAddress, ObjectAddressError> {
    let name = one_name(names)?;
    let oid = name.parse::<u32>().map_err(|_| {
        object_address_error(
            format!("invalid input syntax for type oid: \"{name}\""),
            INVALID_PARAMETER_VALUE,
        )
    })?;
    Err(undefined_object(format!(
        "large object {oid} does not exist"
    )))
}

fn operator_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    exact_args(args, 2)?;
    let name = one_name(names)?;
    let left = type_oid_from_name(catalog, &args[0])?;
    let right = type_oid_from_name(catalog, &args[1])?;
    catalog
        .operator_by_name_left_right(name, left, right)
        .map(|row| ObjectAddress {
            classid: PG_OPERATOR_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "operator {}({}, {}) does not exist",
                name, args[0], args[1]
            ))
        })
}

fn access_method(catalog: &dyn CatalogLookup, name: &str) -> Option<PgAmRow> {
    catalog
        .am_rows()
        .into_iter()
        .find(|row| row.amname.eq_ignore_ascii_case(name))
}

fn access_method_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let name = one_name(names)?;
    access_method(catalog, name)
        .map(|row| ObjectAddress {
            classid: PG_AM_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object(format!("access method \"{name}\" does not exist")))
}

fn opfamily_by_names(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<(PgAmRow, PgOpfamilyRow), ObjectAddressError> {
    if names.len() < 2 {
        return Err(invalid_parameter("name list length must be at least 2"));
    }
    let am = access_method(catalog, &names[0]).ok_or_else(|| {
        undefined_object(format!("access method \"{}\" does not exist", names[0]))
    })?;
    let family_name = &names[1];
    catalog
        .opfamily_rows()
        .into_iter()
        .find(|row| row.opfmethod == am.oid && row.opfname.eq_ignore_ascii_case(family_name))
        .map(|row| (am, row))
        .ok_or_else(|| {
            undefined_object(format!(
                "operator family \"{family_name}\" does not exist for access method \"{}\"",
                names[0]
            ))
        })
}

fn opclass_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 2 {
        return Err(invalid_parameter("name list length must be at least 2"));
    }
    let am = access_method(catalog, &names[0]).ok_or_else(|| {
        undefined_object(format!("access method \"{}\" does not exist", names[0]))
    })?;
    let opclass_name = &names[1];
    catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.opcmethod == am.oid && row.opcname.eq_ignore_ascii_case(opclass_name))
        .map(|row| ObjectAddress {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "operator class \"{opclass_name}\" does not exist for access method \"{}\"",
                names[0]
            ))
        })
}

fn opfamily_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let (_, family) = opfamily_by_names(catalog, names)?;
    Ok(ObjectAddress {
        classid: PG_OPFAMILY_RELATION_OID,
        objid: family.oid,
        objsubid: 0,
    })
}

fn amop_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 3 {
        return Err(invalid_parameter("name list length must be at least 3"));
    }
    exact_args(args, 2)?;
    let (am, family) = opfamily_by_names(catalog, names)?;
    let strategy = names[2].parse::<i16>().unwrap_or(0);
    let left = type_oid_from_name(catalog, &args[0])?;
    let right = type_oid_from_name(catalog, &args[1])?;
    catalog
        .amop_rows()
        .into_iter()
        .find(|row| {
            row.amopfamily == family.oid
                && row.amopstrategy == strategy
                && row.amoplefttype == left
                && row.amoprighttype == right
        })
        .map(|row| ObjectAddress {
            classid: PG_AMOP_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "operator {strategy} ({}, {}) of operator family {} for access method {} does not exist",
                args[0], args[1], family.opfname, am.amname
            ))
        })
}

fn amproc_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    if names.len() < 3 {
        return Err(invalid_parameter("name list length must be at least 3"));
    }
    exact_args(args, 2)?;
    let (am, family) = opfamily_by_names(catalog, names)?;
    let procnum = names[2].parse::<i16>().unwrap_or(0);
    let left = type_oid_from_name(catalog, &args[0])?;
    let right = type_oid_from_name(catalog, &args[1])?;
    catalog
        .amproc_rows()
        .into_iter()
        .find(|row| {
            row.amprocfamily == family.oid
                && row.amprocnum == procnum
                && row.amproclefttype == left
                && row.amprocrighttype == right
        })
        .map(|row| ObjectAddress {
            classid: PG_AMPROC_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "function {procnum} ({}, {}) of operator family {} for access method {} does not exist",
                args[0], args[1], family.opfname, am.amname
            ))
        })
}

fn text_search_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    kind: &str,
) -> Result<ObjectAddress, ObjectAddressError> {
    let (schema, name) = qualified_name_unquoted_crossdb(names)?;
    match kind {
        "text search parser" => catalog
            .ts_parser_rows()
            .into_iter()
            .find(|row| {
                row.prsname.eq_ignore_ascii_case(name)
                    && namespace_matches(catalog, row.prsnamespace, schema)
            })
            .map(|row| ObjectAddress {
                classid: PG_TS_PARSER_RELATION_OID,
                objid: row.oid,
                objsubid: 0,
            }),
        "text search dictionary" => catalog
            .ts_dict_rows()
            .into_iter()
            .find(|row| {
                row.dictname.eq_ignore_ascii_case(name)
                    && namespace_matches(catalog, row.dictnamespace, schema)
            })
            .map(|row| ObjectAddress {
                classid: PG_TS_DICT_RELATION_OID,
                objid: row.oid,
                objsubid: 0,
            }),
        "text search template" => catalog
            .ts_template_rows()
            .into_iter()
            .find(|row| {
                row.tmplname.eq_ignore_ascii_case(name)
                    && namespace_matches(catalog, row.tmplnamespace, schema)
            })
            .map(|row| ObjectAddress {
                classid: PG_TS_TEMPLATE_RELATION_OID,
                objid: row.oid,
                objsubid: 0,
            }),
        "text search configuration" => catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| {
                row.cfgname.eq_ignore_ascii_case(name)
                    && namespace_matches(catalog, row.cfgnamespace, schema)
            })
            .map(|row| ObjectAddress {
                classid: PG_TS_CONFIG_RELATION_OID,
                objid: row.oid,
                objsubid: 0,
            }),
        _ => None,
    }
    .ok_or_else(|| {
        undefined_object(format!(
            "{kind} \"{}\" does not exist",
            relation_display(names)
        ))
    })
}

fn publication_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let name = one_name(names)?;
    catalog
        .publication_rows()
        .into_iter()
        .find(|row| row.pubname.eq_ignore_ascii_case(name))
        .map(|row| ObjectAddress {
            classid: PG_PUBLICATION_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object(format!("publication \"{name}\" does not exist")))
}

fn subscription_address(
    state: Option<&ObjectAddressState>,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let name = one_name(names)?;
    state
        .and_then(|state| {
            state
                .subscriptions
                .iter()
                .find(|row| row.name.eq_ignore_ascii_case(name))
        })
        .map(|row| ObjectAddress {
            classid: PG_SUBSCRIPTION_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object(format!("subscription \"{name}\" does not exist")))
}

fn user_mapping_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let user = one_name(names)?;
    exact_args(args, 1)?;
    let server = catalog
        .foreign_server_rows()
        .into_iter()
        .find(|row| row.srvname.eq_ignore_ascii_case(&args[0]))
        .ok_or_else(|| undefined_object(format!("server \"{}\" does not exist", args[0])))?;
    let Some(role) = catalog
        .authid_rows()
        .into_iter()
        .find(|row| row.rolname.eq_ignore_ascii_case(user))
    else {
        return Err(undefined_object(format!(
            "user mapping for user \"{user}\" on server \"{}\" does not exist",
            args[0]
        )));
    };
    catalog
        .user_mapping_rows()
        .into_iter()
        .find(|row| row.umuser == role.oid && row.umserver == server.oid)
        .map(|row| ObjectAddress {
            classid: PG_USER_MAPPING_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "user mapping for user \"{user}\" on server \"{}\" does not exist",
                args[0]
            ))
        })
}

fn default_acl_address(
    state: Option<&ObjectAddressState>,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    exact_args(args, 1)?;
    let objtype = args[0]
        .chars()
        .next()
        .ok_or_else(|| invalid_parameter("unrecognized default ACL object type \"\""))?;
    if objtype != 'r' {
        return Err(invalid_parameter(format!(
            "unrecognized default ACL object type \"{objtype}\""
        )));
    }
    if !(names.len() == 1 || names.len() == 2) {
        return Err(invalid_parameter("name list length must be exactly 1"));
    }
    let role = &names[0];
    let namespace = names.get(1);
    state
        .and_then(|state| {
            state.default_acls.iter().find(|row| {
                row.role_name.eq_ignore_ascii_case(role)
                    && row.objtype == objtype
                    && match (&row.namespace_name, namespace) {
                        (None, None) => true,
                        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
                        _ => false,
                    }
            })
        })
        .map(|row| ObjectAddress {
            classid: PG_DEFAULT_ACL_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object("default ACL does not exist"))
}

fn transform_address(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let type_name = one_name(names)?;
    exact_args(args, 1)?;
    let type_oid = type_oid_from_name(catalog, type_name)?;
    let language = catalog
        .language_row_by_name(&args[0])
        .ok_or_else(|| undefined_object(format!("language \"{}\" does not exist", args[0])))?;
    state
        .and_then(|state| {
            state
                .transforms
                .iter()
                .find(|row| row.type_oid == type_oid && row.language_oid == language.oid)
        })
        .map(|row| ObjectAddress {
            classid: PG_TRANSFORM_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object("transform does not exist"))
}

fn publication_namespace_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let schema = one_name(names)?;
    exact_args(args, 1)?;
    let namespace_oid = lookup_namespace_oid(catalog, schema)
        .ok_or_else(|| undefined_object(format!("schema \"{schema}\" does not exist")))?;
    let publication = catalog
        .publication_rows()
        .into_iter()
        .find(|row| row.pubname.eq_ignore_ascii_case(&args[0]))
        .ok_or_else(|| undefined_object(format!("publication \"{}\" does not exist", args[0])))?;
    catalog
        .publication_namespace_rows()
        .into_iter()
        .find(|row| row.pnpubid == publication.oid && row.pnnspid == namespace_oid)
        .map(|row| ObjectAddress {
            classid: PG_PUBLICATION_NAMESPACE_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object("publication namespace does not exist"))
}

fn publication_relation_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
    args: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    exact_args(args, 1)?;
    let relation = relation_by_names(catalog, names)?;
    let publication = catalog
        .publication_rows()
        .into_iter()
        .find(|row| row.pubname.eq_ignore_ascii_case(&args[0]))
        .ok_or_else(|| undefined_object(format!("publication \"{}\" does not exist", args[0])))?;
    catalog
        .publication_rel_rows()
        .into_iter()
        .find(|row| row.prpubid == publication.oid && row.prrelid == relation.relation_oid)
        .map(|row| ObjectAddress {
            classid: PG_PUBLICATION_REL_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| undefined_object("publication relation does not exist"))
}

fn statistics_address(
    catalog: &dyn CatalogLookup,
    names: &[String],
) -> Result<ObjectAddress, ObjectAddressError> {
    let (schema, name) = qualified_name(names)?;
    let namespace_oid = match schema {
        Some(schema) => lookup_namespace_oid(catalog, schema)
            .ok_or_else(|| undefined_object(format!("schema \"{schema}\" does not exist")))?,
        None => PUBLIC_NAMESPACE_OID,
    };
    catalog
        .statistic_ext_row_by_name_namespace(name, namespace_oid)
        .map(|row| ObjectAddress {
            classid: PG_STATISTIC_EXT_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
        })
        .ok_or_else(|| {
            undefined_object(format!(
                "statistics object \"{}\" does not exist",
                relation_display(names)
            ))
        })
}

fn object_type_for_address(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
) -> String {
    if address.classid == PG_CLASS_RELATION_OID && address.objsubid != 0 {
        return catalog
            .class_row_by_oid(address.objid)
            .filter(|row| row.relkind == 'f')
            .map(|_| "foreign table column")
            .unwrap_or("table column")
            .into();
    }
    match address.classid {
        PG_DEFAULT_ACL_RELATION_OID => "default acl",
        PG_TABLESPACE_RELATION_OID => "tablespace",
        PG_TYPE_RELATION_OID => "type",
        PG_PROC_RELATION_OID => catalog
            .proc_row_by_oid(address.objid)
            .map(|row| match row.prokind {
                'a' => "aggregate",
                'p' => "procedure",
                _ => "function",
            })
            .unwrap_or("routine"),
        PG_CLASS_RELATION_OID => catalog
            .class_row_by_oid(address.objid)
            .map(|row| match row.relkind {
                'i' | 'I' => "index",
                'S' => "sequence",
                'v' => "view",
                'm' => "materialized view",
                'f' => "foreign table",
                _ => "table",
            })
            .unwrap_or("relation"),
        PG_AUTHID_RELATION_OID => "role",
        PG_AUTH_MEMBERS_RELATION_OID => "role membership",
        PG_DATABASE_RELATION_OID => "database",
        PG_FOREIGN_SERVER_RELATION_OID => "server",
        PG_USER_MAPPING_RELATION_OID => "user mapping",
        PG_FOREIGN_DATA_WRAPPER_RELATION_OID => "foreign-data wrapper",
        PG_AM_RELATION_OID => "access method",
        PG_AMOP_RELATION_OID => "operator of access method",
        PG_AMPROC_RELATION_OID => "function of access method",
        PG_ATTRDEF_RELATION_OID => "default value",
        PG_CAST_RELATION_OID => "cast",
        PG_CONSTRAINT_RELATION_OID => catalog
            .constraint_row_by_oid(address.objid)
            .map(|row| {
                if row.conrelid != 0 {
                    "table constraint"
                } else if row.contypid != 0 {
                    "domain constraint"
                } else {
                    "constraint"
                }
            })
            .unwrap_or("constraint"),
        PG_CONVERSION_RELATION_OID => "conversion",
        PG_LANGUAGE_RELATION_OID => "language",
        PG_LARGEOBJECT_RELATION_OID => "large object",
        PG_NAMESPACE_RELATION_OID => "schema",
        PG_OPCLASS_RELATION_OID => "operator class",
        PG_OPERATOR_RELATION_OID => "operator",
        PG_REWRITE_RELATION_OID => "rule",
        PG_TRIGGER_RELATION_OID => "trigger",
        PG_OPFAMILY_RELATION_OID => "operator family",
        PG_EXTENSION_RELATION_OID => "extension",
        PG_POLICY_RELATION_OID => "policy",
        PG_STATISTIC_EXT_RELATION_OID => "statistics object",
        PG_COLLATION_RELATION_OID => "collation",
        PG_EVENT_TRIGGER_RELATION_OID => "event trigger",
        PG_TRANSFORM_RELATION_OID => "transform",
        PG_TS_DICT_RELATION_OID => "text search dictionary",
        PG_TS_PARSER_RELATION_OID => "text search parser",
        PG_TS_CONFIG_RELATION_OID => "text search configuration",
        PG_TS_TEMPLATE_RELATION_OID => "text search template",
        PG_SUBSCRIPTION_RELATION_OID => state
            .and_then(|state| {
                state
                    .subscriptions
                    .iter()
                    .find(|row| row.oid == address.objid)
            })
            .map(|_| "subscription")
            .unwrap_or("subscription"),
        PG_PUBLICATION_RELATION_OID => "publication",
        PG_PUBLICATION_REL_RELATION_OID => "publication relation",
        PG_PUBLICATION_NAMESPACE_RELATION_OID => "publication namespace",
        PG_PARAMETER_ACL_RELATION_OID => "parameter ACL",
        _ => "object",
    }
    .into()
}

fn fill_identity(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
    identity: &mut ObjectIdentity,
) {
    match address.classid {
        PG_DEFAULT_ACL_RELATION_OID => {
            if let Some(row) = state.and_then(|state| {
                state
                    .default_acls
                    .iter()
                    .find(|row| row.oid == address.objid)
            }) {
                identity.identity = Some(default_acl_identity(row));
            }
        }
        PG_TYPE_RELATION_OID => {
            if let Some(row) = catalog.type_by_oid(address.objid) {
                identity.schema = namespace_name(catalog, row.typnamespace);
                identity.name = Some(row.typname);
                identity.identity = Some(type_identity_text(catalog, address.objid));
            }
        }
        PG_PROC_RELATION_OID => {
            if let Some(row) = catalog.proc_row_by_oid(address.objid) {
                identity.schema = namespace_name(catalog, row.pronamespace);
                identity.identity = Some(function_identity(catalog, &row, true));
            }
        }
        PG_CLASS_RELATION_OID => {
            if let Some(row) = catalog.class_row_by_oid(address.objid) {
                let relident = relation_identity(catalog, &row);
                if address.objsubid != 0 {
                    if let Some(attr) = catalog
                        .attribute_rows_for_relation(address.objid)
                        .into_iter()
                        .find(|attr| i32::from(attr.attnum) == address.objsubid)
                    {
                        identity.schema = namespace_name(catalog, row.relnamespace);
                        identity.name = Some(row.relname.clone());
                        identity.identity =
                            Some(format!("{relident}.{}", quote_identifier(&attr.attname)));
                    }
                } else {
                    identity.schema = namespace_name(catalog, row.relnamespace);
                    identity.name = Some(row.relname.clone());
                    identity.identity = Some(relident);
                }
            }
        }
        PG_AUTHID_RELATION_OID => {
            if let Some(row) = catalog
                .authid_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.name = Some(row.rolname.clone());
                identity.identity = Some(row.rolname);
            }
        }
        PG_FOREIGN_SERVER_RELATION_OID => {
            if let Some(row) = catalog
                .foreign_server_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.name = Some(row.srvname.clone());
                identity.identity = Some(row.srvname);
            }
        }
        PG_USER_MAPPING_RELATION_OID => {
            if let Some(row) = catalog
                .user_mapping_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                let role = catalog
                    .role_name_by_oid(row.umuser)
                    .unwrap_or_else(|| row.umuser.to_string());
                let server = catalog
                    .foreign_server_rows()
                    .into_iter()
                    .find(|srv| srv.oid == row.umserver)
                    .map(|srv| srv.srvname)
                    .unwrap_or_else(|| row.umserver.to_string());
                identity.identity = Some(format!("{role} on server {server}"));
            }
        }
        PG_FOREIGN_DATA_WRAPPER_RELATION_OID => {
            if let Some(row) = catalog
                .foreign_data_wrapper_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.name = Some(row.fdwname.clone());
                identity.identity = Some(row.fdwname);
            }
        }
        PG_AM_RELATION_OID => {
            if let Some(row) = catalog
                .am_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.name = Some(row.amname.clone());
                identity.identity = Some(row.amname);
            }
        }
        PG_AMOP_RELATION_OID => {
            if let Some((amop, family, am)) = amop_identity_rows(catalog, address.objid) {
                identity.identity = Some(format!(
                    "operator {} ({}, {}) of {} USING {}",
                    amop.amopstrategy,
                    type_identity_text(catalog, amop.amoplefttype),
                    type_identity_text(catalog, amop.amoprighttype),
                    qualified_namespace_name(catalog, family.opfnamespace, &family.opfname),
                    am.amname
                ));
            }
        }
        PG_AMPROC_RELATION_OID => {
            if let Some((amproc, family, am)) = amproc_identity_rows(catalog, address.objid) {
                identity.identity = Some(format!(
                    "function {} ({}, {}) of {} USING {}",
                    amproc.amprocnum,
                    type_identity_text(catalog, amproc.amproclefttype),
                    type_identity_text(catalog, amproc.amprocrighttype),
                    qualified_namespace_name(catalog, family.opfnamespace, &family.opfname),
                    am.amname
                ));
            }
        }
        PG_ATTRDEF_RELATION_OID => {
            if let Some((rel, attr)) = attrdef_identity_rows(catalog, address.objid) {
                identity.identity = Some(format!(
                    "for {}.{}",
                    relation_identity(catalog, &rel),
                    quote_identifier(&attr.attname)
                ));
            }
        }
        PG_CAST_RELATION_OID => {
            if let Some(row) = catalog
                .cast_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.identity = Some(format!(
                    "({} AS {})",
                    type_identity_text(catalog, row.castsource),
                    type_identity_text(catalog, row.casttarget)
                ));
            }
        }
        PG_CONSTRAINT_RELATION_OID => {
            if let Some(row) = catalog.constraint_row_by_oid(address.objid) {
                identity.schema = namespace_name(catalog, row.connamespace);
                if row.conrelid != 0 {
                    if let Some(rel) = catalog.class_row_by_oid(row.conrelid) {
                        identity.identity = Some(format!(
                            "{} on {}",
                            quote_identifier(&row.conname),
                            relation_identity(catalog, &rel)
                        ));
                    }
                } else if row.contypid != 0 {
                    identity.identity = Some(format!(
                        "{} on {}",
                        quote_identifier(&row.conname),
                        type_identity_text(catalog, row.contypid)
                    ));
                }
            }
        }
        PG_CONVERSION_RELATION_OID => {
            if let Some(row) = catalog
                .conversion_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.schema = namespace_name(catalog, row.connamespace);
                identity.name = Some(row.conname.clone());
                identity.identity = Some(qualified_namespace_name(
                    catalog,
                    row.connamespace,
                    &row.conname,
                ));
            }
        }
        PG_LANGUAGE_RELATION_OID => {
            if let Some(row) = catalog.language_row_by_oid(address.objid) {
                identity.name = Some(row.lanname.clone());
                identity.identity = Some(row.lanname);
            }
        }
        PG_NAMESPACE_RELATION_OID => {
            if let Some(row) = catalog.namespace_row_by_oid(address.objid) {
                identity.name = Some(row.nspname.clone());
                identity.identity = Some(row.nspname);
            }
        }
        PG_OPCLASS_RELATION_OID => {
            if let Some(row) = catalog
                .opclass_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                if let Some(am) = catalog
                    .am_rows()
                    .into_iter()
                    .find(|am| am.oid == row.opcmethod)
                {
                    identity.schema = namespace_name(catalog, row.opcnamespace);
                    identity.name = Some(row.opcname.clone());
                    identity.identity = Some(format!(
                        "{} USING {}",
                        qualified_namespace_name(catalog, row.opcnamespace, &row.opcname),
                        am.amname
                    ));
                }
            }
        }
        PG_OPERATOR_RELATION_OID => {
            if let Some(row) = catalog.operator_by_oid(address.objid) {
                identity.schema = namespace_name(catalog, row.oprnamespace);
                identity.identity = Some(operator_signature(catalog, &row, true));
            }
        }
        PG_REWRITE_RELATION_OID => {
            if let Some(row) = catalog.rewrite_row_by_oid(address.objid)
                && let Some(rel) = catalog.class_row_by_oid(row.ev_class)
            {
                identity.identity = Some(format!(
                    "{} on {}",
                    quote_identifier(&row.rulename),
                    relation_identity(catalog, &rel)
                ));
            }
        }
        PG_TRIGGER_RELATION_OID => {
            if let Some(row) = catalog
                .trigger_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(rel) = catalog.class_row_by_oid(row.tgrelid)
            {
                identity.identity = Some(format!(
                    "{} on {}",
                    quote_identifier(&row.tgname),
                    relation_identity(catalog, &rel)
                ));
            }
        }
        PG_OPFAMILY_RELATION_OID => {
            if let Some(row) = catalog
                .opfamily_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(am) = catalog
                    .am_rows()
                    .into_iter()
                    .find(|am| am.oid == row.opfmethod)
            {
                identity.schema = namespace_name(catalog, row.opfnamespace);
                identity.name = Some(row.opfname.clone());
                identity.identity = Some(format!(
                    "{} USING {}",
                    qualified_namespace_name(catalog, row.opfnamespace, &row.opfname),
                    am.amname
                ));
            }
        }
        PG_POLICY_RELATION_OID => {
            if let Some(row) = catalog
                .policy_rows_for_relation(0)
                .into_iter()
                .find(|row| row.oid == address.objid)
                .or_else(|| {
                    catalog.class_rows().into_iter().find_map(|class| {
                        catalog
                            .policy_rows_for_relation(class.oid)
                            .into_iter()
                            .find(|row| row.oid == address.objid)
                    })
                })
                && let Some(rel) = catalog.class_row_by_oid(row.polrelid)
            {
                identity.identity = Some(format!(
                    "{} on {}",
                    quote_identifier(&row.polname),
                    relation_identity(catalog, &rel)
                ));
            }
        }
        PG_STATISTIC_EXT_RELATION_OID => {
            if let Some(row) = catalog.statistic_ext_row_by_oid(address.objid) {
                identity.schema = namespace_name(catalog, row.stxnamespace);
                identity.name = Some(row.stxname.clone());
                identity.identity = Some(qualified_namespace_name(
                    catalog,
                    row.stxnamespace,
                    &row.stxname,
                ));
            }
        }
        PG_COLLATION_RELATION_OID => {
            if let Some(row) = catalog
                .collation_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.schema = namespace_name(catalog, row.collnamespace);
                identity.name = Some(quote_identifier(&row.collname));
                identity.identity = Some(qualified_namespace_name(
                    catalog,
                    row.collnamespace,
                    &row.collname,
                ));
            }
        }
        PG_TRANSFORM_RELATION_OID => {
            if let Some(row) =
                state.and_then(|state| state.transforms.iter().find(|row| row.oid == address.objid))
            {
                let lang = catalog
                    .language_row_by_oid(row.language_oid)
                    .map(|row| row.lanname)
                    .unwrap_or_else(|| row.language_oid.to_string());
                identity.identity = Some(format!(
                    "for {} language {lang}",
                    type_identity_text(catalog, row.type_oid)
                ));
            }
        }
        PG_TS_DICT_RELATION_OID => fill_ts_identity(catalog, address, identity, "dict"),
        PG_TS_PARSER_RELATION_OID => fill_ts_identity(catalog, address, identity, "parser"),
        PG_TS_CONFIG_RELATION_OID => fill_ts_identity(catalog, address, identity, "config"),
        PG_TS_TEMPLATE_RELATION_OID => fill_ts_identity(catalog, address, identity, "template"),
        PG_SUBSCRIPTION_RELATION_OID => {
            if let Some(row) = state.and_then(|state| {
                state
                    .subscriptions
                    .iter()
                    .find(|row| row.oid == address.objid)
            }) {
                identity.name = Some(row.name.clone());
                identity.identity = Some(row.name.clone());
            }
        }
        PG_PUBLICATION_RELATION_OID => {
            if let Some(row) = catalog
                .publication_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                identity.name = Some(row.pubname.clone());
                identity.identity = Some(row.pubname);
            }
        }
        PG_PUBLICATION_REL_RELATION_OID => {
            if let Some(row) = catalog
                .publication_rel_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let (Some(publication), Some(rel)) = (
                    catalog
                        .publication_rows()
                        .into_iter()
                        .find(|pubrow| pubrow.oid == row.prpubid),
                    catalog.class_row_by_oid(row.prrelid),
                )
            {
                identity.identity = Some(format!(
                    "{} in publication {}",
                    relation_identity(catalog, &rel),
                    publication.pubname
                ));
            }
        }
        PG_PUBLICATION_NAMESPACE_RELATION_OID => {
            if let Some(row) = catalog
                .publication_namespace_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let (Some(publication), Some(namespace)) = (
                    catalog
                        .publication_rows()
                        .into_iter()
                        .find(|pubrow| pubrow.oid == row.pnpubid),
                    catalog.namespace_row_by_oid(row.pnnspid),
                )
            {
                identity.identity = Some(format!(
                    "{} in publication {}",
                    namespace.nspname, publication.pubname
                ));
            }
        }
        _ => {}
    }
}

fn fill_address_parts(
    catalog: &dyn CatalogLookup,
    state: Option<&ObjectAddressState>,
    address: ObjectAddress,
    parts: &mut ObjectAddressParts,
) {
    match address.classid {
        PG_DEFAULT_ACL_RELATION_OID => {
            if let Some(row) = state.and_then(|state| {
                state
                    .default_acls
                    .iter()
                    .find(|row| row.oid == address.objid)
            }) {
                let mut names = vec![row.role_name.clone()];
                if let Some(namespace) = &row.namespace_name {
                    names.push(namespace.clone());
                }
                parts.object_names = Some(names);
                parts.object_args = Some(vec![row.objtype.to_string()]);
            }
        }
        PG_TYPE_RELATION_OID if catalog.type_by_oid(address.objid).is_some() => {
            parts.object_names = Some(vec![type_identity_text(catalog, address.objid)]);
            parts.object_args = Some(Vec::new());
        }
        PG_PROC_RELATION_OID => {
            if let Some(row) = catalog.proc_row_by_oid(address.objid) {
                parts.object_names = namespace_name(catalog, row.pronamespace)
                    .map(|schema| vec![schema, row.proname.clone()]);
                parts.object_args = Some(
                    oid_list(&row.proargtypes)
                        .into_iter()
                        .map(|oid| routine_type_identity_text(catalog, oid))
                        .collect(),
                );
            }
        }
        PG_CLASS_RELATION_OID => {
            if let Some(row) = catalog.class_row_by_oid(address.objid) {
                if address.objsubid != 0 {
                    if let Some(attr) = catalog
                        .attribute_rows_for_relation(address.objid)
                        .into_iter()
                        .find(|attr| i32::from(attr.attnum) == address.objsubid)
                    {
                        let mut names = namespace_name(catalog, row.relnamespace)
                            .map(|schema| vec![schema, row.relname.clone()])
                            .unwrap_or_else(|| vec![row.relname.clone()]);
                        names.push(attr.attname);
                        parts.object_names = Some(names);
                        parts.object_args = Some(Vec::new());
                    }
                    return;
                }
                let names = namespace_name(catalog, row.relnamespace)
                    .map(|schema| vec![schema, row.relname.clone()])
                    .unwrap_or_else(|| vec![row.relname.clone()]);
                parts.object_names = Some(names);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_CAST_RELATION_OID => {
            if let Some(row) = catalog
                .cast_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                parts.object_names = Some(vec![type_identity_text(catalog, row.castsource)]);
                parts.object_args = Some(vec![type_identity_text(catalog, row.casttarget)]);
            }
        }
        PG_OPERATOR_RELATION_OID => {
            if let Some(row) = catalog.operator_by_oid(address.objid) {
                parts.object_names = Some(vec![row.oprname]);
                parts.object_args = Some(vec![
                    type_identity_text(catalog, row.oprleft),
                    type_identity_text(catalog, row.oprright),
                ]);
            }
        }
        PG_OPCLASS_RELATION_OID => {
            if let Some(row) = catalog
                .opclass_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(am) = catalog
                    .am_rows()
                    .into_iter()
                    .find(|am| am.oid == row.opcmethod)
            {
                parts.object_names = Some(vec![am.amname, row.opcname]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_OPFAMILY_RELATION_OID => {
            if let Some(row) = catalog
                .opfamily_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(am) = catalog
                    .am_rows()
                    .into_iter()
                    .find(|am| am.oid == row.opfmethod)
            {
                parts.object_names = Some(vec![am.amname, row.opfname]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_AMOP_RELATION_OID => {
            if let Some((row, family, am)) = amop_identity_rows(catalog, address.objid) {
                parts.object_names = Some(vec![
                    am.amname,
                    family.opfname,
                    row.amopstrategy.to_string(),
                ]);
                parts.object_args = Some(vec![
                    type_identity_text(catalog, row.amoplefttype),
                    type_identity_text(catalog, row.amoprighttype),
                ]);
            }
        }
        PG_AMPROC_RELATION_OID => {
            if let Some((row, family, am)) = amproc_identity_rows(catalog, address.objid) {
                parts.object_names =
                    Some(vec![am.amname, family.opfname, row.amprocnum.to_string()]);
                parts.object_args = Some(vec![
                    type_identity_text(catalog, row.amproclefttype),
                    type_identity_text(catalog, row.amprocrighttype),
                ]);
            }
        }
        PG_CONSTRAINT_RELATION_OID => {
            if let Some(row) = catalog.constraint_row_by_oid(address.objid) {
                if row.conrelid != 0
                    && let Some(rel) = catalog.class_row_by_oid(row.conrelid)
                {
                    parts.object_names = Some(vec![
                        namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                        rel.relname,
                        row.conname,
                    ]);
                    parts.object_args = Some(Vec::new());
                } else if row.contypid != 0 {
                    parts.object_names = Some(vec![type_identity_text(catalog, row.contypid)]);
                    parts.object_args = Some(vec![row.conname]);
                }
            }
        }
        PG_CONVERSION_RELATION_OID => {
            if let Some(row) = catalog
                .conversion_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, row.connamespace).unwrap_or_default(),
                    row.conname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_ATTRDEF_RELATION_OID => {
            if let Some((rel, attr)) = attrdef_identity_rows(catalog, address.objid) {
                parts.object_names = Some(vec![
                    namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                    rel.relname,
                    attr.attname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_REWRITE_RELATION_OID => {
            if let Some(row) = catalog.rewrite_row_by_oid(address.objid)
                && let Some(rel) = catalog.class_row_by_oid(row.ev_class)
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                    rel.relname,
                    row.rulename,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_TRIGGER_RELATION_OID => {
            if let Some(row) = catalog
                .trigger_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(rel) = catalog.class_row_by_oid(row.tgrelid)
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                    rel.relname,
                    row.tgname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_POLICY_RELATION_OID => {
            if let Some(row) = catalog.class_rows().into_iter().find_map(|class| {
                catalog
                    .policy_rows_for_relation(class.oid)
                    .into_iter()
                    .find(|row| row.oid == address.objid)
            }) && let Some(rel) = catalog.class_row_by_oid(row.polrelid)
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                    rel.relname,
                    row.polname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_TRANSFORM_RELATION_OID => {
            if let Some(row) =
                state.and_then(|state| state.transforms.iter().find(|row| row.oid == address.objid))
                && let Some(lang) = catalog.language_row_by_oid(row.language_oid)
            {
                parts.object_names = Some(vec![type_identity_text(catalog, row.type_oid)]);
                parts.object_args = Some(vec![lang.lanname]);
            }
        }
        PG_USER_MAPPING_RELATION_OID => {
            if let Some(row) = catalog
                .user_mapping_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let Some(server) = catalog
                    .foreign_server_rows()
                    .into_iter()
                    .find(|server| server.oid == row.umserver)
            {
                parts.object_names = Some(vec![
                    catalog
                        .role_name_by_oid(row.umuser)
                        .unwrap_or_else(|| row.umuser.to_string()),
                ]);
                parts.object_args = Some(vec![server.srvname]);
            }
        }
        PG_AUTHID_RELATION_OID => set_one_name_part(catalog.role_name_by_oid(address.objid), parts),
        PG_NAMESPACE_RELATION_OID => {
            set_one_name_part(namespace_name(catalog, address.objid), parts)
        }
        PG_LANGUAGE_RELATION_OID => set_one_name_part(
            catalog
                .language_row_by_oid(address.objid)
                .map(|row| row.lanname),
            parts,
        ),
        PG_COLLATION_RELATION_OID => {
            if let Some(row) = catalog
                .collation_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, row.collnamespace).unwrap_or_default(),
                    row.collname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_TS_DICT_RELATION_OID => fill_ts_address_parts(catalog, address, parts, "dict"),
        PG_TS_PARSER_RELATION_OID => fill_ts_address_parts(catalog, address, parts, "parser"),
        PG_TS_CONFIG_RELATION_OID => fill_ts_address_parts(catalog, address, parts, "config"),
        PG_TS_TEMPLATE_RELATION_OID => fill_ts_address_parts(catalog, address, parts, "template"),
        PG_FOREIGN_DATA_WRAPPER_RELATION_OID => set_one_name_part(
            catalog
                .foreign_data_wrapper_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                .map(|row| row.fdwname),
            parts,
        ),
        PG_FOREIGN_SERVER_RELATION_OID => set_one_name_part(
            catalog
                .foreign_server_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                .map(|row| row.srvname),
            parts,
        ),
        PG_AM_RELATION_OID => set_one_name_part(
            catalog
                .am_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                .map(|row| row.amname),
            parts,
        ),
        PG_PUBLICATION_RELATION_OID => set_one_name_part(
            catalog
                .publication_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                .map(|row| row.pubname),
            parts,
        ),
        PG_SUBSCRIPTION_RELATION_OID => set_one_name_part(
            state.and_then(|state| {
                state
                    .subscriptions
                    .iter()
                    .find(|row| row.oid == address.objid)
                    .map(|row| row.name.clone())
            }),
            parts,
        ),
        PG_STATISTIC_EXT_RELATION_OID => {
            if let Some(row) = catalog.statistic_ext_row_by_oid(address.objid) {
                parts.object_names = Some(vec![
                    namespace_name(catalog, row.stxnamespace).unwrap_or_default(),
                    row.stxname,
                ]);
                parts.object_args = Some(Vec::new());
            }
        }
        PG_PUBLICATION_REL_RELATION_OID => {
            if let Some(row) = catalog
                .publication_rel_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let (Some(publication), Some(rel)) = (
                    catalog
                        .publication_rows()
                        .into_iter()
                        .find(|pubrow| pubrow.oid == row.prpubid),
                    catalog.class_row_by_oid(row.prrelid),
                )
            {
                parts.object_names = Some(vec![
                    namespace_name(catalog, rel.relnamespace).unwrap_or_default(),
                    rel.relname,
                ]);
                parts.object_args = Some(vec![publication.pubname]);
            }
        }
        PG_PUBLICATION_NAMESPACE_RELATION_OID => {
            if let Some(row) = catalog
                .publication_namespace_rows()
                .into_iter()
                .find(|row| row.oid == address.objid)
                && let (Some(publication), Some(namespace)) = (
                    catalog
                        .publication_rows()
                        .into_iter()
                        .find(|pubrow| pubrow.oid == row.pnpubid),
                    catalog.namespace_row_by_oid(row.pnnspid),
                )
            {
                parts.object_names = Some(vec![namespace.nspname]);
                parts.object_args = Some(vec![publication.pubname]);
            }
        }
        _ => {}
    }
}

fn set_one_name_part(name: Option<String>, parts: &mut ObjectAddressParts) {
    if let Some(name) = name {
        parts.object_names = Some(vec![name]);
        parts.object_args = Some(Vec::new());
    }
}

fn default_acl_identity(row: &DefaultAclAddressEntry) -> String {
    let kind = match row.objtype {
        'r' => "tables",
        'S' => "sequences",
        'f' => "functions",
        'T' => "types",
        'n' => "schemas",
        _ => "objects",
    };
    if let Some(namespace) = &row.namespace_name {
        format!("for role {} in schema {namespace} on {kind}", row.role_name)
    } else {
        format!("for role {} on {kind}", row.role_name)
    }
}

fn fill_ts_identity(
    catalog: &dyn CatalogLookup,
    address: ObjectAddress,
    identity: &mut ObjectIdentity,
    kind: &str,
) {
    let row = match kind {
        "dict" => catalog
            .ts_dict_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.dictnamespace, row.dictname)),
        "parser" => catalog
            .ts_parser_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.prsnamespace, row.prsname)),
        "config" => catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.cfgnamespace, row.cfgname)),
        "template" => catalog
            .ts_template_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.tmplnamespace, row.tmplname)),
        _ => None,
    };
    if let Some((namespace_oid, name)) = row {
        identity.schema = namespace_name(catalog, namespace_oid);
        identity.name = Some(name.clone());
        identity.identity = Some(qualified_namespace_name(catalog, namespace_oid, &name));
    }
}

fn fill_ts_address_parts(
    catalog: &dyn CatalogLookup,
    address: ObjectAddress,
    parts: &mut ObjectAddressParts,
    kind: &str,
) {
    let row = match kind {
        "dict" => catalog
            .ts_dict_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.dictnamespace, row.dictname)),
        "parser" => catalog
            .ts_parser_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.prsnamespace, row.prsname)),
        "config" => catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.cfgnamespace, row.cfgname)),
        "template" => catalog
            .ts_template_rows()
            .into_iter()
            .find(|row| row.oid == address.objid)
            .map(|row| (row.tmplnamespace, row.tmplname)),
        _ => None,
    };
    if let Some((namespace_oid, name)) = row {
        parts.object_names = Some(vec![
            namespace_name(catalog, namespace_oid).unwrap_or_default(),
            name,
        ]);
        parts.object_args = Some(Vec::new());
    }
}

fn attrdef_identity_rows(
    catalog: &dyn CatalogLookup,
    oid: u32,
) -> Option<(PgClassRow, PgAttributeRow)> {
    for rel in catalog.class_rows() {
        for attr in catalog.attribute_rows_for_relation(rel.oid) {
            if rel.oid.wrapping_add(attr.attnum as u32) == oid {
                return Some((rel, attr));
            }
        }
    }
    None
}

fn amop_identity_rows(
    catalog: &dyn CatalogLookup,
    oid: u32,
) -> Option<(PgAmopRow, PgOpfamilyRow, PgAmRow)> {
    let amop = catalog.amop_rows().into_iter().find(|row| row.oid == oid)?;
    let family = catalog
        .opfamily_rows()
        .into_iter()
        .find(|row| row.oid == amop.amopfamily)?;
    let am = catalog
        .am_rows()
        .into_iter()
        .find(|row| row.oid == amop.amopmethod)?;
    Some((amop, family, am))
}

fn amproc_identity_rows(
    catalog: &dyn CatalogLookup,
    oid: u32,
) -> Option<(PgAmprocRow, PgOpfamilyRow, PgAmRow)> {
    let amproc = catalog
        .amproc_rows()
        .into_iter()
        .find(|row| row.oid == oid)?;
    let family = catalog
        .opfamily_rows()
        .into_iter()
        .find(|row| row.oid == amproc.amprocfamily)?;
    let am = catalog
        .am_rows()
        .into_iter()
        .find(|row| row.oid == family.opfmethod)?;
    Some((amproc, family, am))
}

fn type_identity_text(catalog: &dyn CatalogLookup, type_oid: u32) -> String {
    catalog
        .type_by_oid(type_oid)
        .map(|row| {
            if row.typelem != 0
                && row.typname.starts_with('_')
                && let Some(element) = catalog.type_by_oid(row.typelem)
            {
                return format!("{}[]", type_identity_text(catalog, element.oid));
            }
            if row.typnamespace == PG_CATALOG_NAMESPACE_OID {
                expr_reg::format_type_text(type_oid, None, catalog)
            } else {
                qualified_namespace_name(catalog, row.typnamespace, &row.typname)
            }
        })
        .unwrap_or_else(|| expr_reg::format_type_text(type_oid, None, catalog))
}

fn routine_type_identity_text(catalog: &dyn CatalogLookup, type_oid: u32) -> String {
    if type_oid == OID_TYPE_OID {
        "pg_catalog.oid".into()
    } else {
        type_identity_text(catalog, type_oid)
    }
}

fn relation_identity(catalog: &dyn CatalogLookup, row: &PgClassRow) -> String {
    qualified_namespace_name(catalog, row.relnamespace, &row.relname)
}

fn qualified_namespace_name(catalog: &dyn CatalogLookup, namespace_oid: u32, name: &str) -> String {
    namespace_name(catalog, namespace_oid)
        .map(|schema| quote_qualified_identifier(&schema, name))
        .unwrap_or_else(|| quote_identifier(name))
}

fn function_identity(catalog: &dyn CatalogLookup, row: &PgProcRow, qualified: bool) -> String {
    let args = oid_list(&row.proargtypes)
        .into_iter()
        .map(|oid| routine_type_identity_text(catalog, oid))
        .collect::<Vec<_>>()
        .join(",");
    let name = if qualified {
        qualified_namespace_name(catalog, row.pronamespace, &row.proname)
    } else {
        quote_identifier(&row.proname)
    };
    format!("{name}({args})")
}

fn operator_signature(catalog: &dyn CatalogLookup, row: &PgOperatorRow, qualified: bool) -> String {
    let name = if qualified {
        qualified_operator_name(catalog, row.oprnamespace, &row.oprname)
    } else {
        quote_operator_name(&row.oprname)
    };
    format!(
        "{}({},{})",
        name,
        type_identity_text(catalog, row.oprleft),
        type_identity_text(catalog, row.oprright)
    )
}

fn qualified_operator_name(catalog: &dyn CatalogLookup, namespace_oid: u32, name: &str) -> String {
    namespace_name(catalog, namespace_oid)
        .map(|schema| {
            format!(
                "{}.{}",
                quote_identifier(&schema),
                quote_operator_name(name)
            )
        })
        .unwrap_or_else(|| quote_operator_name(name))
}

fn quote_operator_name(name: &str) -> String {
    if !name.is_empty() && name.chars().all(|ch| "+-*/<>=~!@#%^&|`?".contains(ch)) {
        name.to_string()
    } else {
        quote_identifier(name)
    }
}

fn quote_identifier(identifier: &str) -> String {
    if !identifier.eq_ignore_ascii_case("default")
        && !identifier.is_empty()
        && identifier.chars().enumerate().all(|(idx, ch)| {
            if idx == 0 {
                ch == '_' || ch.is_ascii_lowercase()
            } else {
                ch == '_' || ch.is_ascii_lowercase() || ch.is_ascii_digit()
            }
        })
    {
        return identifier.into();
    }
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_qualified_identifier(schema_name: &str, object_name: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(object_name)
    )
}
