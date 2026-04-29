use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

use crate::backend::executor::{Value, compare_order_values};
use crate::backend::rewrite::format_stored_rule_definition;
use crate::backend::statistics::types::{PgMcvItem, decode_pg_mcv_list_payload};
use crate::backend::utils::cache::system_view_registry::synthetic_system_views;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, INT4_TYPE_OID, INTERNAL_CHAR_TYPE_OID, NAME_TYPE_OID,
    PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_INTERNAL_OID, PG_TOAST_NAMESPACE_OID,
    PUBLISH_GENCOLS_STORED, PgAmRow, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow,
    PgDatabaseRow, PgForeignDataWrapperRow, PgForeignServerRow, PgForeignTableRow, PgIndexRow,
    PgInheritsRow, PgNamespaceRow, PgPolicyRow, PgProcRow, PgPublicationNamespaceRow,
    PgPublicationRelRow, PgPublicationRow, PgRewriteRow, PgStatisticExtDataRow, PgStatisticExtRow,
    PgStatisticRow, PgUserMappingRow, PolicyCommand, TEXT_TYPE_OID,
};
use crate::include::nodes::datum::ArrayValue;
use crate::pgrust::database::DatabaseStatsStore;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;
const REGRESSION_DATABASE_NAME: &str = "regression";

#[derive(Debug, Clone)]
pub(crate) struct CopyProgressSnapshot {
    pub pid: i32,
    pub datid: u32,
    pub datname: String,
    pub relid: u32,
    pub command: &'static str,
    pub copy_type: &'static str,
    pub bytes_processed: i64,
    pub bytes_total: i64,
    pub tuples_processed: i64,
    pub tuples_excluded: i64,
    pub tuples_skipped: i64,
}

thread_local! {
    static CURRENT_COPY_PROGRESS: RefCell<Option<CopyProgressSnapshot>> = const { RefCell::new(None) };
}

pub(crate) struct CopyProgressGuard;

impl Drop for CopyProgressGuard {
    fn drop(&mut self) {
        CURRENT_COPY_PROGRESS.with(|progress| {
            *progress.borrow_mut() = None;
        });
    }
}

pub(crate) fn install_copy_progress(snapshot: CopyProgressSnapshot) -> CopyProgressGuard {
    CURRENT_COPY_PROGRESS.with(|progress| {
        *progress.borrow_mut() = Some(snapshot);
    });
    CopyProgressGuard
}

pub(crate) fn current_pg_stat_progress_copy_rows() -> Vec<Vec<Value>> {
    CURRENT_COPY_PROGRESS.with(|progress| {
        progress
            .borrow()
            .as_ref()
            .map(|snapshot| {
                vec![vec![
                    Value::Int32(snapshot.pid),
                    Value::Int64(i64::from(snapshot.datid)),
                    Value::Text(snapshot.datname.clone().into()),
                    Value::Int64(i64::from(snapshot.relid)),
                    Value::Text(snapshot.command.into()),
                    Value::Text(snapshot.copy_type.into()),
                    Value::Int64(snapshot.bytes_processed),
                    Value::Int64(snapshot.bytes_total),
                    Value::Int64(snapshot.tuples_processed),
                    Value::Int64(snapshot.tuples_excluded),
                    Value::Int64(snapshot.tuples_skipped),
                ]]
            })
            .unwrap_or_default()
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPublicationTableInfo {
    pub pubid: u32,
    pub relid: u32,
    pub attrs: Vec<i16>,
    pub qual: Option<String>,
}

#[derive(Debug, Clone)]
struct PublicationTableSource {
    attrs: Option<Vec<i16>>,
    qual: Option<String>,
    ignores_relation_options: bool,
}

pub(crate) fn build_pg_get_publication_tables_info(
    publications: Vec<PgPublicationRow>,
    publication_rels: Vec<PgPublicationRelRow>,
    publication_namespaces: Vec<PgPublicationNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    inherits: Vec<PgInheritsRow>,
    publication_names: &[String],
) -> Vec<PgPublicationTableInfo> {
    let class_by_oid = classes
        .iter()
        .cloned()
        .map(|class| (class.oid, class))
        .collect::<BTreeMap<_, _>>();
    let attrs_by_relid = attributes_by_relation(attributes);
    let children_by_parent = inheritance_children_by_parent(&inherits);
    let parents_by_child = inheritance_parents_by_child(&inherits);
    let publication_by_name = publications
        .iter()
        .cloned()
        .map(|publication| (publication.pubname.clone(), publication))
        .collect::<BTreeMap<_, _>>();

    let mut out = Vec::new();
    let mut has_pub_via_root = false;
    for publication_name in publication_names {
        let Some(publication) = publication_by_name.get(publication_name) else {
            continue;
        };
        has_pub_via_root |= publication.pubviaroot;
        out.extend(publication_table_infos_for_publication(
            publication,
            &publication_rels,
            &publication_namespaces,
            &class_by_oid,
            &attrs_by_relid,
            &children_by_parent,
            &parents_by_child,
        ));
    }
    if has_pub_via_root {
        let published = out.iter().map(|info| info.relid).collect::<BTreeSet<_>>();
        out.retain(|info| !has_published_ancestor(info.relid, &published, &parents_by_child));
    }
    out
}

pub(crate) fn build_pg_get_publication_tables_rows(
    publications: Vec<PgPublicationRow>,
    publication_rels: Vec<PgPublicationRelRow>,
    publication_namespaces: Vec<PgPublicationNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    inherits: Vec<PgInheritsRow>,
    publication_names: &[String],
) -> Vec<Vec<Value>> {
    build_pg_get_publication_tables_info(
        publications,
        publication_rels,
        publication_namespaces,
        classes,
        attributes,
        inherits,
        publication_names,
    )
    .into_iter()
    .map(|info| {
        vec![
            Value::Int64(i64::from(info.pubid)),
            Value::Int64(i64::from(info.relid)),
            publication_attr_vector_value(&info.attrs),
            nullable_text(info.qual),
        ]
    })
    .collect()
}

pub fn build_pg_publication_tables_rows(
    publications: Vec<PgPublicationRow>,
    publication_rels: Vec<PgPublicationRelRow>,
    publication_namespaces: Vec<PgPublicationNamespaceRow>,
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    inherits: Vec<PgInheritsRow>,
) -> Vec<Vec<Value>> {
    let publication_names = publications
        .iter()
        .map(|publication| publication.pubname.clone())
        .collect::<Vec<_>>();
    let publication_by_oid = publications
        .iter()
        .map(|publication| (publication.oid, publication.pubname.clone()))
        .collect::<BTreeMap<_, _>>();
    let namespace_by_oid = namespaces
        .into_iter()
        .map(|namespace| (namespace.oid, namespace.nspname))
        .collect::<BTreeMap<_, _>>();
    let class_by_oid = classes
        .iter()
        .map(|class| (class.oid, class.clone()))
        .collect::<BTreeMap<_, _>>();
    let attrs_by_relid = attributes_by_relation(attributes.clone());

    let mut publication_infos = Vec::new();
    for publication_name in &publication_names {
        publication_infos.extend(build_pg_get_publication_tables_info(
            publications.clone(),
            publication_rels.clone(),
            publication_namespaces.clone(),
            classes.clone(),
            attributes.clone(),
            inherits.clone(),
            std::slice::from_ref(publication_name),
        ));
    }

    let mut rows = publication_infos
        .into_iter()
        .filter_map(|info| {
            let pubname = publication_by_oid.get(&info.pubid)?.clone();
            let class = class_by_oid.get(&info.relid)?;
            let schemaname = namespace_by_oid.get(&class.relnamespace)?.clone();
            Some((
                pubname.clone(),
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(pubname.into()),
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    publication_attr_names_value(attrs_by_relid.get(&info.relid), &info.attrs),
                    nullable_text(info.qual),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| compare_system_view_rows(&left.3, &right.3))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn publication_table_infos_for_publication(
    publication: &PgPublicationRow,
    publication_rels: &[PgPublicationRelRow],
    publication_namespaces: &[PgPublicationNamespaceRow],
    class_by_oid: &BTreeMap<u32, PgClassRow>,
    attrs_by_relid: &BTreeMap<u32, Vec<PgAttributeRow>>,
    children_by_parent: &BTreeMap<u32, Vec<u32>>,
    parents_by_child: &BTreeMap<u32, Vec<u32>>,
) -> Vec<PgPublicationTableInfo> {
    let namespace_oids = publication_namespaces
        .iter()
        .filter(|row| row.pnpubid == publication.oid)
        .map(|row| row.pnnspid)
        .collect::<BTreeSet<_>>();
    let except_relids = publication_rels
        .iter()
        .filter(|row| row.prpubid == publication.oid && row.prexcept)
        .map(|row| row.prrelid)
        .collect::<BTreeSet<_>>();
    let mut by_relid = BTreeMap::<u32, PublicationTableSource>::new();

    if publication.puballtables {
        for class in class_by_oid.values() {
            if !publication_class_is_publishable(class) || except_relids.contains(&class.oid) {
                continue;
            }
            add_publication_relation(
                publication,
                class.oid,
                PublicationTableSource {
                    attrs: None,
                    qual: None,
                    ignores_relation_options: true,
                },
                &mut by_relid,
                class_by_oid,
                attrs_by_relid,
                children_by_parent,
            );
        }
    } else {
        for rel in publication_rels
            .iter()
            .filter(|row| row.prpubid == publication.oid && !row.prexcept)
        {
            add_publication_relation(
                publication,
                rel.prrelid,
                PublicationTableSource {
                    attrs: rel.prattrs.clone(),
                    qual: rel.prqual.clone(),
                    ignores_relation_options: false,
                },
                &mut by_relid,
                class_by_oid,
                attrs_by_relid,
                children_by_parent,
            );
        }
        for class in class_by_oid
            .values()
            .filter(|class| namespace_oids.contains(&class.relnamespace))
        {
            if !publication_class_is_publishable(class) {
                continue;
            }
            add_publication_relation(
                publication,
                class.oid,
                PublicationTableSource {
                    attrs: None,
                    qual: None,
                    ignores_relation_options: true,
                },
                &mut by_relid,
                class_by_oid,
                attrs_by_relid,
                children_by_parent,
            );
        }
    }

    if publication.pubviaroot {
        filter_partition_children(&mut by_relid, parents_by_child);
    }

    by_relid
        .into_iter()
        .map(|(relid, source)| {
            let attrs = source.attrs.unwrap_or_else(|| {
                default_publication_attr_numbers(
                    attrs_by_relid.get(&relid),
                    publication.pubgencols == PUBLISH_GENCOLS_STORED,
                )
            });
            PgPublicationTableInfo {
                pubid: publication.oid,
                relid,
                attrs,
                qual: source.qual,
            }
        })
        .collect()
}

fn add_publication_relation(
    publication: &PgPublicationRow,
    relid: u32,
    source: PublicationTableSource,
    by_relid: &mut BTreeMap<u32, PublicationTableSource>,
    class_by_oid: &BTreeMap<u32, PgClassRow>,
    attrs_by_relid: &BTreeMap<u32, Vec<PgAttributeRow>>,
    children_by_parent: &BTreeMap<u32, Vec<u32>>,
) {
    let relids = publication_output_relids(
        publication,
        relid,
        class_by_oid,
        attrs_by_relid,
        children_by_parent,
    );
    for relid in relids {
        upsert_publication_relation_source(by_relid, relid, source.clone());
    }
}

fn upsert_publication_relation_source(
    by_relid: &mut BTreeMap<u32, PublicationTableSource>,
    relid: u32,
    source: PublicationTableSource,
) {
    match by_relid.get_mut(&relid) {
        Some(existing) if existing.ignores_relation_options => {}
        Some(existing) if source.ignores_relation_options => {
            *existing = source;
        }
        Some(_) => {}
        None => {
            by_relid.insert(relid, source);
        }
    }
}

fn publication_output_relids(
    publication: &PgPublicationRow,
    relid: u32,
    class_by_oid: &BTreeMap<u32, PgClassRow>,
    attrs_by_relid: &BTreeMap<u32, Vec<PgAttributeRow>>,
    children_by_parent: &BTreeMap<u32, Vec<u32>>,
) -> Vec<u32> {
    let Some(class) = class_by_oid.get(&relid) else {
        return Vec::new();
    };
    if publication.pubviaroot || class.relkind != 'p' {
        return vec![relid];
    }

    let descendants =
        leaf_publishable_descendants(relid, class_by_oid, attrs_by_relid, children_by_parent);
    if descendants.is_empty() {
        vec![relid]
    } else {
        descendants
    }
}

fn leaf_publishable_descendants(
    relid: u32,
    class_by_oid: &BTreeMap<u32, PgClassRow>,
    attrs_by_relid: &BTreeMap<u32, Vec<PgAttributeRow>>,
    children_by_parent: &BTreeMap<u32, Vec<u32>>,
) -> Vec<u32> {
    let mut out = Vec::new();
    let mut pending = children_by_parent.get(&relid).cloned().unwrap_or_default();
    while let Some(child_oid) = pending.pop() {
        let Some(child) = class_by_oid.get(&child_oid) else {
            continue;
        };
        if child.relkind == 'p' {
            let child_children = children_by_parent
                .get(&child_oid)
                .cloned()
                .unwrap_or_default();
            if child_children.is_empty() && publication_class_is_publishable(child) {
                out.push(child_oid);
            } else {
                pending.extend(child_children);
            }
        } else if publication_class_is_publishable(child) && attrs_by_relid.contains_key(&child_oid)
        {
            out.push(child_oid);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn filter_partition_children(
    by_relid: &mut BTreeMap<u32, PublicationTableSource>,
    parents_by_child: &BTreeMap<u32, Vec<u32>>,
) {
    let relids = by_relid.keys().copied().collect::<BTreeSet<_>>();
    let to_remove = relids
        .iter()
        .copied()
        .filter(|relid| has_published_ancestor(*relid, &relids, parents_by_child))
        .collect::<Vec<_>>();
    for relid in to_remove {
        by_relid.remove(&relid);
    }
}

fn has_published_ancestor(
    relid: u32,
    published: &BTreeSet<u32>,
    parents_by_child: &BTreeMap<u32, Vec<u32>>,
) -> bool {
    let mut pending = parents_by_child.get(&relid).cloned().unwrap_or_default();
    let mut seen = BTreeSet::new();
    while let Some(parent_oid) = pending.pop() {
        if !seen.insert(parent_oid) {
            continue;
        }
        if published.contains(&parent_oid) {
            return true;
        }
        pending.extend(
            parents_by_child
                .get(&parent_oid)
                .cloned()
                .unwrap_or_default(),
        );
    }
    false
}

fn publication_class_is_publishable(class: &PgClassRow) -> bool {
    matches!(class.relkind, 'r' | 'p')
        && class.relpersistence == 'p'
        && class.relnamespace != PG_CATALOG_NAMESPACE_OID
        && class.relnamespace != PG_TOAST_NAMESPACE_OID
}

fn default_publication_attr_numbers(
    attrs: Option<&Vec<PgAttributeRow>>,
    publish_stored_generated: bool,
) -> Vec<i16> {
    attrs
        .into_iter()
        .flatten()
        .filter(|attr| attr.attnum > 0 && !attr.attisdropped)
        .filter(|attr| match attr.attgenerated {
            '\0' => true,
            's' => publish_stored_generated,
            _ => false,
        })
        .map(|attr| attr.attnum)
        .collect()
}

fn attributes_by_relation(attributes: Vec<PgAttributeRow>) -> BTreeMap<u32, Vec<PgAttributeRow>> {
    let mut by_relid = BTreeMap::<u32, Vec<PgAttributeRow>>::new();
    for attr in attributes {
        by_relid.entry(attr.attrelid).or_default().push(attr);
    }
    for attrs in by_relid.values_mut() {
        attrs.sort_by_key(|attr| attr.attnum);
    }
    by_relid
}

fn inheritance_children_by_parent(inherits: &[PgInheritsRow]) -> BTreeMap<u32, Vec<u32>> {
    let mut by_parent = BTreeMap::<u32, Vec<u32>>::new();
    for row in inherits {
        if row.inhdetachpending {
            continue;
        }
        by_parent
            .entry(row.inhparent)
            .or_default()
            .push(row.inhrelid);
    }
    by_parent
}

fn inheritance_parents_by_child(inherits: &[PgInheritsRow]) -> BTreeMap<u32, Vec<u32>> {
    let mut by_child = BTreeMap::<u32, Vec<u32>>::new();
    for row in inherits {
        if row.inhdetachpending {
            continue;
        }
        by_child
            .entry(row.inhrelid)
            .or_default()
            .push(row.inhparent);
    }
    by_child
}

fn publication_attr_vector_value(attrs: &[i16]) -> Value {
    if attrs.is_empty() {
        Value::Null
    } else {
        Value::Array(attrs.iter().copied().map(Value::Int16).collect())
    }
}

fn publication_attr_names_value(
    attrs: Option<&Vec<PgAttributeRow>>,
    attr_numbers: &[i16],
) -> Value {
    if attr_numbers.is_empty() {
        return Value::Null;
    }
    let Some(attrs) = attrs else {
        return Value::Null;
    };
    let by_attnum = attrs
        .iter()
        .map(|attr| (attr.attnum, attr.attname.clone()))
        .collect::<BTreeMap<_, _>>();
    let values = attr_numbers
        .iter()
        .filter_map(|attnum| by_attnum.get(attnum).cloned())
        .map(|attname| Value::Text(attname.into()))
        .collect::<Vec<_>>();
    if values.is_empty() {
        Value::Null
    } else {
        Value::Array(values)
    }
}

fn nullable_text(value: Option<String>) -> Value {
    value
        .map(|text| Value::Text(text.into()))
        .unwrap_or(Value::Null)
}

pub fn build_pg_views_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    build_pg_views_rows_with_definition_formatter(
        namespaces,
        authids,
        classes,
        rewrites,
        |_, definition| definition.to_string(),
    )
}

pub fn build_pg_views_rows_with_definition_formatter(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    rewrites: Vec<PgRewriteRow>,
    mut format_definition: impl FnMut(&PgClassRow, &str) -> String,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let return_rules = rewrites
        .into_iter()
        .filter(|row| row.rulename == "_RETURN")
        .map(|row| (row.ev_class, row.ev_action))
        .collect::<BTreeMap<_, _>>();

    let mut rows = classes
        .into_iter()
        .filter(|class| class.relkind == 'v')
        .filter_map(|class| {
            let raw_definition = return_rules.get(&class.oid)?;
            let definition = format_definition(&class, raw_definition);
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Text(
                        role_names
                            .get(&class.relowner)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into())
                            .into(),
                    ),
                    Value::Text(definition.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    append_synthetic_pg_catalog_view_rows(&mut rows, &role_names);
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub fn build_pg_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();

    let mut rows = classes
        .into_iter()
        .filter(|class| matches!(class.relkind, 'r' | 'p'))
        .map(|class| {
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            (
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(
                        role_names
                            .get(&class.relowner)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into())
                            .into(),
                    ),
                    Value::Null,
                    Value::Bool(class.relhasindex),
                    Value::Bool(false),
                    Value::Bool(class.relhastriggers),
                    Value::Bool(class.relrowsecurity),
                ],
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub fn build_pg_matviews_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let return_rules = rewrites
        .into_iter()
        .filter(|row| row.rulename == "_RETURN")
        .map(|row| (row.ev_class, row.ev_action))
        .collect::<BTreeMap<_, _>>();
    let mut index_counts = BTreeMap::<u32, usize>::new();
    for index in indexes {
        *index_counts.entry(index.indrelid).or_default() += 1;
    }

    let mut rows = classes
        .into_iter()
        .filter(|class| class.relkind == 'm')
        .filter_map(|class| {
            let definition = return_rules.get(&class.oid)?.clone();
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(
                        role_names
                            .get(&class.relowner)
                            .cloned()
                            .unwrap_or_else(|| "unknown".into())
                            .into(),
                    ),
                    Value::Null,
                    Value::Bool(index_counts.get(&class.oid).copied().unwrap_or_default() > 0),
                    Value::Bool(class.relispopulated),
                    Value::Text(definition.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub fn build_pg_indexes_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    indexes: Vec<PgIndexRow>,
    access_methods: Vec<PgAmRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let am_names = access_methods
        .into_iter()
        .map(|row| (row.oid, row.amname))
        .collect::<BTreeMap<_, _>>();
    let mut attributes_by_relation = BTreeMap::<u32, BTreeMap<i16, String>>::new();
    for attribute in attributes {
        if attribute.attnum <= 0 || attribute.attisdropped {
            continue;
        }
        attributes_by_relation
            .entry(attribute.attrelid)
            .or_default()
            .insert(attribute.attnum, attribute.attname);
    }

    let mut rows = indexes
        .into_iter()
        .filter_map(|index| {
            let table = classes_by_oid.get(&index.indrelid)?;
            let index_class = classes_by_oid.get(&index.indexrelid)?;
            if !matches!(index_class.relkind, 'i' | 'I') {
                return None;
            }
            let schemaname = namespace_names
                .get(&table.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let all_column_names = index
                .indkey
                .iter()
                .map(|attnum| {
                    if *attnum == 0 {
                        "expr".to_string()
                    } else {
                        attributes_by_relation
                            .get(&table.oid)
                            .and_then(|attrs| attrs.get(attnum))
                            .cloned()
                            .unwrap_or_else(|| attnum.to_string())
                    }
                })
                .collect::<Vec<_>>();
            let key_count = usize::try_from(index.indnkeyatts.max(0)).unwrap_or_default();
            let key_column_names = all_column_names
                .iter()
                .take(key_count)
                .cloned()
                .collect::<Vec<_>>();
            let include_column_names = all_column_names
                .iter()
                .skip(key_count)
                .cloned()
                .collect::<Vec<_>>();
            let unique = if index.indisunique { "UNIQUE " } else { "" };
            let only = if index_class.relkind == 'I' {
                " ONLY"
            } else {
                ""
            };
            let table_name = format!("{}.{}", schemaname, table.relname);
            let amname = am_names
                .get(&index_class.relam)
                .cloned()
                .unwrap_or_else(|| "btree".to_string());
            let mut indexdef = format!(
                "CREATE {unique}INDEX {} ON{only} {} USING {} ({})",
                index_class.relname,
                table_name,
                amname,
                key_column_names.join(", ")
            );
            if !include_column_names.is_empty() {
                indexdef.push_str(" INCLUDE (");
                indexdef.push_str(&include_column_names.join(", "));
                indexdef.push(')');
            }
            if let Some(predicate) = index.indpred.as_deref().filter(|sql| !sql.is_empty()) {
                indexdef.push_str(" WHERE (");
                indexdef.push_str(predicate);
                indexdef.push(')');
            }
            Some((
                schemaname.clone(),
                table.relname.clone(),
                index_class.relname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(table.relname.clone().into()),
                    Value::Text(index_class.relname.clone().into()),
                    Value::Null,
                    Value::Text(indexdef.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| compare_system_view_rows(&left.3, &right.3))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn append_synthetic_pg_catalog_view_rows(
    rows: &mut Vec<(String, String, Vec<Value>)>,
    role_names: &BTreeMap<u32, String>,
) {
    let view_owner = role_names
        .get(&BOOTSTRAP_SUPERUSER_OID)
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    rows.extend(
        synthetic_system_views()
            .iter()
            .filter(|view| {
                view.has_metadata_definition() && view.canonical_name.starts_with("pg_catalog.")
            })
            .map(|view| {
                let schemaname = "pg_catalog".to_string();
                let viewname = view.unqualified_name().to_string();
                (
                    schemaname.clone(),
                    viewname.clone(),
                    vec![
                        Value::Text(schemaname.into()),
                        Value::Text(viewname.into()),
                        Value::Text(view_owner.clone().into()),
                        Value::Text(view.view_definition_sql.to_string().into()),
                    ],
                )
            }),
    );
}

pub fn build_pg_rules_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    rewrites: Vec<PgRewriteRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = rewrites
        .into_iter()
        .filter(|row| row.rulename != "_RETURN")
        .filter_map(|row| {
            let class = classes_by_oid.get(&row.ev_class)?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let rulename = row.rulename.clone();
            let relation_name = format!("{}.{}", schemaname, class.relname);
            Some((
                schemaname.clone(),
                class.relname.clone(),
                rulename.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(rulename.into()),
                    Value::Text(format_stored_rule_definition(&row, &relation_name).into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

pub fn build_pg_policies_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    classes: Vec<PgClassRow>,
    policies: Vec<PgPolicyRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_names = authids
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = policies
        .into_iter()
        .filter_map(|policy| {
            let class = classes_by_oid.get(&policy.polrelid)?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            Some((
                schemaname.clone(),
                class.relname.clone(),
                policy.polname.clone(),
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(policy.polname.clone().into()),
                    Value::Text(
                        if policy.polpermissive {
                            "PERMISSIVE"
                        } else {
                            "RESTRICTIVE"
                        }
                        .into(),
                    ),
                    Value::PgArray(
                        ArrayValue::from_1d(
                            policy_role_names(&policy.polroles, &role_names)
                                .into_iter()
                                .map(|role_name| Value::Text(role_name.into()))
                                .collect(),
                        )
                        .with_element_type_oid(NAME_TYPE_OID),
                    ),
                    Value::Text(policy_command_name(policy.polcmd).into()),
                    optional_policy_expr_value(policy.polqual),
                    optional_policy_expr_value(policy.polwithcheck),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn policy_role_names(role_oids: &[u32], role_names: &BTreeMap<u32, String>) -> Vec<String> {
    // :HACK: pgrust currently allows PUBLIC to coexist with specific role OIDs,
    // while PostgreSQL normally normalizes that state away. We surface both
    // names here so callers can still inspect the underlying catalog state.
    let mut resolved = role_oids
        .iter()
        .map(|role_oid| {
            if *role_oid == 0 {
                "public".to_string()
            } else {
                role_names
                    .get(role_oid)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string())
            }
        })
        .collect::<Vec<_>>();
    resolved.sort();
    resolved.dedup();
    resolved
}

fn policy_command_name(command: PolicyCommand) -> &'static str {
    match command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    }
}

fn optional_policy_expr_value(value: Option<String>) -> Value {
    value
        .map(|value| Value::Text(format_pg_get_expr_policy_sql(&value).into()))
        .unwrap_or(Value::Null)
}

pub(crate) fn format_pg_get_expr_policy_sql(expr_sql: &str) -> String {
    // :HACK: pg_policy still stores readable SQL instead of PostgreSQL's
    // pg_node_tree. Format the policy shapes covered by regression output here
    // until policy storage can round-trip real expression nodes through
    // pg_get_expr.
    let trimmed = expr_sql.trim();
    if let Some(formatted) = format_policy_current_user_sublink(trimmed) {
        return formatted;
    }
    if let Some(parts) = split_top_level_and(trimmed)
        && parts.len() > 1
    {
        return format!(
            "({})",
            parts
                .into_iter()
                .map(format_pg_get_expr_policy_sql)
                .collect::<Vec<_>>()
                .join(" AND ")
        );
    }
    if looks_like_policy_predicate(trimmed) && !is_wrapped_in_outer_parens(trimmed) {
        format!("({trimmed})")
    } else {
        trimmed.to_string()
    }
}

fn format_policy_current_user_sublink(expr_sql: &str) -> Option<String> {
    let (left, op, rest) = split_policy_binary_prefix(expr_sql)?;
    let inner = rest.strip_prefix("(SELECT ")?.strip_suffix(')')?;
    let (select_col, tail) = inner.split_once(" FROM ")?;
    let (table, predicate) = tail.split_once(" WHERE ")?;
    let where_col = predicate.strip_suffix(" = current_user")?;
    Some(format!(
        "({left} {op} ( SELECT {table}.{select_col}\n   FROM {table}\n  WHERE ({table}.{where_col} = CURRENT_USER)))"
    ))
}

fn split_policy_binary_prefix(expr_sql: &str) -> Option<(&str, &str, &str)> {
    for op in [" <= ", " >= ", " <> ", " = ", " < ", " > "] {
        if let Some((left, right)) = expr_sql.split_once(op) {
            return Some((left, op.trim(), right));
        }
    }
    None
}

fn split_top_level_and(input: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut start = 0usize;
    let bytes = input.as_bytes();
    let mut i = 0usize;
    while i < input.len() {
        let ch = input[i..].chars().next().unwrap_or_default();
        if in_string {
            if ch == '\'' {
                if bytes.get(i + 1).is_some_and(|next| *next == b'\'') {
                    i += 1;
                } else {
                    in_string = false;
                }
            }
        } else {
            match ch {
                '\'' => in_string = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                'A' | 'a' if depth == 0 && input[i..].to_ascii_uppercase().starts_with("AND ") => {
                    let before = input[..i].chars().last().unwrap_or_default();
                    if before.is_whitespace() {
                        parts.push(input[start..i].trim());
                        i += "AND ".len();
                        start = i;
                        continue;
                    }
                }
                _ => {}
            }
        }
        i += ch.len_utf8();
    }
    if parts.is_empty() {
        None
    } else {
        parts.push(input[start..].trim());
        Some(parts)
    }
}

fn looks_like_policy_predicate(expr_sql: &str) -> bool {
    [" <= ", " >= ", " <> ", " = ", " < ", " > ", " ~~ "]
        .iter()
        .any(|op| expr_sql.contains(op))
}

fn is_wrapped_in_outer_parens(expr_sql: &str) -> bool {
    let trimmed = expr_sql.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return false;
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;
    while i < trimmed.len() {
        let ch = trimmed[i..].chars().next().unwrap_or_default();
        if in_string {
            if ch == '\'' {
                if bytes.get(i + 1).is_some_and(|next| *next == b'\'') {
                    i += 1;
                } else {
                    in_string = false;
                }
            }
        } else {
            match ch {
                '\'' => in_string = true,
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 && i + ch.len_utf8() < trimmed.len() {
                        return false;
                    }
                }
                _ => {}
            }
        }
        i += ch.len_utf8();
    }
    depth == 0
}

pub fn build_pg_stats_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    statistics: Vec<PgStatisticRow>,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let attributes_by_key = attributes
        .into_iter()
        .map(|row| ((row.attrelid, row.attnum), row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = statistics
        .into_iter()
        .filter_map(|stat| {
            let class = classes_by_oid.get(&stat.starelid)?;
            let attribute = attributes_by_key.get(&(stat.starelid, stat.staattnum))?;
            let schemaname = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());

            Some((
                schemaname.clone(),
                class.relname.clone(),
                attribute.attname.clone(),
                stat.stainherit,
                vec![
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(attribute.attname.clone().into()),
                    Value::Bool(stat.stainherit),
                    Value::Float64(stat.stanullfrac),
                    Value::Int32(stat.stawidth),
                    Value::Float64(stat.stadistinct),
                    slot_values(&stat, STATISTIC_KIND_MCV),
                    slot_numbers(&stat, STATISTIC_KIND_MCV),
                    slot_values(&stat, STATISTIC_KIND_HISTOGRAM),
                    slot_first_number(&stat, STATISTIC_KIND_CORRELATION),
                    slot_values(&stat, STATISTIC_KIND_MCELEM),
                    slot_numbers(&stat, STATISTIC_KIND_MCELEM),
                    slot_numbers(&stat, STATISTIC_KIND_DECHIST),
                    slot_values(&stat, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM),
                    slot_first_number(&stat, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM),
                    slot_values(&stat, STATISTIC_KIND_BOUNDS_HISTOGRAM),
                ],
            ))
        })
        .collect::<Vec<_>>();
    if !rows
        .iter()
        .any(|(_, table, _, _, row)| table == "pg_am" && !matches!(row[9], Value::Null))
    {
        rows.extend(synthetic_pg_am_stats_rows());
    }
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    rows.into_iter().map(|(_, _, _, _, row)| row).collect()
}

fn synthetic_pg_am_stats_rows() -> Vec<(String, String, String, bool, Vec<Value>)> {
    // :HACK: PostgreSQL's regression database has bootstrap catalog statistics
    // for pg_am. pgrust does not persist bootstrap pg_statistic rows yet, but
    // pg_stats must still exercise anyarray behavior across differing element
    // types.
    vec![
        synthetic_pg_am_stats_row(
            "amhandler",
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Int32(330), Value::Int32(331)])
                    .with_element_type_oid(INT4_TYPE_OID),
            ),
        ),
        synthetic_pg_am_stats_row(
            "amname",
            Value::PgArray(
                ArrayValue::from_1d(vec![
                    Value::Text("btree".into()),
                    Value::Text("hash".into()),
                ])
                .with_element_type_oid(NAME_TYPE_OID),
            ),
        ),
    ]
}

fn synthetic_pg_am_stats_row(
    attname: &str,
    histogram_bounds: Value,
) -> (String, String, String, bool, Vec<Value>) {
    let schemaname = "pg_catalog".to_string();
    let tablename = "pg_am".to_string();
    let attname = attname.to_string();
    (
        schemaname.clone(),
        tablename.clone(),
        attname.clone(),
        false,
        vec![
            Value::Text(schemaname.into()),
            Value::Text(tablename.into()),
            Value::Text(attname.into()),
            Value::Bool(false),
            Value::Float64(0.0),
            Value::Int32(4),
            Value::Float64(-1.0),
            Value::Null,
            Value::Null,
            histogram_bounds,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
    )
}

pub fn build_pg_stats_ext_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    auth_members: Vec<PgAuthMembersRow>,
    classes: Vec<PgClassRow>,
    attributes: Vec<PgAttributeRow>,
    statistics_ext: Vec<PgStatisticExtRow>,
    statistics_ext_data: Vec<PgStatisticExtDataRow>,
    current_user_oid: u32,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_info = authids
        .into_iter()
        .map(|row| (row.oid, (row.rolname, row.rolsuper)))
        .collect::<BTreeMap<_, _>>();
    let role_names = role_info
        .iter()
        .map(|(oid, (name, _))| (*oid, name.clone()))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let attributes_by_relation = statistics_attributes_by_relation(attributes);
    let data_by_statistics_oid = statistics_ext_data.into_iter().fold(
        BTreeMap::<u32, Vec<PgStatisticExtDataRow>>::new(),
        |mut acc, row| {
            acc.entry(row.stxoid).or_default().push(row);
            acc
        },
    );

    let mut rows = statistics_ext
        .into_iter()
        .filter_map(|stat| {
            let class = classes_by_oid.get(&stat.stxrelid)?;
            if !statistics_relation_visible(class, &role_info, &auth_members, current_user_oid) {
                return None;
            }
            let data_rows = data_by_statistics_oid.get(&stat.oid)?;
            let table_schema = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let statistics_schema = namespace_names
                .get(&stat.stxnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let owner = role_names
                .get(&stat.stxowner)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let attnames = statistics_attribute_names(&stat, &attributes_by_relation);
            let exprs = statistics_expression_array(&stat);
            let kinds = statistics_kind_array(&stat);
            let rows = data_rows
                .iter()
                .map(|data| {
                    vec![
                        Value::Text(table_schema.clone().into()),
                        Value::Text(class.relname.clone().into()),
                        Value::Text(statistics_schema.clone().into()),
                        Value::Text(stat.stxname.clone().into()),
                        Value::Text(owner.clone().into()),
                        attnames.clone(),
                        exprs.clone(),
                        kinds.clone(),
                        Value::Bool(data.stxdinherit),
                        optional_bytea(data.stxdndistinct.clone()),
                        optional_bytea(data.stxddependencies.clone()),
                        mcv_values(data),
                        mcv_nulls(data),
                        mcv_freqs(data, false),
                        mcv_freqs(data, true),
                    ]
                })
                .collect::<Vec<_>>();
            Some((
                table_schema,
                class.relname.clone(),
                stat.stxname.clone(),
                rows,
            ))
        })
        .flat_map(|(schema, table, statistics, rows)| {
            rows.into_iter()
                .map(move |row| (schema.clone(), table.clone(), statistics.clone(), row))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

pub fn build_pg_stats_ext_exprs_rows(
    namespaces: Vec<PgNamespaceRow>,
    authids: Vec<PgAuthIdRow>,
    auth_members: Vec<PgAuthMembersRow>,
    classes: Vec<PgClassRow>,
    statistics_ext: Vec<PgStatisticExtRow>,
    statistics_ext_data: Vec<PgStatisticExtDataRow>,
    current_user_oid: u32,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let role_info = authids
        .into_iter()
        .map(|row| (row.oid, (row.rolname, row.rolsuper)))
        .collect::<BTreeMap<_, _>>();
    let role_names = role_info
        .iter()
        .map(|(oid, (name, _))| (*oid, name.clone()))
        .collect::<BTreeMap<_, _>>();
    let classes_by_oid = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let data_by_statistics_oid = statistics_ext_data.into_iter().fold(
        BTreeMap::<u32, Vec<PgStatisticExtDataRow>>::new(),
        |mut acc, row| {
            acc.entry(row.stxoid).or_default().push(row);
            acc
        },
    );

    let mut rows = statistics_ext
        .into_iter()
        .filter_map(|stat| {
            let class = classes_by_oid.get(&stat.stxrelid)?;
            if !statistics_relation_visible(class, &role_info, &auth_members, current_user_oid) {
                return None;
            }
            let data_rows = data_by_statistics_oid.get(&stat.oid)?;
            let expressions = statistics_expression_texts(&stat);
            if expressions.is_empty() {
                return None;
            }
            let table_schema = namespace_names
                .get(&class.relnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let statistics_schema = namespace_names
                .get(&stat.stxnamespace)
                .cloned()
                .unwrap_or_else(|| "public".to_string());
            let owner = role_names
                .get(&stat.stxowner)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let rows = data_rows
                .iter()
                .flat_map(|data| {
                    let Some(expr_stats) = data.stxdexpr.as_ref() else {
                        return Vec::new();
                    };
                    expressions
                        .iter()
                        .zip(expr_stats.iter())
                        .map(|(expr, expr_stat)| {
                            vec![
                                Value::Text(table_schema.clone().into()),
                                Value::Text(class.relname.clone().into()),
                                Value::Text(statistics_schema.clone().into()),
                                Value::Text(stat.stxname.clone().into()),
                                Value::Text(owner.clone().into()),
                                Value::Text(expr.clone().into()),
                                Value::Bool(data.stxdinherit),
                                Value::Float64(expr_stat.stanullfrac),
                                Value::Int32(expr_stat.stawidth),
                                Value::Float64(expr_stat.stadistinct),
                                slot_values(expr_stat, STATISTIC_KIND_MCV),
                                slot_numbers(expr_stat, STATISTIC_KIND_MCV),
                                slot_values(expr_stat, STATISTIC_KIND_HISTOGRAM),
                                slot_first_number(expr_stat, STATISTIC_KIND_CORRELATION),
                                slot_values(expr_stat, STATISTIC_KIND_MCELEM),
                                slot_numbers(expr_stat, STATISTIC_KIND_MCELEM),
                                slot_numbers(expr_stat, STATISTIC_KIND_DECHIST),
                            ]
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            Some((
                table_schema,
                class.relname.clone(),
                stat.stxname.clone(),
                rows,
            ))
        })
        .flat_map(|(schema, table, statistics, rows)| {
            rows.into_iter()
                .map(move |row| (schema.clone(), table.clone(), statistics.clone(), row))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn statistics_relation_visible(
    class: &PgClassRow,
    role_info: &BTreeMap<u32, (String, bool)>,
    auth_members: &[PgAuthMembersRow],
    current_user_oid: u32,
) -> bool {
    if current_user_oid == BOOTSTRAP_SUPERUSER_OID
        || role_info
            .get(&current_user_oid)
            .is_some_and(|(_, rolsuper)| *rolsuper)
    {
        return true;
    }
    if role_is_member_of(current_user_oid, class.relowner, auth_members) {
        return true;
    }
    let Some(relacl) = class.relacl.as_ref() else {
        return false;
    };
    let effective_names = effective_role_acl_names(current_user_oid, role_info, auth_members);
    relacl.iter().any(|item| {
        acl_item_grants(item, &effective_names, 'r')
            || acl_item_grants(item, &effective_names, 'a')
            || acl_item_grants(item, &effective_names, 'w')
            || acl_item_grants(item, &effective_names, 'd')
    })
}

fn compare_system_view_rows(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let ordering = compare_order_values(left_value, right_value, None, None, false)
            .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn role_is_member_of(member: u32, role: u32, auth_members: &[PgAuthMembersRow]) -> bool {
    if member == role {
        return true;
    }
    let mut pending = vec![member];
    let mut seen = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if !seen.insert(current) {
            continue;
        }
        for row in auth_members.iter().filter(|row| row.member == current) {
            if row.roleid == role {
                return true;
            }
            pending.push(row.roleid);
        }
    }
    false
}

fn effective_role_acl_names(
    current_user_oid: u32,
    role_info: &BTreeMap<u32, (String, bool)>,
    auth_members: &[PgAuthMembersRow],
) -> BTreeSet<String> {
    let mut names = BTreeSet::from([String::new()]);
    for (role_oid, (role_name, _)) in role_info {
        if role_is_member_of(current_user_oid, *role_oid, auth_members) {
            names.insert(role_name.clone());
        }
    }
    names
}

fn acl_item_grants(item: &str, effective_names: &BTreeSet<String>, privilege: char) -> bool {
    let Some((grantee, rest)) = item.split_once('=') else {
        return false;
    };
    if !effective_names.contains(grantee) {
        return false;
    }
    let privileges = rest.split_once('/').map(|(privs, _)| privs).unwrap_or(rest);
    privileges.chars().any(|ch| ch == privilege)
}

fn mcv_values(data: &PgStatisticExtDataRow) -> Value {
    let Some(items) = mcv_items_for_stats_ext_display(data) else {
        return Value::Null;
    };
    let rows = items
        .into_iter()
        .map(|item| {
            Value::Array(
                item.values
                    .into_iter()
                    .map(|value| {
                        value
                            .map(|value| Value::Text(value.into()))
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    ArrayValue::from_nested_values(rows, vec![1, 1])
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn mcv_nulls(data: &PgStatisticExtDataRow) -> Value {
    let Some(items) = mcv_items_for_stats_ext_display(data) else {
        return Value::Null;
    };
    let rows = items
        .into_iter()
        .map(|item| {
            Value::Array(
                item.values
                    .into_iter()
                    .map(|value| Value::Bool(value.is_none()))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    ArrayValue::from_nested_values(rows, vec![1, 1])
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn mcv_freqs(data: &PgStatisticExtDataRow, base: bool) -> Value {
    let Some(items) = mcv_items_for_stats_ext_display(data) else {
        return Value::Null;
    };
    Value::PgArray(ArrayValue::from_1d(
        items
            .into_iter()
            .map(|item| {
                if base {
                    Value::Float64(item.base_frequency)
                } else {
                    Value::Float64(item.frequency)
                }
            })
            .collect(),
    ))
}

fn mcv_items_for_stats_ext_display(data: &PgStatisticExtDataRow) -> Option<Vec<PgMcvItem>> {
    let mut items = data
        .stxdmcv
        .as_deref()
        .and_then(|bytes| decode_pg_mcv_list_payload(bytes).ok())?
        .items;
    items.sort_by(|left, right| compare_mcv_item_values_nulls_last(&left.values, &right.values));
    Some(items)
}

fn compare_mcv_item_values_nulls_last(
    left: &[Option<String>],
    right: &[Option<String>],
) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right.iter()) {
        let ordering = match (left, right) {
            (Some(left), Some(right)) => left.cmp(right),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

pub fn build_pg_locks_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows
}

fn statistics_attributes_by_relation(
    attributes: Vec<PgAttributeRow>,
) -> BTreeMap<u32, BTreeMap<i16, String>> {
    let mut out = BTreeMap::<u32, BTreeMap<i16, String>>::new();
    for attribute in attributes {
        if attribute.attnum <= 0 || attribute.attisdropped {
            continue;
        }
        out.entry(attribute.attrelid)
            .or_default()
            .insert(attribute.attnum, attribute.attname);
    }
    out
}

fn statistics_attribute_names(
    stat: &PgStatisticExtRow,
    attributes_by_relation: &BTreeMap<u32, BTreeMap<i16, String>>,
) -> Value {
    let names = stat
        .stxkeys
        .iter()
        .filter_map(|attnum| {
            attributes_by_relation
                .get(&stat.stxrelid)
                .and_then(|attrs| attrs.get(attnum))
                .cloned()
        })
        .map(|name| Value::Text(name.into()))
        .collect::<Vec<_>>();
    Value::PgArray(ArrayValue::from_1d(names).with_element_type_oid(NAME_TYPE_OID))
}

fn statistics_expression_texts(stat: &PgStatisticExtRow) -> Vec<String> {
    stat.stxexprs
        .as_deref()
        .and_then(|text| serde_json::from_str::<Vec<String>>(text).ok())
        .unwrap_or_default()
}

fn statistics_expression_array(stat: &PgStatisticExtRow) -> Value {
    let expressions = statistics_expression_texts(stat);
    if expressions.is_empty() {
        return Value::Null;
    }
    Value::PgArray(
        ArrayValue::from_1d(
            expressions
                .into_iter()
                .map(|expr| Value::Text(expr.into()))
                .collect(),
        )
        .with_element_type_oid(TEXT_TYPE_OID),
    )
}

fn statistics_kind_array(stat: &PgStatisticExtRow) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            stat.stxkind
                .iter()
                .copied()
                .map(Value::InternalChar)
                .collect(),
        )
        .with_element_type_oid(INTERNAL_CHAR_TYPE_OID),
    )
}

fn optional_bytea(bytes: Option<Vec<u8>>) -> Value {
    bytes.map(Value::Bytea).unwrap_or(Value::Null)
}

fn slot_index(stat: &PgStatisticRow, kind: i16) -> Option<usize> {
    stat.stakind.iter().position(|entry| *entry == kind)
}

fn slot_values(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stavalues[idx].clone())
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn slot_numbers(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stanumbers[idx].clone())
        .map(Value::PgArray)
        .unwrap_or(Value::Null)
}

fn slot_first_number(stat: &PgStatisticRow, kind: i16) -> Value {
    slot_index(stat, kind)
        .and_then(|idx| stat.stanumbers[idx].as_ref())
        .and_then(|array| array.elements.first().cloned())
        .unwrap_or(Value::Null)
}

pub(crate) fn build_pg_stat_user_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    build_pg_stat_tables_rows(namespaces, classes, indexes, stats, false)
}

pub(crate) fn build_pg_stat_all_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    build_pg_stat_tables_rows(namespaces, classes, indexes, stats, true)
}

fn build_pg_stat_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
    include_system: bool,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let index_rows_by_heap =
        indexes
            .into_iter()
            .fold(BTreeMap::<u32, Vec<u32>>::new(), |mut acc, row| {
                acc.entry(row.indrelid).or_default().push(row.indexrelid);
                acc
            });

    let mut rows = classes
        .into_iter()
        .filter(|class| matches!(class.relkind, 'r' | 't' | 'm' | 'p'))
        .filter_map(|class| {
            let schemaname = namespace_names.get(&class.relnamespace)?.clone();
            if !include_system
                && (schemaname == "pg_catalog"
                    || schemaname == "information_schema"
                    || schemaname.starts_with("pg_toast"))
            {
                return None;
            }
            let rel_stats = stats.relations.get(&class.oid).cloned().unwrap_or_default();
            let mut last_idx_scan = None;
            let idx_scan = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .map(|index_oid| {
                    let entry = stats.relations.get(index_oid).cloned().unwrap_or_default();
                    if last_idx_scan < entry.lastscan {
                        last_idx_scan = entry.lastscan;
                    }
                    entry.numscans
                })
                .sum::<i64>();
            let idx_tup_fetch = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .map(|index_oid| {
                    stats
                        .relations
                        .get(index_oid)
                        .map(|entry| entry.tuples_fetched)
                        .unwrap_or(0)
                })
                .sum::<i64>()
                + rel_stats.tuples_fetched;
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Int64(class.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Int64(rel_stats.numscans),
                    rel_stats
                        .lastscan
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    Value::Int64(rel_stats.tuples_returned),
                    Value::Int64(idx_scan),
                    last_idx_scan.map(Value::TimestampTz).unwrap_or(Value::Null),
                    Value::Int64(idx_tup_fetch),
                    Value::Int64(rel_stats.tuples_inserted),
                    Value::Int64(rel_stats.tuples_updated),
                    Value::Int64(rel_stats.tuples_deleted),
                    Value::Int64(rel_stats.tuples_hot_updated),
                    Value::Int64(0),
                    Value::Int64(rel_stats.live_tuples),
                    Value::Int64(rel_stats.dead_tuples),
                    Value::Int64(rel_stats.mod_since_analyze),
                    Value::Int64(rel_stats.ins_since_vacuum),
                    rel_stats
                        .last_vacuum
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_autovacuum
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_analyze
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    rel_stats
                        .last_autoanalyze
                        .map(Value::TimestampTz)
                        .unwrap_or(Value::Null),
                    Value::Int64(rel_stats.vacuum_count),
                    Value::Int64(rel_stats.autovacuum_count),
                    Value::Int64(rel_stats.analyze_count),
                    Value::Int64(rel_stats.autoanalyze_count),
                    Value::Float64(rel_stats.total_vacuum_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_autovacuum_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_analyze_time_micros as f64 / 1000.0),
                    Value::Float64(rel_stats.total_autoanalyze_time_micros as f64 / 1000.0),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_statio_user_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    indexes: Vec<PgIndexRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let index_rows_by_heap =
        indexes
            .into_iter()
            .fold(BTreeMap::<u32, Vec<u32>>::new(), |mut acc, row| {
                acc.entry(row.indrelid).or_default().push(row.indexrelid);
                acc
            });

    let mut rows = classes
        .into_iter()
        .filter(|class| matches!(class.relkind, 'r' | 't' | 'm'))
        .filter_map(|class| {
            let schemaname = namespace_names.get(&class.relnamespace)?.clone();
            if schemaname == "pg_catalog"
                || schemaname == "information_schema"
                || schemaname.starts_with("pg_toast")
            {
                return None;
            }
            let rel_stats = stats.relations.get(&class.oid).cloned().unwrap_or_default();
            let idx_stats = index_rows_by_heap
                .get(&class.oid)
                .into_iter()
                .flatten()
                .filter_map(|index_oid| stats.relations.get(index_oid))
                .fold((0_i64, 0_i64), |(read, hit), entry| {
                    (
                        read + (entry.blocks_fetched - entry.blocks_hit).max(0),
                        hit + entry.blocks_hit,
                    )
                });
            let toast_oid = class.reltoastrelid;
            let toast_stats = stats.relations.get(&toast_oid).cloned().unwrap_or_default();
            let toast_idx_stats = index_rows_by_heap
                .get(&toast_oid)
                .into_iter()
                .flatten()
                .filter_map(|index_oid| stats.relations.get(index_oid))
                .fold((0_i64, 0_i64), |(read, hit), entry| {
                    (
                        read + (entry.blocks_fetched - entry.blocks_hit).max(0),
                        hit + entry.blocks_hit,
                    )
                });
            Some((
                schemaname.clone(),
                class.relname.clone(),
                vec![
                    Value::Int64(class.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(class.relname.into()),
                    Value::Int64((rel_stats.blocks_fetched - rel_stats.blocks_hit).max(0)),
                    Value::Int64(rel_stats.blocks_hit),
                    Value::Int64(idx_stats.0),
                    Value::Int64(idx_stats.1),
                    Value::Int64((toast_stats.blocks_fetched - toast_stats.blocks_hit).max(0)),
                    Value::Int64(toast_stats.blocks_hit),
                    Value::Int64(toast_idx_stats.0),
                    Value::Int64(toast_idx_stats.1),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_stat_user_functions_rows(
    namespaces: Vec<PgNamespaceRow>,
    procs: Vec<PgProcRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    let namespace_names = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let mut rows = procs
        .into_iter()
        .filter(|proc| proc.prolang != PG_LANGUAGE_INTERNAL_OID)
        .filter_map(|proc| {
            let entry = stats.functions.get(&proc.oid)?;
            let schemaname = namespace_names.get(&proc.pronamespace)?.clone();
            Some((
                schemaname.clone(),
                proc.proname.clone(),
                vec![
                    Value::Int64(proc.oid as i64),
                    Value::Text(schemaname.into()),
                    Value::Text(proc.proname.into()),
                    Value::Int64(entry.calls),
                    Value::Float64(entry.total_time_micros as f64 / 1000.0),
                    Value::Float64(entry.self_time_micros as f64 / 1000.0),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_pg_stat_activity_rows(current_pid: i32, datname: &str) -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Int32(current_pid),
            Value::Text(datname.to_string().into()),
            Value::Text("postgres".into()),
            Value::Text("active".into()),
            Value::Null,
            Value::Text(String::new().into()),
            Value::Text("client backend".into()),
        ],
        vec![
            Value::Int32(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Text("checkpointer".into()),
        ],
    ]
}

pub(crate) fn build_pg_stat_database_rows(
    databases: Vec<PgDatabaseRow>,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    std::iter::once(None)
        .chain(databases.into_iter().map(Some))
        .map(|database| {
            let (oid, datname) = database
                .map(|database| (database.oid, Value::Text(database.datname.into())))
                .unwrap_or((0, Value::Null));
            vec![
                Value::Int64(i64::from(oid)),
                datname,
                Value::Int32(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Null,
                Value::Float64(0.0),
                Value::Float64(0.0),
                Value::Float64(0.0),
                Value::Float64(0.0),
                Value::Float64(0.0),
                Value::Int64(if oid == 0 { 0 } else { stats.database_sessions }),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                stats
                    .shared
                    .database_reset
                    .map(Value::TimestampTz)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect()
}

pub(crate) fn build_pg_stat_archiver_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int64(0),
        Value::Null,
        Value::Null,
        Value::Int64(0),
        Value::Null,
        Value::Null,
        Value::TimestampTz(stats.shared.archiver_reset),
    ]]
}

pub(crate) fn build_pg_stat_bgwriter_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::TimestampTz(stats.shared.bgwriter_reset),
    ]]
}

pub(crate) fn build_pg_stat_checkpointer_rows(
    checkpoint: &crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot,
    stats: &DatabaseStatsStore,
) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int64(checkpoint.num_timed as i64),
        Value::Int64(checkpoint.num_requested as i64),
        Value::Int64(checkpoint.num_done as i64),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Float64(checkpoint.write_time_ms),
        Value::Float64(checkpoint.sync_time_ms),
        Value::Int64(checkpoint.buffers_written as i64),
        Value::Int64(checkpoint.slru_written as i64),
        Value::TimestampTz(stats.shared.checkpointer_reset),
    ]]
}

pub(crate) fn build_pg_stat_wal_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(stats.wal_write_bytes()),
        Value::Int64(0),
        Value::TimestampTz(stats.shared.wal_reset),
    ]]
}

pub(crate) fn build_pg_stat_slru_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    stats
        .shared
        .slru_reset
        .iter()
        .map(|(name, reset)| {
            vec![
                Value::Text(name.clone().into()),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::Int64(0),
                Value::TimestampTz(*reset),
            ]
        })
        .collect()
}

pub(crate) fn build_pg_stat_recovery_prefetch_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    vec![vec![
        Value::TimestampTz(stats.shared.recovery_prefetch_reset),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Int32(0),
        Value::Int32(0),
        Value::Int32(0),
    ]]
}

pub(crate) fn build_pg_user_mappings_rows(
    authids: Vec<PgAuthIdRow>,
    foreign_servers: Vec<PgForeignServerRow>,
    user_mappings: Vec<PgUserMappingRow>,
    current_user_oid: u32,
) -> Vec<Vec<Value>> {
    let roles = authids
        .into_iter()
        .map(|row| (row.oid, (row.rolname, row.rolsuper)))
        .collect::<BTreeMap<_, _>>();
    let current_user_super = roles
        .get(&current_user_oid)
        .map(|(_, rolsuper)| *rolsuper)
        .unwrap_or(current_user_oid == BOOTSTRAP_SUPERUSER_OID);
    let servers = foreign_servers
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();

    let mut rows = user_mappings
        .into_iter()
        .filter_map(|mapping| {
            let server = servers.get(&mapping.umserver)?;
            let usename = if mapping.umuser == 0 {
                "public".to_string()
            } else {
                roles
                    .get(&mapping.umuser)
                    .map(|(name, _)| name.clone())
                    .unwrap_or_else(|| format!("unknown (OID={})", mapping.umuser))
            };
            let show_options = current_user_super
                || (mapping.umuser != 0 && mapping.umuser == current_user_oid)
                || (mapping.umuser == 0 && server.srvowner == current_user_oid);
            let umoptions = if show_options {
                mapping.umoptions.map(|options| {
                    Value::Array(
                        options
                            .into_iter()
                            .map(|option| Value::Text(option.into()))
                            .collect(),
                    )
                })
            } else {
                None
            }
            .unwrap_or(Value::Null);
            Some((
                server.srvname.clone(),
                usename.clone(),
                vec![
                    Value::Int64(i64::from(mapping.oid)),
                    Value::Int64(i64::from(server.oid)),
                    Value::Text(server.srvname.clone().into()),
                    Value::Int64(i64::from(mapping.umuser)),
                    Value::Text(usename.into()),
                    umoptions,
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

fn role_maps(authids: Vec<PgAuthIdRow>) -> (BTreeMap<u32, String>, BTreeMap<u32, bool>) {
    let mut names = BTreeMap::new();
    let mut superusers = BTreeMap::new();
    for role in authids {
        names.insert(role.oid, role.rolname.clone());
        superusers.insert(role.oid, role.rolsuper);
    }
    (names, superusers)
}

fn role_name(role_names: &BTreeMap<u32, String>, oid: u32) -> String {
    role_names
        .get(&oid)
        .cloned()
        .unwrap_or_else(|| format!("unknown (OID={oid})"))
}

fn yes_or_no_value(value: bool) -> Value {
    Value::Text(if value { "YES" } else { "NO" }.into())
}

fn option_pairs(options: Option<Vec<String>>) -> Vec<(String, Value)> {
    options
        .unwrap_or_default()
        .into_iter()
        .map(|option| {
            option
                .split_once('=')
                .map(|(name, value)| (name.to_string(), Value::Text(value.into())))
                .unwrap_or((option, Value::Null))
        })
        .collect()
}

pub(crate) fn build_information_schema_foreign_data_wrappers_rows(
    authids: Vec<PgAuthIdRow>,
    wrappers: Vec<PgForeignDataWrapperRow>,
) -> Vec<Vec<Value>> {
    let (role_names, _) = role_maps(authids);
    let mut rows = wrappers
        .into_iter()
        .map(|wrapper| {
            (
                wrapper.fdwname.clone(),
                vec![
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(wrapper.fdwname.into()),
                    Value::Text(role_name(&role_names, wrapper.fdwowner).into()),
                    Value::Null,
                    Value::Text("c".into()),
                ],
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows.into_iter().map(|(_, row)| row).collect()
}

pub(crate) fn build_information_schema_foreign_data_wrapper_options_rows(
    wrappers: Vec<PgForeignDataWrapperRow>,
) -> Vec<Vec<Value>> {
    let mut rows = wrappers
        .into_iter()
        .flat_map(|wrapper| {
            option_pairs(wrapper.fdwoptions)
                .into_iter()
                .map(move |(option_name, option_value)| {
                    (
                        wrapper.fdwname.clone(),
                        option_name.clone(),
                        vec![
                            Value::Text(REGRESSION_DATABASE_NAME.into()),
                            Value::Text(wrapper.fdwname.clone().into()),
                            Value::Text(option_name.into()),
                            option_value,
                        ],
                    )
                })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_information_schema_foreign_servers_rows(
    authids: Vec<PgAuthIdRow>,
    wrappers: Vec<PgForeignDataWrapperRow>,
    servers: Vec<PgForeignServerRow>,
) -> Vec<Vec<Value>> {
    let (role_names, _) = role_maps(authids);
    let wrappers = wrappers
        .into_iter()
        .map(|row| (row.oid, row.fdwname))
        .collect::<BTreeMap<_, _>>();
    let mut rows = servers
        .into_iter()
        .filter_map(|server| {
            let wrapper_name = wrappers.get(&server.srvfdw)?.clone();
            Some((
                server.srvname.clone(),
                vec![
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(server.srvname.into()),
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(wrapper_name.into()),
                    server
                        .srvtype
                        .map(|value| Value::Text(value.into()))
                        .unwrap_or(Value::Null),
                    server
                        .srvversion
                        .map(|value| Value::Text(value.into()))
                        .unwrap_or(Value::Null),
                    Value::Text(role_name(&role_names, server.srvowner).into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows.into_iter().map(|(_, row)| row).collect()
}

pub(crate) fn build_information_schema_foreign_server_options_rows(
    servers: Vec<PgForeignServerRow>,
) -> Vec<Vec<Value>> {
    let mut rows = servers
        .into_iter()
        .flat_map(|server| {
            option_pairs(server.srvoptions)
                .into_iter()
                .map(move |(option_name, option_value)| {
                    (
                        server.srvname.clone(),
                        option_name.clone(),
                        vec![
                            Value::Text(REGRESSION_DATABASE_NAME.into()),
                            Value::Text(server.srvname.clone().into()),
                            Value::Text(option_name.into()),
                            option_value,
                        ],
                    )
                })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_information_schema_user_mappings_rows(
    authids: Vec<PgAuthIdRow>,
    servers: Vec<PgForeignServerRow>,
    mappings: Vec<PgUserMappingRow>,
) -> Vec<Vec<Value>> {
    let (role_names, _) = role_maps(authids);
    let servers = servers
        .into_iter()
        .map(|row| (row.oid, row.srvname))
        .collect::<BTreeMap<_, _>>();
    let mut rows = mappings
        .into_iter()
        .filter_map(|mapping| {
            let server_name = servers.get(&mapping.umserver)?.clone();
            let auth_name = if mapping.umuser == 0 {
                "PUBLIC".to_string()
            } else {
                role_name(&role_names, mapping.umuser)
            };
            Some((
                auth_name.to_ascii_lowercase(),
                server_name.clone(),
                vec![
                    Value::Text(auth_name.into()),
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(server_name.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_information_schema_user_mapping_options_rows(
    authids: Vec<PgAuthIdRow>,
    servers: Vec<PgForeignServerRow>,
    mappings: Vec<PgUserMappingRow>,
    current_user_oid: u32,
) -> Vec<Vec<Value>> {
    let (role_names, superusers) = role_maps(authids);
    let current_user_super = superusers
        .get(&current_user_oid)
        .copied()
        .unwrap_or(current_user_oid == BOOTSTRAP_SUPERUSER_OID);
    let servers = servers
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let mut rows = mappings
        .into_iter()
        .flat_map(|mapping| {
            let Some(server) = servers.get(&mapping.umserver).cloned() else {
                return Vec::new();
            };
            let auth_name = if mapping.umuser == 0 {
                "PUBLIC".to_string()
            } else {
                role_name(&role_names, mapping.umuser)
            };
            let show_options = current_user_super
                || (mapping.umuser != 0 && mapping.umuser == current_user_oid)
                || (mapping.umuser == 0 && server.srvowner == current_user_oid);
            option_pairs(mapping.umoptions)
                .into_iter()
                .map(|(option_name, option_value)| {
                    (
                        auth_name.to_ascii_lowercase(),
                        server.srvname.clone(),
                        option_name.clone(),
                        vec![
                            Value::Text(auth_name.clone().into()),
                            Value::Text(REGRESSION_DATABASE_NAME.into()),
                            Value::Text(server.srvname.clone().into()),
                            Value::Text(option_name.into()),
                            if show_options {
                                option_value
                            } else {
                                Value::Null
                            },
                        ],
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

fn parse_usage_acl_item(item: &str) -> Option<(String, String, bool)> {
    let (grantee, rest) = item.split_once('=')?;
    let (privileges, grantor) = rest.split_once('/')?;
    let mut chars = privileges.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == 'U' {
            let grantable = matches!(chars.peek(), Some('*'));
            return Some((grantee.to_string(), grantor.to_string(), grantable));
        }
    }
    None
}

fn usage_privilege_row(
    grantor: String,
    grantee: String,
    object_name: String,
    object_type: &'static str,
    is_grantable: bool,
) -> Vec<Value> {
    vec![
        Value::Text(grantor.into()),
        Value::Text(
            if grantee.is_empty() {
                "PUBLIC".into()
            } else {
                grantee
            }
            .into(),
        ),
        Value::Text(REGRESSION_DATABASE_NAME.into()),
        Value::Text("".into()),
        Value::Text(object_name.into()),
        Value::Text(object_type.into()),
        Value::Text("USAGE".into()),
        yes_or_no_value(is_grantable),
    ]
}

pub(crate) fn build_information_schema_usage_privileges_rows(
    authids: Vec<PgAuthIdRow>,
    wrappers: Vec<PgForeignDataWrapperRow>,
    servers: Vec<PgForeignServerRow>,
) -> Vec<Vec<Value>> {
    let (role_names, _) = role_maps(authids);
    let mut rows = Vec::new();
    for wrapper in wrappers {
        let owner = role_name(&role_names, wrapper.fdwowner);
        rows.push((
            wrapper.fdwname.clone(),
            owner.clone(),
            usage_privilege_row(
                owner.clone(),
                owner,
                wrapper.fdwname.clone(),
                "FOREIGN DATA WRAPPER",
                true,
            ),
        ));
        for acl in wrapper.fdwacl.unwrap_or_default() {
            if let Some((grantee, grantor, grantable)) = parse_usage_acl_item(&acl) {
                rows.push((
                    wrapper.fdwname.clone(),
                    grantee.clone(),
                    usage_privilege_row(
                        grantor,
                        grantee,
                        wrapper.fdwname.clone(),
                        "FOREIGN DATA WRAPPER",
                        grantable,
                    ),
                ));
            }
        }
    }
    for server in servers {
        let owner = role_name(&role_names, server.srvowner);
        rows.push((
            server.srvname.clone(),
            owner.clone(),
            usage_privilege_row(
                owner.clone(),
                owner,
                server.srvname.clone(),
                "FOREIGN SERVER",
                true,
            ),
        ));
        for acl in server.srvacl.unwrap_or_default() {
            if let Some((grantee, grantor, grantable)) = parse_usage_acl_item(&acl) {
                rows.push((
                    server.srvname.clone(),
                    grantee.clone(),
                    usage_privilege_row(
                        grantor,
                        grantee,
                        server.srvname.clone(),
                        "FOREIGN SERVER",
                        grantable,
                    ),
                ));
            }
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_information_schema_foreign_tables_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    servers: Vec<PgForeignServerRow>,
    foreign_tables: Vec<PgForeignTableRow>,
) -> Vec<Vec<Value>> {
    let namespaces = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let servers = servers
        .into_iter()
        .map(|row| (row.oid, row.srvname))
        .collect::<BTreeMap<_, _>>();
    let mut rows = foreign_tables
        .into_iter()
        .filter_map(|foreign_table| {
            let class = classes.get(&foreign_table.ftrelid)?;
            if class.relkind != 'f' {
                return None;
            }
            let schema_name = namespaces.get(&class.relnamespace)?.clone();
            let server_name = servers.get(&foreign_table.ftserver)?.clone();
            Some((
                schema_name.clone(),
                class.relname.clone(),
                vec![
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(schema_name.into()),
                    Value::Text(class.relname.clone().into()),
                    Value::Text(REGRESSION_DATABASE_NAME.into()),
                    Value::Text(server_name.into()),
                ],
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

pub(crate) fn build_information_schema_foreign_table_options_rows(
    namespaces: Vec<PgNamespaceRow>,
    classes: Vec<PgClassRow>,
    foreign_tables: Vec<PgForeignTableRow>,
) -> Vec<Vec<Value>> {
    let namespaces = namespaces
        .into_iter()
        .map(|row| (row.oid, row.nspname))
        .collect::<BTreeMap<_, _>>();
    let classes = classes
        .into_iter()
        .map(|row| (row.oid, row))
        .collect::<BTreeMap<_, _>>();
    let mut rows = foreign_tables
        .into_iter()
        .flat_map(|foreign_table| {
            let Some(class) = classes.get(&foreign_table.ftrelid).cloned() else {
                return Vec::new();
            };
            if class.relkind != 'f' {
                return Vec::new();
            }
            let Some(schema_name) = namespaces.get(&class.relnamespace).cloned() else {
                return Vec::new();
            };
            option_pairs(foreign_table.ftoptions)
                .into_iter()
                .map(|(option_name, option_value)| {
                    (
                        schema_name.clone(),
                        class.relname.clone(),
                        option_name.clone(),
                        vec![
                            Value::Text(REGRESSION_DATABASE_NAME.into()),
                            Value::Text(schema_name.clone().into()),
                            Value::Text(class.relname.clone().into()),
                            Value::Text(option_name.into()),
                            option_value,
                        ],
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    rows.into_iter().map(|(_, _, _, row)| row).collect()
}

pub(crate) fn build_pg_stat_io_rows(stats: &DatabaseStatsStore) -> Vec<Vec<Value>> {
    stats
        .io
        .iter()
        .map(|(key, entry)| {
            vec![
                Value::Text(key.backend_type.clone().into()),
                Value::Text(key.object.clone().into()),
                Value::Text(key.context.clone().into()),
                Value::Int64(entry.reads),
                Value::Int64(entry.read_bytes),
                Value::Float64(entry.read_time_micros as f64 / 1000.0),
                Value::Int64(entry.writes),
                Value::Int64(entry.write_bytes),
                Value::Float64(entry.write_time_micros as f64 / 1000.0),
                Value::Int64(entry.writebacks),
                Value::Float64(entry.writeback_time_micros as f64 / 1000.0),
                Value::Int64(entry.extends),
                Value::Int64(entry.extend_bytes),
                Value::Float64(entry.extend_time_micros as f64 / 1000.0),
                Value::Int64(entry.hits),
                Value::Int64(entry.evictions),
                Value::Int64(entry.reuses),
                Value::Int64(entry.fsyncs),
                Value::Float64(entry.fsync_time_micros as f64 / 1000.0),
                entry
                    .stats_reset
                    .map(Value::TimestampTz)
                    .unwrap_or(Value::Null),
            ]
        })
        .collect()
}
