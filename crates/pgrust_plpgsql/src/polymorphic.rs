use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, PgProcRow, range_type_ref_for_multirange_sql_type,
    range_type_ref_for_sql_type,
};
use pgrust_nodes::{SqlType, SqlTypeKind};

use crate::parse_proc_argtype_oids;

pub fn concrete_polymorphic_proc_row(
    row: &PgProcRow,
    resolved_result_type: Option<SqlType>,
    actual_arg_types: &[Option<SqlType>],
    catalog: &dyn CatalogLookup,
) -> Option<PgProcRow> {
    let mut concrete_row = row.clone();
    let mut changed = false;
    if is_polymorphic_type_oid(row.prorettype)
        && let Some(result_type) = resolved_result_type
        && let Some(result_oid) = concrete_type_oid(result_type, catalog)
        && !is_polymorphic_type_oid(result_oid)
    {
        concrete_row.prorettype = result_oid;
        changed = true;
    }
    let Some(arg_oids) = parse_proc_argtype_oids(&row.proargtypes) else {
        return None;
    };
    let polymorphic_types = infer_concrete_polymorphic_types(row, actual_arg_types);
    let concrete_arg_oids = arg_oids
        .iter()
        .copied()
        .enumerate()
        .map(|(idx, oid)| {
            if is_polymorphic_type_oid(oid)
                && let Some(Some(actual_type)) = actual_arg_types.get(idx)
                && let Some(actual_oid) = concrete_type_oid(*actual_type, catalog)
                && !is_polymorphic_type_oid(actual_oid)
            {
                changed = true;
                actual_oid
            } else {
                oid
            }
        })
        .collect::<Vec<_>>();
    if !changed {
        if let (Some(all_arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) {
            let concrete_all_arg_oids = concrete_polymorphic_all_arg_oids(
                all_arg_types,
                arg_modes,
                actual_arg_types,
                &polymorphic_types,
                catalog,
                &mut changed,
            );
            if changed {
                concrete_row.proallargtypes = Some(concrete_all_arg_oids);
            }
        }
        if !changed {
            return None;
        }
    } else if let (Some(all_arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) {
        concrete_row.proallargtypes = Some(concrete_polymorphic_all_arg_oids(
            all_arg_types,
            arg_modes,
            actual_arg_types,
            &polymorphic_types,
            catalog,
            &mut changed,
        ));
    }
    concrete_row.proargtypes = concrete_arg_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    Some(concrete_row)
}

fn concrete_type_oid(ty: SqlType, catalog: &dyn CatalogLookup) -> Option<u32> {
    catalog
        .type_oid_for_sql_type(ty)
        .or_else(|| (ty.type_oid != 0).then_some(ty.type_oid))
}

#[derive(Default)]
struct InferredPolymorphicTypes {
    anyelement: Option<SqlType>,
    anyarray: Option<SqlType>,
    anyrange: Option<SqlType>,
    anymultirange: Option<SqlType>,
    anycompatible: Option<SqlType>,
    anycompatiblerange: Option<SqlType>,
    anycompatiblemultirange: Option<SqlType>,
}

fn infer_concrete_polymorphic_types(
    row: &PgProcRow,
    actual_arg_types: &[Option<SqlType>],
) -> InferredPolymorphicTypes {
    let mut inferred = InferredPolymorphicTypes::default();
    let Some(arg_oids) = parse_proc_argtype_oids(&row.proargtypes) else {
        return inferred;
    };
    let mut compatible_loose = Vec::new();
    let mut compatible_anchor = None;
    for (oid, actual_type) in arg_oids.into_iter().zip(actual_arg_types.iter().copied()) {
        let Some(actual_type) = actual_type else {
            continue;
        };
        match oid {
            ANYOID | ANYELEMENTOID => {
                merge_exact_sql_type(&mut inferred.anyelement, actual_type);
            }
            ANYNONARRAYOID if !actual_type.is_array => {
                merge_exact_sql_type(&mut inferred.anyelement, actual_type);
            }
            ANYENUMOID if matches!(actual_type.kind, SqlTypeKind::Enum | SqlTypeKind::AnyEnum) => {
                merge_exact_sql_type(&mut inferred.anyelement, actual_type);
            }
            ANYARRAYOID if actual_type.is_array => {
                inferred.anyarray.get_or_insert(actual_type);
                merge_exact_sql_type(&mut inferred.anyelement, actual_type.element_type());
            }
            ANYRANGEOID if actual_type.is_range() => {
                inferred.anyrange.get_or_insert(actual_type);
                if let Some(range_type) = range_type_ref_for_sql_type(actual_type) {
                    merge_exact_sql_type(&mut inferred.anyelement, range_type.subtype);
                }
            }
            ANYMULTIRANGEOID if actual_type.is_multirange() => {
                inferred.anymultirange.get_or_insert(actual_type);
                if let Some(range_type) = range_type_ref_for_multirange_sql_type(actual_type) {
                    merge_exact_sql_type(&mut inferred.anyelement, range_type.subtype);
                }
            }
            ANYCOMPATIBLEOID => compatible_loose.push(actual_type),
            ANYCOMPATIBLENONARRAYOID if !actual_type.is_array => compatible_loose.push(actual_type),
            ANYCOMPATIBLEARRAYOID if actual_type.is_array => {
                compatible_loose.push(actual_type.element_type());
            }
            ANYCOMPATIBLERANGEOID if actual_type.is_range() => {
                inferred.anycompatiblerange.get_or_insert(actual_type);
                if let Some(range_type) = range_type_ref_for_sql_type(actual_type) {
                    compatible_anchor.get_or_insert(range_type.subtype);
                }
            }
            ANYCOMPATIBLEMULTIRANGEOID if actual_type.is_multirange() => {
                inferred.anycompatiblemultirange.get_or_insert(actual_type);
                if let Some(range_type) = range_type_ref_for_multirange_sql_type(actual_type) {
                    compatible_anchor.get_or_insert(range_type.subtype);
                }
            }
            _ => {}
        }
    }
    inferred.anycompatible = if let Some(anchor) = compatible_anchor {
        compatible_loose
            .iter()
            .all(|ty| can_coerce_to_compatible_anchor(*ty, anchor))
            .then_some(anchor)
    } else {
        compatible_loose
            .into_iter()
            .try_fold(None, merge_loose_compatible_type)
            .flatten()
    };
    inferred
}

fn concrete_polymorphic_all_arg_oids(
    all_arg_types: &[u32],
    arg_modes: &[u8],
    actual_arg_types: &[Option<SqlType>],
    inferred: &InferredPolymorphicTypes,
    catalog: &dyn CatalogLookup,
    changed: &mut bool,
) -> Vec<u32> {
    let mut input_index = 0usize;
    all_arg_types
        .iter()
        .copied()
        .zip(arg_modes.iter().copied())
        .map(|(oid, mode)| {
            let replacement = if matches!(mode, b'i' | b'b') {
                let actual_type = actual_arg_types.get(input_index).copied().flatten();
                input_index = input_index.saturating_add(1);
                actual_type
            } else {
                concrete_polymorphic_sql_type(oid, inferred)
            };
            if is_polymorphic_type_oid(oid)
                && let Some(actual_type) = replacement
                && let Some(actual_oid) = concrete_type_oid(actual_type, catalog)
                && !is_polymorphic_type_oid(actual_oid)
            {
                *changed = true;
                actual_oid
            } else {
                oid
            }
        })
        .collect()
}

fn concrete_polymorphic_sql_type(oid: u32, inferred: &InferredPolymorphicTypes) -> Option<SqlType> {
    match oid {
        ANYOID | ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID => inferred.anyelement,
        ANYARRAYOID => inferred
            .anyarray
            .or_else(|| inferred.anyelement.map(SqlType::array_of)),
        ANYRANGEOID => inferred.anyrange,
        ANYMULTIRANGEOID => inferred.anymultirange,
        ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => inferred.anycompatible,
        ANYCOMPATIBLEARRAYOID => inferred.anycompatible.map(SqlType::array_of),
        ANYCOMPATIBLERANGEOID => inferred.anycompatiblerange,
        ANYCOMPATIBLEMULTIRANGEOID => inferred.anycompatiblemultirange,
        _ => None,
    }
}

fn merge_exact_sql_type(existing: &mut Option<SqlType>, next: SqlType) {
    if existing.is_none() {
        *existing = Some(next);
    }
}

fn merge_loose_compatible_type(
    existing: Option<SqlType>,
    next: SqlType,
) -> Option<Option<SqlType>> {
    match existing {
        None => Some(Some(next)),
        Some(existing) if existing == next => Some(Some(existing)),
        Some(existing)
            if matches!(
                existing.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
            ) && next.kind == SqlTypeKind::Numeric =>
        {
            Some(Some(next))
        }
        Some(existing)
            if existing.kind == SqlTypeKind::Numeric
                && matches!(
                    next.kind,
                    SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
                ) =>
        {
            Some(Some(existing))
        }
        Some(existing) if is_text_like_type(next) && !is_text_like_type(existing) => {
            Some(Some(existing))
        }
        Some(existing) if is_text_like_type(existing) && !is_text_like_type(next) => {
            Some(Some(next))
        }
        Some(_) => None,
    }
}

fn is_text_like_type(ty: SqlType) -> bool {
    !ty.is_array
        && matches!(
            ty.kind,
            SqlTypeKind::Text | SqlTypeKind::Name | SqlTypeKind::Char | SqlTypeKind::Varchar
        )
}

fn can_coerce_to_compatible_anchor(value: SqlType, anchor: SqlType) -> bool {
    value == anchor
        || matches!(
            (value.kind, anchor.kind),
            (SqlTypeKind::Int2, SqlTypeKind::Int4)
                | (SqlTypeKind::Int2, SqlTypeKind::Int8)
                | (SqlTypeKind::Int2, SqlTypeKind::Numeric)
                | (SqlTypeKind::Int2, SqlTypeKind::Float4)
                | (SqlTypeKind::Int2, SqlTypeKind::Float8)
                | (SqlTypeKind::Int4, SqlTypeKind::Int8)
                | (SqlTypeKind::Int4, SqlTypeKind::Numeric)
                | (SqlTypeKind::Int4, SqlTypeKind::Float4)
                | (SqlTypeKind::Int4, SqlTypeKind::Float8)
                | (SqlTypeKind::Int8, SqlTypeKind::Numeric)
                | (SqlTypeKind::Int8, SqlTypeKind::Float4)
                | (SqlTypeKind::Int8, SqlTypeKind::Float8)
                | (SqlTypeKind::Numeric, SqlTypeKind::Float4)
                | (SqlTypeKind::Numeric, SqlTypeKind::Float8)
                | (SqlTypeKind::Float4, SqlTypeKind::Float8)
        )
}

pub fn is_polymorphic_type_oid(oid: u32) -> bool {
    matches!(
        oid,
        ANYOID
            | ANYELEMENTOID
            | ANYNONARRAYOID
            | ANYENUMOID
            | ANYARRAYOID
            | ANYRANGEOID
            | ANYMULTIRANGEOID
            | ANYCOMPATIBLEOID
            | ANYCOMPATIBLENONARRAYOID
            | ANYCOMPATIBLEARRAYOID
            | ANYCOMPATIBLERANGEOID
            | ANYCOMPATIBLEMULTIRANGEOID
    )
}
