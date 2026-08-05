// heap.c partition DDL slice: StorePartitionKey / StorePartitionBound;
// update_default_partition_oid (catalog/partition.c) rides here.
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, DEFAULT_COLLATION_OID, RELATION_RELATION_ID};
use types_error::PgResult;
use types_rel::{Relation, RowExclusiveLock, RELKIND_PARTITIONED_TABLE};

use pg_depend::ObjectAddress;

const PartitionedRelationId: Oid = 3350;
const PartitionedRelidIndexId: Oid = 3351;
const Natts_pg_partitioned_table: usize = 8;
const Anum_pg_partitioned_table_partdefid: usize = 4;
const INT2OID: Oid = 21;
const OIDOID: Oid = 26;
const Anum_pg_class_relpartbound: usize = 34;
const Anum_pg_class_relispartition: usize = 28;

#[allow(clippy::too_many_arguments)]
pub fn StorePartitionKey<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    strategy: u8,
    partnatts: i16,
    partattrs: &[AttrNumber],
    partexprs: &types_nodes::NodeList<'mcx>,
    partopclass: &[Oid],
    partcollation: &[Oid],
) -> PgResult<()> {
    debug_assert!(rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE);

    let n = partnatts as usize;
    let mut attr_datums: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut class_datums: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut coll_datums: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        attr_datums.push(Datum::from_i16(partattrs[i]));
        class_datums.push(Datum::from_oid(partopclass[i]));
        coll_datums.push(Datum::from_oid(partcollation[i]));
    }
    let partattrs_vec =
        datum::array_build::construct_vector_image(mcx, &attr_datums, INT2OID, 2, b's')?;
    let partclass_vec =
        datum::array_build::construct_vector_image(mcx, &class_datums, OIDOID, 4, b'i')?;
    let partcollation_vec =
        datum::array_build::construct_vector_image(mcx, &coll_datums, OIDOID, 4, b'i')?;

    let pg_partitioned_table = table::table_open(mcx, PartitionedRelationId, RowExclusiveLock)?;
    let mut values = [Datum::null(); Natts_pg_partitioned_table];
    let mut nulls = [false; Natts_pg_partitioned_table];
    values[0] = Datum::from_oid(rel.rd_id);
    values[1] = Datum::from_char(strategy as i8);
    values[2] = Datum::from_i16(partnatts);
    values[3] = Datum::from_oid(InvalidOid); // partdefid
    values[4] = Datum::from_usize(partattrs_vec.as_ptr() as usize);
    values[5] = Datum::from_usize(partclass_vec.as_ptr() as usize);
    values[6] = Datum::from_usize(partcollation_vec.as_ptr() as usize);
    // exprs_text must outlive heap_form_tuple below (values[7] borrows it).
    let mut exprs_text = None;
    let exprs_node = if partexprs.is_nil() {
        nulls[7] = true;
        None
    } else {
        let node = types_nodes::Node::mk_list(mcx, partexprs.clone_in(mcx)?)?;
        let s = outfuncs::nodeToString(mcx, node)?;
        let text = exprs_text.insert(varlena::cstring_to_text(mcx, s.as_bytes())?);
        values[7] = Datum::from_usize(text.as_bytes().as_ptr() as usize);
        Some(node)
    };

    let mut tuple =
        heaptuple::heap_form_tuple(mcx, pg_partitioned_table.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_partitioned_table, &mut tuple)?;
    pg_partitioned_table.close(RowExclusiveLock)?;

    let myself = ObjectAddress::set(RELATION_RELATION_ID, rel.rd_id);
    for i in 0..n {
        let referenced = ObjectAddress::set(catalog::OperatorClassRelationId, partopclass[i]);
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, pg_depend::DependencyType::Normal)?;
        if partcollation[i] != InvalidOid && partcollation[i] != DEFAULT_COLLATION_OID {
            let referenced = ObjectAddress::set(catalog::CollationRelationId, partcollation[i]);
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &referenced,
                pg_depend::DependencyType::Normal,
            )?;
        }
    }
    for i in 0..n {
        if partattrs[i] == 0 {
            continue;
        }
        let referenced =
            ObjectAddress::sub_set(RELATION_RELATION_ID, rel.rd_id, partattrs[i] as i32);
        pg_depend::recordDependencyOn(
            mcx,
            &referenced,
            &myself,
            pg_depend::DependencyType::Internal,
        )?;
    }
    if let Some(exprs_node) = exprs_node {
        pg_depend::recordDependencyOnSingleRelExpr(
            mcx,
            &myself,
            exprs_node,
            rel.rd_id,
            pg_depend::DependencyType::Normal,
            pg_depend::DependencyType::Internal,
            // C StorePartitionKey (heap.c): reverse the self-deps.
            true,
        )?;
    }

    inval::invalidate::CacheInvalidateRelcache(rel)?;
    Ok(())
}

pub fn StorePartitionBound<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    parent: &Relation<'mcx>,
    bound: types_nodes::Node<'mcx>,
) -> PgResult<()> {
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let keys = [crate::drop::oid_scankey(1, rel.rd_id)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &class_rel,
        catalog::ClassOidIndexId,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {}", rel.rd_id));
    let desc = class_rel.descr();

    let bound_str = outfuncs::nodeToString(mcx, bound)?;
    let bound_text = varlena::cstring_to_text(mcx, bound_str.as_bytes())?;

    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[Anum_pg_class_relpartbound - 1] =
        Datum::from_usize(bound_text.as_bytes().as_ptr() as usize);
    replace[Anum_pg_class_relpartbound - 1] = true;
    values[Anum_pg_class_relispartition - 1] = Datum::from_bool(true);
    replace[Anum_pg_class_relispartition - 1] = true;

    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
    class_rel.close(RowExclusiveLock)?;

    let spec = bound
        .as_variant::<types_nodes::rawnodes::PartitionBoundSpec>()
        .expect("PartitionBoundSpec");
    if spec.is_default {
        update_default_partition_oid(mcx, parent.rd_id, rel.rd_id)?;
    }

    xact::CommandCounterIncrement()?;

    // The default partition's constraint depends on every sibling's bound;
    // invalidate it whenever a partition is added.
    let default_part_oid = partcache::get_default_partition_oid(parent.rd_id)?;
    if default_part_oid != InvalidOid {
        inval::invalidate::CacheInvalidateRelcacheByRelid(default_part_oid)?;
    }
    inval::invalidate::CacheInvalidateRelcache(parent)?;
    Ok(())
}

// RemovePartitionKeyByRelId (heap.c).
pub fn RemovePartitionKeyByRelId<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, PartitionedRelationId, RowExclusiveLock)?;
    let keys = [crate::drop::oid_scankey(1, relid)];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, PartitionedRelidIndexId, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for partition key of relation {relid}"));
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

pub fn update_default_partition_oid<'mcx>(
    mcx: Mcx<'mcx>,
    parent_id: Oid,
    default_part_id: Oid,
) -> PgResult<()> {
    let part_table = table::table_open(mcx, PartitionedRelationId, RowExclusiveLock)?;
    let keys = [crate::drop::oid_scankey(1, parent_id)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &part_table,
        PartitionedRelidIndexId,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for partition key of relation {parent_id}"));
    let desc = part_table.descr();
    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[Anum_pg_partitioned_table_partdefid - 1] = Datum::from_oid(default_part_id);
    replace[Anum_pg_partitioned_table_partdefid - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &part_table, &otid, &mut newtup)?;
    part_table.close(RowExclusiveLock)
}
