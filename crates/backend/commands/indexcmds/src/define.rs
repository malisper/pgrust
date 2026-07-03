// DefineIndex + ComputeIndexAttrs + ChooseIndex*Name* (indexcmds.c), plain
// btree lane. Loud: CONCURRENTLY, INCLUDE, WHERE, expression columns, named
// opclasses/collations, WITH options, TABLESPACE, exclusion/WITHOUT OVERLAPS,
// partitioned tables, constraint-backed (PRIMARY KEY/UNIQUE ... ADD
// CONSTRAINT) indexes, non-btree AMs.
use catalog_index::{IndexCreateExtra, BTREE_AM_OID, INDEX_CREATE_IS_PRIMARY};
use datum::Datum;
use execindexing::IndexInfo;
use mcx::{Mcx, PgString, PgVec};
use types_core::{
    AttrNumber, InvalidOid, Oid, RegProcedure, INDEX_MAX_KEYS, NAMEDATALEN,
    RELATION_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INDETERMINATE_COLLATION,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::rawnodes::{IndexElem, IndexStmt, SortByDir, SortByNulls};
use types_rel::{Relation, ShareLock, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::GetDefaultOpClass;

const NamespaceRelationId: Oid = 2615;
const ClassNameNspIndexId: Oid = 2663;
const Anum_pg_class_relname: AttrNumber = 2;
const Anum_pg_class_relnamespace: AttrNumber = 3;
const F_NAMEEQ: RegProcedure = 62;
const F_OIDEQ: RegProcedure = 184;
const ACL_CREATE: u64 = 1 << 9;
const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: indexcmds {what}")
}

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn DefineIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tableId: Oid,
    stmt: &IndexStmt<'mcx>,
    indexRelationId: Oid,
    is_alter_table: bool,
    check_rights: bool,
    check_not_in_use: bool,
    skip_build: bool,
    quiet: bool,
) -> PgResult<Oid> {
    let _ = (is_alter_table, quiet);
    if stmt.concurrent {
        unported("DefineIndex: CONCURRENTLY");
    }
    if stmt.reset_default_tblspc {
        unported("DefineIndex: reset_default_tblspc");
    }
    if !stmt.excludeOpNames.is_nil() || stmt.iswithoutoverlaps {
        unported("DefineIndex: exclusion / WITHOUT OVERLAPS constraints");
    }
    if !stmt.indexIncludingParams.is_nil() {
        unported("DefineIndex: INCLUDE columns");
    }
    if stmt.whereClause.is_some() {
        unported("DefineIndex: partial-index predicates");
    }
    if !stmt.options.is_nil() {
        unported("DefineIndex: WITH reloptions");
    }
    if stmt.tableSpace.is_some() {
        unported("DefineIndex: TABLESPACE");
    }
    if stmt.isconstraint || stmt.primary || stmt.deferrable || stmt.initdeferred {
        unported("DefineIndex: constraint-backed indexes (index_constraint_create)");
    }
    if stmt.oldNumber != 0 || skip_build {
        unported("DefineIndex: skip_build / oldNumber reuse");
    }
    // Closed-set AM name resolution (C: get_index_am_oid + GetIndexAmRoutine).
    let (accessMethodId, amname, amcanorder, amcanunique, amcanmulticol) =
        match stmt.accessMethod {
            Some("btree") => (BTREE_AM_OID, "btree", true, true, true),
            Some("hash") => (catalog_index::HASH_AM_OID, "hash", false, false, false),
            other => unported(&format!("DefineIndex: access method {other:?} (AMNAME lookup)")),
        };
    if stmt.unique && !amcanunique {
        return Err(err(
            format!("access method \"{amname}\" does not support unique indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let root_save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    let numberOfKeyAttributes = stmt.indexParams.len();
    let numberOfAttributes = numberOfKeyAttributes;
    if numberOfKeyAttributes > 1 && !amcanmulticol {
        return Err(err(
            format!("access method \"{amname}\" does not support multicolumn indexes"),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if numberOfAttributes == 0 {
        return Err(err("must specify at least one column".into(), ERRCODE_INVALID_OBJECT_DEFINITION));
    }
    if numberOfAttributes > INDEX_MAX_KEYS as usize {
        return Err(err(
            format!("cannot use more than {INDEX_MAX_KEYS} columns in an index"),
            ERRCODE_TOO_MANY_COLUMNS,
        ));
    }

    let rel = table::table_open(mcx, tableId, ShareLock)?;
    let (root_save_userid, _) = miscinit::GetUserIdAndSecContext();
    let guard = miscinit::SecContextGuard::security_restricted(rel.rd_rel.relowner);

    let namespaceId = rel.rd_rel.relnamespace;

    match rel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_MATVIEW => {}
        RELKIND_PARTITIONED_TABLE => unported("DefineIndex: partitioned tables"),
        _ => {
            return Err(err(
                format!("cannot create index on relation \"{}\"", rel.name()),
                ERRCODE_WRONG_OBJECT_TYPE,
            ))
        }
    }

    if check_not_in_use {
        catalog_heap::CheckTableNotInUse(&rel, "CREATE INDEX")?;
    }

    if check_rights && !miscinit_seams::is_bootstrap_processing_mode::call() {
        let aclresult = aclchk_seams::object_aclcheck::call(
            NamespaceRelationId,
            namespaceId,
            root_save_userid,
            ACL_CREATE,
        )?;
        if aclresult != 0 {
            let nspname = lsyscache::get_namespace_name(mcx, namespaceId)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(err(
                format!("permission denied for schema {nspname}"),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if rel.rd_rel.relisshared {
        unported("DefineIndex: shared relations");
    }
    let tablespaceId = InvalidOid; // GetDefaultTablespace (DefineRelation precedent)

    let indexColNames = ChooseIndexColumnNames(mcx, &stmt.indexParams)?;
    let name_storage;
    let indexRelationName: &str = match stmt.idxname {
        Some(n) => n,
        None => {
            name_storage = ChooseIndexName(mcx, rel.name(), namespaceId, &indexColNames)?;
            name_storage.as_str()
        }
    };

    let mut indexInfo = IndexInfo {
        ii_NumIndexAttrs: numberOfAttributes as i32,
        ii_NumIndexKeyAttrs: numberOfKeyAttributes as i32,
        ii_IndexAttrNumbers: [0; INDEX_MAX_KEYS as usize],
        ii_Unique: stmt.unique,
        ii_NullsNotDistinct: stmt.nulls_not_distinct,
        ii_ReadyForInserts: true,
        ii_Summarizing: false,
        ii_Concurrent: false,
        ii_BrokenHotChain: false,
    };

    let mut collationIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut opclassIds = [InvalidOid; INDEX_MAX_KEYS as usize];
    let mut coloptions = [0i16; INDEX_MAX_KEYS as usize];
    ComputeIndexAttrs(
        mcx,
        &rel,
        &mut indexInfo,
        &mut collationIds,
        &mut opclassIds,
        &mut coloptions,
        &stmt.indexParams,
        stmt.isconstraint,
        accessMethodId,
        amname,
        amcanorder,
    )?;

    for i in 0..numberOfAttributes {
        if indexInfo.ii_IndexAttrNumbers[i] < 0 {
            return Err(err(
                "index creation on system columns is not supported".into(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }

    let mut colname_refs: PgVec<'_, &str> = PgVec::new_in(mcx);
    for n in indexColNames.iter() {
        colname_refs.push(n.as_str());
    }

    let indexRelationId = catalog_index::index_create(
        mcx,
        &rel,
        indexRelationName,
        indexRelationId,
        &mut indexInfo,
        &colname_refs,
        accessMethodId,
        tablespaceId,
        &collationIds[..numberOfAttributes],
        &opclassIds[..numberOfAttributes],
        &coloptions[..numberOfAttributes],
        &IndexCreateExtra {
            flags: if stmt.primary { INDEX_CREATE_IS_PRIMARY } else { 0 },
            constr_flags: 0,
            allow_system_table_mods: false,
            is_internal: !check_rights,
        },
    )?;

    guc::AtEOXact_GUC(false, root_save_nestlevel);
    guard.restore();

    rel.close(types_rel::NoLock)?;
    Ok(indexRelationId)
}

#[allow(clippy::too_many_arguments)]
fn ComputeIndexAttrs<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    indexInfo: &mut IndexInfo,
    collationIds: &mut [Oid],
    opclassIds: &mut [Oid],
    coloptions: &mut [i16],
    attList: &types_nodes::NodeList<'mcx>,
    isconstraint: bool,
    accessMethodId: Oid,
    amname: &str,
    amcanorder: bool,
) -> PgResult<()> {
    let _ = mcx;
    for (attn, node) in attList.iter().enumerate() {
        let attribute = node
            .as_variant::<IndexElem>()
            .unwrap_or_else(|| panic!("IndexElem expected in indexParams"));
        let Some(name) = attribute.name else {
            unported("ComputeIndexAttrs: expression index columns");
        };
        if !attribute.opclass.is_nil() || !attribute.opclassopts.is_nil() {
            unported("ComputeIndexAttrs: named operator classes (ResolveOpClass)");
        }
        if !attribute.collation.is_nil() {
            unported("ComputeIndexAttrs: COLLATE overrides (get_collation_oid)");
        }

        let desc = rel.descr();
        let mut found = None;
        for i in 0..desc.natts as usize {
            let att = desc.attr(i);
            if !att.attisdropped && att.attname.name_str() == name.as_bytes() {
                found = Some(*att);
                break;
            }
        }
        let Some(attform) = found else {
            let msg = if isconstraint {
                format!("column \"{name}\" named in key does not exist")
            } else {
                format!("column \"{name}\" does not exist")
            };
            return Err(err(msg, ERRCODE_UNDEFINED_COLUMN));
        };
        indexInfo.ii_IndexAttrNumbers[attn] = attform.attnum;
        let atttype = attform.atttypid;
        let attcollation = attform.attcollation;

        if lsyscache::type_is_collatable(atttype)? {
            if attcollation == InvalidOid {
                return Err(err(
                    "could not determine which collation to use for index expression".into(),
                    ERRCODE_INDETERMINATE_COLLATION,
                ));
            }
        } else if attcollation != InvalidOid {
            return Err(err(
                format!(
                    "collations are not supported by type {}",
                    format_type::format_type_be(atttype)?
                ),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
        collationIds[attn] = attcollation;

        opclassIds[attn] = GetDefaultOpClass(atttype, accessMethodId)?;
        if opclassIds[attn] == InvalidOid {
            return Err(err(
                format!(
                    "data type {} has no default operator class for access method \"{amname}\"",
                    format_type::format_type_be(atttype)?
                ),
                ERRCODE_UNDEFINED_OBJECT,
            ));
        }

        coloptions[attn] = 0;
        if amcanorder {
            if attribute.ordering == SortByDir::SORTBY_DESC {
                coloptions[attn] |= INDOPTION_DESC;
            }
            match attribute.nulls_ordering {
                SortByNulls::SORTBY_NULLS_DEFAULT => {
                    if attribute.ordering == SortByDir::SORTBY_DESC {
                        coloptions[attn] |= INDOPTION_NULLS_FIRST;
                    }
                }
                SortByNulls::SORTBY_NULLS_FIRST => coloptions[attn] |= INDOPTION_NULLS_FIRST,
                SortByNulls::SORTBY_NULLS_LAST => {}
            }
        } else {
            if attribute.ordering != SortByDir::SORTBY_DEFAULT {
                return Err(err(
                    format!("access method \"{amname}\" does not support ASC/DESC options"),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
            if attribute.nulls_ordering != SortByNulls::SORTBY_NULLS_DEFAULT {
                return Err(err(
                    format!(
                        "access method \"{amname}\" does not support NULLS FIRST/LAST options"
                    ),
                    types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
                ));
            }
        }
    }
    Ok(())
}

// ChooseIndexName, non-constraint arm (pkey/key/excl labels ride the
// constraint lane).
fn ChooseIndexName<'mcx>(
    mcx: Mcx<'mcx>,
    tabname: &str,
    namespaceId: Oid,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let addition = ChooseIndexNameAddition(mcx, colnames)?;
    ChooseRelationName(mcx, tabname, Some(addition.as_str()), "idx", namespaceId)
}

fn ChooseIndexNameAddition<'mcx>(
    mcx: Mcx<'mcx>,
    colnames: &[PgString<'mcx>],
) -> PgResult<PgString<'mcx>> {
    let mut buf = PgString::new_in(mcx);
    for name in colnames {
        if !buf.is_empty() {
            buf.try_push_str("_")?;
        }
        buf.try_push_str(name.as_str())?;
        if buf.len() >= NAMEDATALEN as usize {
            unported("ChooseIndexNameAddition: name truncation");
        }
    }
    Ok(buf)
}

fn ChooseIndexColumnNames<'mcx>(
    mcx: Mcx<'mcx>,
    indexElems: &types_nodes::NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, PgString<'mcx>>> {
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for node in indexElems.iter() {
        let ielem = node.as_variant::<IndexElem>().expect("IndexElem");
        let origname = ielem
            .indexcolname
            .or(ielem.name)
            .unwrap_or("expr");
        let mut curname = PgString::from_str_in(origname, mcx)?;
        let mut i = 1;
        while result.iter().any(|n| n.as_str() == curname.as_str()) {
            if origname.len() + 10 >= NAMEDATALEN as usize {
                unported("ChooseIndexColumnNames: mbcliplen truncation");
            }
            curname = PgString::from_str_in(origname, mcx)?;
            use core::fmt::Write;
            write!(curname, "{i}").expect("suffix");
            i += 1;
        }
        result.push(curname);
    }
    Ok(result)
}

// ChooseRelationName, isconstraint=false arm. C divergence: probes pg_class
// under the transaction snapshot, not a dirty snapshot (single-backend lane).
fn ChooseRelationName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespaceid: Oid,
) -> PgResult<PgString<'mcx>> {
    let pgclassrel = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let mut pass = 0;
    let mut modlabel = PgString::from_str_in(label, mcx)?;
    let relname = loop {
        let relname = make_object_name(mcx, name1, name2, modlabel.as_str())?;
        let cname = name_arg(mcx, relname.as_str())?;
        let keys = [
            eq_key(Anum_pg_class_relname, F_NAMEEQ, Datum::from_usize(cname.as_ptr() as usize)),
            eq_key(Anum_pg_class_relnamespace, F_OIDEQ, Datum::from_oid(namespaceid)),
        ];
        let mut scan =
            genam::systable_beginscan(mcx, &pgclassrel, ClassNameNspIndexId, true, None, &keys)?;
        let collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        if !collides {
            break relname;
        }
        pass += 1;
        modlabel = PgString::from_str_in(label, mcx)?;
        use core::fmt::Write;
        write!(modlabel, "{pass}").expect("label suffix");
    };
    pgclassrel.close(types_rel::AccessShareLock)?;
    Ok(relname)
}

// makeObjectName without the truncation lane (loud on overflow).
fn make_object_name<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<PgString<'mcx>> {
    let mut s = PgString::from_str_in(name1, mcx)?;
    if let Some(n2) = name2 {
        s.try_push_str("_")?;
        s.try_push_str(n2)?;
    }
    s.try_push_str("_")?;
    s.try_push_str(label)?;
    assert!(
        s.len() < NAMEDATALEN as usize,
        "makeObjectName (indexcmds.c): identifier truncation unported ({:?})",
        s.as_str()
    );
    Ok(s)
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "makeObjectName truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}
