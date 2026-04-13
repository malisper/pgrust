use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::decode_value;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::cache::catcache::format_indkey;
use crate::include::catalog::{
    BootstrapCatalogKind, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow,
    PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow, PgConstraintRow,
    PgDatabaseRow, PgDependRow, PgIndexRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow,
    PgOperatorRow, PgOpfamilyRow, PgProcRow, PgTablespaceRow, PgTypeRow,
    bootstrap_composite_type_rows, builtin_type_rows,
};
use crate::include::nodes::datum::Value;

pub(crate) fn catalog_row_values_for_kind(
    rows: &PhysicalCatalogRows,
    kind: BootstrapCatalogKind,
) -> Vec<Vec<Value>> {
    match kind {
        BootstrapCatalogKind::PgNamespace => rows
            .namespaces
            .iter()
            .cloned()
            .map(namespace_row_values)
            .collect(),
        BootstrapCatalogKind::PgClass => rows
            .classes
            .iter()
            .cloned()
            .map(pg_class_row_values)
            .collect(),
        BootstrapCatalogKind::PgAttribute => rows
            .attributes
            .iter()
            .cloned()
            .map(pg_attribute_row_values)
            .collect(),
        BootstrapCatalogKind::PgType => {
            rows.types.iter().cloned().map(pg_type_row_values).collect()
        }
        BootstrapCatalogKind::PgProc => {
            rows.procs.iter().cloned().map(pg_proc_row_values).collect()
        }
        BootstrapCatalogKind::PgLanguage => rows
            .languages
            .iter()
            .cloned()
            .map(pg_language_row_values)
            .collect(),
        BootstrapCatalogKind::PgOperator => rows
            .operators
            .iter()
            .cloned()
            .map(pg_operator_row_values)
            .collect(),
        BootstrapCatalogKind::PgDatabase => rows
            .databases
            .iter()
            .cloned()
            .map(pg_database_row_values)
            .collect(),
        BootstrapCatalogKind::PgAuthId => rows
            .authids
            .iter()
            .cloned()
            .map(pg_authid_row_values)
            .collect(),
        BootstrapCatalogKind::PgAuthMembers => rows
            .auth_members
            .iter()
            .cloned()
            .map(pg_auth_members_row_values)
            .collect(),
        BootstrapCatalogKind::PgCollation => rows
            .collations
            .iter()
            .cloned()
            .map(pg_collation_row_values)
            .collect(),
        BootstrapCatalogKind::PgTablespace => rows
            .tablespaces
            .iter()
            .cloned()
            .map(pg_tablespace_row_values)
            .collect(),
        BootstrapCatalogKind::PgAm => rows.ams.iter().cloned().map(pg_am_row_values).collect(),
        BootstrapCatalogKind::PgAmop => {
            rows.amops.iter().cloned().map(pg_amop_row_values).collect()
        }
        BootstrapCatalogKind::PgAmproc => rows
            .amprocs
            .iter()
            .cloned()
            .map(pg_amproc_row_values)
            .collect(),
        BootstrapCatalogKind::PgAttrdef => rows
            .attrdefs
            .iter()
            .cloned()
            .map(pg_attrdef_row_values)
            .collect(),
        BootstrapCatalogKind::PgCast => {
            rows.casts.iter().cloned().map(pg_cast_row_values).collect()
        }
        BootstrapCatalogKind::PgConstraint => rows
            .constraints
            .iter()
            .cloned()
            .map(pg_constraint_row_values)
            .collect(),
        BootstrapCatalogKind::PgDepend => rows
            .depends
            .iter()
            .cloned()
            .map(pg_depend_row_values)
            .collect(),
        BootstrapCatalogKind::PgIndex => rows
            .indexes
            .iter()
            .cloned()
            .map(pg_index_row_values)
            .collect(),
        BootstrapCatalogKind::PgOpclass => rows
            .opclasses
            .iter()
            .cloned()
            .map(pg_opclass_row_values)
            .collect(),
        BootstrapCatalogKind::PgOpfamily => rows
            .opfamilies
            .iter()
            .cloned()
            .map(pg_opfamily_row_values)
            .collect(),
    }
}

pub(crate) fn decode_catalog_tuple_values(
    desc: &RelationDesc,
    tuple: &crate::include::access::htup::HeapTuple,
) -> Result<Vec<Value>, CatalogError> {
    let raw = tuple
        .deform(&desc.attribute_descs())
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    desc.columns
        .iter()
        .zip(raw.into_iter())
        .map(|(column, datum)| {
            decode_value(column, datum).map_err(|e| CatalogError::Io(format!("{e:?}")))
        })
        .collect()
}

pub(crate) fn parse_indkey(indkey: &str) -> Vec<i16> {
    indkey
        .split_ascii_whitespace()
        .filter_map(|value| value.parse::<i16>().ok())
        .collect()
}

pub(crate) fn namespace_row_from_values(
    values: Vec<Value>,
) -> Result<PgNamespaceRow, CatalogError> {
    Ok(PgNamespaceRow {
        oid: expect_oid(&values[0])?,
        nspname: expect_text(&values[1])?,
        nspowner: expect_oid(&values[2])?,
    })
}

pub(crate) fn pg_class_row_from_values(values: Vec<Value>) -> Result<PgClassRow, CatalogError> {
    let relpersistence = expect_char(&values[8], "relpersistence")?;
    let relkind = expect_char(&values[9], "relkind")?;
    Ok(PgClassRow {
        oid: expect_oid(&values[0])?,
        relname: expect_text(&values[1])?,
        relnamespace: expect_oid(&values[2])?,
        reltype: expect_oid(&values[3])?,
        relowner: expect_oid(&values[4])?,
        relam: expect_oid(&values[5])?,
        reltablespace: expect_oid(&values[6])?,
        relfilenode: expect_oid(&values[7])?,
        relpersistence,
        relkind,
    })
}

pub(crate) fn pg_am_row_from_values(values: Vec<Value>) -> Result<PgAmRow, CatalogError> {
    Ok(PgAmRow {
        oid: expect_oid(&values[0])?,
        amname: expect_text(&values[1])?,
        amhandler: expect_oid(&values[2])?,
        amtype: expect_char(&values[3], "amtype")?,
    })
}

pub(crate) fn pg_amop_row_from_values(values: Vec<Value>) -> Result<PgAmopRow, CatalogError> {
    Ok(PgAmopRow {
        oid: expect_oid(&values[0])?,
        amopfamily: expect_oid(&values[1])?,
        amoplefttype: expect_oid(&values[2])?,
        amoprighttype: expect_oid(&values[3])?,
        amopstrategy: expect_int16(&values[4])?,
        amoppurpose: expect_char(&values[5], "amoppurpose")?,
        amopopr: expect_oid(&values[6])?,
        amopmethod: expect_oid(&values[7])?,
        amopsortfamily: expect_oid(&values[8])?,
    })
}

pub(crate) fn pg_amproc_row_from_values(values: Vec<Value>) -> Result<PgAmprocRow, CatalogError> {
    Ok(PgAmprocRow {
        oid: expect_oid(&values[0])?,
        amprocfamily: expect_oid(&values[1])?,
        amproclefttype: expect_oid(&values[2])?,
        amprocrighttype: expect_oid(&values[3])?,
        amprocnum: expect_int16(&values[4])?,
        amproc: expect_oid(&values[5])?,
    })
}

pub(crate) fn pg_authid_row_from_values(values: Vec<Value>) -> Result<PgAuthIdRow, CatalogError> {
    Ok(PgAuthIdRow {
        oid: expect_oid(&values[0])?,
        rolname: expect_text(&values[1])?,
        rolsuper: expect_bool(&values[2])?,
        rolinherit: expect_bool(&values[3])?,
        rolcreaterole: expect_bool(&values[4])?,
        rolcreatedb: expect_bool(&values[5])?,
        rolcanlogin: expect_bool(&values[6])?,
        rolreplication: expect_bool(&values[7])?,
        rolbypassrls: expect_bool(&values[8])?,
        rolconnlimit: expect_int32(&values[9])?,
    })
}

pub(crate) fn pg_auth_members_row_from_values(
    values: Vec<Value>,
) -> Result<PgAuthMembersRow, CatalogError> {
    Ok(PgAuthMembersRow {
        oid: expect_oid(&values[0])?,
        roleid: expect_oid(&values[1])?,
        member: expect_oid(&values[2])?,
        grantor: expect_oid(&values[3])?,
        admin_option: expect_bool(&values[4])?,
        inherit_option: expect_bool(&values[5])?,
        set_option: expect_bool(&values[6])?,
    })
}

pub(crate) fn pg_language_row_from_values(
    values: Vec<Value>,
) -> Result<PgLanguageRow, CatalogError> {
    Ok(PgLanguageRow {
        oid: expect_oid(&values[0])?,
        lanname: expect_text(&values[1])?,
        lanowner: expect_oid(&values[2])?,
        lanispl: expect_bool(&values[3])?,
        lanpltrusted: expect_bool(&values[4])?,
        lanplcallfoid: expect_oid(&values[5])?,
        laninline: expect_oid(&values[6])?,
        lanvalidator: expect_oid(&values[7])?,
    })
}

pub(crate) fn pg_operator_row_from_values(
    values: Vec<Value>,
) -> Result<PgOperatorRow, CatalogError> {
    Ok(PgOperatorRow {
        oid: expect_oid(&values[0])?,
        oprname: expect_text(&values[1])?,
        oprnamespace: expect_oid(&values[2])?,
        oprowner: expect_oid(&values[3])?,
        oprkind: expect_char(&values[4], "oprkind")?,
        oprcanmerge: expect_bool(&values[5])?,
        oprcanhash: expect_bool(&values[6])?,
        oprleft: expect_oid(&values[7])?,
        oprright: expect_oid(&values[8])?,
        oprresult: expect_oid(&values[9])?,
        oprcom: expect_oid(&values[10])?,
        oprnegate: expect_oid(&values[11])?,
        oprcode: expect_oid(&values[12])?,
        oprrest: expect_oid(&values[13])?,
        oprjoin: expect_oid(&values[14])?,
    })
}

pub(crate) fn pg_proc_row_from_values(values: Vec<Value>) -> Result<PgProcRow, CatalogError> {
    Ok(PgProcRow {
        oid: expect_oid(&values[0])?,
        proname: expect_text(&values[1])?,
        pronamespace: expect_oid(&values[2])?,
        proowner: expect_oid(&values[3])?,
        prolang: expect_oid(&values[4])?,
        procost: expect_float64(&values[5])?,
        prorows: expect_float64(&values[6])?,
        provariadic: expect_oid(&values[7])?,
        prosupport: expect_oid(&values[8])?,
        prokind: expect_char(&values[9], "prokind")?,
        prosecdef: expect_bool(&values[10])?,
        proleakproof: expect_bool(&values[11])?,
        proisstrict: expect_bool(&values[12])?,
        proretset: expect_bool(&values[13])?,
        provolatile: expect_char(&values[14], "provolatile")?,
        proparallel: expect_char(&values[15], "proparallel")?,
        pronargs: expect_int16(&values[16])?,
        pronargdefaults: expect_int16(&values[17])?,
        prorettype: expect_oid(&values[18])?,
        proargtypes: expect_text(&values[19])?,
        prosrc: expect_text(&values[20])?,
    })
}

pub(crate) fn pg_collation_row_from_values(
    values: Vec<Value>,
) -> Result<PgCollationRow, CatalogError> {
    Ok(PgCollationRow {
        oid: expect_oid(&values[0])?,
        collname: expect_text(&values[1])?,
        collnamespace: expect_oid(&values[2])?,
        collowner: expect_oid(&values[3])?,
        collprovider: expect_char(&values[4], "collprovider")?,
        collisdeterministic: expect_bool(&values[5])?,
        collencoding: expect_int32(&values[6])?,
    })
}

pub(crate) fn pg_cast_row_from_values(values: Vec<Value>) -> Result<PgCastRow, CatalogError> {
    Ok(PgCastRow {
        oid: expect_oid(&values[0])?,
        castsource: expect_oid(&values[1])?,
        casttarget: expect_oid(&values[2])?,
        castfunc: expect_oid(&values[3])?,
        castcontext: expect_char(&values[4], "castcontext")?,
        castmethod: expect_char(&values[5], "castmethod")?,
    })
}

pub(crate) fn pg_constraint_row_from_values(
    values: Vec<Value>,
) -> Result<PgConstraintRow, CatalogError> {
    Ok(PgConstraintRow {
        oid: expect_oid(&values[0])?,
        conname: expect_text(&values[1])?,
        connamespace: expect_oid(&values[2])?,
        contype: expect_char(&values[3], "contype")?,
        condeferrable: expect_bool(&values[4])?,
        condeferred: expect_bool(&values[5])?,
        conenforced: expect_bool(&values[6])?,
        convalidated: expect_bool(&values[7])?,
        conrelid: expect_oid(&values[8])?,
        contypid: expect_oid(&values[9])?,
        conindid: expect_oid(&values[10])?,
        conparentid: expect_oid(&values[11])?,
        confrelid: expect_oid(&values[12])?,
        confupdtype: expect_char(&values[13], "confupdtype")?,
        confdeltype: expect_char(&values[14], "confdeltype")?,
        confmatchtype: expect_char(&values[15], "confmatchtype")?,
        conislocal: expect_bool(&values[16])?,
        coninhcount: expect_int16(&values[17])?,
        connoinherit: expect_bool(&values[18])?,
        conperiod: expect_bool(&values[19])?,
    })
}

pub(crate) fn pg_database_row_from_values(
    values: Vec<Value>,
) -> Result<PgDatabaseRow, CatalogError> {
    Ok(PgDatabaseRow {
        oid: expect_oid(&values[0])?,
        datname: expect_text(&values[1])?,
        datdba: expect_oid(&values[2])?,
        dattablespace: expect_oid(&values[3])?,
        datistemplate: expect_bool(&values[4])?,
        datallowconn: expect_bool(&values[5])?,
    })
}

pub(crate) fn pg_tablespace_row_from_values(
    values: Vec<Value>,
) -> Result<PgTablespaceRow, CatalogError> {
    Ok(PgTablespaceRow {
        oid: expect_oid(&values[0])?,
        spcname: expect_text(&values[1])?,
        spcowner: expect_oid(&values[2])?,
    })
}

pub(crate) fn pg_attribute_row_from_values(
    values: Vec<Value>,
) -> Result<PgAttributeRow, CatalogError> {
    Ok(PgAttributeRow {
        attrelid: expect_oid(&values[0])?,
        attname: expect_text(&values[1])?,
        atttypid: expect_oid(&values[2])?,
        attnum: expect_int16(&values[3])?,
        attnotnull: expect_bool(&values[4])?,
        atttypmod: expect_int32(&values[5])?,
        sql_type: SqlType::new(SqlTypeKind::Text),
    })
}

pub(crate) fn pg_attrdef_row_from_values(values: Vec<Value>) -> Result<PgAttrdefRow, CatalogError> {
    Ok(PgAttrdefRow {
        oid: expect_oid(&values[0])?,
        adrelid: expect_oid(&values[1])?,
        adnum: expect_int16(&values[2])?,
        adbin: expect_text(&values[3])?,
    })
}

pub(crate) fn pg_depend_row_from_values(values: Vec<Value>) -> Result<PgDependRow, CatalogError> {
    Ok(PgDependRow {
        classid: expect_oid(&values[0])?,
        objid: expect_oid(&values[1])?,
        objsubid: expect_int32(&values[2])?,
        refclassid: expect_oid(&values[3])?,
        refobjid: expect_oid(&values[4])?,
        refobjsubid: expect_int32(&values[5])?,
        deptype: expect_char(&values[6], "deptype")?,
    })
}

pub(crate) fn pg_opclass_row_from_values(values: Vec<Value>) -> Result<PgOpclassRow, CatalogError> {
    Ok(PgOpclassRow {
        oid: expect_oid(&values[0])?,
        opcmethod: expect_oid(&values[1])?,
        opcname: expect_text(&values[2])?,
        opcnamespace: expect_oid(&values[3])?,
        opcowner: expect_oid(&values[4])?,
        opcfamily: expect_oid(&values[5])?,
        opcintype: expect_oid(&values[6])?,
        opcdefault: expect_bool(&values[7])?,
        opckeytype: expect_oid(&values[8])?,
    })
}

pub(crate) fn pg_opfamily_row_from_values(
    values: Vec<Value>,
) -> Result<PgOpfamilyRow, CatalogError> {
    Ok(PgOpfamilyRow {
        oid: expect_oid(&values[0])?,
        opfmethod: expect_oid(&values[1])?,
        opfname: expect_text(&values[2])?,
        opfnamespace: expect_oid(&values[3])?,
        opfowner: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_index_row_from_values(values: Vec<Value>) -> Result<PgIndexRow, CatalogError> {
    Ok(PgIndexRow {
        indexrelid: expect_oid(&values[0])?,
        indrelid: expect_oid(&values[1])?,
        indnatts: expect_int16(&values[2])?,
        indnkeyatts: expect_int16(&values[3])?,
        indisunique: expect_bool(&values[4])?,
        indnullsnotdistinct: expect_bool(&values[5])?,
        indisprimary: expect_bool(&values[6])?,
        indisexclusion: expect_bool(&values[7])?,
        indimmediate: expect_bool(&values[8])?,
        indisclustered: expect_bool(&values[9])?,
        indisvalid: expect_bool(&values[10])?,
        indcheckxmin: expect_bool(&values[11])?,
        indisready: expect_bool(&values[12])?,
        indislive: expect_bool(&values[13])?,
        indisreplident: expect_bool(&values[14])?,
        indkey: parse_indkey(&expect_text(&values[15])?),
        indcollation: parse_indkey(&expect_text(&values[16])?)
            .into_iter()
            .map(|value| value as u32)
            .collect(),
        indclass: parse_indkey(&expect_text(&values[17])?)
            .into_iter()
            .map(|value| value as u32)
            .collect(),
        indoption: parse_indkey(&expect_text(&values[18])?),
        indexprs: expect_nullable_text(&values[19])?,
        indpred: expect_nullable_text(&values[20])?,
    })
}

pub(crate) fn pg_type_row_from_values(values: Vec<Value>) -> Result<PgTypeRow, CatalogError> {
    let oid = expect_oid(&values[0])?;
    Ok(PgTypeRow {
        oid,
        typname: expect_text(&values[1])?,
        typnamespace: expect_oid(&values[2])?,
        typowner: expect_oid(&values[3])?,
        typrelid: expect_oid(&values[4])?,
        sql_type: decode_builtin_sql_type(oid).unwrap_or(SqlType::new(SqlTypeKind::Text)),
    })
}

fn namespace_row_values(row: PgNamespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.nspname.into()),
        Value::Int32(row.nspowner as i32),
    ]
}

fn pg_class_row_values(row: PgClassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.relname.into()),
        Value::Int32(row.relnamespace as i32),
        Value::Int32(row.reltype as i32),
        Value::Int32(row.relowner as i32),
        Value::Int32(row.relam as i32),
        Value::Int32(row.reltablespace as i32),
        Value::Int32(row.relfilenode as i32),
        Value::Text(row.relpersistence.to_string().into()),
        Value::Text(row.relkind.to_string().into()),
    ]
}

fn pg_amop_row_values(row: PgAmopRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.amopfamily as i32),
        Value::Int32(row.amoplefttype as i32),
        Value::Int32(row.amoprighttype as i32),
        Value::Int16(row.amopstrategy),
        Value::Text(row.amoppurpose.to_string().into()),
        Value::Int32(row.amopopr as i32),
        Value::Int32(row.amopmethod as i32),
        Value::Int32(row.amopsortfamily as i32),
    ]
}

fn pg_amproc_row_values(row: PgAmprocRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.amprocfamily as i32),
        Value::Int32(row.amproclefttype as i32),
        Value::Int32(row.amprocrighttype as i32),
        Value::Int16(row.amprocnum),
        Value::Int32(row.amproc as i32),
    ]
}

fn pg_am_row_values(row: PgAmRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.amname.into()),
        Value::Int32(row.amhandler as i32),
        Value::Text(row.amtype.to_string().into()),
    ]
}

fn pg_authid_row_values(row: PgAuthIdRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.rolname.into()),
        Value::Bool(row.rolsuper),
        Value::Bool(row.rolinherit),
        Value::Bool(row.rolcreaterole),
        Value::Bool(row.rolcreatedb),
        Value::Bool(row.rolcanlogin),
        Value::Bool(row.rolreplication),
        Value::Bool(row.rolbypassrls),
        Value::Int32(row.rolconnlimit),
    ]
}

fn pg_auth_members_row_values(row: PgAuthMembersRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.roleid as i32),
        Value::Int32(row.member as i32),
        Value::Int32(row.grantor as i32),
        Value::Bool(row.admin_option),
        Value::Bool(row.inherit_option),
        Value::Bool(row.set_option),
    ]
}

fn pg_collation_row_values(row: PgCollationRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.collname.into()),
        Value::Int32(row.collnamespace as i32),
        Value::Int32(row.collowner as i32),
        Value::Text(row.collprovider.to_string().into()),
        Value::Bool(row.collisdeterministic),
        Value::Int32(row.collencoding),
    ]
}

fn pg_language_row_values(row: PgLanguageRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.lanname.into()),
        Value::Int32(row.lanowner as i32),
        Value::Bool(row.lanispl),
        Value::Bool(row.lanpltrusted),
        Value::Int32(row.lanplcallfoid as i32),
        Value::Int32(row.laninline as i32),
        Value::Int32(row.lanvalidator as i32),
    ]
}

fn pg_proc_row_values(row: PgProcRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.proname.into()),
        Value::Int32(row.pronamespace as i32),
        Value::Int32(row.proowner as i32),
        Value::Int32(row.prolang as i32),
        Value::Float64(row.procost),
        Value::Float64(row.prorows),
        Value::Int32(row.provariadic as i32),
        Value::Int32(row.prosupport as i32),
        Value::Text(row.prokind.to_string().into()),
        Value::Bool(row.prosecdef),
        Value::Bool(row.proleakproof),
        Value::Bool(row.proisstrict),
        Value::Bool(row.proretset),
        Value::Text(row.provolatile.to_string().into()),
        Value::Text(row.proparallel.to_string().into()),
        Value::Int16(row.pronargs),
        Value::Int16(row.pronargdefaults),
        Value::Int32(row.prorettype as i32),
        Value::Text(row.proargtypes.into()),
        Value::Text(row.prosrc.into()),
    ]
}

fn pg_operator_row_values(row: PgOperatorRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.oprname.into()),
        Value::Int32(row.oprnamespace as i32),
        Value::Int32(row.oprowner as i32),
        Value::Text(row.oprkind.to_string().into()),
        Value::Bool(row.oprcanmerge),
        Value::Bool(row.oprcanhash),
        Value::Int32(row.oprleft as i32),
        Value::Int32(row.oprright as i32),
        Value::Int32(row.oprresult as i32),
        Value::Int32(row.oprcom as i32),
        Value::Int32(row.oprnegate as i32),
        Value::Int32(row.oprcode as i32),
        Value::Int32(row.oprrest as i32),
        Value::Int32(row.oprjoin as i32),
    ]
}

fn pg_cast_row_values(row: PgCastRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.castsource as i32),
        Value::Int32(row.casttarget as i32),
        Value::Int32(row.castfunc as i32),
        Value::Text(row.castcontext.to_string().into()),
        Value::Text(row.castmethod.to_string().into()),
    ]
}

fn pg_constraint_row_values(row: PgConstraintRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.conname.into()),
        Value::Int32(row.connamespace as i32),
        Value::Text(row.contype.to_string().into()),
        Value::Bool(row.condeferrable),
        Value::Bool(row.condeferred),
        Value::Bool(row.conenforced),
        Value::Bool(row.convalidated),
        Value::Int32(row.conrelid as i32),
        Value::Int32(row.contypid as i32),
        Value::Int32(row.conindid as i32),
        Value::Int32(row.conparentid as i32),
        Value::Int32(row.confrelid as i32),
        Value::Text(row.confupdtype.to_string().into()),
        Value::Text(row.confdeltype.to_string().into()),
        Value::Text(row.confmatchtype.to_string().into()),
        Value::Bool(row.conislocal),
        Value::Int16(row.coninhcount),
        Value::Bool(row.connoinherit),
        Value::Bool(row.conperiod),
    ]
}

fn pg_database_row_values(row: PgDatabaseRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.datname.into()),
        Value::Int32(row.datdba as i32),
        Value::Int32(row.dattablespace as i32),
        Value::Bool(row.datistemplate),
        Value::Bool(row.datallowconn),
    ]
}

fn pg_tablespace_row_values(row: PgTablespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.spcname.into()),
        Value::Int32(row.spcowner as i32),
    ]
}

fn pg_attribute_row_values(row: PgAttributeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.attrelid as i32),
        Value::Text(row.attname.into()),
        Value::Int32(row.atttypid as i32),
        Value::Int16(row.attnum),
        Value::Bool(row.attnotnull),
        Value::Int32(row.atttypmod),
    ]
}

fn pg_type_row_values(row: PgTypeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.typname.into()),
        Value::Int32(row.typnamespace as i32),
        Value::Int32(row.typowner as i32),
        Value::Int32(row.typrelid as i32),
    ]
}

fn pg_attrdef_row_values(row: PgAttrdefRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.adrelid as i32),
        Value::Int16(row.adnum),
        Value::Text(row.adbin.into()),
    ]
}

fn pg_depend_row_values(row: PgDependRow) -> Vec<Value> {
    vec![
        Value::Int32(row.classid as i32),
        Value::Int32(row.objid as i32),
        Value::Int32(row.objsubid),
        Value::Int32(row.refclassid as i32),
        Value::Int32(row.refobjid as i32),
        Value::Int32(row.refobjsubid),
        Value::Text(row.deptype.to_string().into()),
    ]
}

fn pg_index_row_values(row: PgIndexRow) -> Vec<Value> {
    vec![
        Value::Int32(row.indexrelid as i32),
        Value::Int32(row.indrelid as i32),
        Value::Int16(row.indnatts),
        Value::Int16(row.indnkeyatts),
        Value::Bool(row.indisunique),
        Value::Bool(row.indnullsnotdistinct),
        Value::Bool(row.indisprimary),
        Value::Bool(row.indisexclusion),
        Value::Bool(row.indimmediate),
        Value::Bool(row.indisclustered),
        Value::Bool(row.indisvalid),
        Value::Bool(row.indcheckxmin),
        Value::Bool(row.indisready),
        Value::Bool(row.indislive),
        Value::Bool(row.indisreplident),
        Value::Text(format_indkey(&row.indkey).into()),
        Value::Text(
            row.indcollation
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(" ")
                .into(),
        ),
        Value::Text(
            row.indclass
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(" ")
                .into(),
        ),
        Value::Text(format_indkey(&row.indoption).into()),
        row.indexprs.map_or(Value::Null, |v| Value::Text(v.into())),
        row.indpred.map_or(Value::Null, |v| Value::Text(v.into())),
    ]
}

fn pg_opclass_row_values(row: PgOpclassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.opcmethod as i32),
        Value::Text(row.opcname.into()),
        Value::Int32(row.opcnamespace as i32),
        Value::Int32(row.opcowner as i32),
        Value::Int32(row.opcfamily as i32),
        Value::Int32(row.opcintype as i32),
        Value::Bool(row.opcdefault),
        Value::Int32(row.opckeytype as i32),
    ]
}

fn pg_opfamily_row_values(row: PgOpfamilyRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.opfmethod as i32),
        Value::Text(row.opfname.into()),
        Value::Int32(row.opfnamespace as i32),
        Value::Int32(row.opfowner as i32),
    ]
}

fn decode_builtin_sql_type(oid: u32) -> Option<SqlType> {
    for row in builtin_type_rows()
        .into_iter()
        .chain(bootstrap_composite_type_rows())
    {
        if row.oid == oid {
            return Some(row.sql_type);
        }
    }
    None
}

fn expect_oid(value: &Value) -> Result<u32, CatalogError> {
    match value {
        Value::Int64(v) => {
            u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value"))
        }
        Value::Int32(v) => {
            u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value"))
        }
        _ => Err(CatalogError::Corrupt("expected oid value")),
    }
}

fn expect_text(value: &Value) -> Result<String, CatalogError> {
    match value {
        Value::Text(text) => Ok(text.to_string()),
        _ => Err(CatalogError::Corrupt("expected text value")),
    }
}

fn expect_nullable_text(value: &Value) -> Result<Option<String>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.to_string())),
        _ => Err(CatalogError::Corrupt("expected nullable text value")),
    }
}

fn expect_bool(value: &Value) -> Result<bool, CatalogError> {
    match value {
        Value::Bool(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected bool value")),
    }
}

fn expect_int16(value: &Value) -> Result<i16, CatalogError> {
    match value {
        Value::Int16(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int2 value")),
    }
}

fn expect_int32(value: &Value) -> Result<i32, CatalogError> {
    match value {
        Value::Int32(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int4 value")),
    }
}

fn expect_float64(value: &Value) -> Result<f64, CatalogError> {
    match value {
        Value::Float64(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected float value")),
    }
}

fn expect_char(value: &Value, label: &'static str) -> Result<char, CatalogError> {
    match value {
        Value::Text(text) => text
            .chars()
            .next()
            .ok_or(CatalogError::Corrupt(match label {
                "relpersistence" => "empty relpersistence",
                "relkind" => "empty relkind",
                "amtype" => "empty amtype",
                "oprkind" => "empty oprkind",
                "prokind" => "empty prokind",
                "provolatile" => "empty provolatile",
                "proparallel" => "empty proparallel",
                "collprovider" => "empty collprovider",
                "castcontext" => "empty castcontext",
                "castmethod" => "empty castmethod",
                "contype" => "empty contype",
                "confupdtype" => "empty confupdtype",
                "confdeltype" => "empty confdeltype",
                "confmatchtype" => "empty confmatchtype",
                "deptype" => "empty deptype",
                _ => "empty char value",
            })),
        Value::InternalChar(byte) => Ok(char::from(*byte)),
        _ => Err(CatalogError::Corrupt(match label {
            "relpersistence" => "expected relpersistence text",
            "relkind" => "expected relkind text",
            "amtype" => "expected amtype text",
            "oprkind" => "expected oprkind text",
            "prokind" => "expected prokind text",
            "provolatile" => "expected provolatile text",
            "proparallel" => "expected proparallel text",
            "collprovider" => "expected collprovider text",
            "castcontext" => "expected castcontext text",
            "castmethod" => "expected castmethod text",
            "contype" => "expected contype text",
            "confupdtype" => "expected confupdtype text",
            "confdeltype" => "expected confdeltype text",
            "confmatchtype" => "expected confmatchtype text",
            "deptype" => "expected deptype text",
            _ => "expected text char value",
        })),
    }
}
