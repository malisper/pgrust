use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::{decode_value, missing_column_value};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::backend::utils::cache::catcache::format_indkey;
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    BootstrapCatalogKind, PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow,
    PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow, PgConstraintRow,
    PgDatabaseRow, PgDependRow, PgDescriptionRow, PgIndexRow, PgInheritsRow, PgLanguageRow,
    PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow, PgProcRow, PgRewriteRow,
    PgStatisticRow, PgTablespaceRow, PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow,
    PgTsParserRow, PgTsTemplateRow, PgTypeRow, bootstrap_composite_type_rows, builtin_type_rows,
};
use crate::include::nodes::datum::{ArrayValue, Value};

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
        BootstrapCatalogKind::PgTsParser => rows
            .ts_parsers
            .iter()
            .cloned()
            .map(pg_ts_parser_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsTemplate => rows
            .ts_templates
            .iter()
            .cloned()
            .map(pg_ts_template_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsDict => rows
            .ts_dicts
            .iter()
            .cloned()
            .map(pg_ts_dict_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsConfig => rows
            .ts_configs
            .iter()
            .cloned()
            .map(pg_ts_config_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsConfigMap => rows
            .ts_config_maps
            .iter()
            .cloned()
            .map(pg_ts_config_map_row_values)
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
        BootstrapCatalogKind::PgLargeobjectMetadata => Vec::new(),
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
        BootstrapCatalogKind::PgInherits => rows
            .inherits
            .iter()
            .cloned()
            .map(pg_inherits_row_values)
            .collect(),
        BootstrapCatalogKind::PgDescription => rows
            .descriptions
            .iter()
            .cloned()
            .map(pg_description_row_values)
            .collect(),
        BootstrapCatalogKind::PgIndex => rows
            .indexes
            .iter()
            .cloned()
            .map(pg_index_row_values)
            .collect(),
        BootstrapCatalogKind::PgRewrite => rows
            .rewrites
            .iter()
            .cloned()
            .map(pg_rewrite_row_values)
            .collect(),
        BootstrapCatalogKind::PgTrigger => rows
            .triggers
            .iter()
            .cloned()
            .map(pg_trigger_row_values)
            .collect(),
        BootstrapCatalogKind::PgStatistic => rows
            .statistics
            .iter()
            .cloned()
            .map(pg_statistic_row_values)
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
        .enumerate()
        .map(|(index, column)| {
            if let Some(datum) = raw.get(index) {
                decode_value(column, *datum).map_err(|e| CatalogError::Io(format!("{e:?}")))
            } else {
                Ok(missing_column_value(column))
            }
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
    let relpersistence = expect_char(&values[9], "relpersistence")?;
    let relkind = expect_char(&values[10], "relkind")?;
    Ok(PgClassRow {
        oid: expect_oid(&values[0])?,
        relname: expect_text(&values[1])?,
        relnamespace: expect_oid(&values[2])?,
        reltype: expect_oid(&values[3])?,
        relowner: expect_oid(&values[4])?,
        relam: expect_oid(&values[5])?,
        reltablespace: expect_oid(&values[6])?,
        relfilenode: expect_oid(&values[7])?,
        reltoastrelid: expect_oid(&values[8])?,
        relpersistence,
        relkind,
        relhassubclass: expect_bool(&values[11])?,
        relhastriggers: expect_bool(&values[12])?,
        relispartition: expect_bool(&values[13])?,
        relrowsecurity: expect_bool(&values[14])?,
        relforcerowsecurity: expect_bool(&values[15])?,
        relnatts: expect_int16(&values[16])?,
        relpages: expect_int32(&values[17])?,
        reltuples: expect_float64(&values[18])?,
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

pub(crate) fn pg_trigger_row_from_values(values: Vec<Value>) -> Result<PgTriggerRow, CatalogError> {
    Ok(PgTriggerRow {
        oid: expect_oid(&values[0])?,
        tgrelid: expect_oid(&values[1])?,
        tgparentid: expect_oid(&values[2])?,
        tgname: expect_text(&values[3])?,
        tgfoid: expect_oid(&values[4])?,
        tgtype: expect_int16(&values[5])?,
        tgenabled: expect_char(&values[6], "tgenabled")?,
        tgisinternal: expect_bool(&values[7])?,
        tgconstrrelid: expect_oid(&values[8])?,
        tgconstrindid: expect_oid(&values[9])?,
        tgconstraint: expect_oid(&values[10])?,
        tgdeferrable: expect_bool(&values[11])?,
        tginitdeferred: expect_bool(&values[12])?,
        tgnargs: expect_int16(&values[13])?,
        tgattr: nullable_int16_array(&values[14])?
            .ok_or(CatalogError::Corrupt("expected tgattr array"))?,
        tgargs: nullable_text_array(&values[15])?
            .ok_or(CatalogError::Corrupt("expected tgargs array"))?,
        tgqual: nullable_text(&values[16])?,
        tgoldtable: nullable_text(&values[17])?,
        tgnewtable: nullable_text(&values[18])?,
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

pub(crate) fn pg_ts_parser_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsParserRow, CatalogError> {
    Ok(PgTsParserRow {
        oid: expect_oid(&values[0])?,
        prsname: expect_text(&values[1])?,
        prsnamespace: expect_oid(&values[2])?,
        prsstart: expect_oid(&values[3])?,
        prstoken: expect_oid(&values[4])?,
        prsend: expect_oid(&values[5])?,
        prsheadline: expect_nullable_oid(&values[6])?,
        prslextype: expect_oid(&values[7])?,
    })
}

pub(crate) fn pg_ts_template_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsTemplateRow, CatalogError> {
    Ok(PgTsTemplateRow {
        oid: expect_oid(&values[0])?,
        tmplname: expect_text(&values[1])?,
        tmplnamespace: expect_oid(&values[2])?,
        tmplinit: expect_nullable_oid(&values[3])?,
        tmpllexize: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_ts_dict_row_from_values(values: Vec<Value>) -> Result<PgTsDictRow, CatalogError> {
    Ok(PgTsDictRow {
        oid: expect_oid(&values[0])?,
        dictname: expect_text(&values[1])?,
        dictnamespace: expect_oid(&values[2])?,
        dictowner: expect_oid(&values[3])?,
        dicttemplate: expect_oid(&values[4])?,
        dictinitoption: expect_nullable_text(&values[5])?,
    })
}

pub(crate) fn pg_ts_config_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsConfigRow, CatalogError> {
    Ok(PgTsConfigRow {
        oid: expect_oid(&values[0])?,
        cfgname: expect_text(&values[1])?,
        cfgnamespace: expect_oid(&values[2])?,
        cfgowner: expect_oid(&values[3])?,
        cfgparser: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_ts_config_map_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsConfigMapRow, CatalogError> {
    Ok(PgTsConfigMapRow {
        mapcfg: expect_oid(&values[0])?,
        maptokentype: expect_int32(&values[1])?,
        mapseqno: expect_int32(&values[2])?,
        mapdict: expect_oid(&values[3])?,
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
        proallargtypes: nullable_oid_array(&values[20])?,
        proargmodes: nullable_char_array(&values[21])?,
        proargnames: nullable_text_array(&values[22])?,
        prosrc: expect_text(&values[23])?,
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
        conkey: nullable_int16_array(&values[16])?,
        confkey: nullable_int16_array(&values[17])?,
        conpfeqop: nullable_oid_array(&values[18])?,
        conppeqop: nullable_oid_array(&values[19])?,
        conffeqop: nullable_oid_array(&values[20])?,
        confdelsetcols: nullable_int16_array(&values[21])?,
        conexclop: nullable_oid_array(&values[22])?,
        conbin: nullable_text(&values[23])?,
        conislocal: expect_bool(&values[24])?,
        coninhcount: expect_int16(&values[25])?,
        connoinherit: expect_bool(&values[26])?,
        conperiod: expect_bool(&values[27])?,
    })
}

pub(crate) fn pg_database_row_from_values(
    values: Vec<Value>,
) -> Result<PgDatabaseRow, CatalogError> {
    Ok(PgDatabaseRow {
        oid: expect_oid(&values[0])?,
        datname: expect_text(&values[1])?,
        datdba: expect_oid(&values[2])?,
        encoding: expect_int32(&values[3])?,
        datlocprovider: expect_char(&values[4], "datlocprovider")?,
        dattablespace: expect_oid(&values[5])?,
        datistemplate: expect_bool(&values[6])?,
        datallowconn: expect_bool(&values[7])?,
        datconnlimit: expect_int32(&values[8])?,
        datcollate: expect_text(&values[9])?,
        datctype: expect_text(&values[10])?,
        datlocale: expect_nullable_text(&values[11])?,
        daticurules: expect_nullable_text(&values[12])?,
        datcollversion: expect_nullable_text(&values[13])?,
        datacl: nullable_text_array(&values[14])?,
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
    let attalign = expect_char(&values[8], "attalign")?;
    let attstorage = expect_char(&values[9], "attstorage")?;
    let attcompression = match &values[10] {
        Value::Text(text) if text.is_empty() => '\0',
        other => expect_char(other, "attcompression")?,
    };
    Ok(PgAttributeRow {
        attrelid: expect_oid(&values[0])?,
        attname: expect_text(&values[1])?,
        atttypid: expect_oid(&values[2])?,
        attlen: expect_int16(&values[3])?,
        attnum: expect_int16(&values[4])?,
        attnotnull: expect_bool(&values[5])?,
        attisdropped: expect_bool(&values[6])?,
        atttypmod: expect_int32(&values[7])?,
        attalign: AttributeAlign::from_char(attalign)
            .ok_or(CatalogError::Corrupt("unknown attalign"))?,
        attstorage: AttributeStorage::from_char(attstorage)
            .ok_or(CatalogError::Corrupt("unknown attstorage"))?,
        attcompression: AttributeCompression::from_char(attcompression)
            .ok_or(CatalogError::Corrupt("unknown attcompression"))?,
        attstattarget: expect_int16(&values[11])?,
        attinhcount: expect_int16(&values[12])?,
        attislocal: expect_bool(&values[13])?,
        sql_type: SqlType::new(SqlTypeKind::Text),
    })
}

pub(crate) fn pg_inherits_row_from_values(
    values: Vec<Value>,
) -> Result<PgInheritsRow, CatalogError> {
    Ok(PgInheritsRow {
        inhrelid: expect_oid(&values[0])?,
        inhparent: expect_oid(&values[1])?,
        inhseqno: expect_int32(&values[2])?,
        inhdetachpending: expect_bool(&values[3])?,
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

pub(crate) fn pg_description_row_from_values(
    values: Vec<Value>,
) -> Result<PgDescriptionRow, CatalogError> {
    Ok(PgDescriptionRow {
        objoid: expect_oid(&values[0])?,
        classoid: expect_oid(&values[1])?,
        objsubid: expect_int32(&values[2])?,
        description: expect_text(&values[3])?,
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
    let typrelid = expect_oid(&values[7])?;
    let typelem = expect_oid(&values[8])?;
    let typarray = expect_oid(&values[9])?;
    Ok(PgTypeRow {
        oid,
        typname: expect_text(&values[1])?,
        typnamespace: expect_oid(&values[2])?,
        typowner: expect_oid(&values[3])?,
        typlen: expect_int16(&values[4])?,
        typalign: AttributeAlign::from_char(expect_char(&values[5], "typalign")?)
            .ok_or(CatalogError::Corrupt("invalid typalign"))?,
        typstorage: AttributeStorage::from_char(expect_char(&values[6], "typstorage")?)
            .ok_or(CatalogError::Corrupt("invalid typstorage"))?,
        typrelid,
        typelem,
        typarray,
        sql_type: decode_builtin_sql_type(oid).unwrap_or_else(|| {
            if typrelid != 0 {
                SqlType::named_composite(oid, typrelid)
            } else if typelem != 0 {
                SqlType::array_of(SqlType::record(typelem))
            } else {
                SqlType::new(SqlTypeKind::Text)
            }
        }),
    })
}

pub(crate) fn pg_statistic_row_from_values(
    values: Vec<Value>,
) -> Result<PgStatisticRow, CatalogError> {
    Ok(PgStatisticRow {
        starelid: expect_oid(&values[0])?,
        staattnum: expect_int16(&values[1])?,
        stainherit: expect_bool(&values[2])?,
        stanullfrac: expect_float64(&values[3])?,
        stawidth: expect_int32(&values[4])?,
        stadistinct: expect_float64(&values[5])?,
        stakind: [
            expect_int16(&values[6])?,
            expect_int16(&values[7])?,
            expect_int16(&values[8])?,
            expect_int16(&values[9])?,
            expect_int16(&values[10])?,
        ],
        staop: [
            expect_oid(&values[11])?,
            expect_oid(&values[12])?,
            expect_oid(&values[13])?,
            expect_oid(&values[14])?,
            expect_oid(&values[15])?,
        ],
        stacoll: [
            expect_oid(&values[16])?,
            expect_oid(&values[17])?,
            expect_oid(&values[18])?,
            expect_oid(&values[19])?,
            expect_oid(&values[20])?,
        ],
        stanumbers: [
            expect_nullable_array(&values[21])?,
            expect_nullable_array(&values[22])?,
            expect_nullable_array(&values[23])?,
            expect_nullable_array(&values[24])?,
            expect_nullable_array(&values[25])?,
        ],
        stavalues: [
            expect_nullable_array(&values[26])?,
            expect_nullable_array(&values[27])?,
            expect_nullable_array(&values[28])?,
            expect_nullable_array(&values[29])?,
            expect_nullable_array(&values[30])?,
        ],
    })
}

pub(crate) fn pg_rewrite_row_from_values(values: Vec<Value>) -> Result<PgRewriteRow, CatalogError> {
    Ok(PgRewriteRow {
        oid: expect_oid(&values[0])?,
        rulename: expect_text(&values[1])?,
        ev_class: expect_oid(&values[2])?,
        ev_type: expect_char(&values[3], "ev_type")?,
        ev_enabled: expect_char(&values[4], "ev_enabled")?,
        is_instead: expect_bool(&values[5])?,
        ev_qual: expect_text(&values[6])?,
        ev_action: expect_text(&values[7])?,
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
        Value::Int32(row.reltoastrelid as i32),
        Value::Text(row.relpersistence.to_string().into()),
        Value::Text(row.relkind.to_string().into()),
        Value::Bool(row.relhassubclass),
        Value::Bool(row.relhastriggers),
        Value::Bool(row.relispartition),
        Value::Bool(row.relrowsecurity),
        Value::Bool(row.relforcerowsecurity),
        Value::Int16(row.relnatts),
        Value::Int32(row.relpages),
        Value::Float64(row.reltuples),
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

fn pg_ts_parser_row_values(row: PgTsParserRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.prsname.into()),
        Value::Int32(row.prsnamespace as i32),
        Value::Int32(row.prsstart as i32),
        Value::Int32(row.prstoken as i32),
        Value::Int32(row.prsend as i32),
        row.prsheadline
            .map(|oid| Value::Int32(oid as i32))
            .unwrap_or(Value::Null),
        Value::Int32(row.prslextype as i32),
    ]
}

fn pg_ts_template_row_values(row: PgTsTemplateRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.tmplname.into()),
        Value::Int32(row.tmplnamespace as i32),
        row.tmplinit
            .map(|oid| Value::Int32(oid as i32))
            .unwrap_or(Value::Null),
        Value::Int32(row.tmpllexize as i32),
    ]
}

fn pg_ts_dict_row_values(row: PgTsDictRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.dictname.into()),
        Value::Int32(row.dictnamespace as i32),
        Value::Int32(row.dictowner as i32),
        Value::Int32(row.dicttemplate as i32),
        row.dictinitoption
            .map(|text| Value::Text(text.into()))
            .unwrap_or(Value::Null),
    ]
}

fn pg_ts_config_row_values(row: PgTsConfigRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.cfgname.into()),
        Value::Int32(row.cfgnamespace as i32),
        Value::Int32(row.cfgowner as i32),
        Value::Int32(row.cfgparser as i32),
    ]
}

fn pg_ts_config_map_row_values(row: PgTsConfigMapRow) -> Vec<Value> {
    vec![
        Value::Int32(row.mapcfg as i32),
        Value::Int32(row.maptokentype),
        Value::Int32(row.mapseqno),
        Value::Int32(row.mapdict as i32),
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
        nullable_array_value(row.proallargtypes.map(|oids| {
            ArrayValue::from_1d(
                oids.into_iter()
                    .map(|oid| Value::Int32(oid as i32))
                    .collect(),
            )
            .with_element_type_oid(crate::include::catalog::OID_TYPE_OID)
        })),
        nullable_array_value(row.proargmodes.map(|modes| {
            ArrayValue::from_1d(
                modes
                    .into_iter()
                    .map(Value::InternalChar)
                    .collect::<Vec<_>>(),
            )
            .with_element_type_oid(crate::include::catalog::INTERNAL_CHAR_TYPE_OID)
        })),
        nullable_array_value(row.proargnames.map(|names| {
            ArrayValue::from_1d(
                names
                    .into_iter()
                    .map(|name| Value::Text(name.into()))
                    .collect::<Vec<_>>(),
            )
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID)
        })),
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
        nullable_array_value(row.conkey.map(int16_array_value)),
        nullable_array_value(row.confkey.map(int16_array_value)),
        nullable_array_value(row.conpfeqop.map(oid_array_value)),
        nullable_array_value(row.conppeqop.map(oid_array_value)),
        nullable_array_value(row.conffeqop.map(oid_array_value)),
        nullable_array_value(row.confdelsetcols.map(int16_array_value)),
        nullable_array_value(row.conexclop.map(oid_array_value)),
        nullable_text_value(row.conbin),
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
        Value::Int32(row.encoding),
        Value::InternalChar(row.datlocprovider as u8),
        Value::Int32(row.dattablespace as i32),
        Value::Bool(row.datistemplate),
        Value::Bool(row.datallowconn),
        Value::Int32(row.datconnlimit),
        Value::Text(row.datcollate.into()),
        Value::Text(row.datctype.into()),
        row.datlocale
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.daticurules
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.datcollversion
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.datacl.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(
                values
                    .into_iter()
                    .map(|value| Value::Text(value.into()))
                    .collect(),
            ))
        }),
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
        Value::Int16(row.attlen),
        Value::Int16(row.attnum),
        Value::Bool(row.attnotnull),
        Value::Bool(row.attisdropped),
        Value::Int32(row.atttypmod),
        Value::InternalChar(row.attalign.as_char() as u8),
        Value::InternalChar(row.attstorage.as_char() as u8),
        Value::InternalChar(row.attcompression.as_char() as u8),
        Value::Int16(row.attstattarget),
        Value::Int16(row.attinhcount),
        Value::Bool(row.attislocal),
    ]
}

fn pg_inherits_row_values(row: PgInheritsRow) -> Vec<Value> {
    vec![
        Value::Int32(row.inhrelid as i32),
        Value::Int32(row.inhparent as i32),
        Value::Int32(row.inhseqno),
        Value::Bool(row.inhdetachpending),
    ]
}

fn pg_type_row_values(row: PgTypeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.typname.into()),
        Value::Int32(row.typnamespace as i32),
        Value::Int32(row.typowner as i32),
        Value::Int16(row.typlen),
        Value::InternalChar(row.typalign.as_char() as u8),
        Value::InternalChar(row.typstorage.as_char() as u8),
        Value::Int32(row.typrelid as i32),
        Value::Int32(row.typelem as i32),
        Value::Int32(row.typarray as i32),
    ]
}

fn pg_rewrite_row_values(row: PgRewriteRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.rulename.into()),
        Value::Int32(row.ev_class as i32),
        Value::Text(row.ev_type.to_string().into()),
        Value::Text(row.ev_enabled.to_string().into()),
        Value::Bool(row.is_instead),
        Value::Text(row.ev_qual.into()),
        Value::Text(row.ev_action.into()),
    ]
}

fn pg_trigger_row_values(row: PgTriggerRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.tgrelid as i32),
        Value::Int32(row.tgparentid as i32),
        Value::Text(row.tgname.into()),
        Value::Int32(row.tgfoid as i32),
        Value::Int16(row.tgtype),
        Value::InternalChar(row.tgenabled as u8),
        Value::Bool(row.tgisinternal),
        Value::Int32(row.tgconstrrelid as i32),
        Value::Int32(row.tgconstrindid as i32),
        Value::Int32(row.tgconstraint as i32),
        Value::Bool(row.tgdeferrable),
        Value::Bool(row.tginitdeferred),
        Value::Int16(row.tgnargs),
        Value::PgArray(int16_array_value(row.tgattr)),
        Value::PgArray(ArrayValue::from_1d(
            row.tgargs
                .into_iter()
                .map(|arg| Value::Text(arg.into()))
                .collect::<Vec<_>>(),
        )),
        nullable_text_value(row.tgqual),
        nullable_text_value(row.tgoldtable),
        nullable_text_value(row.tgnewtable),
    ]
}

fn pg_statistic_row_values(row: PgStatisticRow) -> Vec<Value> {
    vec![
        Value::Int32(row.starelid as i32),
        Value::Int16(row.staattnum),
        Value::Bool(row.stainherit),
        Value::Float64(row.stanullfrac),
        Value::Int32(row.stawidth),
        Value::Float64(row.stadistinct),
        Value::Int16(row.stakind[0]),
        Value::Int16(row.stakind[1]),
        Value::Int16(row.stakind[2]),
        Value::Int16(row.stakind[3]),
        Value::Int16(row.stakind[4]),
        Value::Int32(row.staop[0] as i32),
        Value::Int32(row.staop[1] as i32),
        Value::Int32(row.staop[2] as i32),
        Value::Int32(row.staop[3] as i32),
        Value::Int32(row.staop[4] as i32),
        Value::Int32(row.stacoll[0] as i32),
        Value::Int32(row.stacoll[1] as i32),
        Value::Int32(row.stacoll[2] as i32),
        Value::Int32(row.stacoll[3] as i32),
        Value::Int32(row.stacoll[4] as i32),
        nullable_array_value(row.stanumbers[0].clone()),
        nullable_array_value(row.stanumbers[1].clone()),
        nullable_array_value(row.stanumbers[2].clone()),
        nullable_array_value(row.stanumbers[3].clone()),
        nullable_array_value(row.stanumbers[4].clone()),
        nullable_array_value(row.stavalues[0].clone()),
        nullable_array_value(row.stavalues[1].clone()),
        nullable_array_value(row.stavalues[2].clone()),
        nullable_array_value(row.stavalues[3].clone()),
        nullable_array_value(row.stavalues[4].clone()),
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

fn pg_description_row_values(row: PgDescriptionRow) -> Vec<Value> {
    vec![
        Value::Int32(row.objoid as i32),
        Value::Int32(row.classoid as i32),
        Value::Int32(row.objsubid),
        Value::Text(row.description.into()),
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

fn expect_nullable_oid(value: &Value) -> Result<Option<u32>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        other => expect_oid(other).map(Some),
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

fn expect_nullable_array(value: &Value) -> Result<Option<ArrayValue>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::PgArray(array) => Ok(Some(array.clone())),
        _ => Err(CatalogError::Corrupt("expected nullable array value")),
    }
}

fn nullable_array_value(value: Option<ArrayValue>) -> Value {
    value.map(Value::PgArray).unwrap_or(Value::Null)
}

fn nullable_oid_array(value: &Value) -> Result<Option<Vec<u32>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Int32(v) if v >= 0 => Ok(v as u32),
            _ => Err(CatalogError::Corrupt("expected oid array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_int16_array(value: &Value) -> Result<Option<Vec<i16>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Int16(v) => Ok(v),
            _ => Err(CatalogError::Corrupt("expected int2 array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_text_array(value: &Value) -> Result<Option<Vec<String>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Text(text) => Ok(text.to_string()),
            _ => Err(CatalogError::Corrupt("expected text array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_char_array(value: &Value) -> Result<Option<Vec<u8>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::InternalChar(v) => Ok(v),
            Value::Text(text) if text.len() == 1 => Ok(text.as_bytes()[0]),
            _ => Err(CatalogError::Corrupt("expected char array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_text(value: &Value) -> Result<Option<String>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.to_string())),
        _ => Err(CatalogError::Corrupt("expected nullable text value")),
    }
}

fn oid_array_value(values: Vec<u32>) -> ArrayValue {
    ArrayValue::from_1d(
        values
            .into_iter()
            .map(|oid| Value::Int32(oid as i32))
            .collect(),
    )
    .with_element_type_oid(crate::include::catalog::OID_TYPE_OID)
}

fn int16_array_value(values: Vec<i16>) -> ArrayValue {
    ArrayValue::from_1d(values.into_iter().map(Value::Int16).collect())
        .with_element_type_oid(crate::include::catalog::INT2_TYPE_OID)
}

fn nullable_text_value(value: Option<String>) -> Value {
    value
        .map(|text| Value::Text(text.into()))
        .unwrap_or(Value::Null)
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
                "attalign" => "empty attalign",
                "attstorage" => "empty attstorage",
                "attcompression" => "empty attcompression",
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
            "attalign" => "expected attalign text",
            "attstorage" => "expected attstorage text",
            "attcompression" => "expected attcompression text",
            _ => "expected text char value",
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::value_io::tuple_from_values;
    use crate::include::catalog::{BootstrapCatalogKind, bootstrap_relation_desc};

    #[test]
    fn pg_statistic_anyarray_catalog_tuple_roundtrips() {
        let row = PgStatisticRow {
            starelid: 42,
            staattnum: 1,
            stainherit: false,
            stanullfrac: 0.2,
            stawidth: 4,
            stadistinct: 3.0,
            stakind: [1, 2, 3, 0, 0],
            staop: [96, 97, 98, 0, 0],
            stacoll: [0; 5],
            stanumbers: [
                Some(
                    ArrayValue::from_1d(vec![Value::Float64(0.5)])
                        .with_element_type_oid(crate::include::catalog::FLOAT4_TYPE_OID),
                ),
                None,
                Some(
                    ArrayValue::from_1d(vec![Value::Float64(1.0)])
                        .with_element_type_oid(crate::include::catalog::FLOAT4_TYPE_OID),
                ),
                None,
                None,
            ],
            stavalues: [
                Some(
                    ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)])
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                ),
                Some(
                    ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                ),
                None,
                None,
                None,
            ],
        };

        let values = pg_statistic_row_values(row.clone());
        let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgStatistic);
        let tuple = tuple_from_values(&desc, &values).unwrap();
        let decoded = decode_catalog_tuple_values(&desc, &tuple).unwrap();
        let roundtrip = pg_statistic_row_from_values(decoded).unwrap();

        assert_eq!(roundtrip.starelid, row.starelid);
        assert_eq!(roundtrip.staattnum, row.staattnum);
        assert_eq!(roundtrip.stainherit, row.stainherit);
        assert!((roundtrip.stanullfrac - row.stanullfrac).abs() < 1e-6);
        assert_eq!(roundtrip.stawidth, row.stawidth);
        assert_eq!(roundtrip.stadistinct, row.stadistinct);
        assert_eq!(roundtrip.stakind, row.stakind);
        assert_eq!(roundtrip.staop, row.staop);
        assert_eq!(roundtrip.stacoll, row.stacoll);
        assert_eq!(roundtrip.stanumbers, row.stanumbers);
        assert_eq!(roundtrip.stavalues, row.stavalues);
    }
}
