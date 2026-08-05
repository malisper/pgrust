//! xml.c SQL/XML mapping section: table/query/cursor/schema/database_to_xml*
//! + the SQL-to-XML-Schema type mappers (xml.c:2728-4139).
#![allow(non_snake_case)]

pub mod builtins;

use core::fmt::Write as _;

use elog::ereport;
use execexpr::ResMcx;
use init_small::globals::MyDatabaseId;
use lsyscache::typ::TYPTYPE_DOMAIN;
use mcx::{Mcx, MemoryContext};
use types_core::catalog::{
    BOOLOID, BPCHAROID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, VARCHAROID, XMLOID,
};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_CURSOR_STATE, ERRCODE_UNDEFINED_CURSOR,
    ERROR,
};
use types_storage::lock::{AccessShareLock, NoLock};
use types_tuple::TupleDescData;

const NAMESPACE_XSD: &str = "http://www.w3.org/2001/XMLSchema";
const NAMESPACE_XSI: &str = "http://www.w3.org/2001/XMLSchema-instance";
const VARHDRSZ: i32 = 4;

fn with_scratch<R>(f: impl FnOnce(Mcx<'_>, &ResMcx) -> PgResult<R>) -> PgResult<R> {
    let ctx = MemoryContext::new("xmlmap scratch");
    let res: ResMcx = Some(core::ptr::NonNull::from(&ctx));
    f(ctx.mcx(), &res)
}

fn xml_name(ident: &[u8], fully_escaped: bool, escape_period: bool) -> PgResult<String> {
    let v = adt_xml::map_sql_identifier_to_xml_name(ident, fully_escaped, escape_period)?;
    Ok(String::from_utf8(v).unwrap_or_else(|_| panic!("non-UTF-8 XML name")))
}

fn rel_name(relid: Oid) -> PgResult<String> {
    with_scratch(|mcx, _| {
        Ok(lsyscache::relation::get_rel_name(mcx, relid)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {}", relid))
            .as_str()
            .to_owned())
    })
}

// C xml.c table_to_xml_internal passes get_rel_name(relid) through
// unchecked: a user-supplied bogus relid yields a NULL tablename
// (query_to_xml_internal substitutes "table") and the "SELECT * FROM <oid>"
// query then fails inside SPI with C's syntax error. The panicking rel_name
// above turned that into a contained backend panic (fnconf campaign-2
// ledger, OID 2923, xmlmap:44).
fn rel_name_opt(relid: Oid) -> PgResult<Option<String>> {
    with_scratch(|mcx, _| {
        Ok(lsyscache::relation::get_rel_name(mcx, relid)?.map(|n| n.as_str().to_owned()))
    })
}

fn namespace_name(nspid: Oid) -> PgResult<String> {
    with_scratch(|mcx, _| {
        Ok(lsyscache::misc::get_namespace_name(mcx, nspid)?
            .unwrap_or_else(|| panic!("cache lookup failed for namespace {}", nspid))
            .as_str()
            .to_owned())
    })
}

fn database_name() -> PgResult<String> {
    Ok(dbcommands::get_database_name(MyDatabaseId())?
        .unwrap_or_else(|| panic!("database with OID {} does not exist", MyDatabaseId())))
}

fn query_to_oid_list(query: &str) -> PgResult<Vec<Oid>> {
    let spi_result = spi::SPI_execute(query, true, 0)?;
    if spi_result != spi::SPI_OK_SELECT {
        return Err(ereport(ERROR)
            .errmsg(format!("SPI_execute returned {spi_result} for {query}"))
            .into_error()
            .into());
    }
    let n = spi::SPI_processed() as usize;
    let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
    Ok(spi::tuptable_with(h, |t| {
        let mut list = Vec::new();
        for i in 0..n {
            let (oid, isnull) = spi::SPI_getbinval(&t.vals[i], &t.tupdesc, 1);
            if !isnull {
                list.push(oid.as_oid());
            }
        }
        list
    }))
}

fn schema_get_xml_visible_tables(nspid: Oid) -> PgResult<Vec<Oid>> {
    query_to_oid_list(&format!(
        "SELECT oid FROM pg_catalog.pg_class WHERE relnamespace = {} AND relkind IN ('r','m','v') \
         AND pg_catalog.has_table_privilege (oid, 'SELECT') ORDER BY relname;",
        nspid
    ))
}

const XML_VISIBLE_SCHEMAS: &str = "SELECT oid FROM pg_catalog.pg_namespace WHERE \
    pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT (nspname ~ '^pg_' OR nspname = 'information_schema')";

fn database_get_xml_visible_schemas() -> PgResult<Vec<Oid>> {
    query_to_oid_list(&format!("{XML_VISIBLE_SCHEMAS} ORDER BY nspname;"))
}

fn database_get_xml_visible_tables() -> PgResult<Vec<Oid>> {
    query_to_oid_list(&format!(
        "SELECT oid FROM pg_catalog.pg_class WHERE relkind IN ('r','m','v') \
         AND pg_catalog.has_table_privilege(pg_class.oid, 'SELECT') \
         AND relnamespace IN ({XML_VISIBLE_SCHEMAS});"
    ))
}

fn xmldata_root_element_start(
    result: &mut String,
    eltname: &str,
    xmlschema: Option<&str>,
    targetns: &str,
    top_level: bool,
) {
    let _ = write!(result, "<{eltname}");
    if top_level {
        let _ = write!(result, " xmlns:xsi=\"{NAMESPACE_XSI}\"");
        if !targetns.is_empty() {
            let _ = write!(result, " xmlns=\"{targetns}\"");
        }
    }
    if xmlschema.is_some() {
        if !targetns.is_empty() {
            let _ = write!(result, " xsi:schemaLocation=\"{targetns} #\"");
        } else {
            result.push_str(" xsi:noNamespaceSchemaLocation=\"#\"");
        }
    }
    result.push_str(">\n");
}

fn xmldata_root_element_end(result: &mut String, eltname: &str) {
    let _ = write!(result, "</{eltname}>\n");
}

fn spi_sql_row_to_xmlelement(
    t: &spi::TuptabData<'_>,
    rownum: usize,
    result: &mut String,
    tablename: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
    resmcx: &ResMcx,
) -> PgResult<()> {
    let xmltn = match tablename {
        Some(tn) => xml_name(tn.as_bytes(), true, false)?,
        None => (if tableforest { "row" } else { "table" }).to_owned(),
    };

    if tableforest {
        xmldata_root_element_start(result, &xmltn, None, targetns, top_level);
    } else {
        result.push_str("<row>\n");
    }

    for i in 1..=t.tupdesc.natts {
        let att = t.tupdesc.attr(i as usize - 1);
        let colname = xml_name(att.attname.name_str(), true, false)?;
        let (colval, isnull) = spi::SPI_getbinval(&t.vals[rownum], &t.tupdesc, i);
        if isnull {
            if nulls {
                let _ = write!(result, "  <{colname} xsi:nil=\"true\"/>\n");
            }
        } else {
            let v = execexpr::map_sql_value_to_xml_value(colval, att.atttypid, true, resmcx)?;
            let _ = write!(result, "  <{colname}>{v}</{colname}>\n");
        }
    }

    if tableforest {
        xmldata_root_element_end(result, &xmltn);
        result.push('\n');
    } else {
        result.push_str("</row>\n\n");
    }
    Ok(())
}

fn query_to_xml_internal(
    query: &str,
    tablename: Option<&str>,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<String> {
    let xmltn = match tablename {
        Some(tn) => xml_name(tn.as_bytes(), true, false)?,
        None => "table".to_owned(),
    };

    let mut result = String::new();

    spi::SPI_connect()?;
    let r = (|| -> PgResult<()> {
        if spi::SPI_execute(query, true, 0)? != spi::SPI_OK_SELECT {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATA_EXCEPTION)
                .errmsg("invalid query")
                .into_error()
                .into());
        }

        if !tableforest {
            xmldata_root_element_start(&mut result, &xmltn, xmlschema, targetns, top_level);
            result.push('\n');
        }

        if let Some(xs) = xmlschema {
            let _ = write!(result, "{xs}\n\n");
        }

        let n = spi::SPI_processed() as usize;
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        with_scratch(|_, resmcx| {
            spi::tuptable_with(h, |t| -> PgResult<()> {
                for i in 0..n {
                    spi_sql_row_to_xmlelement(
                        t, i, &mut result, tablename, nulls, tableforest, targetns, top_level,
                        resmcx,
                    )?;
                }
                Ok(())
            })
        })?;

        if !tableforest {
            xmldata_root_element_end(&mut result, &xmltn);
        }
        Ok(())
    })();
    spi::SPI_finish()?;
    r?;
    Ok(result)
}

fn table_to_xml_internal(
    relid: Oid,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<String> {
    let query = with_scratch(|mcx, _| {
        let name = adt_regproc::regclassout(mcx, relid)?;
        Ok(format!(
            "SELECT * FROM {}",
            core::str::from_utf8(&name).expect("regclassout is UTF-8").trim_end_matches('\0')
        ))
    })?;
    let tablename = rel_name_opt(relid)?;
    query_to_xml_internal(
        &query,
        tablename.as_deref(),
        xmlschema,
        nulls,
        tableforest,
        targetns,
        top_level,
    )
}

pub fn table_to_xml(relid: Oid, nulls: bool, tableforest: bool, targetns: &str) -> PgResult<String> {
    table_to_xml_internal(relid, None, nulls, tableforest, targetns, true)
}

pub fn query_to_xml(query: &str, nulls: bool, tableforest: bool, targetns: &str) -> PgResult<String> {
    query_to_xml_internal(query, None, None, nulls, tableforest, targetns, true)
}

fn undefined_cursor(name: &str) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_CURSOR)
        .errmsg(format!("cursor \"{name}\" does not exist"))
        .into_error()
        .into()
}

pub fn cursor_to_xml(
    name: &str,
    count: i32,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let mut result = String::new();

    if !tableforest {
        xmldata_root_element_start(&mut result, "table", None, targetns, true);
        result.push('\n');
    }

    spi::SPI_connect()?;
    let r = (|| -> PgResult<()> {
        let cursor = spi::SPI_cursor_find(name).ok_or_else(|| undefined_cursor(name))?;
        spi::SPI_cursor_fetch(&cursor, true, count as i64)?;
        let n = spi::SPI_processed() as usize;
        let h = spi::SPI_tuptable().expect("fetch leaves a tuptable");
        with_scratch(|_, resmcx| {
            spi::tuptable_with(h, |t| -> PgResult<()> {
                for i in 0..n {
                    spi_sql_row_to_xmlelement(
                        t, i, &mut result, None, nulls, tableforest, targetns, true, resmcx,
                    )?;
                }
                Ok(())
            })
        })
    })();
    spi::SPI_finish()?;
    r?;

    if !tableforest {
        xmldata_root_element_end(&mut result, "table");
    }
    Ok(result)
}

pub fn table_to_xmlschema(
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    with_scratch(|mcx, _| {
        let rel = table::table_open(mcx, relid, AccessShareLock)?;
        let result = map_sql_table_to_xmlschema(&rel.rd_att, relid, nulls, tableforest, targetns);
        table::table_close(rel, NoLock)?;
        result
    })
}

pub fn query_to_xmlschema(
    query: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    spi::SPI_connect()?;
    let r = (|| -> PgResult<String> {
        let plan = spi::SPI_prepare(query, &[])?;
        let cursor = spi::SPI_cursor_open(None, plan, &[], &[], true)?;
        let td = cursor
            .portal
            .borrow()
            .tupDesc
            .clone()
            .unwrap_or_else(|| panic!("SPI_cursor_open(\"{query}\") returned no tupdesc"));
        let result = map_sql_table_to_xmlschema(&td, InvalidOid, nulls, tableforest, targetns);
        spi::SPI_cursor_close(cursor)?;
        result
    })();
    spi::SPI_finish()?;
    r
}

pub fn cursor_to_xmlschema(
    name: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    spi::SPI_connect()?;
    let r = (|| -> PgResult<String> {
        let cursor = spi::SPI_cursor_find(name).ok_or_else(|| undefined_cursor(name))?;
        let td = cursor.portal.borrow().tupDesc.clone().ok_or_else(|| {
            let e = ereport(ERROR)
                .errcode(ERRCODE_INVALID_CURSOR_STATE)
                .errmsg(format!("portal \"{name}\" does not return tuples"))
                .into_error();
            Box::new(e)
        })?;
        map_sql_table_to_xmlschema(&td, InvalidOid, nulls, tableforest, targetns)
    })();
    spi::SPI_finish()?;
    r
}

pub fn table_to_xml_and_xmlschema(
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let xmlschema = table_to_xmlschema(relid, nulls, tableforest, targetns)?;
    table_to_xml_internal(relid, Some(&xmlschema), nulls, tableforest, targetns, true)
}

pub fn query_to_xml_and_xmlschema(
    query: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let xmlschema = query_to_xmlschema(query, nulls, tableforest, targetns)?;
    query_to_xml_internal(query, None, Some(&xmlschema), nulls, tableforest, targetns, true)
}

fn schema_to_xml_internal(
    nspid: Oid,
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> PgResult<String> {
    let xmlsn = xml_name(namespace_name(nspid)?.as_bytes(), true, false)?;
    let mut result = String::new();

    xmldata_root_element_start(&mut result, &xmlsn, xmlschema, targetns, top_level);
    result.push('\n');

    if let Some(xs) = xmlschema {
        let _ = write!(result, "{xs}\n\n");
    }

    spi::SPI_connect()?;
    let r = (|| -> PgResult<()> {
        for relid in schema_get_xml_visible_tables(nspid)? {
            let subres = table_to_xml_internal(relid, None, nulls, tableforest, targetns, false)?;
            result.push_str(&subres);
            result.push('\n');
        }
        Ok(())
    })();
    spi::SPI_finish()?;
    r?;

    xmldata_root_element_end(&mut result, &xmlsn);
    Ok(result)
}

pub fn schema_to_xml(
    schemaname: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let nspid = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
    schema_to_xml_internal(nspid, None, nulls, tableforest, targetns, true)
}

fn xsd_schema_element_start(result: &mut String, targetns: &str) {
    let _ = write!(result, "<xsd:schema\n    xmlns:xsd=\"{NAMESPACE_XSD}\"");
    if !targetns.is_empty() {
        let _ = write!(
            result,
            "\n    targetNamespace=\"{targetns}\"\n    elementFormDefault=\"qualified\""
        );
    }
    result.push_str(">\n\n");
}

fn xsd_schema_element_end(result: &mut String) {
    result.push_str("</xsd:schema>");
}

fn schema_to_xmlschema_internal(
    schemaname: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let nspid = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
    let mut result = String::new();

    xsd_schema_element_start(&mut result, targetns);

    spi::SPI_connect()?;
    let r = with_scratch(|mcx, _| {
        let relid_list = schema_get_xml_visible_tables(nspid)?;
        let mut tupdescs = Vec::new();
        for relid in &relid_list {
            let rel = table::table_open(mcx, *relid, AccessShareLock)?;
            tupdescs.push(tupdesc::CreateTupleDescCopy(mcx, &rel.rd_att)?);
            table::table_close(rel, NoLock)?;
        }
        let refs: Vec<&TupleDescData<'_>> = tupdescs.iter().collect();
        result.push_str(&map_sql_typecoll_to_xmlschema_types(&refs)?);
        result.push_str(&map_sql_schema_to_xmlschema_types(
            nspid, &relid_list, nulls, tableforest, targetns,
        )?);
        Ok(())
    });
    spi::SPI_finish()?;
    r?;

    xsd_schema_element_end(&mut result);
    Ok(result)
}

pub fn schema_to_xmlschema(
    schemaname: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    schema_to_xmlschema_internal(schemaname, nulls, tableforest, targetns)
}

pub fn schema_to_xml_and_xmlschema(
    schemaname: &str,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let nspid = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
    let xmlschema = schema_to_xmlschema_internal(schemaname, nulls, tableforest, targetns)?;
    schema_to_xml_internal(nspid, Some(&xmlschema), nulls, tableforest, targetns, true)
}

fn database_to_xml_internal(
    xmlschema: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let xmlcn = xml_name(database_name()?.as_bytes(), true, false)?;
    let mut result = String::new();

    xmldata_root_element_start(&mut result, &xmlcn, xmlschema, targetns, true);
    result.push('\n');

    if let Some(xs) = xmlschema {
        let _ = write!(result, "{xs}\n\n");
    }

    spi::SPI_connect()?;
    let r = (|| -> PgResult<()> {
        for nspid in database_get_xml_visible_schemas()? {
            let subres = schema_to_xml_internal(nspid, None, nulls, tableforest, targetns, false)?;
            result.push_str(&subres);
            result.push('\n');
        }
        Ok(())
    })();
    spi::SPI_finish()?;
    r?;

    xmldata_root_element_end(&mut result, &xmlcn);
    Ok(result)
}

pub fn database_to_xml(nulls: bool, tableforest: bool, targetns: &str) -> PgResult<String> {
    database_to_xml_internal(None, nulls, tableforest, targetns)
}

fn database_to_xmlschema_internal(
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let mut result = String::new();
    xsd_schema_element_start(&mut result, targetns);

    spi::SPI_connect()?;
    let r = with_scratch(|mcx, _| {
        let relid_list = database_get_xml_visible_tables()?;
        let nspid_list = database_get_xml_visible_schemas()?;
        let mut tupdescs = Vec::new();
        for relid in &relid_list {
            let rel = table::table_open(mcx, *relid, AccessShareLock)?;
            tupdescs.push(tupdesc::CreateTupleDescCopy(mcx, &rel.rd_att)?);
            table::table_close(rel, NoLock)?;
        }
        let refs: Vec<&TupleDescData<'_>> = tupdescs.iter().collect();
        result.push_str(&map_sql_typecoll_to_xmlschema_types(&refs)?);
        result.push_str(&map_sql_catalog_to_xmlschema_types(&nspid_list, targetns)?);
        Ok(())
    });
    spi::SPI_finish()?;
    r?;

    xsd_schema_element_end(&mut result);
    Ok(result)
}

pub fn database_to_xmlschema(nulls: bool, tableforest: bool, targetns: &str) -> PgResult<String> {
    database_to_xmlschema_internal(nulls, tableforest, targetns)
}

pub fn database_to_xml_and_xmlschema(
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let xmlschema = database_to_xmlschema_internal(nulls, tableforest, targetns)?;
    database_to_xml_internal(Some(&xmlschema), nulls, tableforest, targetns)
}

fn map_multipart_sql_identifier_to_xml_name(
    a: Option<&str>,
    b: Option<&str>,
    c: Option<&str>,
    d: Option<&str>,
) -> PgResult<String> {
    let mut result = String::new();
    if let Some(a) = a {
        result.push_str(&xml_name(a.as_bytes(), true, true)?);
    }
    for part in [b, c, d].into_iter().flatten() {
        let _ = write!(result, ".{}", xml_name(part.as_bytes(), true, true)?);
    }
    Ok(result)
}

fn map_sql_table_to_xmlschema(
    tupdesc: &TupleDescData<'_>,
    relid: Oid,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
) -> PgResult<String> {
    let (xmltn, tabletypename, rowtypename);
    if relid != InvalidOid {
        let relname = rel_name(relid)?;
        let nspname = namespace_name(lsyscache::relation::get_rel_namespace(relid)?)?;
        let dbname = database_name()?;
        xmltn = xml_name(relname.as_bytes(), true, false)?;
        tabletypename = map_multipart_sql_identifier_to_xml_name(
            Some("TableType"),
            Some(&dbname),
            Some(&nspname),
            Some(&relname),
        )?;
        rowtypename = map_multipart_sql_identifier_to_xml_name(
            Some("RowType"),
            Some(&dbname),
            Some(&nspname),
            Some(&relname),
        )?;
    } else {
        xmltn = (if tableforest { "row" } else { "table" }).to_owned();
        tabletypename = "TableType".to_owned();
        rowtypename = "RowType".to_owned();
    }

    let mut result = String::new();
    xsd_schema_element_start(&mut result, targetns);

    result.push_str(&map_sql_typecoll_to_xmlschema_types(&[tupdesc])?);

    let _ = write!(result, "<xsd:complexType name=\"{rowtypename}\">\n  <xsd:sequence>\n");

    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        let _ = write!(
            result,
            "    <xsd:element name=\"{}\" type=\"{}\"{}></xsd:element>\n",
            xml_name(att.attname.name_str(), true, false)?,
            map_sql_type_to_xml_name(att.atttypid, -1)?,
            if nulls { " nillable=\"true\"" } else { " minOccurs=\"0\"" }
        );
    }

    result.push_str("  </xsd:sequence>\n</xsd:complexType>\n\n");

    if !tableforest {
        let _ = write!(
            result,
            "<xsd:complexType name=\"{tabletypename}\">\n  <xsd:sequence>\n    \
             <xsd:element name=\"row\" type=\"{rowtypename}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n  \
             </xsd:sequence>\n</xsd:complexType>\n\n"
        );
        let _ = write!(result, "<xsd:element name=\"{xmltn}\" type=\"{tabletypename}\"/>\n\n");
    } else {
        let _ = write!(result, "<xsd:element name=\"{xmltn}\" type=\"{rowtypename}\"/>\n\n");
    }

    xsd_schema_element_end(&mut result);
    Ok(result)
}

fn map_sql_schema_to_xmlschema_types(
    nspid: Oid,
    relid_list: &[Oid],
    _nulls: bool,
    tableforest: bool,
    _targetns: &str,
) -> PgResult<String> {
    let dbname = database_name()?;
    let nspname = namespace_name(nspid)?;
    let xmlsn = xml_name(nspname.as_bytes(), true, false)?;
    let schematypename = map_multipart_sql_identifier_to_xml_name(
        Some("SchemaType"),
        Some(&dbname),
        Some(&nspname),
        None,
    )?;

    let mut result = String::new();
    let _ = write!(result, "<xsd:complexType name=\"{schematypename}\">\n");
    result.push_str(if !tableforest { "  <xsd:all>\n" } else { "  <xsd:sequence>\n" });

    for relid in relid_list {
        let relname = rel_name(*relid)?;
        let xmltn = xml_name(relname.as_bytes(), true, false)?;
        let tabletypename = map_multipart_sql_identifier_to_xml_name(
            Some(if tableforest { "RowType" } else { "TableType" }),
            Some(&dbname),
            Some(&nspname),
            Some(&relname),
        )?;
        if !tableforest {
            let _ = write!(result, "    <xsd:element name=\"{xmltn}\" type=\"{tabletypename}\"/>\n");
        } else {
            let _ = write!(
                result,
                "    <xsd:element name=\"{xmltn}\" type=\"{tabletypename}\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n"
            );
        }
    }

    result.push_str(if !tableforest { "  </xsd:all>\n" } else { "  </xsd:sequence>\n" });
    result.push_str("</xsd:complexType>\n\n");
    let _ = write!(result, "<xsd:element name=\"{xmlsn}\" type=\"{schematypename}\"/>\n\n");
    Ok(result)
}

fn map_sql_catalog_to_xmlschema_types(nspid_list: &[Oid], _targetns: &str) -> PgResult<String> {
    let dbname = database_name()?;
    let xmlcn = xml_name(dbname.as_bytes(), true, false)?;
    let catalogtypename =
        map_multipart_sql_identifier_to_xml_name(Some("CatalogType"), Some(&dbname), None, None)?;

    let mut result = String::new();
    let _ = write!(result, "<xsd:complexType name=\"{catalogtypename}\">\n");
    result.push_str("  <xsd:all>\n");

    for nspid in nspid_list {
        let nspname = namespace_name(*nspid)?;
        let xmlsn = xml_name(nspname.as_bytes(), true, false)?;
        let schematypename = map_multipart_sql_identifier_to_xml_name(
            Some("SchemaType"),
            Some(&dbname),
            Some(&nspname),
            None,
        )?;
        let _ = write!(result, "    <xsd:element name=\"{xmlsn}\" type=\"{schematypename}\"/>\n");
    }

    result.push_str("  </xsd:all>\n");
    result.push_str("</xsd:complexType>\n\n");
    let _ = write!(result, "<xsd:element name=\"{xmlcn}\" type=\"{catalogtypename}\"/>\n\n");
    Ok(result)
}

fn map_sql_type_to_xml_name(typeoid: Oid, typmod: i32) -> PgResult<String> {
    let mut result = String::new();
    match typeoid {
        BPCHAROID => {
            if typmod == -1 {
                result.push_str("CHAR");
            } else {
                let _ = write!(result, "CHAR_{}", typmod - VARHDRSZ);
            }
        }
        VARCHAROID => {
            if typmod == -1 {
                result.push_str("VARCHAR");
            } else {
                let _ = write!(result, "VARCHAR_{}", typmod - VARHDRSZ);
            }
        }
        NUMERICOID => {
            if typmod == -1 {
                result.push_str("NUMERIC");
            } else {
                let _ = write!(
                    result,
                    "NUMERIC_{}_{}",
                    ((typmod - VARHDRSZ) >> 16) & 0xffff,
                    (typmod - VARHDRSZ) & 0xffff
                );
            }
        }
        INT4OID => result.push_str("INTEGER"),
        INT2OID => result.push_str("SMALLINT"),
        INT8OID => result.push_str("BIGINT"),
        FLOAT4OID => result.push_str("REAL"),
        FLOAT8OID => result.push_str("DOUBLE"),
        BOOLOID => result.push_str("BOOLEAN"),
        TIMEOID => {
            if typmod == -1 {
                result.push_str("TIME");
            } else {
                let _ = write!(result, "TIME_{typmod}");
            }
        }
        TIMETZOID => {
            if typmod == -1 {
                result.push_str("TIME_WTZ");
            } else {
                let _ = write!(result, "TIME_WTZ_{typmod}");
            }
        }
        TIMESTAMPOID => {
            if typmod == -1 {
                result.push_str("TIMESTAMP");
            } else {
                let _ = write!(result, "TIMESTAMP_{typmod}");
            }
        }
        TIMESTAMPTZOID => {
            if typmod == -1 {
                result.push_str("TIMESTAMP_WTZ");
            } else {
                let _ = write!(result, "TIMESTAMP_WTZ_{typmod}");
            }
        }
        DATEOID => result.push_str("DATE"),
        XMLOID => result.push_str("XML"),
        _ => {
            let (typname, typnamespace) = syscache_seams::pg_type_name_namespace::call(typeoid)?
                .unwrap_or_else(|| panic!("cache lookup failed for type {}", typeoid));
            let typtype = lsyscache::typ::get_typtype(typeoid)?;
            let name = core::str::from_utf8(typname.name_str())
                .unwrap_or_else(|_| panic!("non-UTF-8 pg_type.typname"))
                .to_owned();
            let nspname = namespace_name(typnamespace)?;
            let dbname = database_name()?;
            result.push_str(&map_multipart_sql_identifier_to_xml_name(
                Some(if typtype == TYPTYPE_DOMAIN { "Domain" } else { "UDT" }),
                Some(&dbname),
                Some(&nspname),
                Some(&name),
            )?);
        }
    }
    Ok(result)
}

fn map_sql_typecoll_to_xmlschema_types(tupdesc_list: &[&TupleDescData<'_>]) -> PgResult<String> {
    let mut uniquetypes: Vec<Oid> = Vec::new();

    for tupdesc in tupdesc_list {
        for i in 0..tupdesc.natts as usize {
            let att = tupdesc.attr(i);
            if att.attisdropped {
                continue;
            }
            if !uniquetypes.contains(&att.atttypid) {
                uniquetypes.push(att.atttypid);
            }
        }
    }

    let mut i = 0;
    while i < uniquetypes.len() {
        let typid = uniquetypes[i];
        let basetypid = lsyscache::typ::getBaseType(typid)?;
        if basetypid != typid && !uniquetypes.contains(&basetypid) {
            uniquetypes.push(basetypid);
        }
        i += 1;
    }

    let mut result = String::new();
    for typid in uniquetypes {
        let _ = write!(result, "{}\n", map_sql_type_to_xmlschema_type(typid, -1)?);
    }
    Ok(result)
}

fn map_sql_type_to_xmlschema_type(typeoid: Oid, typmod: i32) -> PgResult<String> {
    let typename = map_sql_type_to_xml_name(typeoid, typmod)?;
    let mut result = String::new();

    if typeoid == XMLOID {
        result.push_str(
            "<xsd:complexType mixed=\"true\">\n  <xsd:sequence>\n    \
             <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n  \
             </xsd:sequence>\n</xsd:complexType>\n",
        );
        return Ok(result);
    }

    let _ = write!(result, "<xsd:simpleType name=\"{typename}\">\n");

    match typeoid {
        BPCHAROID | VARCHAROID | TEXTOID => {
            result.push_str("  <xsd:restriction base=\"xsd:string\">\n");
            if typmod != -1 {
                let _ = write!(result, "    <xsd:maxLength value=\"{}\"/>\n", typmod - VARHDRSZ);
            }
            result.push_str("  </xsd:restriction>\n");
        }
        BYTEAOID => {
            let _ = write!(
                result,
                "  <xsd:restriction base=\"xsd:{}\">\n  </xsd:restriction>\n",
                if adt_xml::xmlbinary() == adt_xml::XmlBinaryType::XMLBINARY_BASE64 {
                    "base64Binary"
                } else {
                    "hexBinary"
                }
            );
        }
        NUMERICOID => {
            if typmod != -1 {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:decimal\">\n    \
                     <xsd:totalDigits value=\"{}\"/>\n    \
                     <xsd:fractionDigits value=\"{}\"/>\n  </xsd:restriction>\n",
                    ((typmod - VARHDRSZ) >> 16) & 0xffff,
                    (typmod - VARHDRSZ) & 0xffff
                );
            }
        }
        INT2OID => {
            let _ = write!(
                result,
                "  <xsd:restriction base=\"xsd:short\">\n    \
                 <xsd:maxInclusive value=\"{}\"/>\n    \
                 <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i16::MAX,
                i16::MIN
            );
        }
        INT4OID => {
            let _ = write!(
                result,
                "  <xsd:restriction base=\"xsd:int\">\n    \
                 <xsd:maxInclusive value=\"{}\"/>\n    \
                 <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i32::MAX,
                i32::MIN
            );
        }
        INT8OID => {
            let _ = write!(
                result,
                "  <xsd:restriction base=\"xsd:long\">\n    \
                 <xsd:maxInclusive value=\"{}\"/>\n    \
                 <xsd:minInclusive value=\"{}\"/>\n  </xsd:restriction>\n",
                i64::MAX,
                i64::MIN
            );
        }
        FLOAT4OID => {
            result.push_str("  <xsd:restriction base=\"xsd:float\"></xsd:restriction>\n");
        }
        FLOAT8OID => {
            result.push_str("  <xsd:restriction base=\"xsd:double\"></xsd:restriction>\n");
        }
        BOOLOID => {
            result.push_str("  <xsd:restriction base=\"xsd:boolean\"></xsd:restriction>\n");
        }
        TIMEOID | TIMETZOID => {
            let tz = if typeoid == TIMETZOID { "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}" } else { "" };
            if typmod == -1 {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:time\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{tz}\"/>\n  \
                     </xsd:restriction>\n"
                );
            } else if typmod == 0 {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:time\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{tz}\"/>\n  \
                     </xsd:restriction>\n"
                );
            } else {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:time\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{tz}\"/>\n  \
                     </xsd:restriction>\n",
                    typmod - VARHDRSZ
                );
            }
        }
        TIMESTAMPOID | TIMESTAMPTZOID => {
            let tz = if typeoid == TIMESTAMPTZOID { "(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}" } else { "" };
            if typmod == -1 {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}(.\\p{{Nd}}+)?{tz}\"/>\n  \
                     </xsd:restriction>\n"
                );
            } else if typmod == 0 {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}{tz}\"/>\n  \
                     </xsd:restriction>\n"
                );
            } else {
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"xsd:dateTime\">\n    \
                     <xsd:pattern value=\"\\p{{Nd}}{{4}}-\\p{{Nd}}{{2}}-\\p{{Nd}}{{2}}T\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}:\\p{{Nd}}{{2}}.\\p{{Nd}}{{{}}}{tz}\"/>\n  \
                     </xsd:restriction>\n",
                    typmod - VARHDRSZ
                );
            }
        }
        DATEOID => {
            result.push_str(
                "  <xsd:restriction base=\"xsd:date\">\n    \
                 <xsd:pattern value=\"\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}\"/>\n  \
                 </xsd:restriction>\n",
            );
        }
        _ => {
            if lsyscache::typ::get_typtype(typeoid)? == TYPTYPE_DOMAIN {
                let mut base_typmod: i32 = -1;
                let base_typeoid = lsyscache::typ::getBaseTypeAndTypmod(typeoid, &mut base_typmod)?;
                let _ = write!(
                    result,
                    "  <xsd:restriction base=\"{}\"/>\n",
                    map_sql_type_to_xml_name(base_typeoid, base_typmod)?
                );
            }
        }
    }
    result.push_str("</xsd:simpleType>\n");
    Ok(result)
}
