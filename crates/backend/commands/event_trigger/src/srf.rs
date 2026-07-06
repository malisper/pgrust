use datum::Datum;
use fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData};
use mcx::Mcx;
use types_core::{OidIsValid, TEXTOID};
use types_error::{PgResult, ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED, ERROR};

use crate::{CollectedCommandData, CURRENT_STATE};

#[cold]
#[inline(never)]
fn protocol_violation(fname: &str, ctx: &str) -> Box<types_error::PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED)
            .errmsg(format!("{fname}() can only be called in {ctx} event trigger function"))
            .into_error(),
    )
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(fmgr::varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

fn text_array_datum(mcx: Mcx<'_>, items: &[String]) -> PgResult<Datum> {
    let mut elems: Vec<Datum> = Vec::with_capacity(items.len());
    for s in items {
        elems.push(text_datum(mcx, s)?);
    }
    let img = arrayfuncs::construct_md_array(
        mcx,
        &elems,
        None,
        1,
        &[items.len() as i32],
        &[1],
        TEXTOID,
        -1,
        false,
        b'i',
    )?;
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}

fn empty_text_array_datum(mcx: Mcx<'_>) -> PgResult<Datum> {
    let img = arrayfuncs::construct_empty_array(mcx, TEXTOID)?;
    Ok(Datum::from_usize(img.leak().as_ptr() as usize))
}

fn fc_pg_event_trigger_dropped_objects(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_event_trigger_dropped_objects: resolved FmgrInfo required");
    let in_drop =
        CURRENT_STATE.with(|s| s.borrow().last().map(|st| st.in_sql_drop).unwrap_or(false));
    if !in_drop {
        return Err(protocol_violation("pg_event_trigger_dropped_objects", "a sql_drop").into());
    }

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    CURRENT_STATE.with(|s| -> PgResult<()> {
        let stack = s.borrow();
        let st = stack.last().expect("in_sql_drop implies state");
        for obj in st.sql_drop_list.iter() {
            let mut values = [Datum::null(); 12];
            let mut nulls = [false; 12];
            values[0] = Datum::from_oid(obj.address.classId);
            values[1] = Datum::from_oid(obj.address.objectId);
            values[2] = Datum::from_i32(obj.address.objectSubId);
            values[3] = Datum::from_bool(obj.original);
            values[4] = Datum::from_bool(obj.normal);
            values[5] = Datum::from_bool(obj.istemp);
            values[6] = text_datum(mcx, obj.objecttype.as_deref().unwrap_or(""))?;
            match &obj.schemaname {
                Some(v) => values[7] = text_datum(mcx, v)?,
                None => nulls[7] = true,
            }
            match &obj.objname {
                Some(v) => values[8] = text_datum(mcx, v)?,
                None => nulls[8] = true,
            }
            match &obj.objidentity {
                Some(v) => values[9] = text_datum(mcx, v)?,
                None => nulls[9] = true,
            }
            match &obj.addrnames {
                Some(names) => {
                    values[10] = text_array_datum(mcx, names)?;
                    match &obj.addrargs {
                        Some(args) if !args.is_empty() => {
                            values[11] = text_array_datum(mcx, args)?
                        }
                        _ => values[11] = empty_text_array_datum(mcx)?,
                    }
                }
                None => {
                    nulls[10] = true;
                    nulls[11] = true;
                }
            }
            srf.putvalues(&values, &nulls)?;
        }
        Ok(())
    })?;

    Ok(srf.finish(fcinfo))
}

fn fc_pg_event_trigger_ddl_commands(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_event_trigger_ddl_commands: resolved FmgrInfo required");
    if !crate::state_is_set() {
        return Err(elog::ereport(ERROR)
            .errcode(ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED)
            .errmsg(
                "pg_event_trigger_ddl_commands() can only be called in an event trigger function",
            )
            .into_error()
            .into());
    }

    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    CURRENT_STATE.with(|s| -> PgResult<()> {
        let stack = s.borrow();
        let st = stack.last().expect("state_is_set");
        for cmd in st.command_list.iter() {
            let mut values = [Datum::null(); 9];
            let mut nulls = [false; 9];

            let addr = match &cmd.data {
                CollectedCommandData::Simple => {
                    if !OidIsValid(cmd.address.objectId) {
                        // IF NOT EXISTS on an existing object: nothing to report.
                        continue;
                    }
                    cmd.address
                }
                CollectedCommandData::AlterTable { class_id, object_id, .. } => {
                    pg_depend::ObjectAddress::set(*class_id, *object_id)
                }
                CollectedCommandData::Grant { objtype } => {
                    nulls[0] = true;
                    nulls[1] = true;
                    nulls[2] = true;
                    values[3] = text_datum(mcx, cmdtag::GetCommandTagName(cmd.tag))?;
                    values[4] = text_datum(mcx, stringify_grant_objtype(*objtype)?)?;
                    nulls[5] = true;
                    nulls[6] = true;
                    values[7] = Datum::from_bool(cmd.in_extension);
                    values[8] = Datum::from_usize(cmd as *const _ as usize);
                    srf.putvalues(&values, &nulls)?;
                    continue;
                }
                CollectedCommandData::DefPrivs { objtype } => {
                    nulls[0] = true;
                    nulls[1] = true;
                    nulls[2] = true;
                    values[3] = text_datum(mcx, cmdtag::GetCommandTagName(cmd.tag))?;
                    values[4] = text_datum(mcx, stringify_adefprivs_objtype(*objtype)?)?;
                    nulls[5] = true;
                    nulls[6] = true;
                    values[7] = Datum::from_bool(cmd.in_extension);
                    values[8] = Datum::from_usize(cmd as *const _ as usize);
                    srf.putvalues(&values, &nulls)?;
                    continue;
                }
            };

            let Some(identity) =
                catalog_objectaddress::getObjectIdentityParts(mcx, &addr, true)?
            else {
                // Object dropped by the same command; skip rather than fail.
                continue;
            };
            let typedesc =
                catalog_objectaddress::getObjectTypeDescription(mcx, &addr, true)?
                    .expect("object type description is never NULL");
            let schema = object_schema_name(mcx, &addr)?;

            values[0] = Datum::from_oid(addr.classId);
            values[1] = Datum::from_oid(addr.objectId);
            values[2] = Datum::from_i32(addr.objectSubId);
            values[3] = text_datum(mcx, cmdtag::GetCommandTagName(cmd.tag))?;
            values[4] = text_datum(mcx, &typedesc)?;
            match schema {
                Some(nsp) => values[5] = text_datum(mcx, &nsp)?,
                None => nulls[5] = true,
            }
            values[6] = text_datum(mcx, &identity.identity)?;
            values[7] = Datum::from_bool(cmd.in_extension);
            // pg_ddl_command: by-value pointer datum, consumable only by
            // extension deparse (none ported); never dereferenced in-core.
            values[8] = Datum::from_usize(cmd as *const _ as usize);

            srf.putvalues(&values, &nulls)?;
        }
        Ok(())
    })?;

    Ok(srf.finish(fcinfo))
}

// stringify_grant_objtype (event_trigger.c): the ObjectType spelling used by
// GRANT/REVOKE; types GRANT cannot reach are C's elog(ERROR) arm.
// stringify_adefprivs_objtype (event_trigger.c).
fn stringify_adefprivs_objtype(
    objtype: types_nodes::parsenodes::ObjectType,
) -> PgResult<&'static str> {
    use types_nodes::parsenodes::ObjectType::*;
    Ok(match objtype {
        OBJECT_COLUMN => "COLUMNS",
        OBJECT_TABLE => "TABLES",
        OBJECT_SEQUENCE => "SEQUENCES",
        OBJECT_DATABASE => "DATABASES",
        OBJECT_DOMAIN => "DOMAINS",
        OBJECT_FDW => "FOREIGN DATA WRAPPERS",
        OBJECT_FOREIGN_SERVER => "FOREIGN SERVERS",
        OBJECT_FUNCTION => "FUNCTIONS",
        OBJECT_LANGUAGE => "LANGUAGES",
        OBJECT_LARGEOBJECT => "LARGE OBJECTS",
        OBJECT_SCHEMA => "SCHEMAS",
        OBJECT_PROCEDURE => "PROCEDURES",
        OBJECT_ROUTINE => "ROUTINES",
        OBJECT_TABLESPACE => "TABLESPACES",
        OBJECT_TYPE => "TYPES",
        other => panic!("unsupported object type: {other:?}"),
    })
}

fn stringify_grant_objtype(objtype: types_nodes::parsenodes::ObjectType) -> PgResult<&'static str> {
    use types_nodes::parsenodes::ObjectType::*;
    Ok(match objtype {
        OBJECT_COLUMN => "COLUMN",
        OBJECT_TABLE => "TABLE",
        OBJECT_SEQUENCE => "SEQUENCE",
        OBJECT_DATABASE => "DATABASE",
        OBJECT_DOMAIN => "DOMAIN",
        OBJECT_FDW => "FOREIGN DATA WRAPPER",
        OBJECT_FOREIGN_SERVER => "FOREIGN SERVER",
        OBJECT_FUNCTION => "FUNCTION",
        OBJECT_LANGUAGE => "LANGUAGE",
        OBJECT_LARGEOBJECT => "LARGE OBJECT",
        OBJECT_SCHEMA => "SCHEMA",
        OBJECT_PARAMETER_ACL => "PARAMETER",
        OBJECT_PROCEDURE => "PROCEDURE",
        OBJECT_ROUTINE => "ROUTINE",
        OBJECT_TABLESPACE => "TABLESPACE",
        OBJECT_TYPE => "TYPE",
        other => {
            return Err(elog::ereport(ERROR)
                .errmsg(format!("unsupported object type: {}", other as i32))
                .into_error()
                .into())
        }
    })
}

fn object_schema_name(mcx: Mcx<'_>, addr: &pg_depend::ObjectAddress) -> PgResult<Option<String>> {
    let nsp = match addr.classId {
        types_core::RELATION_RELATION_ID => Some(lsyscache::relation::get_rel_namespace(addr.objectId)?),
        _ => crate::sqldrop::object_namespace(addr)?,
    };
    match nsp {
        Some(nsp) if OidIsValid(nsp) => Ok(lsyscache::misc::get_namespace_name_or_temp(mcx, nsp)?
            .map(|s| s.as_str().to_string())),
        _ => Ok(None),
    }
}

fn fc_pg_event_trigger_table_rewrite_oid(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let oid = CURRENT_STATE.with(|s| s.borrow().last().map(|st| st.table_rewrite_oid));
    match oid {
        Some(o) if OidIsValid(o) => Ok(Datum::from_oid(o)),
        _ => Err(protocol_violation("pg_event_trigger_table_rewrite_oid", "a table_rewrite")
            .into()),
    }
}

fn fc_pg_event_trigger_table_rewrite_reason(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let reason = CURRENT_STATE.with(|s| s.borrow().last().map(|st| st.table_rewrite_reason));
    match reason {
        Some(r) if r != 0 => Ok(Datum::from_i32(r)),
        _ => Err(protocol_violation(
            "pg_event_trigger_table_rewrite_reason",
            "a table_rewrite",
        )
        .into()),
    }
}

pub static EVENT_TRIGGER_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3566,
        name: "pg_event_trigger_dropped_objects",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_event_trigger_dropped_objects,
    },
    FmgrBuiltin {
        foid: 4566,
        name: "pg_event_trigger_table_rewrite_oid",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_event_trigger_table_rewrite_oid,
    },
    FmgrBuiltin {
        foid: 4567,
        name: "pg_event_trigger_table_rewrite_reason",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_event_trigger_table_rewrite_reason,
    },
    FmgrBuiltin {
        foid: 4568,
        name: "pg_event_trigger_ddl_commands",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_event_trigger_ddl_commands,
    },
];
