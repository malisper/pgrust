// RemoveObjects (dropcmds.c), OBJECT_RULE and OBJECT_EVENT_TRIGGER arms;
// other object classes stay with their own DDL lanes.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType};
use rel_vocab::RangeVar;
use types_rel::AccessExclusiveLock;

const REWRITE_RELATION_ID: Oid = 2618;

pub fn RemoveObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    match stmt.removeType {
        ObjectType::OBJECT_RULE => remove_rules(mcx, stmt),
        ObjectType::OBJECT_EVENT_TRIGGER => remove_event_triggers(mcx, stmt),
        other => panic!(
            "RemoveObjects (dropcmds.c): {other:?} arm unported (its DDL lane owns it)"
        ),
    }
}

fn remove_event_triggers<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    let mut objects = catalog_dependency::ObjectAddresses::new();
    for cell in stmt.objects.iter() {
        let name = match cell.as_string() {
            Some(s) => s.sval,
            None => {
                let names = cell.as_list().expect("DROP EVENT TRIGGER object");
                if names.len() != 1 {
                    return Err(Box::new(
                        PgError::error("event trigger name cannot be qualified".to_string())
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                    ));
                }
                names.iter().next().and_then(|n| n.as_string()).expect("name").sval
            }
        };
        let oid = event_trigger::get_event_trigger_oid(name, stmt.missing_ok)?;
        if oid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!("event trigger \"{name}\" does not exist, skipping"),
                None,
            )?;
            continue;
        }
        // object_ownercheck superuser fast path; C also locks the object in
        // get_object_address — AcquireDeletionLock covers it at deletion.
        if !superuser::superuser_arg(miscinit::GetUserId())? {
            panic!("unported: RemoveObjects object_ownercheck for non-superusers");
        }
        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            event_trigger::EVENT_TRIGGER_RELATION_ID,
            oid,
        ));
    }
    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)?;
    Ok(())
}

fn remove_rules<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    let mut objects = catalog_dependency::ObjectAddresses::new();
    for cell in stmt.objects.iter() {
        let names = cell.as_list().expect("DROP RULE object is a name list");
        debug_assert!(names.len() >= 2);
        let rulename = names
            .last()
            .and_then(|n| n.as_string())
            .expect("rule name is a String")
            .sval;
        let mut it = names.iter().take(names.len() - 1);
        let mut rv = RangeVar {
            catalogname: None,
            schemaname: None,
            relname: "",
            inh: true,
            relpersistence: b'p',
            location: -1,
        };
        match names.len() - 1 {
            1 => {
                rv.relname = it.next().and_then(|n| n.as_string()).expect("name").sval;
            }
            2 => {
                rv.schemaname =
                    Some(it.next().and_then(|n| n.as_string()).expect("name").sval);
                rv.relname = it.next().and_then(|n| n.as_string()).expect("name").sval;
            }
            _ => panic!("improper relation name (too many dotted names)"),
        }

        // get_object_address_relobject (objectaddress.c): lock the relation,
        // then resolve the rule; IF EXISTS skips at either level.
        let relid =
            catalog_namespace::RangeVarGetRelid(&rv, AccessExclusiveLock, stmt.missing_ok)?;
        if relid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "relation \"{}\" does not exist, skipping",
                    rv.relname
                ),
                None,
            )?;
            continue;
        }
        let rule_oid = rewrite_define::get_rewrite_oid(mcx, relid, rulename, stmt.missing_ok)?;
        if rule_oid == InvalidOid {
            let relname = rv.relname;
            elog_seams::ereport_msg::call(
                NOTICE,
                format!(
                    "rule \"{rulename}\" for relation \"{relname}\" does not exist, skipping"
                ),
                None,
            )?;
            continue;
        }
        objects.add_exact_object_address(pg_depend::ObjectAddress::set(
            REWRITE_RELATION_ID,
            rule_oid,
        ));
    }
    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)?;
    Ok(())
}
