// RemoveObjects (dropcmds.c), OBJECT_RULE arm; other object classes stay
// with their own DDL lanes.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType};
use rel_vocab::RangeVar;
use types_rel::AccessExclusiveLock;

const REWRITE_RELATION_ID: Oid = 2618;

pub fn RemoveObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    if stmt.removeType != ObjectType::OBJECT_RULE {
        panic!(
            "RemoveObjects (dropcmds.c): {:?} arm unported (its DDL lane owns it)",
            stmt.removeType
        );
    }
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
