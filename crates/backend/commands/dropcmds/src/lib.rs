// dropcmds.c RemoveObjects over the objtypes get_object_address serves
// (TYPE/DOMAIN/SCHEMA); ownership checks run the superuser fast path only —
// non-superuser DROP is a named panic (aclchk object_ownercheck unported).
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::xact::XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
use types_error::{PgResult, NOTICE};
use types_nodes::parsenodes::{DropStmt, ObjectType};
use types_nodes::rawnodes::TypeName;
use types_nodes::{Node, NodeList};
use types_rel::AccessExclusiveLock;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: dropcmds.c {what}")
}

fn notice(msg: String) -> PgResult<()> {
    elog_seams::ereport_msg::call(NOTICE, msg, None)
}

fn schema_does_not_exist_skipping(names: &NodeList<'_>) -> PgResult<Option<String>> {
    let rv = catalog_objectaddress::makeRangeVarFromNameList(names);
    if let Some(schemaname) = rv.schemaname {
        if catalog_namespace::LookupNamespaceNoError(schemaname)? == types_core::InvalidOid {
            return Ok(Some(format!(
                "schema \"{schemaname}\" does not exist, skipping"
            )));
        }
    }
    Ok(None)
}

fn does_not_exist_skipping(objtype: ObjectType, object: Node<'_>) -> PgResult<()> {
    let msg = match objtype {
        ObjectType::OBJECT_TYPE | ObjectType::OBJECT_DOMAIN => {
            let tn: &TypeName<'_> = object.as_type_name().expect("type object is a TypeName");
            match schema_does_not_exist_skipping(&tn.names)? {
                Some(msg) => msg,
                None => format!(
                    "type \"{}\" does not exist, skipping",
                    catalog_objectaddress::TypeNameToString(tn)
                ),
            }
        }
        ObjectType::OBJECT_SCHEMA => {
            let name = object.as_string().expect("schema name is a String node").sval;
            format!("schema \"{name}\" does not exist, skipping")
        }
        other => unported(&format!("does_not_exist_skipping {other:?}")),
    };
    notice(msg)
}

pub fn RemoveObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropStmt<'mcx>) -> PgResult<()> {
    let mut objects = catalog_dependency::ObjectAddresses::new();

    for object in stmt.objects.iter() {
        let (address, relation) = catalog_objectaddress::get_object_address(
            mcx,
            stmt.removeType,
            object,
            AccessExclusiveLock,
            stmt.missing_ok,
        )?;

        if !OidIsValid(address.objectId) {
            debug_assert!(stmt.missing_ok);
            does_not_exist_skipping(stmt.removeType, object)?;
            continue;
        }

        if stmt.removeType == ObjectType::OBJECT_FUNCTION {
            unported("RemoveObjects OBJECT_FUNCTION prokind gate");
        }

        // C: namespace-owner shortcut, else check_object_ownership.
        let namespaceId = catalog_objectaddress::get_object_namespace(&address)?;
        if !superuser::superuser_arg(miscinit::GetUserId())? {
            unported("RemoveObjects check_object_ownership for non-superusers");
        }

        if OidIsValid(namespaceId) && catalog_namespace::isTempNamespace(namespaceId) {
            xact::OrMyXactFlags(XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
        }

        if let Some(rel) = relation {
            rel.close(types_rel::NoLock)?;
        }

        objects.add_exact_object_address(address);
    }

    catalog_dependency::performMultipleDeletions(mcx, &objects, stmt.behavior, 0)
}
