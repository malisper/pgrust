use types_core::{catalog::BOOTSTRAP_SUPERUSERID, InvalidOid, Oid};
use types_error::PgError;
use types_nodes::parsenodes::CreateSchemaStmt;
use types_nodes::NodeList;

const EXISTING_SCHEMA: Oid = 2200;
const CURRENT_EXTENSION: Oid = 9999;
const DEPEND_RELATION_ID: Oid = 2608;

fn install_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|nspname| {
            if nspname == "preexist" {
                Ok(EXISTING_SCHEMA)
            } else {
                Ok(InvalidOid)
            }
        });
        elog_seams::ereport::set(|_| Ok(()));
        relation_seams::relation_open::set(|_mcx, oid, _lockmode| {
            Err(PgError::error(format!("test probe: relation {oid} opened")).into())
        });
    });
}

fn with_extension_script<T>(f: impl FnOnce() -> T) -> T {
    pg_depend::set_creating_extension(true);
    pg_depend::set_current_extension_object(CURRENT_EXTENSION);
    let r = f();
    pg_depend::set_creating_extension(false);
    pg_depend::set_current_extension_object(InvalidOid);
    r
}

fn ifne_stmt<'a>() -> CreateSchemaStmt<'a> {
    CreateSchemaStmt {
        schemaname: Some("preexist"),
        authrole: None,
        schemaElts: NodeList::nil(),
        if_not_exists: true,
    }
}

#[test]
fn if_not_exists_membership_gate_open_outside_extension() {
    install_seams();
    miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);
    assert!(!pg_depend::creating_extension());
    let ctx = mcx::MemoryContext::new("t");
    let oid = crate::CreateSchemaCommand(ctx.mcx(), &ifne_stmt(), &mut |_, _, _| Ok(())).unwrap();
    assert_eq!(oid, InvalidOid);
}

#[test]
fn if_not_exists_inside_extension_script_is_55000() {
    install_seams();
    miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);
    let ctx = mcx::MemoryContext::new("t");
    let e = with_extension_script(|| {
        crate::CreateSchemaCommand(ctx.mcx(), &ifne_stmt(), &mut |_, _, _| Ok(()))
    })
    .unwrap_err();
    assert_eq!(e.message(), format!("test probe: relation {DEPEND_RELATION_ID} opened"));
}
