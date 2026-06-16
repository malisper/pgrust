//! Out-of-F0-scope executor / SPI / catalog command bodies of `extension.c`.
//!
//! F0 ports `extension.c`'s parse/analysis half (control-file analysis, the
//! version-update-path graph, the catalog-read cores, `RemoveExtensionById`, the
//! `CreateExtension` parse-side, and the backend-globals). The CREATE / ALTER /
//! DROP orchestration and the supporting SRFs / catalog edits drive the
//! parser/analyzer/planner/executor/utility pipeline, SPI, catalog DML, the
//! fmgr/`Datum` value layer, and filesystem write/IO — subsystems not yet wired.
//!
//! Each entry point is a loud `panic!` (mirror-pg-and-panic) naming the C
//! function it stands in for. When those subsystems land, the bodies will be
//! ported against the same node tree the parse half already uses, reusing the
//! in-crate analysis functions directly.

macro_rules! deferred {
    ($cfn:literal, $what:literal) => {
        panic!(concat!(
            "backend-commands-extension: ",
            $cfn,
            " is out of F0 scope (F0 ports extension.c's parse/analysis half). ",
            "It drives ",
            $what,
            ", which is not yet wired.",
        ))
    };
}

/// `CreateExtensionInternal` (C 1784-2017): the CREATE EXTENSION driver
/// (version/schema selection, CASCADE requires loop, script run, catalog insert).
pub fn CreateExtensionInternal() -> ! {
    deferred!(
        "CreateExtensionInternal",
        "the syscache/namespace/ACL lookups, the schema-create + extension-script \
         execution pipeline, and the pg_extension catalog insert"
    )
}

/// `get_required_extension` (C 2023-2088): CASCADE recursion + cyclic-dependency
/// detection.
pub fn get_required_extension() -> ! {
    deferred!(
        "get_required_extension",
        "the CASCADE-recursive CreateExtensionInternal call (executor/catalog)"
    )
}

/// `InsertExtensionTuple` (C 2192-2271): build + insert the pg_extension row and
/// its owner/schema/required-extension dependencies.
pub fn InsertExtensionTuple() -> ! {
    deferred!(
        "InsertExtensionTuple",
        "the fmgr/Datum column assembly, the pg_extension catalog insert, and the \
         pg_depend dependency records"
    )
}

/// `ExecAlterExtensionStmt` (C 3408-3544) + `ApplyExtensionUpdates` (C 3555-3702):
/// ALTER EXTENSION ... UPDATE.
pub fn ExecAlterExtensionStmt() -> ! {
    deferred!(
        "ExecAlterExtensionStmt/ApplyExtensionUpdates",
        "the syscache version lookup, the per-version pg_extension tuple edit, the \
         dependency delete/recreate, and the extension-script execution pipeline"
    )
}

/// `ExecAlterExtensionContentsStmt` (C 3713-3789) + `…Recurse` (C 3799-3929):
/// ALTER EXTENSION ... ADD/DROP member object.
pub fn ExecAlterExtensionContentsStmt() -> ! {
    deferred!(
        "ExecAlterExtensionContentsStmt/…Recurse",
        "get_object_address, the ObjectAddresses dependency bookkeeping, and the \
         type->array/relation->rowtype recursion"
    )
}

/// `AlterExtensionNamespace` (C 3193-3402): ALTER EXTENSION ... SET SCHEMA.
pub fn AlterExtensionNamespace() -> ! {
    deferred!(
        "AlterExtensionNamespace",
        "the namespace lookup/create, the pg_depend relocate scan, and the per-member \
         AlterObjectNamespace_oid catalog updates"
    )
}

/// `execute_extension_script` (C 1196-1458) + `execute_sql_string` (C 1046-1165):
/// run an extension install/update script.
pub fn execute_extension_script() -> ! {
    deferred!(
        "execute_extension_script/execute_sql_string",
        "the superuser/GUC security-context switch and the whole \
         parser/analyzer/planner/executor/utility script-execution loop"
    )
}

/// `read_extension_script_file` (C 871-898): read a script file and convert it to
/// the database encoding.
pub fn read_extension_script_file() -> ! {
    deferred!(
        "read_extension_script_file",
        "read_whole_file filesystem IO and the pg_verify_mbstr/pg_any_to_server \
         encoding conversion"
    )
}

/// `read_whole_file` (C 3939-4001): slurp a file into a string.
pub fn read_whole_file() -> ! {
    deferred!("read_whole_file", "AllocateFile/fread/ferror filesystem IO")
}

/// `extension_is_trusted` (C 1174-1186): the trusted + ACL_CREATE-on-database
/// install policy.
pub fn extension_is_trusted() -> ! {
    deferred!(
        "extension_is_trusted",
        "the object_aclcheck(DatabaseRelationId, MyDatabaseId, …, ACL_CREATE) call"
    )
}

/// `get_extension_schema` (C 232-246): OID -> namespace syscache lookup.
pub fn get_extension_schema() -> ! {
    deferred!(
        "get_extension_schema",
        "the SearchSysCache1(EXTENSIONOID, …) syscache lookup"
    )
}

/// `get_function_sibling_type` (C 272-331) + `ext_sibling_callback` (C 343-354):
/// the function-sibling type cache.
pub fn get_function_sibling_type() -> ! {
    deferred!(
        "get_function_sibling_type",
        "the pg_depend extension lookup, the pg_type syscache resolution, and the \
         syscache-invalidation callback registration"
    )
}

/// `pg_available_extensions` (C 2334-2420): SRF over the available extensions.
pub fn pg_available_extensions() -> ! {
    deferred!(
        "pg_available_extensions",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_available_extension_versions` (C 2432-2501) +
/// `get_available_versions_for_extension` (C 2508-2611).
pub fn pg_available_extension_versions() -> ! {
    deferred!(
        "pg_available_extension_versions/get_available_versions_for_extension",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_extension_update_paths` (C 2707-2782): SRF over the update paths.
pub fn pg_extension_update_paths() -> ! {
    deferred!(
        "pg_extension_update_paths",
        "the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_get_loaded_modules` (C 2975-3018): SRF over the loaded modules.
pub fn pg_get_loaded_modules() -> ! {
    deferred!(
        "pg_get_loaded_modules",
        "the loaded-module walk and the SRF tuplestore / fmgr Datum layer"
    )
}

/// `pg_extension_config_dump` (C 2792-2965): record a table/sequence in the
/// extension's extconfig/extcondition arrays.
pub fn pg_extension_config_dump() -> ! {
    deferred!(
        "pg_extension_config_dump",
        "the fmgr/Datum array machinery and the pg_extension catalog update"
    )
}

/// `extension_config_remove` (C 3028-3187): remove a table from the extension's
/// extconfig/extcondition arrays.
pub fn extension_config_remove() -> ! {
    deferred!(
        "extension_config_remove",
        "the fmgr/Datum array machinery and the pg_extension catalog update"
    )
}

/// `convert_requires_to_datum` (C 2681-2700): build the name[] Datum.
pub fn convert_requires_to_datum() -> ! {
    deferred!(
        "convert_requires_to_datum",
        "the fmgr/Datum array construction"
    )
}

/// `extension_file_exists` (C 2622-2675): does a control file for the extension
/// exist anywhere on the control path?
pub fn extension_file_exists() -> ! {
    deferred!(
        "extension_file_exists",
        "the AllocateDir/ReadDir filesystem scan"
    )
}

/// `script_error_callback` (C 904-1030): the error-context callback for
/// script-execution failures.
pub fn script_error_callback() -> ! {
    deferred!(
        "script_error_callback",
        "the errcontext/errposition error-context primitives bound to the live executor"
    )
}
