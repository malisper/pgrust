-- information_schema differential matrix: every view in the schema, the _pg_*
-- helper functions, and the schema's domain casts. Captured on the same
-- datadir by C postgres and pgrust; outputs byte-diffed.
SET timezone = 'UTC';
SET search_path = ischeck, public;

SELECT * FROM information_schema.information_schema_catalog_name;
SELECT * FROM information_schema.applicable_roles WHERE role_name LIKE 'ischeck%' ORDER BY grantee, role_name;
SELECT * FROM information_schema.administrable_role_authorizations WHERE role_name LIKE 'ischeck%' ORDER BY grantee, role_name;
SELECT * FROM information_schema.attributes WHERE udt_schema = 'ischeck' ORDER BY udt_name, ordinal_position;
SELECT * FROM information_schema.character_sets;
SELECT * FROM information_schema.check_constraint_routine_usage WHERE constraint_schema = 'ischeck' ORDER BY constraint_name, specific_name;
SELECT * FROM information_schema.check_constraints WHERE constraint_schema = 'ischeck' ORDER BY constraint_name;
SELECT * FROM information_schema.collations WHERE collation_schema = 'ischeck' ORDER BY collation_name;
SELECT * FROM information_schema.collation_character_set_applicability WHERE collation_schema = 'ischeck' ORDER BY collation_name;
SELECT * FROM information_schema.column_column_usage WHERE table_schema = 'ischeck' ORDER BY table_name, column_name, dependent_column;
SELECT * FROM information_schema.column_domain_usage WHERE domain_schema = 'ischeck' ORDER BY domain_name, table_name, column_name;
SELECT * FROM information_schema.column_privileges WHERE table_schema = 'ischeck' ORDER BY grantee, table_name, column_name, privilege_type;
SELECT * FROM information_schema.column_udt_usage WHERE table_schema = 'ischeck' ORDER BY table_name, column_name;
SELECT * FROM information_schema.columns WHERE table_schema = 'ischeck' ORDER BY table_name, ordinal_position;
SELECT * FROM information_schema.constraint_column_usage WHERE table_schema = 'ischeck' ORDER BY constraint_name, table_name, column_name;
SELECT * FROM information_schema.constraint_table_usage WHERE table_schema = 'ischeck' ORDER BY constraint_name, table_name;
SELECT * FROM information_schema.domain_constraints WHERE domain_schema = 'ischeck' ORDER BY domain_name, constraint_name;
SELECT * FROM information_schema.domain_udt_usage WHERE domain_schema = 'ischeck' ORDER BY domain_name;
SELECT * FROM information_schema.domains WHERE domain_schema = 'ischeck' ORDER BY domain_name;
SELECT * FROM information_schema.enabled_roles WHERE role_name LIKE 'ischeck%' ORDER BY role_name;
SELECT * FROM information_schema.key_column_usage WHERE constraint_schema = 'ischeck' ORDER BY constraint_name, ordinal_position;
SELECT * FROM information_schema.parameters WHERE specific_schema = 'ischeck' ORDER BY specific_name, ordinal_position;
SELECT * FROM information_schema.referential_constraints WHERE constraint_schema = 'ischeck' ORDER BY constraint_name;
SELECT * FROM information_schema.role_column_grants WHERE table_schema = 'ischeck' ORDER BY grantee, table_name, column_name, privilege_type;
SELECT * FROM information_schema.routine_column_usage WHERE specific_schema = 'ischeck' ORDER BY specific_name, table_name, column_name;
SELECT * FROM information_schema.routine_privileges WHERE specific_schema = 'ischeck' ORDER BY specific_name, grantee, privilege_type;
SELECT * FROM information_schema.role_routine_grants WHERE specific_schema = 'ischeck' ORDER BY specific_name, grantee, privilege_type;
SELECT * FROM information_schema.routine_routine_usage WHERE specific_schema = 'ischeck' ORDER BY specific_name, routine_name;
SELECT * FROM information_schema.routine_sequence_usage WHERE specific_schema = 'ischeck' ORDER BY specific_name, sequence_name;
SELECT * FROM information_schema.routine_table_usage WHERE specific_schema = 'ischeck' ORDER BY specific_name, table_name;
SELECT * FROM information_schema.routines WHERE specific_schema = 'ischeck' ORDER BY specific_name;
SELECT * FROM information_schema.schemata WHERE schema_name = 'ischeck' ORDER BY schema_name;
SELECT * FROM information_schema.sequences WHERE sequence_schema = 'ischeck' ORDER BY sequence_name;
SELECT * FROM information_schema.table_constraints WHERE constraint_schema = 'ischeck' ORDER BY table_name, constraint_name;
SELECT * FROM information_schema.table_privileges WHERE table_schema = 'ischeck' ORDER BY grantee, table_name, privilege_type;
SELECT * FROM information_schema.role_table_grants WHERE table_schema = 'ischeck' ORDER BY grantee, table_name, privilege_type;
SELECT * FROM information_schema.tables WHERE table_schema = 'ischeck' ORDER BY table_name;
SELECT * FROM information_schema.transforms ORDER BY udt_schema, udt_name;
SELECT * FROM information_schema.triggered_update_columns WHERE trigger_schema = 'ischeck' ORDER BY trigger_name, event_object_table, event_object_column;
SELECT * FROM information_schema.triggers WHERE trigger_schema = 'ischeck' ORDER BY trigger_name, event_manipulation;
SELECT * FROM information_schema.udt_privileges WHERE udt_schema = 'ischeck' ORDER BY grantee, udt_name, privilege_type;
SELECT * FROM information_schema.role_udt_grants WHERE udt_schema = 'ischeck' ORDER BY grantee, udt_name, privilege_type;
SELECT * FROM information_schema.usage_privileges WHERE object_schema = 'ischeck' OR object_name LIKE 'ischeck%' ORDER BY grantee, object_type, object_name, privilege_type;
SELECT * FROM information_schema.role_usage_grants WHERE object_schema = 'ischeck' OR object_name LIKE 'ischeck%' ORDER BY grantee, object_type, object_name, privilege_type;
SELECT * FROM information_schema.user_defined_types WHERE user_defined_type_schema = 'ischeck' ORDER BY user_defined_type_name;
SELECT * FROM information_schema.view_column_usage WHERE view_schema = 'ischeck' ORDER BY view_name, table_name, column_name;
SELECT * FROM information_schema.view_routine_usage WHERE table_schema = 'ischeck' ORDER BY table_name, specific_name;
SELECT * FROM information_schema.view_table_usage WHERE view_schema = 'ischeck' ORDER BY view_name, table_name;
SELECT * FROM information_schema.views WHERE table_schema = 'ischeck' ORDER BY table_name;
SELECT * FROM information_schema.data_type_privileges WHERE object_schema = 'ischeck' ORDER BY object_name, object_type, dtd_identifier;
SELECT * FROM information_schema.element_types WHERE object_schema = 'ischeck' ORDER BY object_name, collection_type_identifier;

SELECT * FROM information_schema._pg_foreign_data_wrappers ORDER BY foreign_data_wrapper_name;
SELECT * FROM information_schema.foreign_data_wrapper_options ORDER BY foreign_data_wrapper_name, option_name;
SELECT * FROM information_schema.foreign_data_wrappers ORDER BY foreign_data_wrapper_name;
SELECT * FROM information_schema._pg_foreign_servers ORDER BY foreign_server_name;
SELECT * FROM information_schema.foreign_server_options ORDER BY foreign_server_name, option_name;
SELECT * FROM information_schema.foreign_servers ORDER BY foreign_server_name;
SELECT * FROM information_schema._pg_foreign_tables ORDER BY foreign_table_schema, foreign_table_name;
SELECT * FROM information_schema.foreign_table_options ORDER BY foreign_table_name, option_name;
SELECT * FROM information_schema.foreign_tables ORDER BY foreign_table_name;
SELECT * FROM information_schema._pg_foreign_table_columns ORDER BY nspname, relname, attname;
SELECT * FROM information_schema.column_options ORDER BY table_name, column_name, option_name;
SELECT * FROM information_schema._pg_user_mappings ORDER BY foreign_server_name, authorization_identifier;
SELECT * FROM information_schema.user_mapping_options ORDER BY foreign_server_name, authorization_identifier, option_name;
SELECT * FROM information_schema.user_mappings ORDER BY foreign_server_name, authorization_identifier;

-- Foreign-table rows in schema-filtered views (handler-less FDW).
SELECT * FROM information_schema.tables WHERE table_schema = 'ischeck_ft' ORDER BY table_name;
SELECT * FROM information_schema.columns WHERE table_schema = 'ischeck_ft' ORDER BY table_name, ordinal_position;

-- Full-catalog executions (no filter): exercises the whole view body.
SELECT count(*) FROM information_schema.columns;
SELECT count(*) FROM information_schema.tables;
SELECT count(*) FROM information_schema.table_constraints;
SELECT count(*) FROM information_schema.routines;
SELECT count(*) FROM information_schema.parameters;
SELECT count(*) FROM information_schema.triggers;
SELECT count(*) FROM information_schema.sequences;
SELECT count(*) FROM information_schema.element_types;
SELECT count(*) FROM information_schema.views;
SELECT count(*) FROM information_schema.key_column_usage;

-- Helper functions used throughout the schema.
SELECT information_schema._pg_expandarray(ARRAY['a','b','c']);
SELECT (information_schema._pg_expandarray(ARRAY[10,20,30])).*;
SELECT information_schema._pg_index_position('ischeck.parent_pk'::regclass::oid, 1::smallint);
SELECT information_schema._pg_truetypid(a.*, t.*) AS typid,
       information_schema._pg_truetypmod(a.*, t.*) AS typmod
  FROM pg_attribute a JOIN pg_type t ON a.atttypid = t.oid
 WHERE a.attrelid = 'ischeck.child'::regclass AND a.attname = 'label';
SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, 36);
SELECT information_schema._pg_char_octet_length('varchar'::regtype::oid, 36);
SELECT information_schema._pg_numeric_precision('numeric'::regtype::oid, 786436);
SELECT information_schema._pg_numeric_precision_radix('numeric'::regtype::oid, 786436);
SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, 786436);
SELECT information_schema._pg_datetime_precision('timestamp'::regtype::oid, 3);
SELECT information_schema._pg_interval_type('interval'::regtype::oid, 201392127);

-- Domain casts used by every view column.
SELECT 'some_ident'::information_schema.sql_identifier;
SELECT 5::information_schema.cardinal_number;
SELECT (-1)::information_schema.cardinal_number;
SELECT 'free text'::information_schema.character_data;
SELECT 'YES'::information_schema.yes_or_no;
SELECT '2020-06-01 12:00:00+00'::timestamptz::information_schema.time_stamp;

-- UNION ALL member whose nested subquery pulls up into a join: after the
-- recursive pull_up_subqueries the member's jointree bottoms out at a
-- JoinExpr, so the post-recursion is_safe_append_member recheck must reject
-- the pullup (r4 panic shape; the information_schema *_privileges /
-- check_constraints UNION ALL views hit this).
SELECT x FROM (SELECT a.id AS x FROM ischeck.parent a JOIN ischeck.parent b ON a.id = b.id) s
UNION ALL
SELECT 99 ORDER BY 1;

-- psql describe family against the zoo.
\d ischeck.parent
\d ischeck.child
\d+ ischeck.v_child
\d ischeck.part
\d ischeck_ft.ftab
\dD ischeck.*
\dT ischeck.*
\ds ischeck.*
\dy
