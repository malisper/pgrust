--
-- regproc
--

/* If objects exist, return oids */

CREATE ROLE regress_regrole_test;

-- without schemaname

-- pgrust:rowsort
SELECT regoper('||/');
-- pgrust:rowsort
SELECT regoperator('+(int4,int4)');
-- pgrust:rowsort
SELECT regproc('now');
-- pgrust:rowsort
SELECT regprocedure('abs(numeric)');
-- pgrust:rowsort
SELECT regclass('pg_class');
-- pgrust:rowsort
SELECT regtype('int4');
-- pgrust:rowsort
SELECT regcollation('"POSIX"');

-- pgrust:rowsort
SELECT to_regoper('||/');
-- pgrust:rowsort
SELECT to_regoperator('+(int4,int4)');
-- pgrust:rowsort
SELECT to_regproc('now');
-- pgrust:rowsort
SELECT to_regprocedure('abs(numeric)');
-- pgrust:rowsort
SELECT to_regclass('pg_class');
-- pgrust:rowsort
SELECT to_regtype('int4');
-- pgrust:rowsort
SELECT to_regcollation('"POSIX"');

-- with schemaname

-- pgrust:rowsort
SELECT regoper('pg_catalog.||/');
-- pgrust:rowsort
SELECT regoperator('pg_catalog.+(int4,int4)');
-- pgrust:rowsort
SELECT regproc('pg_catalog.now');
-- pgrust:rowsort
SELECT regprocedure('pg_catalog.abs(numeric)');
-- pgrust:rowsort
SELECT regclass('pg_catalog.pg_class');
-- pgrust:rowsort
SELECT regtype('pg_catalog.int4');
-- pgrust:rowsort
SELECT regcollation('pg_catalog."POSIX"');

-- pgrust:rowsort
SELECT to_regoper('pg_catalog.||/');
-- pgrust:rowsort
SELECT to_regproc('pg_catalog.now');
-- pgrust:rowsort
SELECT to_regprocedure('pg_catalog.abs(numeric)');
-- pgrust:rowsort
SELECT to_regclass('pg_catalog.pg_class');
-- pgrust:rowsort
SELECT to_regtype('pg_catalog.int4');
-- pgrust:rowsort
SELECT to_regcollation('pg_catalog."POSIX"');

-- schemaname not applicable

-- pgrust:rowsort
SELECT regrole('regress_regrole_test');
-- pgrust:rowsort
SELECT regrole('"regress_regrole_test"');
-- pgrust:rowsort
SELECT regnamespace('pg_catalog');
-- pgrust:rowsort
SELECT regnamespace('"pg_catalog"');

-- pgrust:rowsort
SELECT to_regrole('regress_regrole_test');
-- pgrust:rowsort
SELECT to_regrole('"regress_regrole_test"');
-- pgrust:rowsort
SELECT to_regnamespace('pg_catalog');
-- pgrust:rowsort
SELECT to_regnamespace('"pg_catalog"');

/* If objects don't exist, raise errors. */

DROP ROLE regress_regrole_test;

-- without schemaname

SELECT regoper('||//');
SELECT regoperator('++(int4,int4)');
SELECT regproc('know');
SELECT regprocedure('absinthe(numeric)');
SELECT regclass('pg_classes');
SELECT regtype('int3');

-- with schemaname

SELECT regoper('ng_catalog.||/');
SELECT regoperator('ng_catalog.+(int4,int4)');
SELECT regproc('ng_catalog.now');
SELECT regprocedure('ng_catalog.abs(numeric)');
SELECT regclass('ng_catalog.pg_class');
SELECT regtype('ng_catalog.int4');
\set VERBOSITY sqlstate \\ -- error message is encoding-dependent
SELECT regcollation('ng_catalog."POSIX"');
\set VERBOSITY default

-- schemaname not applicable

SELECT regrole('regress_regrole_test');
SELECT regrole('"regress_regrole_test"');
SELECT regrole('Nonexistent');
SELECT regrole('"Nonexistent"');
SELECT regrole('foo.bar');
SELECT regnamespace('Nonexistent');
SELECT regnamespace('"Nonexistent"');
SELECT regnamespace('foo.bar');

/* If objects don't exist, return NULL with no error. */

-- without schemaname

-- pgrust:rowsort
SELECT to_regoper('||//');
-- pgrust:rowsort
SELECT to_regoperator('++(int4,int4)');
-- pgrust:rowsort
SELECT to_regproc('know');
-- pgrust:rowsort
SELECT to_regprocedure('absinthe(numeric)');
-- pgrust:rowsort
SELECT to_regclass('pg_classes');
-- pgrust:rowsort
SELECT to_regtype('int3');
-- pgrust:rowsort
SELECT to_regcollation('notacollation');

-- with schemaname

-- pgrust:rowsort
SELECT to_regoper('ng_catalog.||/');
-- pgrust:rowsort
SELECT to_regoperator('ng_catalog.+(int4,int4)');
-- pgrust:rowsort
SELECT to_regproc('ng_catalog.now');
-- pgrust:rowsort
SELECT to_regprocedure('ng_catalog.abs(numeric)');
-- pgrust:rowsort
SELECT to_regclass('ng_catalog.pg_class');
-- pgrust:rowsort
SELECT to_regtype('ng_catalog.int4');
-- pgrust:rowsort
SELECT to_regcollation('ng_catalog."POSIX"');

-- schemaname not applicable

-- pgrust:rowsort
SELECT to_regrole('regress_regrole_test');
-- pgrust:rowsort
SELECT to_regrole('"regress_regrole_test"');
-- pgrust:rowsort
SELECT to_regrole('foo.bar');
-- pgrust:rowsort
SELECT to_regrole('Nonexistent');
-- pgrust:rowsort
SELECT to_regrole('"Nonexistent"');
-- pgrust:rowsort
SELECT to_regrole('foo.bar');
-- pgrust:rowsort
SELECT to_regnamespace('Nonexistent');
-- pgrust:rowsort
SELECT to_regnamespace('"Nonexistent"');
-- pgrust:rowsort
SELECT to_regnamespace('foo.bar');

-- Test to_regtypemod
-- pgrust:rowsort
SELECT to_regtypemod('text');
-- pgrust:rowsort
SELECT to_regtypemod('timestamp(4)');
-- pgrust:rowsort
SELECT to_regtypemod('no_such_type(4)');
-- pgrust:rowsort
SELECT format_type(to_regtype('varchar(32)'), to_regtypemod('varchar(32)'));
-- pgrust:rowsort
SELECT format_type(to_regtype('bit'), to_regtypemod('bit'));
-- pgrust:rowsort
SELECT format_type(to_regtype('"bit"'), to_regtypemod('"bit"'));

-- Test soft-error API

SELECT * FROM pg_input_error_info('ng_catalog.pg_class', 'regclass');
SELECT pg_input_is_valid('ng_catalog."POSIX"', 'regcollation');
SELECT * FROM pg_input_error_info('no_such_config', 'regconfig');
SELECT * FROM pg_input_error_info('no_such_dictionary', 'regdictionary');
SELECT * FROM pg_input_error_info('Nonexistent', 'regnamespace');
SELECT * FROM pg_input_error_info('ng_catalog.||/', 'regoper');
SELECT * FROM pg_input_error_info('-', 'regoper');
SELECT * FROM pg_input_error_info('ng_catalog.+(int4,int4)', 'regoperator');
SELECT * FROM pg_input_error_info('-', 'regoperator');
SELECT * FROM pg_input_error_info('ng_catalog.now', 'regproc');
SELECT * FROM pg_input_error_info('ng_catalog.abs(numeric)', 'regprocedure');
SELECT * FROM pg_input_error_info('ng_catalog.abs(numeric', 'regprocedure');
SELECT * FROM pg_input_error_info('regress_regrole_test', 'regrole');
SELECT * FROM pg_input_error_info('no_such_type', 'regtype');

-- Some cases that should be soft errors, but are not yet
SELECT * FROM pg_input_error_info('incorrect type name syntax', 'regtype');
SELECT * FROM pg_input_error_info('numeric(1,2,3)', 'regtype');  -- bogus typmod
SELECT * FROM pg_input_error_info('way.too.many.names', 'regtype');
SELECT * FROM pg_input_error_info('no_such_catalog.schema.name', 'regtype');
