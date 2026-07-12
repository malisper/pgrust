--
-- TEXT
--

-- pgrust:rowsort
SELECT text 'this is a text string' = text 'this is a text string' AS true;

-- pgrust:rowsort
SELECT text 'this is a text string' = text 'this is a text strin' AS false;

-- text_tbl was already created and filled in test_setup.sql.
-- pgrust:rowsort
SELECT * FROM TEXT_TBL;

-- As of 8.3 we have removed most implicit casts to text, so that for example
-- this no longer works:

select length(42);

-- But as a special exception for usability's sake, we still allow implicit
-- casting to text in concatenations, so long as the other input is text or
-- an unknown literal.  So these work:

-- pgrust:rowsort
select 'four: '::text || 2+2;
-- pgrust:rowsort
select 'four: ' || 2+2;

-- but not this:

select 3 || 4.0;

/*
 * various string functions
 */
select concat('one');
-- pgrust:rowsort
select concat(1,2,3,'hello',true, false, to_date('20100309','YYYYMMDD'));
-- pgrust:rowsort
select concat_ws('#','one');
-- pgrust:rowsort
select concat_ws('#',1,2,3,'hello',true, false, to_date('20100309','YYYYMMDD'));
-- pgrust:rowsort
select concat_ws(',',10,20,null,30);
-- pgrust:rowsort
select concat_ws('',10,20,null,30);
-- pgrust:rowsort
select concat_ws(NULL,10,20,null,30) is null;
-- pgrust:rowsort
select reverse('abcde');
select i, left('ahoj', i), right('ahoj', i) from generate_series(-5, 5) t(i) order by i;
-- pgrust:rowsort
select quote_literal('');
-- pgrust:rowsort
select quote_literal('abc''');
-- pgrust:rowsort
select quote_literal(e'\\');
-- check variadic labeled argument
-- pgrust:rowsort
select concat(variadic array[1,2,3]);
-- pgrust:rowsort
select concat_ws(',', variadic array[1,2,3]);
-- pgrust:rowsort
select concat_ws(',', variadic NULL::int[]);
-- pgrust:rowsort
select concat(variadic NULL::int[]) is NULL;
-- pgrust:rowsort
select concat(variadic '{}'::int[]) = '';
--should fail
select concat_ws(',', variadic 10);

/*
 * format
 */
select format(NULL);
-- pgrust:rowsort
select format('Hello');
-- pgrust:rowsort
select format('Hello %s', 'World');
-- pgrust:rowsort
select format('Hello %%');
-- pgrust:rowsort
select format('Hello %%%%');
-- should fail
select format('Hello %s %s', 'World');
select format('Hello %s');
select format('Hello %x', 20);
-- check literal and sql identifiers
-- pgrust:rowsort
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, 'Hello');
-- pgrust:rowsort
select format('%s%s%s','Hello', NULL,'World');
-- pgrust:rowsort
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, NULL);
-- pgrust:rowsort
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', NULL, 'Hello');
-- should fail, sql identifier cannot be NULL
select format('INSERT INTO %I VALUES(%L,%L)', NULL, 10, 'Hello');
-- check positional placeholders
-- pgrust:rowsort
select format('%1$s %3$s', 1, 2, 3);
-- pgrust:rowsort
select format('%1$s %12$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
-- should fail
select format('%1$s %4$s', 1, 2, 3);
select format('%1$s %13$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
select format('%0$s', 'Hello');
select format('%*0$s', 'Hello');
select format('%1$', 1);
select format('%1$1', 1);
-- check mix of positional and ordered placeholders
-- pgrust:rowsort
select format('Hello %s %1$s %s', 'World', 'Hello again');
-- pgrust:rowsort
select format('Hello %s %s, %2$s %2$s', 'World', 'Hello again');
-- check variadic labeled arguments
-- pgrust:rowsort
select format('%s, %s', variadic array['Hello','World']);
-- pgrust:rowsort
select format('%s, %s', variadic array[1, 2]);
-- pgrust:rowsort
select format('%s, %s', variadic array[true, false]);
-- pgrust:rowsort
select format('%s, %s', variadic array[true, false]::text[]);
-- check variadic with positional placeholders
-- pgrust:rowsort
select format('%2$s, %1$s', variadic array['first', 'second']);
-- pgrust:rowsort
select format('%2$s, %1$s', variadic array[1, 2]);
-- variadic argument can be array type NULL, but should not be referenced
-- pgrust:rowsort
select format('Hello', variadic NULL::int[]);
-- variadic argument allows simulating more than FUNC_MAX_ARGS parameters
-- pgrust:rowsort
select format(string_agg('%s',','), variadic array_agg(i))
from generate_series(1,200) g(i);
-- check field widths and left, right alignment
-- pgrust:rowsort
select format('>>%10s<<', 'Hello');
-- pgrust:rowsort
select format('>>%10s<<', NULL);
-- pgrust:rowsort
select format('>>%10s<<', '');
-- pgrust:rowsort
select format('>>%-10s<<', '');
-- pgrust:rowsort
select format('>>%-10s<<', 'Hello');
-- pgrust:rowsort
select format('>>%-10s<<', NULL);
-- pgrust:rowsort
select format('>>%1$10s<<', 'Hello');
-- pgrust:rowsort
select format('>>%1$-10I<<', 'Hello');
-- pgrust:rowsort
select format('>>%2$*1$L<<', 10, 'Hello');
-- pgrust:rowsort
select format('>>%2$*1$L<<', 10, NULL);
-- pgrust:rowsort
select format('>>%2$*1$L<<', -10, NULL);
-- pgrust:rowsort
select format('>>%*s<<', 10, 'Hello');
-- pgrust:rowsort
select format('>>%*1$s<<', 10, 'Hello');
-- pgrust:rowsort
select format('>>%-s<<', 'Hello');
-- pgrust:rowsort
select format('>>%10L<<', NULL);
-- pgrust:rowsort
select format('>>%2$*1$L<<', NULL, 'Hello');
-- pgrust:rowsort
select format('>>%2$*1$L<<', 0, 'Hello');
