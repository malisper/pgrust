-- Tests for multirange data types.

--
-- test input parser
--

-- negative tests; should fail
select ''::textmultirange;
select '{,}'::textmultirange;
select '{(,)}.'::textmultirange;
select '{[a,c),}'::textmultirange;
select '{,[a,c)}'::textmultirange;
select '{-[a,z)}'::textmultirange;
select '{[a,z) - }'::textmultirange;
select '{(",a)}'::textmultirange;
select '{(,,a)}'::textmultirange;
select '{(),a)}'::textmultirange;
select '{(a,))}'::textmultirange;
select '{(],a)}'::textmultirange;
select '{(a,])}'::textmultirange;
select '{[z,a]}'::textmultirange;

-- should succeed
-- pgrust:rowsort
select '{}'::textmultirange;
-- pgrust:rowsort
select '  {}  '::textmultirange;
-- pgrust:rowsort
select ' { empty, empty }  '::textmultirange;
-- pgrust:rowsort
select ' {( " a " " a ", " z " " z " )  }'::textmultirange;
-- pgrust:rowsort
select textrange('\\\\', repeat('a', 200))::textmultirange;
-- pgrust:rowsort
select '{(,z)}'::textmultirange;
-- pgrust:rowsort
select '{(a,)}'::textmultirange;
-- pgrust:rowsort
select '{[,z]}'::textmultirange;
-- pgrust:rowsort
select '{[a,]}'::textmultirange;
-- pgrust:rowsort
select '{(,)}'::textmultirange;
-- pgrust:rowsort
select '{[ , ]}'::textmultirange;
-- pgrust:rowsort
select '{["",""]}'::textmultirange;
-- pgrust:rowsort
select '{[",",","]}'::textmultirange;
-- pgrust:rowsort
select '{["\\","\\"]}'::textmultirange;
-- pgrust:rowsort
select '{["""","\""]}'::textmultirange;
-- pgrust:rowsort
select '{(\\,a)}'::textmultirange;
-- pgrust:rowsort
select '{((,z)}'::textmultirange;
-- pgrust:rowsort
select '{([,z)}'::textmultirange;
-- pgrust:rowsort
select '{(!,()}'::textmultirange;
-- pgrust:rowsort
select '{(!,[)}'::textmultirange;
-- pgrust:rowsort
select '{[a,a]}'::textmultirange;
-- pgrust:rowsort
select '{[a,a],[a,b]}'::textmultirange;
-- pgrust:rowsort
select '{[a,b), [b,e]}'::textmultirange;
-- pgrust:rowsort
select '{[a,d), [b,f]}'::textmultirange;
-- pgrust:rowsort
select '{[a,a],[b,b]}'::textmultirange;
-- without canonicalization, we can't join these:
-- pgrust:rowsort
select '{[a,a], [b,b]}'::textmultirange;
-- with canonicalization, we can join these:
-- pgrust:rowsort
select '{[1,2], [3,4]}'::int4multirange;
-- pgrust:rowsort
select '{[a,a], [b,b], [c,c]}'::textmultirange;
-- pgrust:rowsort
select '{[a,d], [b,e]}'::textmultirange;
-- pgrust:rowsort
select '{[a,d), [d,e)}'::textmultirange;
-- these are allowed but normalize to empty:
-- pgrust:rowsort
select '{[a,a)}'::textmultirange;
-- pgrust:rowsort
select '{(a,a]}'::textmultirange;
-- pgrust:rowsort
select '{(a,a)}'::textmultirange;

-- Also try it with non-error-throwing API
select pg_input_is_valid('{[1,2], [4,5]}', 'int4multirange');
select pg_input_is_valid('{[1,2], [4,5]', 'int4multirange');
select * from pg_input_error_info('{[1,2], [4,5]', 'int4multirange');
select pg_input_is_valid('{[1,2], [4,zed]}', 'int4multirange');
select * from pg_input_error_info('{[1,2], [4,zed]}', 'int4multirange');

--
-- test the constructor
---
-- pgrust:rowsort
select textmultirange();
-- pgrust:rowsort
select textmultirange(textrange('a', 'c'));
-- pgrust:rowsort
select textmultirange(textrange('a', 'c'), textrange('f', 'g'));
-- pgrust:rowsort
select textmultirange(textrange('\\\\', repeat('a', 200)), textrange('c', 'd'));

--
-- test casts, both a built-in range type and a user-defined one:
--
-- pgrust:rowsort
select 'empty'::int4range::int4multirange;
-- pgrust:rowsort
select int4range(1, 3)::int4multirange;
-- pgrust:rowsort
select int4range(1, null)::int4multirange;
-- pgrust:rowsort
select int4range(null, null)::int4multirange;
-- pgrust:rowsort
select 'empty'::textrange::textmultirange;
-- pgrust:rowsort
select textrange('a', 'c')::textmultirange;
-- pgrust:rowsort
select textrange('a', null)::textmultirange;
-- pgrust:rowsort
select textrange(null, null)::textmultirange;

--
-- test unnest(multirange) function
--
-- pgrust:rowsort
select unnest(int4multirange(int4range('5', '6'), int4range('1', '2')));
-- pgrust:rowsort
select unnest(textmultirange(textrange('a', 'b'), textrange('d', 'e')));
-- pgrust:rowsort
select unnest(textmultirange(textrange('\\\\', repeat('a', 200)), textrange('c', 'd')));

--
-- create some test data and test the operators
--

CREATE TABLE nummultirange_test (nmr NUMMULTIRANGE);
CREATE INDEX nummultirange_test_btree ON nummultirange_test(nmr);

INSERT INTO nummultirange_test VALUES('{}');
INSERT INTO nummultirange_test VALUES('{[,)}');
INSERT INTO nummultirange_test VALUES('{[3,]}');
INSERT INTO nummultirange_test VALUES('{[,), [3,]}');
INSERT INTO nummultirange_test VALUES('{[, 5)}');
INSERT INTO nummultirange_test VALUES(nummultirange());
INSERT INTO nummultirange_test VALUES(nummultirange(variadic '{}'::numrange[]));
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test VALUES('{empty}');
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.7, 1.7, '[]'), numrange(1.7, 1.9)));
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.7, 1.7, '[]'), numrange(1.9, 2.1)));

SELECT nmr, isempty(nmr), lower(nmr), upper(nmr) FROM nummultirange_test ORDER BY nmr;
SELECT nmr, lower_inc(nmr), lower_inf(nmr), upper_inc(nmr), upper_inf(nmr) FROM nummultirange_test ORDER BY nmr;

-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr = '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr = '{(,5)}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr = '{[3,)}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr = '{[1.7,1.7]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr = '{[1.7,1.7],[1.9,2.1)}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr < '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr < '{[-1000.0, -1000.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr < '{[0.0, 1.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr < '{[1000.0, 1001.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr <= '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr <= '{[3,)}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr >= '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr >= '{[3,)}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr > '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr > '{[-1000.0, -1000.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr > '{[0.0, 1.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr > '{[1000.0, 1001.0]}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr <> '{}';
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr <> '{(,5)}';

select nummultirange(numrange(2.0, 1.0));
-- pgrust:rowsort
select nummultirange(numrange(5.0, 6.0), numrange(1.0, 2.0));

analyze nummultirange_test;

-- overlaps
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE range_overlaps_multirange(numrange(4.0, 4.2), nmr);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE numrange(4.0, 4.2) && nmr;
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_overlaps_range(nmr, numrange(4.0, 4.2));
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr && numrange(4.0, 4.2);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_overlaps_multirange(nmr, nummultirange(numrange(4.0, 4.2), numrange(6.0, 7.0)));
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr && nummultirange(numrange(4.0, 4.2), numrange(6.0, 7.0));
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr && nummultirange(numrange(6.0, 7.0));
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr && nummultirange(numrange(6.0, 7.0), numrange(8.0, 9.0));

-- mr contains x
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_contains_elem(nmr, 4.0);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr @> 4.0;
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_contains_range(nmr, numrange(4.0, 4.2));
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr @> numrange(4.0, 4.2);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_contains_multirange(nmr, '{[4.0,4.2), [6.0, 8.0)}');
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE nmr @> '{[4.0,4.2), [6.0, 8.0)}'::nummultirange;

-- x is contained by mr
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE elem_contained_by_multirange(4.0, nmr);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE 4.0 <@ nmr;
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE range_contained_by_multirange(numrange(4.0, 4.2), nmr);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE numrange(4.0, 4.2) <@ nmr;
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE multirange_contained_by_multirange('{[4.0,4.2), [6.0, 8.0)}', nmr);
-- pgrust:rowsort
SELECT * FROM nummultirange_test WHERE '{[4.0,4.2), [6.0, 8.0)}'::nummultirange <@ nmr;

-- overlaps
-- pgrust:rowsort
SELECT 'empty'::numrange && nummultirange();
-- pgrust:rowsort
SELECT 'empty'::numrange && nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange() && 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) && 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange() && nummultirange();
-- pgrust:rowsort
SELECT nummultirange() && nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) && nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) && nummultirange(numrange(1,2), numrange(7,8));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(7,8)) && nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) && nummultirange(numrange(1,2), numrange(3.5,8));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(3.5,8)) && numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(3.5,8)) && nummultirange(numrange(3,4));
-- pgrust:rowsort
select '{(10,20),(30,40),(50,60)}'::nummultirange && '(42,92)'::numrange;

-- contains
-- pgrust:rowsort
SELECT nummultirange() @> nummultirange();
-- pgrust:rowsort
SELECT nummultirange() @> 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange(numrange(null,null)) @> numrange(1,2);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,null)) @> numrange(null,2);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,null)) @> numrange(2,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,5)) @> numrange(null,3);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,5)) @> numrange(null,8);
-- pgrust:rowsort
SELECT nummultirange(numrange(5,null)) @> numrange(8,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(5,null)) @> numrange(3,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5)) @> numrange(8,9);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5)) @> numrange(3,9);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5)) @> numrange(1,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5)) @> numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(-4,-2), numrange(1,5)) @> numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(8,9)) @> numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(8,9)) @> numrange(6,7);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(6,9)) @> numrange(6,7);
-- pgrust:rowsort
SELECT '{[1,5)}'::nummultirange @> '{[1,5)}';
-- pgrust:rowsort
SELECT '{[-4,-2), [1,5)}'::nummultirange @> '{[1,5)}';
-- pgrust:rowsort
SELECT '{[1,5), [8,9)}'::nummultirange @> '{[1,5)}';
-- pgrust:rowsort
SELECT '{[1,5), [8,9)}'::nummultirange @> '{[6,7)}';
-- pgrust:rowsort
SELECT '{[1,5), [6,9)}'::nummultirange @> '{[6,7)}';
-- pgrust:rowsort
select '{(10,20),(30,40),(50,60)}'::nummultirange @> '(52,56)'::numrange;
-- pgrust:rowsort
SELECT numrange(null,null) @> nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT numrange(null,null) @> nummultirange(numrange(null,2));
-- pgrust:rowsort
SELECT numrange(null,null) @> nummultirange(numrange(2,null));
-- pgrust:rowsort
SELECT numrange(null,5) @> nummultirange(numrange(null,3));
-- pgrust:rowsort
SELECT numrange(null,5) @> nummultirange(numrange(null,8));
-- pgrust:rowsort
SELECT numrange(5,null) @> nummultirange(numrange(8,null));
-- pgrust:rowsort
SELECT numrange(5,null) @> nummultirange(numrange(3,null));
-- pgrust:rowsort
SELECT numrange(1,5) @> nummultirange(numrange(8,9));
-- pgrust:rowsort
SELECT numrange(1,5) @> nummultirange(numrange(3,9));
-- pgrust:rowsort
SELECT numrange(1,5) @> nummultirange(numrange(1,4));
-- pgrust:rowsort
SELECT numrange(1,5) @> nummultirange(numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,9) @> nummultirange(numrange(-4,-2), numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,9) @> nummultirange(numrange(1,5), numrange(8,9));
-- pgrust:rowsort
SELECT numrange(1,9) @> nummultirange(numrange(1,5), numrange(6,9));
-- pgrust:rowsort
SELECT numrange(1,9) @> nummultirange(numrange(1,5), numrange(6,10));
-- pgrust:rowsort
SELECT '{[1,9)}' @> '{[1,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,9)}' @> '{[-4,-2), [1,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,9)}' @> '{[1,5), [8,9)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,9)}' @> '{[1,5), [6,9)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,9)}' @> '{[1,5), [6,10)}'::nummultirange;

-- is contained by
-- pgrust:rowsort
SELECT nummultirange() <@ nummultirange();
-- pgrust:rowsort
SELECT 'empty'::numrange <@ nummultirange();
-- pgrust:rowsort
SELECT numrange(1,2) <@ nummultirange(numrange(null,null));
-- pgrust:rowsort
SELECT numrange(null,2) <@ nummultirange(numrange(null,null));
-- pgrust:rowsort
SELECT numrange(2,null) <@ nummultirange(numrange(null,null));
-- pgrust:rowsort
SELECT numrange(null,3) <@ nummultirange(numrange(null,5));
-- pgrust:rowsort
SELECT numrange(null,8) <@ nummultirange(numrange(null,5));
-- pgrust:rowsort
SELECT numrange(8,null) <@ nummultirange(numrange(5,null));
-- pgrust:rowsort
SELECT numrange(3,null) <@ nummultirange(numrange(5,null));
-- pgrust:rowsort
SELECT numrange(8,9) <@ nummultirange(numrange(1,5));
-- pgrust:rowsort
SELECT numrange(3,9) <@ nummultirange(numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,4) <@ nummultirange(numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,5) <@ nummultirange(numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,5) <@ nummultirange(numrange(-4,-2), numrange(1,5));
-- pgrust:rowsort
SELECT numrange(1,5) <@ nummultirange(numrange(1,5), numrange(8,9));
-- pgrust:rowsort
SELECT numrange(6,7) <@ nummultirange(numrange(1,5), numrange(8,9));
-- pgrust:rowsort
SELECT numrange(6,7) <@ nummultirange(numrange(1,5), numrange(6,9));
-- pgrust:rowsort
SELECT '{[1,5)}' <@ '{[1,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,5)}' <@ '{[-4,-2), [1,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,5)}' <@ '{[1,5), [8,9)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[6,7)}' <@ '{[1,5), [8,9)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[6,7)}' <@ '{[1,5), [6,9)}'::nummultirange;
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) <@ numrange(null,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,2)) <@ numrange(null,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(2,null)) <@ numrange(null,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,3)) <@ numrange(null,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(null,8)) <@ numrange(null,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(8,null)) <@ numrange(5,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,null)) <@ numrange(5,null);
-- pgrust:rowsort
SELECT nummultirange(numrange(8,9)) <@ numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,9)) <@ numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) <@ numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5)) <@ numrange(1,5);
-- pgrust:rowsort
SELECT nummultirange(numrange(-4,-2), numrange(1,5)) <@ numrange(1,9);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(8,9)) <@ numrange(1,9);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(6,9)) <@ numrange(1,9);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,5), numrange(6,10)) <@ numrange(1,9);
-- pgrust:rowsort
SELECT '{[1,5)}'::nummultirange <@ '{[1,9)}';
-- pgrust:rowsort
SELECT '{[-4,-2), [1,5)}'::nummultirange <@ '{[1,9)}';
-- pgrust:rowsort
SELECT '{[1,5), [8,9)}'::nummultirange <@ '{[1,9)}';
-- pgrust:rowsort
SELECT '{[1,5), [6,9)}'::nummultirange <@ '{[1,9)}';
-- pgrust:rowsort
SELECT '{[1,5), [6,10)}'::nummultirange <@ '{[1,9)}';

-- overleft
-- pgrust:rowsort
SELECT 'empty'::numrange &< nummultirange();
-- pgrust:rowsort
SELECT 'empty'::numrange &< nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange() &< 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &< 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange() &< nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &< nummultirange();
-- pgrust:rowsort
SELECT nummultirange() &< nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT numrange(6,7) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT numrange(1,2) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT numrange(1,4) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT numrange(1,6) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT numrange(3.5,6) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(6,7)) &< numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &< numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) &< numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,6)) &< numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(3.5,6)) &< numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(6,7)) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,6)) &< nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(3.5,6)) &< nummultirange(numrange(3,4));

-- overright
-- pgrust:rowsort
SELECT nummultirange() &> 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &> 'empty'::numrange;
-- pgrust:rowsort
SELECT 'empty'::numrange &> nummultirange();
-- pgrust:rowsort
SELECT 'empty'::numrange &> nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange() &> nummultirange();
-- pgrust:rowsort
SELECT nummultirange() &> nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) &> nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> numrange(6,7);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> numrange(1,2);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> numrange(1,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> numrange(1,6);
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> numrange(3.5,6);
-- pgrust:rowsort
SELECT numrange(3,4) &> nummultirange(numrange(6,7));
-- pgrust:rowsort
SELECT numrange(3,4) &> nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT numrange(3,4) &> nummultirange(numrange(1,4));
-- pgrust:rowsort
SELECT numrange(3,4) &> nummultirange(numrange(1,6));
-- pgrust:rowsort
SELECT numrange(3,4) &> nummultirange(numrange(3.5,6));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> nummultirange(numrange(6,7));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> nummultirange(numrange(1,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> nummultirange(numrange(1,6));
-- pgrust:rowsort
SELECT nummultirange(numrange(3,4)) &> nummultirange(numrange(3.5,6));

-- meets
-- pgrust:rowsort
SELECT 'empty'::numrange -|- nummultirange();
-- pgrust:rowsort
SELECT 'empty'::numrange -|- nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange() -|- 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- 'empty'::numrange;
-- pgrust:rowsort
SELECT nummultirange() -|- nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- nummultirange();
-- pgrust:rowsort
SELECT nummultirange() -|- nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT numrange(1,2) -|- nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT numrange(1,2) -|- nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- numrange(2,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- numrange(3,4);
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(5,6)) -|- nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(5,6)) -|- nummultirange(numrange(6,7));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(5,6)) -|- nummultirange(numrange(8,9));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) -|- nummultirange(numrange(2,4), numrange(6,7));

-- strictly left
-- pgrust:rowsort
select 'empty'::numrange << nummultirange();
-- pgrust:rowsort
select numrange(1,2) << nummultirange();
-- pgrust:rowsort
select numrange(1,2) << nummultirange(numrange(3,4));
-- pgrust:rowsort
select numrange(1,2) << nummultirange(numrange(0,4));
-- pgrust:rowsort
select numrange(1,2) << nummultirange(numrange(0,4), numrange(7,8));
-- pgrust:rowsort
select nummultirange() << 'empty'::numrange;
-- pgrust:rowsort
select nummultirange() << numrange(1,2);
-- pgrust:rowsort
select nummultirange(numrange(3,4)) << numrange(3,6);
-- pgrust:rowsort
select nummultirange(numrange(0,2)) << numrange(3,6);
-- pgrust:rowsort
select nummultirange(numrange(0,2), numrange(7,8)) << numrange(3,6);
-- pgrust:rowsort
select nummultirange(numrange(-4,-2), numrange(0,2)) << numrange(3,6);
-- pgrust:rowsort
select nummultirange() << nummultirange();
-- pgrust:rowsort
select nummultirange() << nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(1,2)) << nummultirange();
-- pgrust:rowsort
select nummultirange(numrange(1,2)) << nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(1,2)) << nummultirange(numrange(3,4));
-- pgrust:rowsort
select nummultirange(numrange(1,2)) << nummultirange(numrange(3,4), numrange(7,8));
-- pgrust:rowsort
select nummultirange(numrange(1,2), numrange(4,5)) << nummultirange(numrange(3,4), numrange(7,8));

-- strictly right
-- pgrust:rowsort
select nummultirange() >> 'empty'::numrange;
-- pgrust:rowsort
select nummultirange() >> numrange(1,2);
-- pgrust:rowsort
select nummultirange(numrange(3,4)) >> numrange(1,2);
-- pgrust:rowsort
select nummultirange(numrange(0,4)) >> numrange(1,2);
-- pgrust:rowsort
select nummultirange(numrange(0,4), numrange(7,8)) >> numrange(1,2);
-- pgrust:rowsort
select 'empty'::numrange >> nummultirange();
-- pgrust:rowsort
select numrange(1,2) >> nummultirange();
-- pgrust:rowsort
select numrange(3,6) >> nummultirange(numrange(3,4));
-- pgrust:rowsort
select numrange(3,6) >> nummultirange(numrange(0,2));
-- pgrust:rowsort
select numrange(3,6) >> nummultirange(numrange(0,2), numrange(7,8));
-- pgrust:rowsort
select numrange(3,6) >> nummultirange(numrange(-4,-2), numrange(0,2));
-- pgrust:rowsort
select nummultirange() >> nummultirange();
-- pgrust:rowsort
select nummultirange(numrange(1,2)) >> nummultirange();
-- pgrust:rowsort
select nummultirange() >> nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(1,2)) >> nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(3,4)) >> nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(3,4), numrange(7,8)) >> nummultirange(numrange(1,2));
-- pgrust:rowsort
select nummultirange(numrange(3,4), numrange(7,8)) >> nummultirange(numrange(1,2), numrange(4,5));

-- union
-- pgrust:rowsort
SELECT nummultirange() + nummultirange();
-- pgrust:rowsort
SELECT nummultirange() + nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) + nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) + nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) + nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) + nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) + nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) + nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) + nummultirange(numrange(0,9));

-- merge
-- pgrust:rowsort
SELECT range_merge(nummultirange());
-- pgrust:rowsort
SELECT range_merge(nummultirange(numrange(1,2)));
-- pgrust:rowsort
SELECT range_merge(nummultirange(numrange(1,2), numrange(7,8)));

-- minus
-- pgrust:rowsort
SELECT nummultirange() - nummultirange();
-- pgrust:rowsort
SELECT nummultirange() - nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) - nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(3,4)) - nummultirange();
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) - nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) - nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) - nummultirange(numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) - nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) - nummultirange(numrange(2,3));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) - nummultirange(numrange(0,8));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,4)) - nummultirange(numrange(0,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,8)) - nummultirange(numrange(0,2), numrange(3,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,8)) - nummultirange(numrange(2,3), numrange(5,null));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(-2,0));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(2,4));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(3,5));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(0,9));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,3), numrange(4,5)) - nummultirange(numrange(2,9));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(8,9));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2), numrange(4,5)) - nummultirange(numrange(-2,0), numrange(8,9));

-- intersection
-- pgrust:rowsort
SELECT nummultirange() * nummultirange();
-- pgrust:rowsort
SELECT nummultirange() * nummultirange(numrange(1,2));
-- pgrust:rowsort
SELECT nummultirange(numrange(1,2)) * nummultirange();
-- pgrust:rowsort
SELECT '{[1,3)}'::nummultirange * '{[1,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,3)}'::nummultirange * '{[0,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,3)}'::nummultirange * '{[0,2)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,3)}'::nummultirange * '{[2,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4)}'::nummultirange * '{[2,3)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4)}'::nummultirange * '{[0,2), [3,5)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4), [7,10)}'::nummultirange * '{[0,8), [9,12)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4), [7,10)}'::nummultirange * '{[9,12)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4), [7,10)}'::nummultirange * '{[-5,-4), [5,6), [9,12)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4), [7,10)}'::nummultirange * '{[0,2), [3,8), [9,12)}'::nummultirange;
-- pgrust:rowsort
SELECT '{[1,4), [7,10)}'::nummultirange * '{[0,2), [3,8), [9,12)}'::nummultirange;

-- test GiST index
create table test_multirange_gist(mr int4multirange);
insert into test_multirange_gist select int4multirange(int4range(g, g+10),int4range(g+20, g+30),int4range(g+40, g+50)) from generate_series(1,2000) g;
insert into test_multirange_gist select '{}'::int4multirange from generate_series(1,500) g;
insert into test_multirange_gist select int4multirange(int4range(g, g+10000)) from generate_series(1,1000) g;
insert into test_multirange_gist select int4multirange(int4range(NULL, g*10, '(]'), int4range(g*10, g*20, '(]')) from generate_series(1,100) g;
insert into test_multirange_gist select int4multirange(int4range(g*10, g*20, '(]'), int4range(g*20, NULL, '(]')) from generate_series(1,100) g;
create index test_mulrirange_gist_idx on test_multirange_gist using gist (mr);

-- test statistics and selectivity estimation as well
--
-- We don't check the accuracy of selectivity estimation, but at least check
-- it doesn't fall.
analyze test_multirange_gist;

-- first, verify non-indexed results
SET enable_seqscan    = t;
SET enable_indexscan  = f;
SET enable_bitmapscan = f;

-- pgrust:rowsort
select count(*) from test_multirange_gist where mr = '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- '{}'::int4multirange;

-- pgrust:rowsort
select count(*) from test_multirange_gist where mr = int4multirange(int4range(10,20), int4range(30,40), int4range(50,60));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> 10;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> int4range(10,20);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && int4range(10,20);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ int4range(10,50);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> int4multirange(int4range(10,20), int4range(30,40));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && '{(10,20),(30,40),(50,60)}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ '{(10,30),(40,60),(70,90)}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- int4multirange(int4range(100,200), int4range(400,500));

-- now check same queries using index
SET enable_seqscan    = f;
SET enable_indexscan  = t;
SET enable_bitmapscan = f;

-- pgrust:rowsort
select count(*) from test_multirange_gist where mr = '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- '{}'::int4multirange;

-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> 'empty'::int4range;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr = int4multirange(int4range(10,20), int4range(30,40), int4range(50,60));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> 10;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> int4range(10,20);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && int4range(10,20);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ int4range(10,50);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- int4range(100,500);
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> '{}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr @> int4multirange(int4range(10,20), int4range(30,40));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr && '{(10,20),(30,40),(50,60)}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr <@ '{(10,30),(40,60),(70,90)}'::int4multirange;
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr << int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr >> int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &< int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr &> int4multirange(int4range(100,200), int4range(400,500));
-- pgrust:rowsort
select count(*) from test_multirange_gist where mr -|- int4multirange(int4range(100,200), int4range(400,500));

drop table test_multirange_gist;

--
-- range_agg function
--
create table reservations ( room_id integer not null, booked_during daterange );
insert into reservations values
-- 1: has a meets and a gap
(1, daterange('2018-07-01', '2018-07-07')),
(1, daterange('2018-07-07', '2018-07-14')),
(1, daterange('2018-07-20', '2018-07-22')),
-- 2: just a single row
(2, daterange('2018-07-01', '2018-07-03')),
-- 3: one null range
(3, NULL),
-- 4: two null ranges
(4, NULL),
(4, NULL),
-- 5: a null range and a non-null range
(5, NULL),
(5, daterange('2018-07-01', '2018-07-03')),
-- 6: has overlap
(6, daterange('2018-07-01', '2018-07-07')),
(6, daterange('2018-07-05', '2018-07-10')),
-- 7: two ranges that meet: no gap or overlap
(7, daterange('2018-07-01', '2018-07-07')),
(7, daterange('2018-07-07', '2018-07-14')),
-- 8: an empty range
(8, 'empty'::daterange)
;
SELECT   room_id, range_agg(booked_during)
FROM     reservations
GROUP BY room_id
ORDER BY room_id;

-- range_agg on a custom range type too
-- pgrust:rowsort
SELECT  range_agg(r)
FROM    (VALUES
          ('[a,c]'::textrange),
          ('[b,b]'::textrange),
          ('[c,f]'::textrange),
          ('[g,h)'::textrange),
          ('[h,j)'::textrange)
        ) t(r);

-- range_agg with multirange inputs
-- pgrust:rowsort
select range_agg(nmr) from nummultirange_test;
-- pgrust:rowsort
select range_agg(nmr) from nummultirange_test where false;
-- pgrust:rowsort
select range_agg(null::nummultirange) from nummultirange_test;
-- pgrust:rowsort
select range_agg(nmr) from (values ('{}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{}'::nummultirange), ('{}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{[1,2]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{[1,2], [5,6]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{[1,2], [2,3]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{[1,2]}'::nummultirange), ('{[5,6]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_agg(nmr) from (values ('{[1,2]}'::nummultirange), ('{[2,3]}'::nummultirange)) t(nmr);

--
-- range_intersect_agg function
--
-- pgrust:rowsort
select range_intersect_agg(nmr) from nummultirange_test;
-- pgrust:rowsort
select range_intersect_agg(nmr) from nummultirange_test where false;
-- pgrust:rowsort
select range_intersect_agg(null::nummultirange) from nummultirange_test;
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{[1,3]}'::nummultirange), ('{[6,12]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{[1,6]}'::nummultirange), ('{[3,12]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{[1,6], [10,12]}'::nummultirange), ('{[4,14]}'::nummultirange)) t(nmr);
-- test with just one input:
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{[1,2]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_intersect_agg(nmr) from (values ('{[1,6], [10,12]}'::nummultirange)) t(nmr);
-- pgrust:rowsort
select range_intersect_agg(nmr) from nummultirange_test where nmr @> 4.0;

create table nummultirange_test2(nmr nummultirange);
create index nummultirange_test2_hash_idx on nummultirange_test2 using hash (nmr);

INSERT INTO nummultirange_test2 VALUES('{[, 5)}');
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2,'()')));
INSERT INTO nummultirange_test2 VALUES('{}');

-- pgrust:rowsort
select * from nummultirange_test2 where nmr = '{}';
-- pgrust:rowsort
select * from nummultirange_test2 where nmr = nummultirange(numrange(1.1, 2.2));
-- pgrust:rowsort
select * from nummultirange_test2 where nmr = nummultirange(numrange(1.1, 2.3));

set enable_nestloop=t;
set enable_hashjoin=f;
set enable_mergejoin=f;
select * from nummultirange_test natural join nummultirange_test2 order by nmr;
set enable_nestloop=f;
set enable_hashjoin=t;
set enable_mergejoin=f;
select * from nummultirange_test natural join nummultirange_test2 order by nmr;
set enable_nestloop=f;
set enable_hashjoin=f;
set enable_mergejoin=t;
select * from nummultirange_test natural join nummultirange_test2 order by nmr;

set enable_nestloop to default;
set enable_hashjoin to default;
set enable_mergejoin to default;

DROP TABLE nummultirange_test2;

--
-- Test user-defined multirange of floats
--

-- pgrust:rowsort
select '{[123.001, 5.e9)}'::float8multirange @> 888.882::float8;
create table float8multirange_test(f8mr float8multirange, i int);
insert into float8multirange_test values(float8multirange(float8range(-100.00007, '1.111113e9')), 42);
-- pgrust:rowsort
select * from float8multirange_test;
drop table float8multirange_test;

--
-- Test multirange types over domains
--

create domain mydomain as int4;
create type mydomainrange as range(subtype=mydomain);
-- pgrust:rowsort
select '{[4,50)}'::mydomainmultirange @> 7::mydomain;
drop domain mydomain cascade;

--
-- Test domains over multirange types
--

create domain restrictedmultirange as int4multirange check (upper(value) < 10);
-- pgrust:rowsort
select '{[4,5)}'::restrictedmultirange @> 7;
select '{[4,50)}'::restrictedmultirange @> 7; -- should fail
drop domain restrictedmultirange;

---
-- Check automatic naming of multiranges
---

create type intr as range(subtype=int);
-- pgrust:rowsort
select intr_multirange(intr(1,10));
drop type intr;
create type intmultirange as (x int, y int);
create type intrange as range(subtype=int); -- should fail
drop type intmultirange;
create type intr_multirange as (x int, y int);
create type intr as range(subtype=int); -- should fail
drop type intr_multirange;

--
-- Test multiple multirange types over the same subtype and manual naming of
-- the multirange type.
--

-- should fail
create type textrange1 as range(subtype=text, multirange_type_name=int, collation="C");
-- should pass
create type textrange1 as range(subtype=text, multirange_type_name=multirange_of_text, collation="C");
-- should pass, because existing _textrange1 is automatically renamed
create type textrange2 as range(subtype=text, multirange_type_name=_textrange1, collation="C");

select multirange_of_text(textrange2('a','Z'));  -- should fail
select multirange_of_text(textrange1('a','Z')) @> 'b'::text;
-- pgrust:rowsort
select unnest(multirange_of_text(textrange1('a','b'), textrange1('d','e')));
-- pgrust:rowsort
select _textrange1(textrange2('a','z')) @> 'b'::text;

drop type textrange1;
drop type textrange2;

--
-- Multiranges don't have their own ownership or permissions.
--
create type textrange1 as range(subtype=text, multirange_type_name=multitextrange1, collation="C");
create role regress_multirange_owner;

alter type multitextrange1 owner to regress_multirange_owner;  -- fail
alter type textrange1 owner to regress_multirange_owner;
set role regress_multirange_owner;
revoke usage on type multitextrange1 from public;  -- fail
revoke usage on type textrange1 from public;
\dT+ *textrange1*
create temp table test1(f1 multitextrange1[]);
revoke usage on type textrange1 from regress_multirange_owner;
create temp table test2(f1 multitextrange1[]);  -- fail

drop table test1;
drop type textrange1;
reset role;
drop role regress_multirange_owner;

--
-- Test polymorphic type system
--

create function anyarray_anymultirange_func(a anyarray, r anymultirange)
  returns anyelement as 'select $1[1] + lower($2);' language sql;

-- pgrust:rowsort
select anyarray_anymultirange_func(ARRAY[1,2], int4multirange(int4range(10,20)));

-- should fail
select anyarray_anymultirange_func(ARRAY[1,2], nummultirange(numrange(10,20)));

drop function anyarray_anymultirange_func(anyarray, anymultirange);

-- should fail
create function bogus_func(anyelement)
  returns anymultirange as 'select int4multirange(int4range(1,10))' language sql;

-- should fail
create function bogus_func(int)
  returns anymultirange as 'select int4multirange(int4range(1,10))' language sql;

create function range_add_bounds(anymultirange)
  returns anyelement as 'select lower($1) + upper($1)' language sql;

-- pgrust:rowsort
select range_add_bounds(int4multirange(int4range(1, 17)));
-- pgrust:rowsort
select range_add_bounds(nummultirange(numrange(1.0001, 123.123)));

create function multirangetypes_sql(q anymultirange, b anyarray, out c anyelement)
  as $$ select upper($1) + $2[1] $$
  language sql;

-- pgrust:rowsort
select multirangetypes_sql(int4multirange(int4range(1,10)), ARRAY[2,20]);
select multirangetypes_sql(nummultirange(numrange(1,10)), ARRAY[2,20]);  -- match failure

create function anycompatiblearray_anycompatiblemultirange_func(a anycompatiblearray, mr anycompatiblemultirange)
  returns anycompatible as 'select $1[1] + lower($2);' language sql;

-- pgrust:rowsort
select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1,2], multirange(int4range(10,20)));

-- pgrust:rowsort
select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1,2], multirange(numrange(10,20)));

-- should fail
select anycompatiblearray_anycompatiblemultirange_func(ARRAY[1.1,2], multirange(int4range(10,20)));

drop function anycompatiblearray_anycompatiblemultirange_func(anycompatiblearray, anycompatiblemultirange);

create function anycompatiblerange_anycompatiblemultirange_func(r anycompatiblerange, mr anycompatiblemultirange)
  returns anycompatible as 'select lower($1) + lower($2);' language sql;

-- pgrust:rowsort
select anycompatiblerange_anycompatiblemultirange_func(int4range(1,2), multirange(int4range(10,20)));

-- should fail
select anycompatiblerange_anycompatiblemultirange_func(numrange(1,2), multirange(int4range(10,20)));

drop function anycompatiblerange_anycompatiblemultirange_func(anycompatiblerange, anycompatiblemultirange);

-- should fail
create function bogus_func(anycompatible)
  returns anycompatiblerange as 'select int4range(1,10)' language sql;

--
-- Arrays of multiranges
--

-- pgrust:rowsort
select ARRAY[nummultirange(numrange(1.1, 1.2)), nummultirange(numrange(12.3, 155.5))];

create table i8mr_array (f1 int, f2 int8multirange[]);
insert into i8mr_array values (42, array[int8multirange(int8range(1,10)), int8multirange(int8range(2,20))]);
-- pgrust:rowsort
select * from i8mr_array;
drop table i8mr_array;

--
-- Multiranges of arrays
--

-- pgrust:rowsort
select arraymultirange(arrayrange(ARRAY[1,2], ARRAY[2,1]));
select arraymultirange(arrayrange(ARRAY[2,1], ARRAY[1,2]));  -- fail

-- pgrust:rowsort
select array[1,1] <@ arraymultirange(arrayrange(array[1,2], array[2,1]));
-- pgrust:rowsort
select array[1,3] <@ arraymultirange(arrayrange(array[1,2], array[2,1]));

--
-- Ranges of composites
--

create type two_ints as (a int, b int);
create type two_ints_range as range (subtype = two_ints);

-- with debug_parallel_query on, this exercises tqueue.c's range remapping
-- pgrust:rowsort
select *, row_to_json(upper(t)) as u from
  (values (two_ints_multirange(two_ints_range(row(1,2), row(3,4)))),
          (two_ints_multirange(two_ints_range(row(5,6), row(7,8))))) v(t);

drop type two_ints cascade;

--
-- Check behavior when subtype lacks a hash function
--

set enable_sort = off;  -- try to make it pick a hash setop implementation

-- pgrust:rowsort
select '{(01,10)}'::varbitmultirange except select '{(10,11)}'::varbitmultirange;

reset enable_sort;

--
-- OUT/INOUT/TABLE functions
--

-- infer anymultirange from anymultirange
create function mr_outparam_succeed(i anymultirange, out r anymultirange, out t text)
  as $$ select $1, 'foo'::text $$ language sql;

-- pgrust:rowsort
select * from mr_outparam_succeed(int4multirange(int4range(1,2)));

-- infer anyarray from anymultirange
create function mr_outparam_succeed2(i anymultirange, out r anyarray, out t text)
  as $$ select ARRAY[upper($1)], 'foo'::text $$ language sql;

-- pgrust:rowsort
select * from mr_outparam_succeed2(int4multirange(int4range(1,2)));

-- infer anyrange from anymultirange
create function mr_outparam_succeed3(i anymultirange, out r anyrange, out t text)
  as $$ select range_merge($1), 'foo'::text $$ language sql;
-- pgrust:rowsort
select * from mr_outparam_succeed3(int4multirange(int4range(1,2)));

-- infer anymultirange from anyrange
create function mr_outparam_succeed4(i anyrange, out r anymultirange, out t text)
  as $$ select multirange($1), 'foo'::text $$ language sql;

-- pgrust:rowsort
select * from mr_outparam_succeed4(int4range(1,2));

-- infer anyelement from anymultirange
create function mr_inoutparam_succeed(out i anyelement, inout r anymultirange)
  as $$ select upper($1), $1 $$ language sql;

-- pgrust:rowsort
select * from mr_inoutparam_succeed(int4multirange(int4range(1,2)));

-- infer anyelement+anymultirange from anyelement+anymultirange
create function mr_table_succeed(i anyelement, r anymultirange) returns table(i anyelement, r anymultirange)
  as $$ select $1, $2 $$ language sql;

-- pgrust:rowsort
select * from mr_table_succeed(123, int4multirange(int4range(1,11)));

-- use anymultirange in plpgsql
create function mr_polymorphic(i anyrange) returns anymultirange
  as $$ begin return multirange($1); end; $$ language plpgsql;
-- pgrust:rowsort
select mr_polymorphic(int4range(1, 4));

-- should fail
create function mr_outparam_fail(i anyelement, out r anymultirange, out t text)
  as $$ select '[1,10]', 'foo' $$ language sql;

--should fail
create function mr_inoutparam_fail(inout i anyelement, out r anymultirange)
  as $$ select $1, '[1,10]' $$ language sql;

--should fail
create function mr_table_fail(i anyelement) returns table(i anyelement, r anymultirange)
  as $$ select $1, '[1,10]' $$ language sql;
