-- Array fmgr family matrix vs C: dims/comparisons/hash/contains/mutators/io,
-- byte-compared incl. error text. Volatile fns (shuffle/sample) are asserted
-- by shape only.
SELECT array_ndims('{}'::int[]), array_ndims('{1,2}'::int[]), array_ndims('{{1,2},{3,4}}'::int[]);
SELECT array_dims('{}'::int[]), array_dims('{1,2,3}'::int[]), array_dims('{{1,2},{3,4}}'::int[]);
SELECT array_dims('[2:4]={1,2,3}'::int[]);
SELECT array_lower('{1,2,3}'::int[], 1), array_upper('{1,2,3}'::int[], 1);
SELECT array_lower('[2:4]={1,2,3}'::int[], 1), array_upper('[2:4]={1,2,3}'::int[], 1);
SELECT array_lower('{1,2,3}'::int[], 0), array_upper('{1,2,3}'::int[], 2);
SELECT cardinality('{}'::int[]), cardinality('{1,2,3}'::int[]), cardinality('{{1,2},{3,4},{5,6}}'::int[]);
SELECT array_length('{1,2,3}'::int[], 1), array_length('{1,2,3}'::int[], 2);

SELECT '{1,2,3}'::int[] = '{1,2,3}'::int[], '{1,2,3}'::int[] = '{1,2,4}'::int[];
SELECT '{1,2,3}'::int[] <> '{1,2,3}'::int[];
SELECT '{1,2,NULL}'::int[] = '{1,2,NULL}'::int[], '{1,2,NULL}'::int[] = '{1,2,3}'::int[];
SELECT '[2:4]={1,2,3}'::int[] = '{1,2,3}'::int[];
SELECT '{{1,2},{3,4}}'::int[] = '{1,2,3,4}'::int[];
SELECT '{1,2,3}'::int[] < '{1,2,4}'::int[], '{1,2,3}'::int[] < '{1,2}'::int[];
SELECT '{1,2,3}'::int[] > '{1,2}'::int[], '{1,2}'::int[] <= '{1,2}'::int[], '{3}'::int[] >= '{2,9}'::int[];
SELECT '{NULL}'::int[] < '{1}'::int[], '{NULL}'::int[] > '{1}'::int[];
SELECT btarraycmp('{1,2}'::int[], '{1,3}'::int[]), btarraycmp('{a,b}'::text[], '{a,b}'::text[]), btarraycmp('{2}'::int[], '{1,1}'::int[]);
SELECT '{a,b}'::text[] < '{a,c}'::text[], '{A}'::text[] = '{a}'::text[];
SELECT hash_array('{1,2,3}'::int[]), hash_array('{}'::int[]), hash_array('{NULL,1}'::int[]);
SELECT hash_array_extended('{1,2,3}'::int[], 0), hash_array_extended('{1,2,3}'::int[], 1), hash_array_extended('{NULL}'::int[], 42);

SELECT '{1,2,3}'::int[] && '{3,4}'::int[], '{1,2}'::int[] && '{3,4}'::int[], '{}'::int[] && '{1}'::int[];
SELECT '{1,2,3}'::int[] @> '{2,3}'::int[], '{1,2,3}'::int[] @> '{4}'::int[], '{1,2,3}'::int[] @> '{}'::int[];
SELECT '{2,3}'::int[] <@ '{1,2,3}'::int[], '{NULL}'::int[] <@ '{1,2,3}'::int[];
SELECT '{{1,2},{3,4}}'::int[] @> '{1,4}'::int[];

SELECT array_append(ARRAY[1,2], 3), array_append(ARRAY[1,2], NULL), array_append(NULL::int[], 1);
SELECT array_append('[2:3]={1,2}'::int[], 9);
SELECT array_prepend(0, ARRAY[1,2]), array_prepend(NULL, ARRAY[1,2]), array_prepend(1, NULL::int[]);
SELECT array_prepend(9, '[2:3]={1,2}'::int[]);
SELECT array_append('{{1,2},{3,4}}'::int[], 5);
SELECT array_cat(ARRAY[1,2], ARRAY[3,4]), array_cat(ARRAY[1,2], NULL), array_cat(NULL, ARRAY[3,4]);
SELECT array_cat('{}'::int[], ARRAY[1,2]), array_cat(ARRAY[1,2], '{}'::int[]);
SELECT array_cat('{{1,2},{3,4}}'::int[], '{{5,6}}'::int[]);
SELECT array_cat('{{1,2},{3,4}}'::int[], '{5,6}'::int[]);
SELECT array_cat('{5,6}'::int[], '{{1,2},{3,4}}'::int[]);
SELECT array_cat('{{1,2},{3,4}}'::int[], '{{5,6,7}}'::int[]);
SELECT array_cat('{{1,2}}'::int[], '{{{1},{2}}}'::int[]);
SELECT array_cat('[2:3]={1,2}'::int[], '{3}'::int[]);

SELECT array_larger('{1,2}'::int[], '{1,3}'::int[]), array_smaller('{1,2}'::int[], '{1,3}'::int[]);
SELECT array_larger('{a,b}'::text[], '{a}'::text[]);

SELECT array_position(ARRAY[1,2,3,2], 2), array_position(ARRAY[1,2,3,2], 2, 3), array_position(ARRAY[1,2,3], 9);
SELECT array_position(ARRAY[1,NULL,3], NULL), array_position(ARRAY['a','b'], 'b');
SELECT array_position('[2:4]={1,2,3}'::int[], 3);
SELECT array_position('{{1,2},{3,4}}'::int[], 3);
SELECT array_positions(ARRAY[1,2,3,2], 2), array_positions(ARRAY[1,2,3], 9), array_positions(NULL::int[], 1);
SELECT array_positions(ARRAY[NULL,1,NULL], NULL);

SELECT array_remove(ARRAY[1,2,3,2], 2), array_remove(ARRAY[1,NULL,3], NULL), array_remove(ARRAY[1,1], 1);
SELECT array_remove('{{1,2},{3,4}}'::int[], 1);
SELECT array_replace(ARRAY[1,2,3,2], 2, 9), array_replace(ARRAY[1,NULL,3], NULL, 0), array_replace(ARRAY[1,2], 5, 6);
SELECT array_replace('{{1,2},{3,4}}'::int[], 3, 9);

SELECT array_fill(7, ARRAY[3]), array_fill(NULL::int, ARRAY[2]), array_fill('x'::text, ARRAY[2,2]);
SELECT array_fill(1, ARRAY[3], ARRAY[2]);
SELECT array_fill(1, ARRAY[0]), array_fill(1, '{}'::int[]);
SELECT array_fill(1, ARRAY[3], ARRAY[1,2]);
SELECT array_fill(1, ARRAY[-1]);
SELECT array_fill(1, ARRAY[NULL::int]);

SELECT generate_subscripts('{10,20,30}'::int[], 1) AS s;
SELECT generate_subscripts('{10,20,30}'::int[], 1, true) AS s;
SELECT generate_subscripts('[3:5]={10,20,30}'::int[], 1) AS s;
SELECT generate_subscripts('{{1,2},{3,4}}'::int[], 2) AS s;
SELECT generate_subscripts('{}'::int[], 1) AS s;
SELECT generate_subscripts('{1}'::int[], 2) AS s;

SELECT string_to_array('a,b,c', ','), string_to_array('', ','), string_to_array('abc', NULL);
SELECT string_to_array('a,b,c', ''), string_to_array('a,,c', ','), string_to_array('a,b,c', ',', 'b');
SELECT string_to_array(NULL, ','), string_to_array('a,b', ',', NULL);
SELECT array_to_string(ARRAY[1,2,3], ','), array_to_string(ARRAY[1,NULL,3], ','), array_to_string(ARRAY[1,NULL,3], ',', '*');
SELECT array_to_string('{}'::int[], ','), array_to_string('{{1,2},{3,4}}'::int[], ',');
SELECT array_to_string(ARRAY[1,2], NULL);

SELECT trim_array(ARRAY[1,2,3,4], 2), trim_array(ARRAY[1,2], 0), trim_array('{{1,2},{3,4}}'::int[], 1);
SELECT trim_array(ARRAY[1,2], 3);
SELECT trim_array(ARRAY[1,2], -1);
SELECT trim_array('{}'::int[], 1);

SELECT array_reverse(ARRAY[1,2,3]), array_reverse('{}'::int[]), array_reverse(ARRAY[NULL,1]);
SELECT array_reverse('[2:4]={1,2,3}'::int[]), array_reverse('{{1,2},{3,4}}'::int[]);
SELECT array_sort(ARRAY[3,1,2]), array_sort(ARRAY[3,NULL,1]), array_sort('{}'::int[]);
SELECT array_sort(ARRAY[3,1,2], false), array_sort(ARRAY[3,NULL,1], false);
SELECT array_sort(ARRAY[3,NULL,1], true, true), array_sort(ARRAY[3,NULL,1], false, false);
SELECT array_sort(ARRAY['b','a','c']), array_sort('[2:4]={3,1,2}'::int[]);
SELECT array_sort('{{3,4},{1,2}}'::int[]);

SELECT array_length(array_shuffle(ARRAY[1,2,3,4]), 1);
SELECT (SELECT array_agg(x ORDER BY x) FROM unnest(array_shuffle(ARRAY[5,6,7])) x);
SELECT array_length(array_sample(ARRAY[1,2,3,4,5], 3), 1);
SELECT array_sample(ARRAY[1,2,3], 0);
SELECT array_sample(ARRAY[1,2,3], 4);

CREATE TABLE arrfm_t (id int, xs int[], ts text[]);
INSERT INTO arrfm_t VALUES (1, '{1,2}', '{a,b}'), (2, '{3,4}', '{c,d}'), (3, NULL, NULL);
SELECT array_agg(xs) FROM arrfm_t WHERE xs IS NOT NULL;
SELECT array_agg(xs ORDER BY id DESC) FROM arrfm_t WHERE xs IS NOT NULL;
SELECT array_agg(ts) FROM arrfm_t WHERE ts IS NOT NULL;
INSERT INTO arrfm_t VALUES (4, '{5,6,7}', NULL);
SELECT array_agg(xs) FROM arrfm_t WHERE xs IS NOT NULL;
SELECT array_agg(xs) FILTER (WHERE id <= 2) FROM arrfm_t;

CREATE TABLE arrfm_big (id int, xs int[]);
INSERT INTO arrfm_big SELECT 1, array_agg(g) FROM generate_series(1, 100000) g;
INSERT INTO arrfm_big SELECT 2, array_agg(g * 2) FROM generate_series(1, 50000) g;
SELECT id, cardinality(xs), array_dims(xs), xs[1], xs[cardinality(xs)] FROM arrfm_big ORDER BY id;
SELECT cardinality(array_cat(a.xs, b.xs)) FROM arrfm_big a, arrfm_big b WHERE a.id = 1 AND b.id = 2;
SELECT array_position(xs, 99999) FROM arrfm_big WHERE id = 1;
SELECT hash_array(xs) = hash_array(xs) FROM arrfm_big WHERE id = 1;
SELECT xs = xs, xs @> ARRAY[12345, 67890] FROM arrfm_big WHERE id = 1;
SELECT cardinality(array_remove(xs, 500)) FROM arrfm_big WHERE id = 1;
SELECT md5(array_to_string(xs, ',')) FROM arrfm_big ORDER BY id;
SELECT cardinality(string_to_array(array_to_string(xs, ','), ',')) FROM arrfm_big WHERE id = 1;

DROP TABLE arrfm_t;
DROP TABLE arrfm_big;
