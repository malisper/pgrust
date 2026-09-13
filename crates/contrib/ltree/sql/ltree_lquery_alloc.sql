CREATE EXTENSION ltree;

-- parse_lquery sizes every level's nodeitem array by the query's pipe count;
-- the palloc0 request is checked before any syntax error.
SELECT ('a?' || repeat('|', 44739243))::lquery;
SELECT pg_input_is_valid('a?' || repeat('|', 44739243), 'lquery');
SELECT ('a?' || repeat('|', 44739241))::lquery;
SELECT pg_input_is_valid('a?' || repeat('|', 44739241), 'lquery');
