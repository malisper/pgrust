--
-- CHAR
--

-- Per SQL standard, CHAR means character(1), that is a varlena type
-- with a constraint restricting it to one character (not byte)

-- pgrust:rowsort
SELECT char 'c' = char 'c' AS true;

--
-- Build a table for testing
-- (This temporarily hides the table created in test_setup.sql)
--

CREATE TEMP TABLE CHAR_TBL(f1 char);

INSERT INTO CHAR_TBL (f1) VALUES ('a');

INSERT INTO CHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO CHAR_TBL (f1) VALUES ('1');

INSERT INTO CHAR_TBL (f1) VALUES (2);

INSERT INTO CHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO CHAR_TBL (f1) VALUES ('');

-- try char's of greater than 1 length
INSERT INTO CHAR_TBL (f1) VALUES ('cd');
INSERT INTO CHAR_TBL (f1) VALUES ('c     ');


-- pgrust:rowsort
SELECT * FROM CHAR_TBL;

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 <> 'a';

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 = 'a';

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 < 'a';

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 <= 'a';

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 > 'a';

-- pgrust:rowsort
SELECT c.*
   FROM CHAR_TBL c
   WHERE c.f1 >= 'a';

DROP TABLE CHAR_TBL;

--
-- Now test longer arrays of char
--
-- This char_tbl was already created and filled in test_setup.sql.
-- Here we just try to insert bad values.
--

INSERT INTO CHAR_TBL (f1) VALUES ('abcde');

-- pgrust:rowsort
SELECT * FROM CHAR_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('abcd  ', 'char(4)');
SELECT pg_input_is_valid('abcde', 'char(4)');
SELECT * FROM pg_input_error_info('abcde', 'char(4)');

--
-- Also test "char", which is an ad-hoc one-byte type.  It can only
-- really store ASCII characters, but we allow high-bit-set characters
-- to be accessed via bytea-like escapes.
--

-- pgrust:rowsort
SELECT 'a'::"char";
-- pgrust:rowsort
SELECT '\101'::"char";
-- pgrust:rowsort
SELECT '\377'::"char";
-- pgrust:rowsort
SELECT 'a'::"char"::text;
-- pgrust:rowsort
SELECT '\377'::"char"::text;
-- pgrust:rowsort
SELECT '\000'::"char"::text;
-- pgrust:rowsort
SELECT 'a'::text::"char";
-- pgrust:rowsort
SELECT '\377'::text::"char";
-- pgrust:rowsort
SELECT ''::text::"char";
