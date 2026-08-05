--
-- BOOLEAN
--

--
-- sanity check - if this fails go insane!
--
-- pgrust:rowsort
SELECT 1 AS one;


-- ******************testing built-in type bool********************

-- check bool input syntax

-- pgrust:rowsort
SELECT true AS true;

-- pgrust:rowsort
SELECT false AS false;

-- pgrust:rowsort
SELECT bool 't' AS true;

-- pgrust:rowsort
SELECT bool '   f           ' AS false;

-- pgrust:rowsort
SELECT bool 'true' AS true;

SELECT bool 'test' AS error;

-- pgrust:rowsort
SELECT bool 'false' AS false;

SELECT bool 'foo' AS error;

-- pgrust:rowsort
SELECT bool 'y' AS true;

-- pgrust:rowsort
SELECT bool 'yes' AS true;

SELECT bool 'yeah' AS error;

-- pgrust:rowsort
SELECT bool 'n' AS false;

-- pgrust:rowsort
SELECT bool 'no' AS false;

SELECT bool 'nay' AS error;

-- pgrust:rowsort
SELECT bool 'on' AS true;

-- pgrust:rowsort
SELECT bool 'off' AS false;

-- pgrust:rowsort
SELECT bool 'of' AS false;

SELECT bool 'o' AS error;

SELECT bool 'on_' AS error;

SELECT bool 'off_' AS error;

-- pgrust:rowsort
SELECT bool '1' AS true;

SELECT bool '11' AS error;

-- pgrust:rowsort
SELECT bool '0' AS false;

SELECT bool '000' AS error;

SELECT bool '' AS error;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('true', 'bool');
SELECT pg_input_is_valid('asdf', 'bool');
SELECT * FROM pg_input_error_info('junk', 'bool');

-- and, or, not in qualifications

-- pgrust:rowsort
SELECT bool 't' or bool 'f' AS true;

-- pgrust:rowsort
SELECT bool 't' and bool 'f' AS false;

-- pgrust:rowsort
SELECT not bool 'f' AS true;

-- pgrust:rowsort
SELECT bool 't' = bool 'f' AS false;

-- pgrust:rowsort
SELECT bool 't' <> bool 'f' AS true;

-- pgrust:rowsort
SELECT bool 't' > bool 'f' AS true;

-- pgrust:rowsort
SELECT bool 't' >= bool 'f' AS true;

-- pgrust:rowsort
SELECT bool 'f' < bool 't' AS true;

-- pgrust:rowsort
SELECT bool 'f' <= bool 't' AS true;

-- explicit casts to/from text
-- pgrust:rowsort
SELECT 'TrUe'::text::boolean AS true, 'fAlse'::text::boolean AS false;
-- pgrust:rowsort
SELECT '    true   '::text::boolean AS true,
       '     FALSE'::text::boolean AS false;
-- pgrust:rowsort
SELECT true::boolean::text AS true, false::boolean::text AS false;

SELECT '  tru e '::text::boolean AS invalid;    -- error
SELECT ''::text::boolean AS invalid;            -- error

CREATE TABLE BOOLTBL1 (f1 bool);

INSERT INTO BOOLTBL1 (f1) VALUES (bool 't');

INSERT INTO BOOLTBL1 (f1) VALUES (bool 'True');

INSERT INTO BOOLTBL1 (f1) VALUES (bool 'true');


-- BOOLTBL1 should be full of true's at this point
-- pgrust:rowsort
SELECT BOOLTBL1.* FROM BOOLTBL1;


-- pgrust:rowsort
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'true';


-- pgrust:rowsort
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> bool 'false';

-- pgrust:rowsort
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE booleq(bool 'false', f1);

INSERT INTO BOOLTBL1 (f1) VALUES (bool 'f');

-- pgrust:rowsort
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'false';


CREATE TABLE BOOLTBL2 (f1 bool);

INSERT INTO BOOLTBL2 (f1) VALUES (bool 'f');

INSERT INTO BOOLTBL2 (f1) VALUES (bool 'false');

INSERT INTO BOOLTBL2 (f1) VALUES (bool 'False');

INSERT INTO BOOLTBL2 (f1) VALUES (bool 'FALSE');

-- This is now an invalid expression
-- For pre-v6.3 this evaluated to false - thomas 1997-10-23
INSERT INTO BOOLTBL2 (f1)
   VALUES (bool 'XXX');

-- BOOLTBL2 should be full of false's at this point
-- pgrust:rowsort
SELECT BOOLTBL2.* FROM BOOLTBL2;


-- pgrust:rowsort
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 <> BOOLTBL1.f1;


-- pgrust:rowsort
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE boolne(BOOLTBL2.f1,BOOLTBL1.f1);


-- pgrust:rowsort
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = bool 'false';


SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = bool 'true'
   ORDER BY BOOLTBL1.f1, BOOLTBL2.f1;

--
-- SQL syntax
-- Try all combinations to ensure that we get nothing when we expect nothing
-- - thomas 2000-01-04
--

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS TRUE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS NOT FALSE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS FALSE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS NOT TRUE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS TRUE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS NOT FALSE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS FALSE;

-- pgrust:rowsort
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS NOT TRUE;

--
-- Tests for BooleanTest
--
CREATE TABLE BOOLTBL3 (d text, b bool, o int);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('true', true, 1);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('false', false, 2);
INSERT INTO BOOLTBL3 (d, b, o) VALUES ('null', null, 3);

SELECT
    d,
    b IS TRUE AS istrue,
    b IS NOT TRUE AS isnottrue,
    b IS FALSE AS isfalse,
    b IS NOT FALSE AS isnotfalse,
    b IS UNKNOWN AS isunknown,
    b IS NOT UNKNOWN AS isnotunknown
FROM booltbl3 ORDER BY o;


-- Test to make sure short-circuiting and NULL handling is
-- correct. Use a table as source to prevent constant simplification
-- from interfering.
CREATE TABLE booltbl4(isfalse bool, istrue bool, isnul bool);
INSERT INTO booltbl4 VALUES (false, true, null);
\pset null '(null)'

-- AND expression need to return null if there's any nulls and not all
-- of the value are true
-- pgrust:rowsort
SELECT istrue AND isnul AND istrue FROM booltbl4;
-- pgrust:rowsort
SELECT istrue AND istrue AND isnul FROM booltbl4;
-- pgrust:rowsort
SELECT isnul AND istrue AND istrue FROM booltbl4;
-- pgrust:rowsort
SELECT isfalse AND isnul AND istrue FROM booltbl4;
-- pgrust:rowsort
SELECT istrue AND isfalse AND isnul FROM booltbl4;
-- pgrust:rowsort
SELECT isnul AND istrue AND isfalse FROM booltbl4;

-- OR expression need to return null if there's any nulls and none
-- of the value is true
-- pgrust:rowsort
SELECT isfalse OR isnul OR isfalse FROM booltbl4;
-- pgrust:rowsort
SELECT isfalse OR isfalse OR isnul FROM booltbl4;
-- pgrust:rowsort
SELECT isnul OR isfalse OR isfalse FROM booltbl4;
-- pgrust:rowsort
SELECT isfalse OR isnul OR istrue FROM booltbl4;
-- pgrust:rowsort
SELECT istrue OR isfalse OR isnul FROM booltbl4;
-- pgrust:rowsort
SELECT isnul OR istrue OR isfalse FROM booltbl4;

-- Casts
-- pgrust:rowsort
SELECT 0::boolean;
-- pgrust:rowsort
SELECT 1::boolean;
-- pgrust:rowsort
SELECT 2::boolean;


--
-- Clean up
-- Many tables are retained by the regression test, but these do not seem
--  particularly useful so just get rid of them for now.
--  - thomas 1997-11-30
--

DROP TABLE  BOOLTBL1;

DROP TABLE  BOOLTBL2;

DROP TABLE  BOOLTBL3;

DROP TABLE  BOOLTBL4;
