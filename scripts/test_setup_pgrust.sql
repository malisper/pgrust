--
-- Minimal regression bootstrap for pgrust.
--
-- This is intentionally narrower than PostgreSQL's upstream test_setup.sql.
-- It creates only fixtures that pgrust can currently express, so later tests
-- fail on real feature gaps instead of immediately on missing shared tables.
--

CREATE TABLE CHAR_TBL (f1 char(4));
INSERT INTO CHAR_TBL (f1) VALUES ('a'), ('ab'), ('abcd'), ('abcd');

CREATE TABLE FLOAT8_TBL (f1 float8);
INSERT INTO FLOAT8_TBL (f1) VALUES
  (0.0::float8),
  ((-34.84)::float8),
  ((-1004.30)::float8),
  ((-1.2345678901234e+200)::float8),
  ((-1.2345678901234e-200)::float8);

CREATE TABLE INT2_TBL (f1 int2);
INSERT INTO INT2_TBL (f1) VALUES (0::int2), (1234::int2), ((-1234)::int2), (32767::int2), ((-32767)::int2);

CREATE TABLE INT4_TBL (f1 int4);
INSERT INTO INT4_TBL (f1) VALUES (0::int4), (123456::int4), ((-123456)::int4), (2147483647::int4), ((-2147483647)::int4);

CREATE TABLE INT8_TBL (q1 int8, q2 int8);
INSERT INTO INT8_TBL (q1, q2) VALUES
  (123::int8, 456::int8),
  (123::int8, 4567890123456789::int8),
  (4567890123456789::int8, 123::int8),
  (4567890123456789::int8, 4567890123456789::int8),
  (4567890123456789::int8, (-4567890123456789)::int8);

CREATE TABLE POINT_TBL (f1 point);
INSERT INTO POINT_TBL (f1) VALUES
  ('(0.0,0.0)'),
  ('(-10.0,0.0)'),
  ('(-3.0,4.0)'),
  ('(5.1, 34.5)'),
  ('(-5.0,-12.0)'),
  ('(1e-300,-1e-300)'),
  ('(1e+300,Inf)'),
  ('(Inf,1e+300)'),
  (' ( Nan , NaN ) '),
  ('10.0,10.0');

CREATE TABLE TEXT_TBL (f1 text);
INSERT INTO TEXT_TBL (f1) VALUES ('doh!'), ('hi de ho neighbor');

CREATE TABLE VARCHAR_TBL (f1 varchar(4));
INSERT INTO VARCHAR_TBL (f1) VALUES ('a'), ('ab'), ('abcd'), ('abcd');

CREATE TABLE onek (
  unique1 int4,
  unique2 int4,
  two int4,
  four int4,
  ten int4,
  twenty int4,
  hundred int4,
  thousand int4,
  twothousand int4,
  fivethous int4,
  tenthous int4,
  odd int4,
  even int4,
  stringu1 text,
  stringu2 text,
  string4 text
);
INSERT INTO onek VALUES
  (494::int4, 11::int4, 0::int4, 2::int4, 4::int4, 14::int4, 4::int4, 94::int4, 94::int4, 494::int4, 494::int4, 8::int4, 9::int4, 'ATAAAA'::text, 'LAAAAA'::text, 'VVVVxx'::text),
  (147::int4, 0::int4, 1::int4, 3::int4, 7::int4, 7::int4, 7::int4, 47::int4, 147::int4, 147::int4, 147::int4, 14::int4, 15::int4, 'RFAAAA'::text, 'AAAAAA'::text, 'AAAAxx'::text),
  (931::int4, 1::int4, 1::int4, 3::int4, 1::int4, 11::int4, 1::int4, 31::int4, 131::int4, 431::int4, 931::int4, 2::int4, 3::int4, 'VJAAAA'::text, 'BAAAAA'::text, 'HHHHxx'::text),
  (9::int4, 49::int4, 1::int4, 1::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 18::int4, 19::int4, 'JAAAAA'::text, 'XBAAAA'::text, 'HHHHxx'::text),
  (995::int4, 144::int4, 1::int4, 3::int4, 5::int4, 15::int4, 5::int4, 95::int4, 195::int4, 495::int4, 995::int4, 10::int4, 11::int4, 'HMAAAA'::text, 'OFAAAA'::text, 'AAAAxx'::text),
  (999::int4, 152::int4, 1::int4, 3::int4, 9::int4, 19::int4, 9::int4, 99::int4, 199::int4, 499::int4, 999::int4, 18::int4, 19::int4, 'LMAAAA'::text, 'WFAAAA'::text, 'VVVVxx'::text),
  (983::int4, 168::int4, 1::int4, 3::int4, 3::int4, 3::int4, 3::int4, 83::int4, 183::int4, 483::int4, 983::int4, 6::int4, 7::int4, 'VLAAAA'::text, 'MGAAAA'::text, 'AAAAxx'::text),
  (1::int4, 214::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 2::int4, 3::int4, 'BAAAAA'::text, 'GIAAAA'::text, 'OOOOxx'::text),
  (989::int4, 227::int4, 1::int4, 1::int4, 9::int4, 9::int4, 9::int4, 89::int4, 189::int4, 489::int4, 989::int4, 18::int4, 19::int4, 'BMAAAA'::text, 'TIAAAA'::text, 'VVVVxx'::text),
  (986::int4, 237::int4, 0::int4, 2::int4, 6::int4, 6::int4, 6::int4, 86::int4, 186::int4, 486::int4, 986::int4, 12::int4, 13::int4, 'YLAAAA'::text, 'DJAAAA'::text, 'HHHHxx'::text),
  (996::int4, 258::int4, 0::int4, 0::int4, 6::int4, 16::int4, 6::int4, 96::int4, 196::int4, 496::int4, 996::int4, 12::int4, 13::int4, 'IMAAAA'::text, 'YJAAAA'::text, 'OOOOxx'::text),
  (2::int4, 326::int4, 0::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 4::int4, 5::int4, 'CAAAAA'::text, 'OMAAAA'::text, 'OOOOxx'::text),
  (982::int4, 360::int4, 0::int4, 2::int4, 2::int4, 2::int4, 2::int4, 82::int4, 182::int4, 482::int4, 982::int4, 4::int4, 5::int4, 'ULAAAA'::text, 'WNAAAA'::text, 'AAAAxx'::text),
  (992::int4, 363::int4, 0::int4, 0::int4, 2::int4, 12::int4, 2::int4, 92::int4, 192::int4, 492::int4, 992::int4, 4::int4, 5::int4, 'EMAAAA'::text, 'ZNAAAA'::text, 'VVVVxx'::text),
  (990::int4, 369::int4, 0::int4, 2::int4, 0::int4, 10::int4, 0::int4, 90::int4, 190::int4, 490::int4, 990::int4, 0::int4, 1::int4, 'CMAAAA'::text, 'FOAAAA'::text, 'HHHHxx'::text),
  (991::int4, 426::int4, 1::int4, 3::int4, 1::int4, 11::int4, 1::int4, 91::int4, 191::int4, 491::int4, 991::int4, 2::int4, 3::int4, 'DMAAAA'::text, 'KQAAAA'::text, 'OOOOxx'::text),
  (3::int4, 431::int4, 1::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 6::int4, 7::int4, 'DAAAAA'::text, 'PQAAAA'::text, 'VVVVxx'::text),
  (984::int4, 475::int4, 0::int4, 0::int4, 4::int4, 4::int4, 4::int4, 84::int4, 184::int4, 484::int4, 984::int4, 8::int4, 9::int4, 'WLAAAA'::text, 'HSAAAA'::text, 'VVVVxx'::text),
  (981::int4, 480::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 81::int4, 181::int4, 481::int4, 981::int4, 2::int4, 3::int4, 'TLAAAA'::text, 'MSAAAA'::text, 'AAAAxx'::text),
  (5::int4, 541::int4, 1::int4, 1::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 10::int4, 11::int4, 'FAAAAA'::text, 'VUAAAA'::text, 'HHHHxx'::text),
  (998::int4, 549::int4, 0::int4, 2::int4, 8::int4, 18::int4, 8::int4, 98::int4, 198::int4, 498::int4, 998::int4, 16::int4, 17::int4, 'KMAAAA'::text, 'DVAAAA'::text, 'HHHHxx'::text),
  (7::int4, 647::int4, 1::int4, 3::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 14::int4, 15::int4, 'HAAAAA'::text, 'XYAAAA'::text, 'VVVVxx'::text),
  (8::int4, 653::int4, 0::int4, 0::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 16::int4, 17::int4, 'IAAAAA'::text, 'DZAAAA'::text, 'HHHHxx'::text),
  (993::int4, 661::int4, 1::int4, 1::int4, 3::int4, 13::int4, 3::int4, 93::int4, 193::int4, 493::int4, 993::int4, 6::int4, 7::int4, 'FMAAAA'::text, 'LZAAAA'::text, 'AAAAxx'::text),
  (994::int4, 695::int4, 0::int4, 2::int4, 4::int4, 14::int4, 4::int4, 94::int4, 194::int4, 494::int4, 994::int4, 8::int4, 9::int4, 'GMAAAA'::text, 'TABAAA'::text, 'VVVVxx'::text),
  (988::int4, 766::int4, 0::int4, 0::int4, 8::int4, 8::int4, 8::int4, 88::int4, 188::int4, 488::int4, 988::int4, 16::int4, 17::int4, 'AMAAAA'::text, 'MDBAAA'::text, 'OOOOxx'::text),
  (987::int4, 806::int4, 1::int4, 3::int4, 7::int4, 7::int4, 7::int4, 87::int4, 187::int4, 487::int4, 987::int4, 14::int4, 15::int4, 'ZLAAAA'::text, 'AFBAAA'::text, 'HHHHxx'::text),
  (4::int4, 833::int4, 0::int4, 0::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 8::int4, 9::int4, 'EAAAAA'::text, 'BGBAAA'::text, 'HHHHxx'::text),
  (985::int4, 870::int4, 1::int4, 1::int4, 5::int4, 5::int4, 5::int4, 85::int4, 185::int4, 485::int4, 985::int4, 10::int4, 11::int4, 'XLAAAA'::text, 'MHBAAA'::text, 'AAAAxx'::text);

CREATE TABLE onek2 (
  unique1 int4,
  unique2 int4,
  two int4,
  four int4,
  ten int4,
  twenty int4,
  hundred int4,
  thousand int4,
  twothousand int4,
  fivethous int4,
  tenthous int4,
  odd int4,
  even int4,
  stringu1 text,
  stringu2 text,
  string4 text
);
INSERT INTO onek2 SELECT * FROM onek;

CREATE TABLE tenk1 (
  unique1 int4,
  unique2 int4,
  two int4,
  four int4,
  ten int4,
  twenty int4,
  hundred int4,
  thousand int4,
  twothousand int4,
  fivethous int4,
  tenthous int4,
  odd int4,
  even int4,
  stringu1 text,
  stringu2 text,
  string4 text
);
INSERT INTO tenk1 VALUES
  (8800::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 800::int4, 800::int4, 3800::int4, 8800::int4, 0::int4, 1::int4, 'MAAAAA'::text, 'AAAAAA'::text, 'AAAAxx'::text),
  (9850::int4, 3::int4, 0::int4, 2::int4, 0::int4, 10::int4, 50::int4, 850::int4, 1850::int4, 4850::int4, 9850::int4, 100::int4, 101::int4, 'WOAAAA'::text, 'DAAAAA'::text, 'VVVVxx'::text),
  (18::int4, 376::int4, 0::int4, 2::int4, 8::int4, 18::int4, 18::int4, 18::int4, 18::int4, 18::int4, 18::int4, 36::int4, 37::int4, 'SAAAAA'::text, 'MOAAAA'::text, 'AAAAxx'::text),
  (23::int4, 1236::int4, 1::int4, 3::int4, 3::int4, 3::int4, 23::int4, 23::int4, 23::int4, 23::int4, 23::int4, 46::int4, 47::int4, 'XAAAAA'::text, 'OVBAAA'::text, 'AAAAxx'::text),
  (15::int4, 1358::int4, 1::int4, 3::int4, 5::int4, 15::int4, 15::int4, 15::int4, 15::int4, 15::int4, 15::int4, 30::int4, 31::int4, 'PAAAAA'::text, 'GACAAA'::text, 'OOOOxx'::text),
  (4::int4, 1621::int4, 0::int4, 0::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 4::int4, 8::int4, 9::int4, 'EAAAAA'::text, 'JKCAAA'::text, 'HHHHxx'::text),
  (21::int4, 1628::int4, 1::int4, 1::int4, 1::int4, 1::int4, 21::int4, 21::int4, 21::int4, 21::int4, 21::int4, 42::int4, 43::int4, 'VAAAAA'::text, 'QKCAAA'::text, 'AAAAxx'::text),
  (2::int4, 2716::int4, 0::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 2::int4, 4::int4, 5::int4, 'CAAAAA'::text, 'MAEAAA'::text, 'AAAAxx'::text),
  (1::int4, 2838::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 1::int4, 2::int4, 3::int4, 'BAAAAA'::text, 'EFEAAA'::text, 'OOOOxx'::text),
  (6::int4, 2855::int4, 0::int4, 2::int4, 6::int4, 6::int4, 6::int4, 6::int4, 6::int4, 6::int4, 6::int4, 12::int4, 13::int4, 'GAAAAA'::text, 'VFEAAA'::text, 'VVVVxx'::text),
  (24::int4, 3246::int4, 0::int4, 0::int4, 4::int4, 4::int4, 24::int4, 24::int4, 24::int4, 24::int4, 24::int4, 48::int4, 49::int4, 'YAAAAA'::text, 'WUEAAA'::text, 'OOOOxx'::text),
  (14::int4, 4341::int4, 0::int4, 2::int4, 4::int4, 14::int4, 14::int4, 14::int4, 14::int4, 14::int4, 14::int4, 28::int4, 29::int4, 'OAAAAA'::text, 'ZKGAAA'::text, 'HHHHxx'::text),
  (9::int4, 4463::int4, 1::int4, 1::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 9::int4, 18::int4, 19::int4, 'JAAAAA'::text, 'RPGAAA'::text, 'VVVVxx'::text),
  (8::int4, 5435::int4, 0::int4, 0::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 8::int4, 16::int4, 17::int4, 'IAAAAA'::text, 'BBIAAA'::text, 'VVVVxx'::text),
  (5::int4, 5557::int4, 1::int4, 1::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 5::int4, 10::int4, 11::int4, 'FAAAAA'::text, 'TFIAAA'::text, 'HHHHxx'::text),
  (20::int4, 5574::int4, 0::int4, 0::int4, 0::int4, 0::int4, 20::int4, 20::int4, 20::int4, 20::int4, 20::int4, 40::int4, 41::int4, 'UAAAAA'::text, 'KGIAAA'::text, 'OOOOxx'::text),
  (3::int4, 5679::int4, 1::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 3::int4, 6::int4, 7::int4, 'DAAAAA'::text, 'LKIAAA'::text, 'VVVVxx'::text),
  (13::int4, 5696::int4, 1::int4, 1::int4, 3::int4, 13::int4, 13::int4, 13::int4, 13::int4, 13::int4, 13::int4, 26::int4, 27::int4, 'NAAAAA'::text, 'CLIAAA'::text, 'AAAAxx'::text),
  (12::int4, 6605::int4, 0::int4, 0::int4, 2::int4, 12::int4, 12::int4, 12::int4, 12::int4, 12::int4, 12::int4, 24::int4, 25::int4, 'MAAAAA'::text, 'BUJAAA'::text, 'HHHHxx'::text),
  (22::int4, 7045::int4, 0::int4, 2::int4, 2::int4, 2::int4, 22::int4, 22::int4, 22::int4, 22::int4, 22::int4, 44::int4, 45::int4, 'WAAAAA'::text, 'ZKKAAA'::text, 'HHHHxx'::text),
  (19::int4, 7303::int4, 1::int4, 3::int4, 9::int4, 19::int4, 19::int4, 19::int4, 19::int4, 19::int4, 19::int4, 38::int4, 39::int4, 'TAAAAA'::text, 'XUKAAA'::text, 'VVVVxx'::text),
  (17::int4, 8274::int4, 1::int4, 1::int4, 7::int4, 17::int4, 17::int4, 17::int4, 17::int4, 17::int4, 17::int4, 34::int4, 35::int4, 'RAAAAA'::text, 'GGMAAA'::text, 'OOOOxx'::text),
  (11::int4, 8396::int4, 1::int4, 3::int4, 1::int4, 11::int4, 11::int4, 11::int4, 11::int4, 11::int4, 11::int4, 22::int4, 23::int4, 'LAAAAA'::text, 'YKMAAA'::text, 'AAAAxx'::text),
  (7::int4, 8518::int4, 1::int4, 3::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 7::int4, 14::int4, 15::int4, 'HAAAAA'::text, 'QPMAAA'::text, 'VVVVxx'::text),
  (10::int4, 8788::int4, 0::int4, 2::int4, 0::int4, 10::int4, 10::int4, 10::int4, 10::int4, 10::int4, 10::int4, 20::int4, 21::int4, 'KAAAAA'::text, 'AANAAA'::text, 'AAAAxx'::text),
  (16::int4, 9675::int4, 0::int4, 0::int4, 6::int4, 16::int4, 16::int4, 16::int4, 16::int4, 16::int4, 16::int4, 32::int4, 33::int4, 'QAAAAA'::text, 'DIOAAA'::text, 'VVVVxx'::text),
  (0::int4, 9998::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 0::int4, 1::int4, 'AAAAAA'::text, 'OUOAAA'::text, 'OOOOxx'::text);

CREATE TABLE tenk2 (
  unique1 int4,
  unique2 int4,
  two int4,
  four int4,
  ten int4,
  twenty int4,
  hundred int4,
  thousand int4,
  twothousand int4,
  fivethous int4,
  tenthous int4,
  odd int4,
  even int4,
  stringu1 text,
  stringu2 text,
  string4 text
);
INSERT INTO tenk2 SELECT * FROM tenk1;

--
-- Shared index-capable fixtures for pgrust regression coverage.
--
-- Keep these separate from PostgreSQL's canonical create_index.sql object
-- names so later regression files can still create their own indexes.
--

CREATE TABLE pgrust_index_tbl (
  id int4 not null,
  bucket int4 not null,
  payload text
);
INSERT INTO pgrust_index_tbl (id, bucket, payload) VALUES
  (1::int4, 1::int4, 'alpha'),
  (2::int4, 1::int4, 'beta'),
  (3::int4, 2::int4, 'gamma'),
  (4::int4, 2::int4, 'delta'),
  (5::int4, 3::int4, 'epsilon'),
  (6::int4, 3::int4, 'zeta'),
  (7::int4, 4::int4, 'eta'),
  (8::int4, 4::int4, 'theta');

CREATE INDEX pgrust_index_tbl_id_idx ON pgrust_index_tbl (id);
CREATE INDEX pgrust_index_tbl_bucket_id_idx ON pgrust_index_tbl (bucket, id);

CREATE TABLE pgrust_unique_tbl (
  id int4,
  note text
);
INSERT INTO pgrust_unique_tbl (id, note) VALUES
  (1::int4, 'one'),
  (2::int4, 'two'),
  (3::int4, 'three'),
  (NULL, 'null-a'),
  (NULL, 'null-b');

CREATE UNIQUE INDEX pgrust_unique_tbl_id_key ON pgrust_unique_tbl (id);

CREATE TABLE person (
  name text,
  age int4,
  location text
);
INSERT INTO person (name, age, location) VALUES
  ('alice', 30::int4, '(0,0)'),
  ('bob', 40::int4, '(1,1)'),
  ('carol', 25::int4, '(2,2)');

CREATE TABLE emp (
  name text,
  age int4,
  location text,
  salary int4,
  manager text
);
INSERT INTO emp (name, age, location, salary, manager) VALUES
  ('dave', 35::int4, '(3,3)', 50000::int4, 'alice'),
  ('erin', 45::int4, '(4,4)', 75000::int4, 'alice');

CREATE TABLE student (
  name text,
  age int4,
  location text,
  gpa float8
);
INSERT INTO student (name, age, location, gpa) VALUES
  ('frank', 20::int4, '(5,5)', 3.4::float8),
  ('grace', 21::int4, '(6,6)', 3.7::float8);

CREATE TABLE stud_emp (
  name text,
  age int4,
  location text,
  salary int4,
  manager text,
  gpa float8,
  percent int4
);
INSERT INTO stud_emp (name, age, location, salary, manager, gpa, percent) VALUES
  ('heidi', 22::int4, '(7,7)', 20000::int4, 'dave', 3.5::float8, 50::int4);
