-- Issue malisper/pgrust#74: a second GRANT/REVOKE on a function whose
-- pg_proc tuple is TOASTed (compressible ~5.6kB body) must read the stored
-- proacl back through detoast (C: DatumGetAclP = PG_DETOAST_DATUM, acl.h),
-- not assume a plain inline varlena.
CREATE ROLE issue74_grantee LOGIN;
DO $do$
DECLARE pad text;
BEGIN
  SELECT string_agg('  -- ' || md5(i::text) || ' lorem ipsum dolor sit amet consectetur adipiscing elit', E'\n')
    INTO pad FROM generate_series(1, 60) i;
  EXECUTE 'CREATE FUNCTION issue74_fn() RETURNS void LANGUAGE plpgsql AS $fn$'
       || E'BEGIN\n  RAISE NOTICE ''x'';\n' || pad || E'\nEND;\n$fn$';
END $do$;
REVOKE ALL ON FUNCTION issue74_fn() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION issue74_fn() TO issue74_grantee;
REVOKE EXECUTE ON FUNCTION issue74_fn() FROM issue74_grantee;
DROP FUNCTION issue74_fn();
DROP ROLE issue74_grantee;
