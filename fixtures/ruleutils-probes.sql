SELECT relname, pg_get_viewdef(oid)
  FROM pg_class WHERE relkind = 'v' AND relnamespace = 2200 ORDER BY relname;
SELECT relname, pg_get_viewdef(oid, true)
  FROM pg_class WHERE relkind = 'v' AND relnamespace = 2200 ORDER BY relname;
SELECT relname, pg_get_indexdef(oid)
  FROM pg_class WHERE relkind = 'i' AND relnamespace = 2200 ORDER BY relname;
SELECT relname, pg_get_indexdef(oid, 1, true)
  FROM pg_class WHERE relkind = 'i' AND relnamespace = 2200 ORDER BY relname;
SELECT conname, pg_get_constraintdef(oid)
  FROM pg_constraint WHERE connamespace = 2200 ORDER BY conname;
SELECT conname, pg_get_constraintdef(oid, true)
  FROM pg_constraint WHERE connamespace = 2200 ORDER BY conname;
SELECT adrelid, adnum, pg_get_expr(adbin, adrelid)
  FROM pg_attrdef ORDER BY adrelid, adnum;
