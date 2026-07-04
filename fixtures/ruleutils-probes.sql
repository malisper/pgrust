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
SELECT c.relname, r.rulename, pg_get_ruledef(r.oid)
  FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class
  WHERE c.relnamespace = 2200 ORDER BY c.relname, r.rulename;
SELECT c.relname, r.rulename, pg_get_ruledef(r.oid, true)
  FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class
  WHERE c.relnamespace = 2200 ORDER BY c.relname, r.rulename;
SELECT p.proname, pg_get_functiondef(p.oid)
  FROM pg_proc p WHERE p.pronamespace = 2200 ORDER BY p.proname;
SELECT p.proname, pg_get_function_arguments(p.oid),
       pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid)
  FROM pg_proc p WHERE p.pronamespace = 2200 ORDER BY p.proname;
SELECT t.tgname, pg_get_triggerdef(t.oid)
  FROM pg_trigger t WHERE NOT t.tgisinternal ORDER BY t.tgname;
SELECT t.tgname, pg_get_triggerdef(t.oid, true)
  FROM pg_trigger t WHERE NOT t.tgisinternal ORDER BY t.tgname;
