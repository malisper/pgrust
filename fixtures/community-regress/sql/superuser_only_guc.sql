-- Issue malisper/pgrust#71: GUC_SUPERUSER_ONLY settings must not be
-- readable by unprivileged roles through current_setting()/SHOW
-- (C: GetConfigOptionByName -> ConfigOptionIsVisible; needs privileges of
-- pg_read_all_settings). The missing_ok form only forgives unknown names,
-- never the privilege check.
CREATE ROLE issue71_lowpriv;
SET ROLE issue71_lowpriv;
SELECT current_setting('data_directory');
SHOW data_directory;
SELECT current_setting('data_directory', true);
RESET ROLE;
SELECT length(current_setting('data_directory')) > 0 AS super_can_read;
DROP ROLE issue71_lowpriv;
