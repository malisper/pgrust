/* src/test/modules/injection_points/injection_points--1.0.sql, trimmed to
 * the functions this port implements (attach/detach/wakeup — the three the
 * recovery TAP suite uses). */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION injection_points" to load this file. \quit

--
-- injection_points_attach()
--
-- Attaches the action to the given injection point.
--
CREATE FUNCTION injection_points_attach(IN point_name TEXT,
    IN action text)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_attach'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_wakeup()
--
-- Wakes up a waiting injection point.
--
CREATE FUNCTION injection_points_wakeup(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_wakeup'
LANGUAGE C STRICT PARALLEL UNSAFE;

--
-- injection_points_detach()
--
-- Detaches the current action, if any, from the given injection point.
--
CREATE FUNCTION injection_points_detach(IN point_name TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'injection_points_detach'
LANGUAGE C STRICT PARALLEL UNSAFE;

-- These functions alter global server behavior (they attach/detach/wake
-- injection points process-wide), so they must not be callable by ordinary
-- roles. Restrict EXECUTE to superusers, who bypass ACL checks; a non-superuser
-- then gets a permission error instead of being able to steer the server.
REVOKE ALL ON FUNCTION injection_points_attach(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION injection_points_wakeup(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION injection_points_detach(TEXT) FROM PUBLIC;
