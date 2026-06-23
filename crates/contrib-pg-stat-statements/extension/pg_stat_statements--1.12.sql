/* contrib/pg_stat_statements/pg_stat_statements--1.12.sql */

-- Squashed base install script for the pgrust port: creates the final 1.12
-- objects directly, instead of the upstream 1.4-base + 1.4->...->1.12 upgrade
-- chain. The chain's intermediate steps use `ALTER EXTENSION ... ADD|DROP`
-- (the unported pgrust utility-slow arm); the resulting catalog state of this
-- single script is identical to running the full chain to 1.12. Documented
-- pgrust packaging divergence; the C bodies are byte-faithful.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_statements" to load this file. \quit

--- Define pg_stat_statements_info

CREATE FUNCTION pg_stat_statements_info(
    OUT dealloc bigint,
    OUT stats_reset timestamp with time zone
)
RETURNS record
AS 'MODULE_PATHNAME', 'pg_stat_statements_info'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

--- Define pg_stat_statements_reset

CREATE FUNCTION pg_stat_statements_reset(IN userid Oid DEFAULT 0,
	IN dbid Oid DEFAULT 0,
	IN queryid bigint DEFAULT 0,
	IN minmax_only boolean DEFAULT false
)
RETURNS timestamp with time zone
AS 'MODULE_PATHNAME', 'pg_stat_statements_reset_1_11'
LANGUAGE C STRICT PARALLEL SAFE;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_statements_reset(Oid, Oid, bigint, boolean) FROM PUBLIC;

--- Define pg_stat_statements

CREATE FUNCTION pg_stat_statements(IN showtext boolean,
    OUT userid oid,
    OUT dbid oid,
    OUT toplevel bool,
    OUT queryid bigint,
    OUT query text,
    OUT plans int8,
    OUT total_plan_time float8,
    OUT min_plan_time float8,
    OUT max_plan_time float8,
    OUT mean_plan_time float8,
    OUT stddev_plan_time float8,
    OUT calls int8,
    OUT total_exec_time float8,
    OUT min_exec_time float8,
    OUT max_exec_time float8,
    OUT mean_exec_time float8,
    OUT stddev_exec_time float8,
    OUT rows int8,
    OUT shared_blks_hit int8,
    OUT shared_blks_read int8,
    OUT shared_blks_dirtied int8,
    OUT shared_blks_written int8,
    OUT local_blks_hit int8,
    OUT local_blks_read int8,
    OUT local_blks_dirtied int8,
    OUT local_blks_written int8,
    OUT temp_blks_read int8,
    OUT temp_blks_written int8,
    OUT shared_blk_read_time float8,
    OUT shared_blk_write_time float8,
    OUT local_blk_read_time float8,
    OUT local_blk_write_time float8,
    OUT temp_blk_read_time float8,
    OUT temp_blk_write_time float8,
    OUT wal_records int8,
    OUT wal_fpi int8,
    OUT wal_bytes numeric,
    OUT wal_buffers_full int8,
    OUT jit_functions int8,
    OUT jit_generation_time float8,
    OUT jit_inlining_count int8,
    OUT jit_inlining_time float8,
    OUT jit_optimization_count int8,
    OUT jit_optimization_time float8,
    OUT jit_emission_count int8,
    OUT jit_emission_time float8,
    OUT jit_deform_count int8,
    OUT jit_deform_time float8,
    OUT parallel_workers_to_launch int8,
    OUT parallel_workers_launched int8,
    OUT stats_since timestamp with time zone,
    OUT minmax_stats_since timestamp with time zone
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_statements_1_12'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

-- Register a view on the function for ease of use.
CREATE VIEW pg_stat_statements AS
  SELECT * FROM pg_stat_statements(true);

GRANT SELECT ON pg_stat_statements TO PUBLIC;
