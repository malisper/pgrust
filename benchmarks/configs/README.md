# Configuration files

`oltp-common.conf` is applied to all four OLTP datadirs — both pgrust arms and
both C PostgreSQL 18.3 arms. It is one file, used four times, so that the two
engines cannot drift apart.

`clickbench.conf` is the ClickBench server configuration. It is separate
because ClickBench is a single-engine benchmark: pgrust is scored against
*published results from other systems*, not against a C PostgreSQL arm on the
same box. There is no parity requirement, only a fidelity requirement — the
config must match the one our published cut used.

To confirm what a running server is actually using, rather than what a file
says it should be using:

```sql
SELECT name, setting, source FROM pg_settings
WHERE name IN ('shared_buffers','effective_cache_size','work_mem',
               'effective_io_concurrency','max_parallel_workers',
               'max_connections','io_method','max_stack_depth');
```

Do this on both arms before believing any ratio. Configuration drift between
arms is the single easiest way to produce a wrong comparison.
