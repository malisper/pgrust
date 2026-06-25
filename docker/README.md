# Docker

The root `Dockerfile` builds a PostgreSQL-compatible container that runs the
pgrust server.

Build locally:

```bash
docker build -t pgrust .
```

Run:

```bash
docker run --rm \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  pgrust
```

Connect:

```bash
psql postgres://postgres:secret@localhost:5432/postgres \
  -c "select version(), 1 + 1 as two"
```

The image follows the shape of the official `postgres` image: `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`, `PGDATA`, port 5432, and
`/docker-entrypoint-initdb.d`.

The final image contains the pgrust server and a PostgreSQL 18 `psql` client.
The data directory is initialized with pgrust's own `--initdb` driver.
