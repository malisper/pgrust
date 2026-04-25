# Antithesis main workload

This directory is the home for the first Antithesis-style workload.

The first version should stay narrow:

- connect with at least two clients
- create one ordinary table
- insert and update a small number of rows
- force reconnects and interrupted sessions
- verify final rows and simple metadata

This should mirror the common-use-case invariants in
`docs/testing/README.md`, not chase exotic PostgreSQL surface first.
