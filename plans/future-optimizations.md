# Future Optimization Ideas

## Reuse buffers instead of per-row allocations

The profile shows ~25% of CPU time in malloc/free, mostly from per-row allocations
in the SELECT path. Key allocation sites:

1. **`decode_tuple_from_bytes`** (`executor/expr.rs:229`): allocates a `Vec<Value>`
   for every row. Each `Value::Text` also allocates a `String`. With 100K rows,
   that's 100K Vec + 100K String allocations.

2. **`TupleSlot::virtual_row`** (`executor/nodes.rs`): wraps the Vec<Value> in a
   new TupleSlot per row.

### Possible approaches

- **Reusable value buffer**: Pass a `&mut Vec<Value>` into `decode_tuple_from_bytes`
  that gets cleared and refilled each row. Avoids Vec allocation but `Value::Text`
  still allocates Strings.

- **Arena/bump allocator for Strings**: Use a per-query bump allocator for text
  values so String allocations are near-free and freed in bulk.

- **Format directly from tuple bytes**: For the wire protocol hot path, skip
  `Value` entirely and format directly from on-page tuple bytes to the wire buffer.
  This is what PostgreSQL's `printtup` does — it reads attributes from the
  `TupleTableSlot` and writes them directly to the output buffer without an
  intermediate value representation. This would require a `send_data_row_from_bytes`
  function that understands tuple layout.

- **Lazy deformation**: Only decode columns that are actually needed (e.g., for
  projections or WHERE clauses). Currently all columns are decoded even if only
  a subset is used.

## Cache parsed statements

`parse_statement` is 8-10% of inclusive time. The same SQL is re-parsed on every
query. A simple LRU cache keyed on the SQL string would eliminate this for
repeated queries (e.g., benchmarks, application query patterns).

## Increase default buffer pool size

The server defaults to 128 buffers. With tables larger than 128 pages (~1MB),
scans cause constant eviction and disk reads. PostgreSQL defaults to
`shared_buffers = 128MB` (16K pages). Increasing to 1024+ would eliminate disk
I/O for medium-sized tables.
