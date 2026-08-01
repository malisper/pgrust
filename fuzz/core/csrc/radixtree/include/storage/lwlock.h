/*
 * EMPTY SHIM storage/lwlock.h — NOT PostgreSQL code (radixtree_diff oracle).
 * lib/radixtree.h includes this unconditionally but consumes it only
 * under RT_SHMEM, which this oracle never defines (shared-memory arm
 * carved per the ranking cell; the Rust SharedRadixTree stand-in is
 * compared against the non-shmem template semantics).
 */
