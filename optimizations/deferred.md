# Deferred Optimizations

Items to revisit later when relevant.

## WAL Segmentation
Split WAL into 16MB segment files for recycling/cleanup of old WAL data.
Operational concern — doesn't directly improve throughput.

## Group Commit
Release WAL mutex before fdatasync so concurrent committers share a single fsync.
Only helps multi-threaded concurrent commit workloads.

## Background WAL Writer / Dedicated WAL Writer Thread
A dedicated writer thread would subsume both the background flush and WAL insert
lock optimizations — other threads enqueue records, the writer batches and flushes.
Eliminates mutex contention on the write path.
