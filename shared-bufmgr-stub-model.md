# Shared Buffer Manager Stub Model

This document defines the first Rust test harness and model shape.

## Goal

Build a deterministic model of PostgreSQL shared buffers that is small enough to test thoroughly and explicit enough to evolve into a future integration target.

## Model Principles

- no real shared memory
- no real PostgreSQL locking
- no OS-thread scheduling requirements
- no filesystem dependency
- explicit I/O completion controlled by tests

## Public Model Surface

The Rust crate should expose:

- relation and page identity types
- `BufferPool`
- `StorageBackend`
- a fake in-memory storage implementation for tests

The primary operations should be:

- request a page for a client
- complete a pending read
- mark a page dirty
- mutate page contents
- unpin a page
- flush a page
- complete a pending write
- invalidate a relation's pages when eligible

## Observable Behavior

The test surface should care about:

- hit vs miss
- whether I/O was issued
- whether a request is waiting on existing I/O
- pin counts
- dirty/valid/I/O state
- buffer identity reuse
- storage contents after flush

The test surface should not care about:

- exact internal lock order
- exact PostgreSQL atomic bit layout
- background writer pacing
- checkpoint progress accounting

## Deferred Functionality

- multi-block read APIs
- bulk strategy rings
- checkpointer/bgwriter coordination
- WAL and redo semantics
- real `smgr` integration

