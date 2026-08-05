/*
 * SHIM nodes/nodes.h — NOT PostgreSQL code. (tsq oracle family, p1-laneaf)
 *
 * Just enough NodeTag machinery for nodes/miscnodes.h (vendored verbatim
 * beside this file) and elog.h's soft-error protocol: Node, IsA, and the
 * one tag this family ever tests (T_ErrorSaveContext). Tag VALUES are
 * process-internal — no node ever crosses the C/Rust boundary.
 * Upstream: src/include/nodes/nodes.h.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_NODES_H
#define PG_DIFFFUZZ_TSQ_SHIM_NODES_H

#include "postgres.h"

typedef enum NodeTag
{
	T_Invalid = 0,
	T_ErrorSaveContext = 1,
} NodeTag;

typedef struct Node
{
	NodeTag		type;
} Node;

#define nodeTag(nodeptr) (((const Node *) (nodeptr))->type)
#define IsA(nodeptr, _type_) (nodeTag(nodeptr) == T_##_type_)

/* opaque; miscnodes.h's details_wanted path is never exercised here */
typedef struct ErrorData ErrorData;

#endif							/* PG_DIFFFUZZ_TSQ_SHIM_NODES_H */
