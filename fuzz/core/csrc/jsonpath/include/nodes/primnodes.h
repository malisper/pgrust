/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef PRIMNODES_H
#define PRIMNODES_H
#include "nodes/nodes.h"
#include "nodes/value.h"
/* JsonWrapper enum VERBATIM from nodes/primnodes.h @ 18.3 (jsonpath.h
 * declares (never defines here) executor entry points that name it) */
typedef enum JsonWrapper
{
	JSW_UNSPEC,
	JSW_NONE,
	JSW_CONDITIONAL,
	JSW_UNCONDITIONAL,
} JsonWrapper;
#endif
