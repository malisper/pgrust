/*
 * SHIM utils/memutils.h — NOT PostgreSQL code. (tsq oracle family)
 * MaxAllocSize verbatim from src/include/utils/memutils.h.
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_MEMUTILS_H
#define PG_DIFFFUZZ_TSQ_SHIM_MEMUTILS_H

#include "postgres.h"

#define MaxAllocSize ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */

/*
 * MemoryContext surface reduced to what tsquery_rewrite.c's SPI half
 * (the lane's NAMED CARVE — compiled, never called) needs to build: the
 * family arena is one flat context, so switching is the identity.
 */
typedef void *MemoryContext;
extern MemoryContext CurrentMemoryContext;

static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	(void) context;
	return CurrentMemoryContext;
}

#endif
