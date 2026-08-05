/* SHIM utils/memutils.h — palloc family lives in shim postgres.h. */
#ifndef PG_JSONBFAM_SHIM_MEMUTILS_H
#define PG_JSONBFAM_SHIM_MEMUTILS_H
typedef struct MemoryContextData *MemoryContext;
#endif
