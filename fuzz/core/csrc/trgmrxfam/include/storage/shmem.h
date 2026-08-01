/* SHIM (trgmrxfam): shared-memory decls referenced only from dynahash's
 * HASH_SHARED_MEM paths, which trgm_regexp never requests — abort stubs
 * keep them loud (defined in pg_trgm_regexp_io.c). */
#ifndef TRGMRX_SHMEM_H
#define TRGMRX_SHMEM_H
#include "postgres.h"
#include "utils/hsearch.h"	/* real shmem.h includes hsearch.h; dynahash.c relies on it */
extern void *ShmemAllocNoError(Size size);
extern Size add_size(Size s1, Size s2);
extern Size mul_size(Size s1, Size s2);
#endif
