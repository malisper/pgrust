/* shim: cache-entry allocation for formatting.c (TopMemoryContext = arena) */
#ifndef FMTV_MEMUTILS_H
#define FMTV_MEMUTILS_H
#define MemoryContextAllocZero fmtv_mcx_alloc_zero
#define TopMemoryContext fmtv_top_mcx
extern MemoryContext fmtv_top_mcx;
extern void *fmtv_mcx_alloc_zero(MemoryContext cxt, Size size);
#define repalloc fmtv_repalloc
extern void *repalloc(void *ptr, Size size);
#endif
