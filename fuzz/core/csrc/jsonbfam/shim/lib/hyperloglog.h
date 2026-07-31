/* SHIM lib/hyperloglog.h — abbrev arms not extracted; layout-only stub so
 * the NumericSortSupport struct in the segment header compiles. */
#ifndef PG_JSONBFAM_SHIM_HLL_H
#define PG_JSONBFAM_SHIM_HLL_H
typedef struct hyperLogLogState { void *p[8]; } hyperLogLogState;
#endif
