/*
 * SHIM executor/spi.h — NOT PostgreSQL code. (tsq oracle family)
 *
 * LINK-ONLY stubs so the vendored tsquery_rewrite.c compiles VERBATIM.
 * Only tsquery_rewrite_query (oid 3685) touches SPI; that function is the
 * lane's documented NAMED CARVE (SPI = executor/session state, outside the
 * phase-1 pure surface) and NO pg_diff_* entry ever calls it. Every stub
 * body abort()s loudly if reached (pg_tsq_shim.c).
 */
#ifndef PG_DIFFFUZZ_TSQ_SHIM_SPI_H
#define PG_DIFFFUZZ_TSQ_SHIM_SPI_H

#include "postgres.h"
#include "fmgr.h"

typedef struct SpiTupleDescData
{
	int			natts;
} *TupleDesc;

typedef void *HeapTuple;

typedef struct SPITupleTable
{
	TupleDesc	tupdesc;
	HeapTuple  *vals;
} SPITupleTable;

typedef void *SPIPlanPtr;
typedef void *Portal;

extern SPITupleTable *SPI_tuptable;
extern uint64 SPI_processed;

extern int	SPI_connect(void);
extern int	SPI_finish(void);
extern SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes);
extern Portal SPI_cursor_open(const char *name, SPIPlanPtr plan,
							  Datum *Values, const char *Nulls, bool read_only);
extern void SPI_cursor_fetch(Portal portal, bool forward, long count);
extern void SPI_cursor_close(Portal portal);
extern int	SPI_freeplan(SPIPlanPtr plan);
extern void SPI_freetuptable(SPITupleTable *tuptable);
extern Datum SPI_getbinval(HeapTuple row, TupleDesc rowdesc, int colnumber,
						   bool *isnull);
extern Oid	SPI_gettypeid(TupleDesc rowdesc, int colnumber);

#endif
