#ifndef NV_PG_LSN_H
#define NV_PG_LSN_H
#define PG_RETURN_LSN(x) return UInt64GetDatum(x)
#define PG_GETARG_LSN(n) ((XLogRecPtr) PG_GETARG_DATUM(n))
#endif
