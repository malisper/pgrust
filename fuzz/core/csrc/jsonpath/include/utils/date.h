/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Typedefs VERBATIM shapes from src/include/utils/date.h @ 18.3. The whole
 * datetime method family is CARVED AT THE DRIVER LEVEL; every function
 * declared here is a LOUD ABORT sentinel stub in pg_jsonpath_exec_env.c. */
#ifndef DATE_H
#define DATE_H
#include "postgres.h"
#include "fmgr.h"
#include "utils/timestamp.h"

typedef int32 DateADT;
typedef int64 TimeADT;

typedef struct
{
	TimeADT		time;			/* all time units other than months and years */
	int32		zone;			/* numeric time zone, in seconds */
} TimeTzADT;

#define MAX_TIME_PRECISION 6

static inline DateADT
DatumGetDateADT(Datum X)
{
	return (DateADT) X;
}

static inline TimeADT
DatumGetTimeADT(Datum X)
{
	return (TimeADT) X;
}

static inline TimeTzADT *
DatumGetTimeTzADTP(Datum X)
{
	return (TimeTzADT *) DatumGetPointer(X);
}

static inline Datum
DateADTGetDatum(DateADT X)
{
	return (Datum) X;
}

static inline Datum
TimeADTGetDatum(TimeADT X)
{
	return (Datum) X;
}

static inline Datum
TimeTzADTPGetDatum(const TimeTzADT *X)
{
	return PointerGetDatum(X);
}

/* datetime carve sentinels (see header comment) */
extern void AdjustTimeForTypmod(TimeADT *time, int32 typmod);
extern int32 anytime_typmod_check(bool istz, int32 typmod);
extern int32 date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2);
extern int32 date_cmp_timestamptz_internal(DateADT dateVal, TimestampTz dt2);
#endif
