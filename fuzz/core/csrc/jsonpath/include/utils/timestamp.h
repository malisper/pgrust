/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Typedefs/constants VERBATIM shapes from src/include/datatype/timestamp.h
 * @ 18.3; every function declared here is part of the driver-level datetime
 * carve and stubs to a LOUD ABORT in pg_jsonpath_exec_env.c. */
#ifndef TIMESTAMP_H
#define TIMESTAMP_H
#include "postgres.h"
#include "fmgr.h"

typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeOffset;
typedef int32 fsec_t;

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* origin 1, not 0! */
	int			tm_year;		/* relative to 1900 */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	const char *tm_zone;		/* NULL if not known */
};

#define USECS_PER_DAY	INT64CONST(86400000000)
#define USECS_PER_HOUR	INT64CONST(3600000000)
#define USECS_PER_MINUTE INT64CONST(60000000)
#define USECS_PER_SEC	INT64CONST(1000000)

#define DatumGetTimestamp(X)  ((Timestamp) (X))
#define DatumGetTimestampTz(X)	((TimestampTz) (X))
#define TimestampGetDatum(X) ((Datum) (X))
#define TimestampTzGetDatum(X) ((Datum) (X))

extern void AdjustTimestampForTypmod(Timestamp *time, int32 typmod,
									 struct Node *escontext);
extern int32 anytimestamp_typmod_check(bool istz, int32 typmod);
extern Datum timestamp_cmp(FunctionCallInfo fcinfo);
extern Datum timestamp_tz(FunctionCallInfo fcinfo);
extern Datum timestamp_date(FunctionCallInfo fcinfo);
extern Datum timestamp_time(FunctionCallInfo fcinfo);
extern Datum timestamp_timestamptz(FunctionCallInfo fcinfo);
extern Datum timestamptz_date(FunctionCallInfo fcinfo);
extern Datum timestamptz_time(FunctionCallInfo fcinfo);
extern Datum timestamptz_timetz(FunctionCallInfo fcinfo);
extern Datum timestamptz_timestamp(FunctionCallInfo fcinfo);
extern int32 timestamp_cmp_timestamp_internal(Timestamp dt1, Timestamp dt2);
extern int32 timestamp_cmp_timestamptz_internal(Timestamp timestampVal,
												TimestampTz dt2);
#endif
