/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * Only the surface the vendored TUs compile against. The DATETIME METHOD
 * FAMILY IS CARVED AT THE DRIVER LEVEL (session-timezone state): every
 * function declared here is a LOUD ABORT sentinel stub in
 * pg_jsonpath_exec_env.c — a carved input escaping the driver filter is a
 * harness bug, never a silent divergence. Constants VERBATIM from
 * src/include/utils/datetime.h @ 18.3. */
#ifndef DATETIME_H
#define DATETIME_H
#include "postgres.h"
#include "utils/timestamp.h"

#define MAXDATELEN		128

/* pg_tz is opaque here; session_timezone is a sentinel NULL */
typedef struct pg_tz pg_tz;
extern pg_tz *session_timezone;

#define POSTGRES_EPOCH_JDATE 2451545	/* == date2j(2000, 1, 1) */
extern void j2date(int jd, int *year, int *month, int *day);
extern int	DetermineTimeZoneOffset(struct pg_tm *tm, pg_tz *tzp);
extern int	timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm,
						 fsec_t *fsec, const char **tzn, pg_tz *attimezone);
#endif
