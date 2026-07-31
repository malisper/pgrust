/* shim: DateADT lives in the datetime.h shim */
#ifndef FMTV_DATE_H
#define FMTV_DATE_H
#include "utils/datetime.h"
#define MAX_TIME_PRECISION 6
#define PG_RETURN_DATEADT(x) return Int32GetDatum(x)
#define PG_RETURN_TIMEADT(x) return Int64GetDatum(x)
extern bool time_overflows(int hour, int min, int sec, fsec_t fsec);
#endif
