/* SHIM utils/datetime.h — MAXDATELEN + JsonEncodeDateTime decl; the
 * jbvDatetime arm is unreachable here (abort-stub in driver TU). */
#ifndef PG_JSONBFAM_SHIM_DATETIME_H
#define PG_JSONBFAM_SHIM_DATETIME_H
#define MAXDATELEN 128
#endif
