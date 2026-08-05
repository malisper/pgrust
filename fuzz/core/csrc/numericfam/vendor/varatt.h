/* shim: extra fcinfo accessor macros for the formatting TU (varlena
 * accessors themselves live in the vendored postgres.h) */
#ifndef FMTV_VARATT_H
#define FMTV_VARATT_H
#define PG_GETARG_TEXT_PP(n) ((text *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_GETARG_TIMESTAMP(n) ((Timestamp) DatumGetInt64(PG_GETARG_DATUM(n)))
#define PG_GETARG_INTERVAL_P(n) ((Interval *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_RETURN_TEXT_P(x) return PointerGetDatum(x)
#define PG_RETURN_TIMESTAMP(x) return Int64GetDatum(x)
#define PG_GET_COLLATION() (fcinfo->fncollation)
#define TimestampGetDatum(X) Int64GetDatum(X)
#endif
#ifndef OidIsValid
#define OidIsValid(objectId) ((bool) ((objectId) != InvalidOid))
#endif
