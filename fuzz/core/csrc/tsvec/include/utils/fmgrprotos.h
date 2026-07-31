/* SHIM utils/fmgrprotos.h (tsvec oracle) — NOT PostgreSQL code.
 * Prototypes for the vendored SQL-callable functions (subset). */
#ifndef PG_DIFFFUZZ_TSVEC_FMGRPROTOS_H
#define PG_DIFFFUZZ_TSVEC_FMGRPROTOS_H
extern Datum tsvectorin(FunctionCallInfo fcinfo);
extern Datum tsvectorout(FunctionCallInfo fcinfo);
extern Datum tsvectorsend(FunctionCallInfo fcinfo);
extern Datum tsvectorrecv(FunctionCallInfo fcinfo);
extern Datum ts_match_vq(FunctionCallInfo fcinfo);
extern Datum to_tsvector(FunctionCallInfo fcinfo);	/* decl only; carved */
extern Datum to_tsquery(FunctionCallInfo fcinfo);	/* decl only; carved */
extern Datum plainto_tsquery(FunctionCallInfo fcinfo);	/* decl only */
#endif
