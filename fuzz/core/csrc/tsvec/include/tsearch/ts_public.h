/* SHIM tsearch/ts_public.h (tsvec oracle) — NOT PostgreSQL code.
 * ts_utils.h includes it for headline/dictionary types that only appear
 * in DECLARATIONS of functions this oracle never defines or calls. */
#ifndef PG_DIFFFUZZ_TSVEC_TS_PUBLIC_H
#define PG_DIFFFUZZ_TSVEC_TS_PUBLIC_H
typedef struct HeadlineParsedText HeadlineParsedText;
typedef struct TSQueryParserStateData *TSQueryParserState;
#endif
