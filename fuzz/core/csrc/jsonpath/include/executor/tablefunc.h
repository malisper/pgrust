/* SHIM header for the jsonpathexec_diff oracle - NOT PostgreSQL code.
 * TableFuncRoutine VERBATIM from src/include/executor/tablefunc.h @ 18.3
 * (comments elided); TableFuncScanState's shim shape is in
 * nodes/execnodes.h. */
#ifndef _TABLEFUNC_H
#define _TABLEFUNC_H
#include "postgres.h"

struct TableFuncScanState;

typedef struct TableFuncRoutine
{
	void		(*InitOpaque) (struct TableFuncScanState *state, int natts);
	void		(*SetDocument) (struct TableFuncScanState *state, Datum value);
	void		(*SetNamespace) (struct TableFuncScanState *state, const char *name,
								 const char *uri);
	void		(*SetRowFilter) (struct TableFuncScanState *state, const char *path);
	void		(*SetColumnFilter) (struct TableFuncScanState *state,
									const char *path, int colnum);
	bool		(*FetchRow) (struct TableFuncScanState *state);
	Datum		(*GetValue) (struct TableFuncScanState *state, int colnum,
							 Oid typid, int32 typmod, bool *isnull);
	void		(*DestroyOpaque) (struct TableFuncScanState *state);
} TableFuncRoutine;
#endif
