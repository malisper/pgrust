/* shim: soft-error surface; fmt lanes never pass an escontext */
#ifndef FMTV_MISCNODES_H
#define FMTV_MISCNODES_H
typedef struct ErrorSaveContext
{
	NodeTag		type;
	bool		error_occurred;
	bool		details_wanted;
	void	   *error_data;
} ErrorSaveContext;
#define SOFT_ERROR_OCCURRED(escontext) \
	((escontext) != NULL && IsA(escontext, ErrorSaveContext) && \
	 ((ErrorSaveContext *) (escontext))->error_occurred)
#define errsave(escontext, ...) bench_elog_abort(__FILE__, __LINE__)
#endif
