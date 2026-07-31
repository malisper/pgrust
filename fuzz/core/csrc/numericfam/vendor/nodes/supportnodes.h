#ifndef NV_SUPPORTNODES_H
#define NV_SUPPORTNODES_H
typedef struct SupportRequestSimplify
{
	NodeTag		type;
	void	   *root;
	FuncExpr   *fcall;
} SupportRequestSimplify;
typedef struct SupportRequestRows
{
	NodeTag		type;
	void	   *root;
	Oid			funcid;
	Node	   *node;
	double		rows;
} SupportRequestRows;
#endif
