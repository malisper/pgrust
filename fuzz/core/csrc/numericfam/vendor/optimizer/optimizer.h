#ifndef NV_OPTIMIZER_H
#define NV_OPTIMIZER_H
extern Node *relabel_to_typmod(Node *expr, int32 typmod);
extern bool is_funcclause(const void *clause);
extern Node *estimate_expression_value(void *root, Node *node);
#define lthird(l) ((l)->elements[2])
#endif
