#ifndef CREF_REGEX_MISCADMIN_H
#define CREF_REGEX_MISCADMIN_H

extern bool stack_is_too_deep(void);

/* FUZZ-VENDOR ADDITION (p1-lanew): regexport.c calls check_stack_depth()
 * (upstream miscadmin.h; ereport(ERROR) when too deep). The fuzz oracle has
 * no ereport; the abort matches the shim postgres.h's ereport arm, and the
 * export walk depth is bounded by the (capped) NFA size so it never fires. */
extern void check_stack_depth(void);

#endif
