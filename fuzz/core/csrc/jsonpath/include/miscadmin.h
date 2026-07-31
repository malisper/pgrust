/* SHIM header for the jsonpath_diff oracle - NOT PostgreSQL code (plumbing only, never logic). */
#ifndef MISCADMIN_H
#define MISCADMIN_H
#include "postgres.h"
/* The fuzz driver caps input length, bounding recursion far below either
 * side's real guard; both hooks are documented no-ops (carve in the driver
 * header). */
static inline void check_stack_depth(void) {}
static inline bool stack_is_too_deep(void) { return false; }
#define CHECK_FOR_INTERRUPTS() ((void) 0)
#define INTERRUPTS_PENDING_CONDITION() (false)
#endif
