/*
 * SHIM storage/bufmgr.h — NOT PostgreSQL code. access/bufmask.h pulls the
 * real bufmgr.h only for the Page/Block typedefs it re-exports; the vendored
 * verbatim storage/bufpage.h (../include) provides everything bufmask.c
 * touches. Plumbing only.
 */
#ifndef PG_DIFFFUZZ_PORTFAM_SHIM_BUFMGR_H
#define PG_DIFFFUZZ_PORTFAM_SHIM_BUFMGR_H

#include "storage/bufpage.h"

#endif
