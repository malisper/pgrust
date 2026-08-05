/*
 * SHIM pg_config_os.h — platform chooser only; both included files are
 * VERBATIM vendored src/include/port/{darwin,linux}.h (Stamp-18.3).
 */
#if defined(__APPLE__)
#include "port/darwin.h"
#elif defined(__linux__)
#include "port/linux.h"
#else
#error "nodesfam oracle: unsupported platform (vendor the port header)"
#endif
