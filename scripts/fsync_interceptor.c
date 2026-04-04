/*
 * fsync_interceptor.c — DYLD_INSERT_LIBRARIES shim for macOS.
 *
 * Intercepts fdatasync, fsync, and fcntl(F_FULLFSYNC) calls, resolves the
 * file path via fcntl(F_GETPATH), and writes a log line to stderr:
 *
 *   [sync] fdatasync           fd=5 path=/tmp/.../pg_wal/wal.log
 *   [sync] fcntl(F_FULLFSYNC)  fd=5 path=/tmp/.../pg_wal/wal.log
 *
 * Why fcntl(F_FULLFSYNC)?  Rust's File::sync_data() on macOS calls
 * fcntl(F_FULLFSYNC) instead of fdatasync() because APFS/HFS+ require
 * F_FULLFSYNC to guarantee that data reaches stable storage.
 *
 * Build:
 *   cc -dynamiclib -o scripts/fsync_interceptor.dylib scripts/fsync_interceptor.c
 *
 * Use:
 *   DYLD_INSERT_LIBRARIES=scripts/fsync_interceptor.dylib \
 *       target/debug/wal_syscall_check
 */

#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/syslimits.h>
#include <unistd.h>

#ifndef F_FULLFSYNC
#define F_FULLFSYNC 51
#endif

/* -------------------------------------------------------------------------- */
/* Cached real function pointers                                               */
/* -------------------------------------------------------------------------- */

typedef int (*fcntl_fn)(int, int, ...);
typedef int (*fdatasync_fn)(int);
typedef int (*fsync_fn)(int);

static fcntl_fn   real_fcntl    = NULL;
static fdatasync_fn real_fdatasync = NULL;
static fsync_fn   real_fsync    = NULL;

__attribute__((constructor))
static void interceptor_init(void) {
    real_fcntl     = (fcntl_fn)   dlsym(RTLD_NEXT, "fcntl");
    real_fdatasync = (fdatasync_fn)dlsym(RTLD_NEXT, "fdatasync");
    real_fsync     = (fsync_fn)   dlsym(RTLD_NEXT, "fsync");
    fprintf(stderr, "[interceptor] loaded — watching fdatasync / fsync / fcntl(F_FULLFSYNC)\n");
    fflush(stderr);
}

/* -------------------------------------------------------------------------- */
/* Path resolution                                                             */
/* -------------------------------------------------------------------------- */

static void resolve_path(int fd, char *buf) {
    /* Call the real fcntl directly — never re-enters our override. */
    if (!real_fcntl || real_fcntl(fd, F_GETPATH, buf) != 0) {
        strcpy(buf, "(unknown)");
    }
}

static void log_sync(const char *call, int fd) {
    char path[PATH_MAX];
    resolve_path(fd, path);
    fprintf(stderr, "[sync] %-22s fd=%-3d path=%s\n", call, fd, path);
    fflush(stderr);
}

/* -------------------------------------------------------------------------- */
/* Interceptors                                                                */
/* -------------------------------------------------------------------------- */

int fdatasync(int fd) {
    log_sync("fdatasync", fd);
    return real_fdatasync ? real_fdatasync(fd) : -1;
}

int fsync(int fd) {
    log_sync("fsync", fd);
    return real_fsync ? real_fsync(fd) : -1;
}

/*
 * fcntl is variadic.  We capture one pointer-sized argument so we can
 * forward it to the real implementation unchanged for every command.
 * For commands that take no argument (F_FULLFSYNC, F_GETFL, …) the
 * forwarded value is harmlessly ignored by the kernel.
 */
int fcntl(int fd, int cmd, ...) {
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);

    if (cmd == F_FULLFSYNC) {
        log_sync("fcntl(F_FULLFSYNC)", fd);
    }

    return real_fcntl ? real_fcntl(fd, cmd, arg) : -1;
}
