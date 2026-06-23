//! `help` from `main.c`.

use std::fmt::Write as _;

/// `PACKAGE_BUGREPORT` (`pg_config.h`).
const PACKAGE_BUGREPORT: &str = "pgsql-bugs@lists.postgresql.org";
/// `PACKAGE_NAME` (`pg_config.h`).
const PACKAGE_NAME: &str = "PostgreSQL";
/// `PACKAGE_URL` (`pg_config.h`).
const PACKAGE_URL: &str = "https://www.postgresql.org/";

/// `help(progname)` (main.c): the `--help` text. The C version `printf`s each
/// line to stdout; this builds and returns the same text so the caller decides
/// where it goes (matching `GucInfoMain`'s render-and-return shape).
///
/// Should match the options accepted by `PostmasterMain()` and `PostgresMain()`.
/// The `USE_SSL`-gated `-l` line is included (SSL is built here).
pub fn help(progname: &str) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "{progname} is the PostgreSQL server.\n");
    let _ = writeln!(s, "Usage:\n  {progname} [OPTION]...\n");
    let _ = writeln!(s, "Options:");
    let _ = writeln!(s, "  -B NBUFFERS        number of shared buffers");
    let _ = writeln!(s, "  -c NAME=VALUE      set run-time parameter");
    let _ = writeln!(s, "  -C NAME            print value of run-time parameter, then exit");
    let _ = writeln!(s, "  -d 1-5             debugging level");
    let _ = writeln!(s, "  -D DATADIR         database directory");
    let _ = writeln!(s, "  -e                 use European date input format (DMY)");
    let _ = writeln!(s, "  -F                 turn fsync off");
    let _ = writeln!(s, "  -h HOSTNAME        host name or IP address to listen on");
    let _ = writeln!(s, "  -i                 enable TCP/IP connections (deprecated)");
    let _ = writeln!(s, "  -k DIRECTORY       Unix-domain socket location");
    // #ifdef USE_SSL
    let _ = writeln!(s, "  -l                 enable SSL connections");
    let _ = writeln!(s, "  -N MAX-CONNECT     maximum number of allowed connections");
    let _ = writeln!(s, "  -p PORT            port number to listen on");
    let _ = writeln!(s, "  -s                 show statistics after each query");
    let _ = writeln!(s, "  -S WORK-MEM        set amount of memory for sorts (in kB)");
    let _ = writeln!(s, "  -V, --version      output version information, then exit");
    let _ = writeln!(s, "  --NAME=VALUE       set run-time parameter");
    let _ = writeln!(s, "  --describe-config  describe configuration parameters, then exit");
    let _ = writeln!(s, "  -?, --help         show this help, then exit");

    let _ = writeln!(s, "\nDeveloper options:");
    let _ = writeln!(s, "  -f s|i|o|b|t|n|m|h forbid use of some plan types");
    let _ = writeln!(s, "  -O                 allow system table structure changes");
    let _ = writeln!(s, "  -P                 disable system indexes");
    let _ = writeln!(s, "  -t pa|pl|ex        show timings after each query");
    let _ = writeln!(s, "  -T                 send SIGABRT to all backend processes if one dies");
    let _ = writeln!(s, "  -W NUM             wait NUM seconds to allow attach from a debugger");

    let _ = writeln!(s, "\nOptions for single-user mode:");
    let _ = writeln!(s, "  --single           selects single-user mode (must be first argument)");
    let _ = writeln!(s, "  DBNAME             database name (defaults to user name)");
    let _ = writeln!(s, "  -d 0-5             override debugging level");
    let _ = writeln!(s, "  -E                 echo statement before execution");
    let _ = writeln!(s, "  -j                 do not use newline as interactive query delimiter");
    let _ = writeln!(s, "  -r FILENAME        send stdout and stderr to given file");

    let _ = writeln!(s, "\nOptions for bootstrapping mode:");
    let _ = writeln!(s, "  --boot             selects bootstrapping mode (must be first argument)");
    let _ = writeln!(s, "  --check            selects check mode (must be first argument)");
    let _ = writeln!(
        s,
        "  DBNAME             database name (mandatory argument in bootstrapping mode)"
    );
    let _ = writeln!(s, "  -r FILENAME        send stdout and stderr to given file");

    let _ = writeln!(
        s,
        "\nPlease read the documentation for the complete list of run-time\n\
         configuration settings and how to set them on the command line or in\n\
         the configuration file.\n\n\
         Report bugs to <{PACKAGE_BUGREPORT}>."
    );
    let _ = writeln!(s, "{PACKAGE_NAME} home page: <{PACKAGE_URL}>");
    s
}
