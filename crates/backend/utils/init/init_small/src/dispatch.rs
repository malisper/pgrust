//! The `postgres` subprogram dispatch table (main.c DispatchOption /
//! DispatchOptionNames / parse_dispatch_option). It lives below main_main
//! because postmaster.c consults it too (the "--%s must be first argument"
//! check, postmaster.c:624) and the postmaster crate sits under main_main.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchOption {
    Check,
    Boot,
    Forkchild,
    DescribeConfig,
    Single,
    // pgrust extension (no C counterpart): one wire-protocol session over
    // the boot-installed stdio transport provider (§2.4 seam). The
    // wasm32-wasip1 client-server mode — WASI p1 has no socket(); native
    // --stdio-wire is the differential arm.
    StdioWire,
    // pgrust extension (P4 sim-net, `--cfg pgrust_sim` builds only): one
    // deterministic wire-protocol session over the in-memory sim-net
    // transport pair, driven by the in-process scripted client.
    #[cfg(pgrust_sim)]
    SimNet,
    Postmaster,
}

const DISPATCH_OPTION_NAMES: &[(DispatchOption, &str)] = &[
    (DispatchOption::Check, "check"),
    (DispatchOption::Boot, "boot"),
    (DispatchOption::Forkchild, "forkchild"),
    (DispatchOption::DescribeConfig, "describe-config"),
    (DispatchOption::Single, "single"),
    (DispatchOption::StdioWire, "stdio-wire"),
    #[cfg(pgrust_sim)]
    (DispatchOption::SimNet, "sim-net"),
];

pub fn parse_dispatch_option(name: &str) -> DispatchOption {
    for &(option, option_name) in DISPATCH_OPTION_NAMES {
        // "forkchild" is EXEC_BACKEND-only (prefix-matched there); never built here.
        if option == DispatchOption::Forkchild {
            continue;
        }
        if option_name == name {
            return option;
        }
    }
    DispatchOption::Postmaster
}
