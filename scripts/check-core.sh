#!/usr/bin/env bash

set -euo pipefail

# Keep the production server buildable from the lockfile.
cargo check --locked --bin postgres

# Fast coverage for the core paths guarded below.
cargo test --locked -p coerce -p clauses -p planner -p utility \
    -p nodes_core -p outfuncs -p catalog_dependency -p catalog_objectaddress -p tablecmds \
    -p spgist -p nodeworktablescan -p nodefunctionscan -p nodetablefuncscan -p nodeagg \
    -p execmain -p tcop_dest -p tuplesort -p indexam -p execexpr \
    -p parse_expr -p parse_collate -p parse_target -p ruleutils -p parse_utilcmd

# The workspace still has legacy warnings. Turn the classes fixed by this
# change into hard errors only for the crates that are clean today, so new
# regressions cannot silently reappear.
cargo rustc --locked -p coerce --lib -- -D unreachable-patterns
cargo rustc --locked -p clauses --lib -- -D unreachable-patterns
cargo rustc --locked -p utility --lib -- -D unreachable-patterns
cargo rustc --locked -p planner --lib -- -D unused-must-use -D unreachable-patterns
cargo rustc --locked -p nodes_core --lib -- -D unreachable-patterns
cargo rustc --locked -p outfuncs --lib -- -D unreachable-patterns
cargo rustc --locked -p catalog_dependency --lib -- -D unreachable-patterns
cargo rustc --locked -p catalog_objectaddress --lib -- -D unreachable-patterns
cargo rustc --locked -p tablecmds --lib -- -D unreachable-patterns
cargo rustc --locked -p spgist --lib -- -D unused-must-use
cargo rustc --locked -p nodeworktablescan --lib -- -D unused-must-use
cargo rustc --locked -p nodefunctionscan --lib -- -D unused-must-use
cargo rustc --locked -p nodetablefuncscan --lib -- -D unused-must-use
cargo rustc --locked -p nodeagg --lib -- -D unused-must-use
cargo rustc --locked -p tcop_dest --lib -- -D unreachable-patterns
cargo rustc --locked -p tuplesort --lib -- -D unreachable-patterns -D unused-attributes
cargo rustc --locked -p indexam --lib -- -D unreachable-patterns
cargo rustc --locked -p execexpr --lib -- -D unreachable-patterns -D unused-attributes
cargo rustc --locked -p parse_expr --lib -- -D unreachable-patterns
cargo rustc --locked -p parse_collate --lib -- -D unreachable-patterns
cargo rustc --locked -p parse_target --lib -- -D unreachable-patterns
cargo rustc --locked -p ruleutils --lib -- -D unreachable-patterns
cargo rustc --locked -p parse_utilcmd --lib -- -D unused-attributes
