#!/bin/bash
set -e

cargo build --release
sudo dtrace -x ustackframes=100 -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' -c './target/release/bench_insert --rows 100000' -o /tmp/dtrace_insert_stacks.out
echo "Done. Output in /tmp/dtrace_insert_stacks.out"
