#!/bin/bash
set -e

sudo rm -rf /tmp/pgrust_flamegraph_bench
./target/release/full_scan_bench --dir /tmp/pgrust_flamegraph_bench --rows 10000 --iterations 1 --pool-size 1000
sudo dtrace -x ustackframes=100 -n 'profile-997 /pid == $target/ { @[ustack()] = count(); }' -c './target/release/full_scan_bench --preserve-existing --skip-load --dir /tmp/pgrust_flamegraph_bench --rows 10000 --iterations 100 --pool-size 1000' -o /tmp/dtrace_stacks.out
echo "Done. Output in /tmp/dtrace_stacks.out"
