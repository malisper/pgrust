#!/bin/bash
set -euo pipefail

sysroot=$(rustc --print sysroot)
host=$(rustc -vV | sed -n 's/^host: //p')
lld="$sysroot/lib/rustlib/$host/bin/rust-lld"
sdkroot=$(xcrun --sdk macosx --show-sdk-path)

if [[ ! -x "$lld" ]]; then
    echo "rust-lld not found at $lld" >&2
    exit 1
fi

args=("-syslibroot" "$sdkroot")

while (($#)); do
    case "$1" in
        -mmacosx-version-min=*)
            version="${1#*=}"
            args+=("-platform_version" "macos" "$version" "$version")
            ;;
        -Wl,*)
            linker_args="${1#-Wl,}"
            while [[ "$linker_args" == *,* ]]; do
                args+=("${linker_args%%,*}")
                linker_args="${linker_args#*,}"
            done
            args+=("$linker_args")
            ;;
        -dynamiclib)
            args+=("-dylib")
            ;;
        -nodefaultlibs)
            ;;
        -o|-arch|-L|-F|-l|-rpath|-syslibroot)
            args+=("$1" "$2")
            shift
            ;;
        *)
            args+=("$1")
            ;;
    esac
    shift
done

exec "$lld" -flavor darwin "${args[@]}"
