#!/usr/bin/env sh
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: build-native.sh CLASSES_DIR OUTPUT_DIR" >&2
    exit 2
fi
if [ -z "${GRAALVM_HOME:-}" ]; then
    echo "GRAALVM_HOME must point to a GraalVM JDK 22 installation" >&2
    exit 2
fi

classes_dir=$(cd "$1" && pwd)
mkdir -p "$2"
output_dir=$(cd "$2" && pwd)

cd "$output_dir"
export MACOSX_DEPLOYMENT_TARGET=11.0
extra_linker=""
if [ "$(uname)" = "Darwin" ]; then
    extra_linker="-H:NativeLinkerOption=-mmacosx-version-min=11.0"
fi
"$GRAALVM_HOME/bin/native-image" --shared -O3 --no-fallback -o libnodusdb -cp "$classes_dir" $extra_linker
if [ "$(uname)" = "Darwin" ]; then
    minos=$(otool -l libnodusdb.dylib | awk '/minos/ {print $2}' | sort -u)
    if [ "$minos" != "11.0" ]; then
        echo "libnodusdb.dylib minos is '$minos', expected 11.0" >&2
        exit 1
    fi
fi
cp libnodusdb.h nodusdb.h
