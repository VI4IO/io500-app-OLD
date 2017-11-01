#!/bin/bash -e
# This script builds the optional parallel find

CC=mpicc
CFLAGS="-I../src/ -g -O2 -fstack-protector-all -Wextra"

rm *.o 2>&1 || true

echo "Building parallel find"

$CC $CFLAGS -Wall -I ../ior-1/src -c io500-find-main.c || exit 1
$CC $CFLAGS -Wall -I ../ior-1/src -c io500-options.c || exit 1
$CC $CFLAGS -Wall -I ../ior-1/src -c ../src/io500-functions.c || exit 1
$CC $CFLAGS -Wall -I ../ior-1/src -c ../src/io500-utils.c || exit 1
$CC $CFLAGS -Wall -I ../ior-1/src -I ../libcircle/libcircle/ -c ../src/io500-find.c || exit 1
$CC $CFLAGS -o pfind ../ior-1/*.o *.o ../libcircle/.libs/libcircle.a -lm  || exit 1

echo "[OK]"
