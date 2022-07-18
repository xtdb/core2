#!/usr/bin/env bash

CORE2_PATH=$(realpath $(dirname $0)/..)

rm -r $CORE2_PATH/_manifest/

(
  cd $CORE2_PATH
  ./sbom-tool-osx-x64 generate -b . -bc . -pn "core2" -pv "0.1.0" -nsb "https://zig"
)

