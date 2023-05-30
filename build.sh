#! /bin/bash

set -x

pushd build/
rm -rf *
cmake ..
make
cp edgefs ../
cp centerserver ../
popd