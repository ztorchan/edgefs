#! /bin/bash

set -x

pushd build/
rm -rf *
cmake -DCMAKE_BUILD_TYPE=Release ..
make
cp edgefs ../
cp centerserver ../
popd
