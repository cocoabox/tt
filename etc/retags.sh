#!/bin/bash
pushd ..
ctags --verbose=yes --tag-relative=yes -R -o ./tags .
popd
