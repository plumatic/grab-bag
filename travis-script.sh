#!/bin/bash
set -ev

pushd lib/plumbing && lein test-all
popd
