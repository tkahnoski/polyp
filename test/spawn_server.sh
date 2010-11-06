#!/bin/sh

TEST_DIR=`dirname $0`
cd $TEST_DIR/..
python polyp_server.py -g "test/test.lst" -b -p $1 > "test/polyp.$1.log" 2>&1
