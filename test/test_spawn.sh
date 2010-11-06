#!/bin/sh

DIR=`dirname $0`
ports="9990
9991
9992
9993
9999" 

for p in $ports;
do
	echo "Starting on port: $p"
	echo "localhost:$p" >> $DIR/test.lst
	sh $DIR/spawn_server.sh $p &
done
