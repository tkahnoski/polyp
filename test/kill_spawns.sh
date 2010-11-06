#!/bin/sh

DIR=`dirname $0`

ps aux | grep 'polyp_server' | grep -v 'grep' | cut -f 3 -d ' ' | xargs kill && rm -f "$DIR/test.lst"
