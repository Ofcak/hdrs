#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hdrs-config.sh

if [ ! -f $HDRS_HOME/target/classpath.txt ]; then
  . "$bin"/gen-cp.sh
fi

CLASSPATH=`cat "${HDRS_HOME}/target/classpath.txt"`
CLASSPATH=${CLASSPATH}:$HDRS_HOME/target/classes

java -cp "$CLASSPATH" de.hpi.fgis.hdrs.Shell ${HDRS_HOME}/conf/peers
