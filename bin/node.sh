#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

if [ ! -f $HDRS_HOME/target/classpath.txt ]; then
  . "$bin"/gen-cp.sh
fi

CLASSPATH=`cat "${HDRS_HOME}/target/classpath.txt"`
CLASSPATH=${CLASSPATH}:$HDRS_HOME/conf:$HDRS_HOME/target/classes

java -Xmx2048m -cp "$CLASSPATH" de.hpi.fgis.hdrs.node.NodeLauncher "$@"
