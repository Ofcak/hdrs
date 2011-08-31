#!/bin/bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hdrs-config.sh

. "$bin"/node.sh start ${HDRS_HOME}/conf/indexes ${HDRS_HOME}/conf/peers `cat "${HDRS_HOME}"/conf/node`

