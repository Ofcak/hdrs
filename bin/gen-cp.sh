#!/bin/bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hdrs-config.sh

mvn -f "${HDRS_HOME}/pom.xml" dependency:build-classpath -Dmdep.outputFile="${HDRS_HOME}/target/classpath.txt"

