
#
# this script sets HDRS_HOME etc.
# (modelled after hbase-config.sh)

this="${BASH_SOURCE-$0}"

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the hdrs installation
if [ -z "$HDRS_HOME" ]; then
  export HDRS_HOME=`dirname "$this"`/..
fi
