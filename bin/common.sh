VERSION=0.0.1

BASE=$BIN/../
HADOOP=$HADOOP_HOME/bin/hadoop
if test ! -f $HADOOP; then
    HADOOP=`which hadoop`
    if test ! -f $HADOOP; then
	echo Missing hadoop executable, please put it in the path or set the HADOOP_HOME variable.
	exit
    fi
fi
