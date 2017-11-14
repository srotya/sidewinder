#!/bin/bash
#
# Author: Ambud Sharma
#
# 

source /etc/sidewinder/sidewinder-env.sh

$JAVA_HOME/bin/java $JAVA_OPTS -cp $SIDEWINDER_HOME/lib/*.jar com.srotya.sidewinder.cluster.SidewinderClusteredServer server $SIDEWINDER_CONF/config.yaml