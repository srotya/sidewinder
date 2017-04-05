#!/bin/bash
#
# Author: Ambud Sharma
#

envsubst < /opt/sidewinder/template.properties > /opt/sidewinder/config.properties

java -cp /usr/local/sidewinder/sidewinder.jar com.srotya.sidewinder.core.SidewinderServer server /opt/sidewinder/config.yaml
