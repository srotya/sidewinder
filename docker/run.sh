#!/bin/bash
#
# Author: Ambud Sharma
#

envsubst < /opt/sidewinder/template.properties > /opt/sidewinder/config.properties

java -jar /usr/local/sidewinder/sidewinder.jar server /opt/sidewinder/config.yaml
