#!/bin/bash
#
# Author: Ambud Sharma
#
# Sidewinder environment scripts
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source /etc/environment

export CHECK_UPGRADE="true"

function getCurrentVersion() {
  export VERSION=`rpm -qa | grep sidewinder |  awk -F'-' '{ print $3 }'`
  echo $VERSION
}

function getLatestVersion() {
  export NEW_VERSION=`curl -s "http://search.maven.org/solrsearch/select?q=sidewinder-core&rows=20&wt=json" | $DIR/jq -r '.response.docs[0].latestVersion'`
  echo $NEW_VERSION
}

function versionToNumber() {
   echo `echo $1 | awk -F'.' '{ print $1*100+$2*10+$3*1 }'`
}

function checkUpdate() {
	if [[ "$CHECK_UPGRADE" == "true" ]];then
		echo "Checking for upgrades"
		VERSION=`getCurrentVersion`
		NEW_VERSION=`getLatestVersion`
		V1=`versionToNumber $VERSION`
		V2=`versionToNumber $NEW_VERSION`
		if [[ $V2 -gt $V1 ]];then
			echo "New version($V2) of Sidewinder available for download"
		fi
	fi
}

checkUpdate

export SIDEWINDER_CONF=/etc/sidewinder
export SIDEWINDER_HOME=/usr/sidewinder

if [ -z $JAVA_HOME ]; then
        echo "JAVA_HOME missing exporting to /usr/java/default"
        export JAVA_HOME=/usr/java/default
fi