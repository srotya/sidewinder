#!/bin/bash
#
# Author: Ambud Sharma
#
# Purpose: Run telegraf with the supplied configuration
#

type telegraf >/dev/null 2>&1 || { echo >&2 "telegraf command doesn't exist, please ensure telegraf is installed; exiting...."; exit 1; }

telegraf -config ./telegraf.conf
