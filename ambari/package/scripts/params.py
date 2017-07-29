#!/usr/bin/env python
"""
Copyright 2017 Ambud Sharma

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Sidewinder Params configurations
"""

import os
from resource_management.libraries.functions import format
from resource_management.libraries.script.script import Script
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.get_stack_version import get_stack_version
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import conf_select

# server configurations
config = Script.get_config()

sidewinder_home = '/usr/sidewinder'
sidewinder_bin = '/usr/sidewinder/bin/'
sidewinder_user = 'sidewinder'
conf_dir = "/etc/sidewinder"
pid_dir = '/var/run/sidewinder'
pid_file = '/var/run/sidewinder/sidewinder.pid'

# Environment properties
log_dir = config['configurations']['sidewinder-env']['log_dir']
hostname = config['hostname']
java64_home = config['hostLevelParams']['java_home']
sidewinder_env_sh_template = config['configurations']['sidewinder-env']['content']

# YAML properties
http_port = config['configurations']['sidewinder-yaml']['http_port']

# Basic properties
data_dir = config['configurations']['sidewinder-props']['data.dir']
index_dir = config['configurations']['sidewinder-props']['index.dir']
max_open_files = config['configurations']['sidewinder-props']['max_open_files']
