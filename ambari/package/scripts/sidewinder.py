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
sidewinder service params

"""

from resource_management import *
from resource_management.core.resources.system import Directory, Execute, File, Link
import sys
import os, errno
from copy import deepcopy

def sidewinder():
    import params
    config = Script.get_config()

    params.data_dir = params.data_dir.replace('"','')
    data_path = params.data_dir.replace(' ','').split(',')
    data_path[:]=[x.replace('"','') for x in data_path]

    directories = [params.pid_dir, params.conf_dir, params.index_dir]
    directories = directories+data_path;
    
    for d in directories:
    	     mkdir_p(d)

    Directory(directories,
              owner=params.sidewinder_user,
              group=params.sidewinder_user
          )

    File(format("{conf_dir}/sidewinder-env.sh"),
          owner=params.sidewinder_user,
          content=InlineTemplate(params.sidewinder_env_sh_template)
     )
     
    if params.cluster!=True: 
    		cmd_launch = "sed -i 's/com.srotya.sidewinder.cluster.SidewinderClusteredServer/com.srotya.sidewinder.core.SidewinderServer/g' /usr/sidewinder/bin/sidewinder.sh"
    		Execute(cmd_launch)
    		

    props = mutable_config_dict(config['configurations']['sidewinder-props'])
    props = add_configs(props, config['configurations']['sidewinder-advanced-props'])

    PropertiesFile("sidewinder.properties",
                      dir=params.conf_dir,
                      properties=props,
                      owner=params.sidewinder_user,
                      group=params.sidewinder_user,
    )

    yyaml = config['configurations']['sidewinder-yaml']

    File(format("{conf_dir}/config.yaml"),
       content=Template(
       "config.yaml.j2", yyaml = yyaml),
       owner=params.sidewinder_user,
       group=params.sidewinder_user
    )

def add_configs(server_config, sidewinder_config):
    for key, value in sidewinder_config.iteritems():
        server_config[key] = value
    return server_config

def mutable_config_dict(sidewinder_config):
    server_config = {}
    for key, value in sidewinder_config.iteritems():
        server_config[key] = value
    return server_config

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise