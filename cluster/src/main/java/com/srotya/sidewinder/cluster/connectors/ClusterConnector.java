/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.cluster.connectors;

import java.util.Map;

import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;

/**
 * @author ambud
 */
public abstract class ClusterConnector {

	public abstract void init(Map<String, String> conf) throws Exception;

	public abstract void initializeRouterHooks(final RoutingEngine engine) throws Exception;

	public abstract int getClusterSize() throws Exception;

	public abstract boolean isBootstrap();

	public abstract boolean isLeader();

	public abstract Object fetchRoutingTable();

	public abstract void updateTable(Object table) throws Exception;

	public abstract Node getLocalNode();

}
