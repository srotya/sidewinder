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
package com.srotya.sidewinder.cluster.routing.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.cluster.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * {@link RoutingEngine} implementation for a Leader Follower design for
 * Sidewinder cluster.
 * 
 * When this {@link RoutingEngine} is used, the cluster has only a single Master
 * which is responsible for accepting all writes. Slaves are read-only replicas
 * of the Master.
 * 
 * @author ambud
 */
public class MasterSlaveRoutingEngine extends RoutingEngine {

	private List<Node> nodeSet;

	public MasterSlaveRoutingEngine() {
		nodeSet = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void init(Map<String, String> conf, StorageEngine engine, ClusterConnector connector) throws Exception {
		super.init(conf, engine, connector);
		connector.initializeRouterHooks(this);
	}

	@Override
	public List<Node> routeData(Point point) {
		return nodeSet;
	}

	@Override
	public void nodeAdded(Node node) {
		nodeSet.add(node);
	}

	@Override
	public void nodeDeleted(Node node) {
		nodeSet.add(node);
	}

	@Override
	public void addRoutableKey(Point point, int replicationFactor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void makeCoordinator() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getRoutingTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateLocalRouteTable(Object routingTable) {
		// TODO Auto-generated method stub
		
	}

}
