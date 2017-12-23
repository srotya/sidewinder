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
package com.srotya.sidewinder.cluster.push.routing.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.push.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.Query;
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

	private static final Logger logger = Logger.getLogger(MasterSlaveRoutingEngine.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private WriteLock write = lock.writeLock();
	private List<Node> nodeSet;
	private ClusterConnector connector;

	public MasterSlaveRoutingEngine() {
		nodeSet = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void init(Map<String, String> conf, StorageEngine engine, ClusterConnector connector) throws Exception {
		this.connector = connector;
		super.init(conf, engine, connector);
		connector.initializeRouterHooks(this);
	}

	@Override
	public List<Node> routeData(Point point) {
		return nodeSet;
	}

	@Override
	public void nodeAdded(Node node) {
		write.lock();
		logger.info("Node added:" + node);
		getNodeMap().put(node.getNodeKey(), node);
		nodeSet.add(node);
		write.unlock();
	}

	@Override
	public void nodeDeleted(Node node) {
		write.lock();
		logger.info("Node deleted:" + node);
		nodeSet.remove(node);
		getNodeMap().remove(node.getNodeKey());
		write.unlock();
	}

	@Override
	public void addRoutableKey(Point point, int replicationFactor) {
	}

	@Override
	public void makeCoordinator() throws Exception {
		write.lock();
		logger.info("Fetched latest route table from metastore:" + nodeSet);
		if (nodeSet == null) {
			connector.updateTable(this.nodeSet = Collections.synchronizedList(new ArrayList<>()));
			logger.info("No route table in metastore, creating empty one");
		} else {
			if (!getLeader().equals(connector.getLocalNode())) {
				nodeDeleted(getLeader());
			} else {
				logger.warning("Ignoring route table correction because of self last leader");
			}
		}
		write.unlock();
	}
	
	@Override
	public List<Node> routeQuery(Query query) throws IOException {
		return nodeSet;
	}
	
	public static String getRoutingKey(String dbName, String measurementName) {
		return dbName + "@" + measurementName;
	}

	@Override
	public Object getRoutingTable() {
		return nodeSet;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void updateLocalRouteTable(Object routingTable) {
		write.lock();
		nodeSet = (List<Node>) routingTable;
		for (int i = 0; i < nodeSet.size(); i++) {
			Node node = nodeSet.get(i);
			nodeSet.set(i, getNodeMap().get(node.getNodeKey()));
		}
		write.unlock();
	}

}
