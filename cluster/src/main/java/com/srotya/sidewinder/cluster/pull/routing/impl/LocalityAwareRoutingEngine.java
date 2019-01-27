/**
 * Copyright Ambud Sharma
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
package com.srotya.sidewinder.cluster.pull.routing.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.push.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.push.routing.EndpointService;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.Query;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class LocalityAwareRoutingEngine extends RoutingEngine {

	private static final Logger logger = Logger.getLogger(LocalityAwareRoutingEngine.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadLock read = lock.readLock();
	private WriteLock write = lock.writeLock();
	private Map<String, List<Node>> routingTable;
	private Queue<Node> nodes;
	private ClusterConnector connector;

	public LocalityAwareRoutingEngine() {
		this.nodes = new LinkedList<>();
		this.routingTable = new HashMap<>();
	}

	@Override
	public void init(Map<String, String> conf, StorageEngine engine, ClusterConnector connector) throws Exception {
		this.connector = connector;
		super.init(conf, engine, connector);
		connector.initializeRouterHooks(this);
	}

	@Override
	public List<Node> routeData(Point point) throws IOException, InterruptedException {
		List<Node> list = getRoutedNodes(point);
		if (list == null) {
			// cluster request route table for data point
			logger.info("Routing info for point:" + point.getDbName() + ":" + point.getMeasurementName()
					+ " does not exist, requesting coordinator to create an entry");
			EndpointService eps = getLeader().getEndpointService();
			eps.requestRouteEntry(point);
			logger.info("Requested route entry from coordinator");
			for (int i = 0; i < 100; i++) {
				logger.info("Checking local route entry for:" + point.getDbName() + ":" + point.getMeasurementName());
				list = getRoutedNodes(point);
				if (list != null) {
					return list;
				} else {
					logger.info("Local route entry for:" + point.getDbName() + ":" + point.getMeasurementName()
							+ " not found waiting....");
					Thread.sleep(1000);
				}
			}
		}
		return list;
	}

	@Override
	public List<Node> routeQuery(Query query) throws IOException {
		read.lock();
		List<Node> list = routingTable.get(getRoutingKey(query.getDbName(), query.getMeasurementName()));
		read.unlock();
		return list;
	}
	
	public static String getRoutingKey(String dbName, String measurementName) {
		return dbName + "@" + measurementName;
	}

	private List<Node> getRoutedNodes(Point point) {
		read.lock();
		List<Node> list = routingTable.get(getRoutingKey(point.getDbName(), point.getMeasurementName()));
		read.unlock();
		return list;
	}

	@Override
	public void addRoutableKey(Point point, int replicationFactor) {
		if (!connector.isLeader()) {
			throw new UnsupportedOperationException("This is not a leader node, can't perform route modifications");
		}
		String routingKey = getRoutingKey(point.getDbName(), point.getMeasurementName());
		if (replicationFactor < nodes.size()) {
			throw new IllegalArgumentException("Fewer nodes in the cluster than requested replication factor");
		}
		List<Node> list = routingTable.get(routingKey);
		if (list == null) {
			logger.info("Nodes in the cluster:" + nodes);
			write.lock();
			list = new ArrayList<>(replicationFactor);
			routingTable.put(routingKey, list);
			for (int i = 0; i < replicationFactor; i++) {
				Node node = nodes.poll();
				list.add(node);
				nodes.add(node);
			}
			try {
				connector.updateTable(routingTable);
				logger.info("Added " + routingKey + " to route table");
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Failed to update route table in distributed metastore", e);
			} finally {
			}
			write.unlock();
		}
	}

	@Override
	public void nodeAdded(Node node) {
		write.lock();
		logger.info("Node added:" + node);
		getNodeMap().put(node.getNodeKey(), node);
		nodes.add(node);
		write.unlock();
	}

	@Override
	public void nodeDeleted(Node node) throws Exception {
		write.lock();
		getNodeMap().remove(node.getNodeKey());
		logger.info("Node deleted " + node.getNodeKey() + " cleaning up entries");
		Iterator<Entry<String, List<Node>>> itr = routingTable.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<String, List<Node>> entry = itr.next();
			logger.info("Checking entry: " + entry.getKey());
			for (Iterator<Node> iterator = entry.getValue().iterator(); iterator.hasNext();) {
				Node node2 = iterator.next();
				logger.info("Validating node:" + node2.getNodeKey());
				if (node2.equals(node)) {
					iterator.remove();
					logger.info("Removing node " + node.getNodeKey() + " from route table entry for:" + entry.getKey());
				}
			}
		}
		nodes.remove(node);
		connector.updateTable(routingTable);
		write.unlock();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void makeCoordinator() throws Exception {
		write.lock();
		routingTable = (Map<String, List<Node>>) connector.fetchRoutingTable();
		logger.info("Fetched latest route table from metastore:" + routingTable);
		if (routingTable == null) {
			connector.updateTable(this.routingTable = new HashMap<>());
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
	public Object getRoutingTable() {
		return routingTable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void updateLocalRouteTable(Object routingTable) {
		write.lock();
		this.routingTable = (Map<String, List<Node>>) routingTable;
		for (Entry<String, List<Node>> entry : this.routingTable.entrySet()) {
			for (int i = 0; i < entry.getValue().size(); i++) {
				Node node = entry.getValue().get(i);
				entry.getValue().set(i, getNodeMap().get(node.getNodeKey()));
			}
		}
		logger.info("Route table update, new route table has:" + this.routingTable.size() + " entries");
		write.unlock();
	}

}
