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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.push.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.push.routing.LocalEndpointService;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.push.routing.RoutingStrategy;
import com.srotya.sidewinder.cluster.rpc.Query;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class ScalingRoutingEngine extends RoutingEngine {

	private static final String DEFAULT_CLUSTER_ROUTING_STRATEGY = "com.srotya.sidewinder.cluster.routing.ConsistentHashRoutingStrategy";
	private static final String CLUSTER_ROUTING_STRATEGY = "cluster.routing.strategy";
	private static final Logger logger = Logger.getLogger(RoutingEngine.class.getName());
	private RoutingStrategy strategy;
	private StorageEngine engine;
	private int replica;

	public ScalingRoutingEngine() {
	}

	@Override
	public void init(Map<String, String> conf, StorageEngine engine, ClusterConnector connector) throws Exception {
		super.init(conf, engine, connector);
		replica = Integer.parseInt(conf.getOrDefault("replication.factor", "3"));
		String strategyClass = conf.getOrDefault(CLUSTER_ROUTING_STRATEGY, DEFAULT_CLUSTER_ROUTING_STRATEGY);
		logger.info("Using rebalancing strategy:" + strategyClass);
		try {
			strategy = (RoutingStrategy) Class.forName(strategyClass).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
		connector.initializeRouterHooks(this);
	}
	
	@Override
	public List<Node> routeQuery(Query query) throws IOException {
		List<Node> list = strategy.getNodes(getRoutingKey(query.getDbName(), query.getMeasurementName()), 3);
		return list;
	}
	
	public static String getRoutingKey(String dbName, String measurementName) {
		return dbName + "@" + measurementName;
	}

	@Override
	public void nodeAdded(Node node) {
		List<String> keyMovements = strategy.addNode(node);
		checkAndMoveKeys(keyMovements);
	}

	@Override
	public void nodeDeleted(Node node) {
		List<String> removeNode = strategy.removeNode(node);
		checkAndMoveKeys(removeNode);
	}

	private void checkAndMoveKeys(List<String> keys) {
		for (String key : keys) {
			List<Node> nodes = strategy.getNodes(key, strategy.getReplicationFactor(key));
			if (nodes.get(0).getEndpointService() instanceof LocalEndpointService) {
				// check if data exists locally
				String[] decodeKey = decodeKey(key);
				String dbName = decodeKey[0];
				String measurementName = decodeKey[1];
				try {
					engine.checkIfExists(dbName, measurementName);
					Set<String> fields = engine.getFieldsForMeasurement(dbName, measurementName);
					for (String field : fields) {
						List<List<String>> tagsSet = engine.getTagsForMeasurement(dbName, measurementName, field);
						for (List<String> tags : tagsSet) {
							seriesToRawBucket(engine, dbName, measurementName, field, tags);
						}
					}
				} catch (Exception e) {
					// if not exists then fix
				}
				// overwrite other endpoints if this is the leader
			}
			// else ignore this
		}
	}

	public void uninitialize() throws InterruptedException, ExecutionException, IOException {
		logger.info("Leaving cluster");
		for (Node node : strategy.getAllNodes()) {
			node.getEndpointService().close();
		}
	}

	@Override
	public List<Node> routeData(Point dp) {
		String key = encodeKey(dp);
		return strategy.getNodes(key, replica);
	}

	public static String encodeKey(Point dp) {
		return dp.getDbName() + "." + dp.getMeasurementName();
	}

	public static String[] decodeKey(String key) {
		String[] split = key.split("\\.");
		return split;
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
