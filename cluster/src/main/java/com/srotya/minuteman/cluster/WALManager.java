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
package com.srotya.minuteman.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.wal.MappedWAL;
import com.srotya.minuteman.wal.WAL;

/**
 * Manages {@link WAL}
 * 
 * @author ambud
 */
public abstract class WALManager {

	public static final String DEFAULT_CLUSTER_GRPC_PORT = "55021";
	public static final String CLUSTER_GRPC_PORT = "cluster.grpc.port";
	public static final String DEFAULT_CLUSTER_HOST = "localhost";
	public static final String CLUSTER_HOST = "cluster.host";
	public static final String CLUSTER_GRPC_COMPRESSION = "cluster.grpc.compression";
	public static final String DEFAULT_CLUSTER_GRPC_COMPRESSION = "gzip";
	public static final String WAL_CLIENT_CLASS = "wal.client.class";
	private int port;
	private String address;
	private Map<String, String> conf;
	private Map<String, Node> nodeMap;
	private Node coordinatorKey;
	private ScheduledExecutorService bgtask;
	protected Object storageObject;

	public WALManager() {
		this.nodeMap = new HashMap<>();
	}

	public void init(Map<String, String> conf, ClusterConnector connector, ScheduledExecutorService bgtask, Object storageObject)
			throws Exception {
		this.conf = conf;
		this.bgtask = bgtask;
		this.storageObject = storageObject;
		this.port = Integer.parseInt(conf.getOrDefault(CLUSTER_GRPC_PORT, DEFAULT_CLUSTER_GRPC_PORT));
		this.address = conf.getOrDefault(CLUSTER_HOST, DEFAULT_CLUSTER_HOST);
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public WAL initializeWAL(String routeKey) throws IOException {
		WAL wal = new MappedWAL();
		Map<String, String> local = new HashMap<>(conf);
		local.put(WAL.WAL_DIR, conf.getOrDefault(WAL.WAL_DIR, WAL.DEFAULT_WAL_DIR) + "/" + routeKey);
		wal.configure(local, bgtask);
		return wal;
	}

	public Map<String, Node> getNodeMap() {
		return nodeMap;
	}

	public Map<String, String> getConf() {
		return conf;
	}

	public String getThisNodeKey() {
		return address + ":" + port;
	}

	public Node getCoordinator() {
		return coordinatorKey;
	}

	public void setCoordinator(Node node) {
		Node n = getNodeMap().get(node.getNodeKey());
		if (n == null) {
			n = node;
			getNodeMap().put(node.getNodeKey(), node);
		}
		this.coordinatorKey = n;
	}

	public abstract void addNode(Node node) throws IOException;

	public abstract void removeNode(String nodeId) throws Exception;

	public abstract void makeCoordinator() throws Exception;

	public abstract WAL getWAL(String key) throws IOException;

	public abstract List<Replica> addRoutableKey(String routingKey, int replicationFactor) throws Exception;

	public abstract void resume() throws IOException;

	public abstract void replicaUpdated(Replica node) throws IOException;

	public abstract void replicaRemoved(Replica node) throws Exception;

	public abstract Object getRoutingTable();

	public abstract void stop() throws InterruptedException, IOException;

	public abstract String getReplicaLeader(String routeKey);

	public abstract void updateReplicaIsrStatus(String routeKey, Map<String, Boolean> isrUpdateMap) throws Exception;

	public abstract void setRouteTable(Object newValue);

}
