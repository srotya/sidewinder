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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.IsrUpdateRequest;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.ReplicationServiceImpl;
import com.srotya.minuteman.wal.LocalWALClient;
import com.srotya.minuteman.wal.RemoteWALClient;
import com.srotya.minuteman.wal.WAL;

import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

/**
 * @author ambud
 */
public class WALManagerImpl extends WALManager {

	private static final Logger logger = Logger.getLogger(WALManagerImpl.class.getName());
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private WriteLock write = lock.writeLock();
	private Map<String, Replica> localReplicaTable;
	private Map<String, List<Replica>> routeTable;
	private List<Node> nodes;
	private ClusterConnector connector;
	private Server server;
	private Class<LocalWALClient> walClientClass;
	private long allocator;
	private int isrUpdateFrequency;

	public WALManagerImpl() {
		super();
		this.nodes = new LinkedList<>();
		this.localReplicaTable = new HashMap<>();
		this.routeTable = new HashMap<>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(Map<String, String> conf, ClusterConnector connector, ScheduledExecutorService bgTasks,
			Object storageObject) throws Exception {
		super.init(conf, connector, bgTasks, storageObject);
		this.connector = connector;
		walClientClass = (Class<LocalWALClient>) Class
				.forName(conf.getOrDefault(WAL_CLIENT_CLASS, LocalWALClient.class.getName()));
		connector.initializeRouterHooks(this);
		server = NettyServerBuilder.forPort(getPort()).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new ReplicationServiceImpl(this)).build().start();
		logger.info("Listening for GRPC requests on port:" + getPort());
		isrUpdateFrequency = Integer.parseInt(conf.getOrDefault(WAL.WAL_ISRCHECK_FREQUENCY, "10"));
		bgTasks.scheduleAtFixedRate(() -> {
			if (connector.getCoordinator() != null) {
				ReplicationServiceBlockingStub stub = ReplicationServiceGrpc
						.newBlockingStub(connector.getCoordinator().getChannel());
				for (Entry<String, Replica> entry : localReplicaTable.entrySet()) {
					if (isLeader(entry.getValue())) {
						Map<String, Boolean> isrMap = new HashMap<>();
						WAL wal = entry.getValue().getWal();
						logger.fine("Updating ISRs for(" + entry.getKey() + ") followers:" + wal.getFollowers());
						for (String followerId : wal.getFollowers()) {
							boolean isr = wal.isIsr(followerId);
							isrMap.put(followerId, isr);
						}
						GenericResponse response = stub.updateIsr(
								IsrUpdateRequest.newBuilder().setRouteKey(entry.getKey()).putAllIsrMap(isrMap).build());
						if (response.getResponseCode() == 200) {
							logger.fine("Updated ISRs with coordinator for routeKey:" + entry.getKey());
						} else {
							logger.severe(
									"ISR update with coordinator failed for routeKey:" + entry.getKey() + " reason:"
											+ response.getResponseString() + " code:" + response.getResponseCode());
						}
					}
				}
			}
		}, 0, isrUpdateFrequency, TimeUnit.SECONDS);
	}

	@Override
	public void updateReplicaIsrStatus(String routingKey, Map<String, Boolean> isrUpdateMap) throws Exception {
		if (!connector.isCoordinator()) {
			throw new UnsupportedOperationException("This(" + getThisNodeKey() + ") is not a coordinator("
					+ getCoordinator().getNodeKey() + ") node, can't perform ISR updates");
		}
		write.lock();
		try {
			List<Replica> list = routeTable.get(routingKey);
			if (list != null) {
				for (Replica replica : list) {
					Boolean status = isrUpdateMap.get(replica.getReplicaNodeKey());
					if (status != null) {
						replica.setIsr(status);
					} else {
						logger.severe("Missing ISR status for replica(" + replica.getReplicaNodeKey()
								+ ") for route key(" + routingKey + ")");
					}
				}
				connector.updateTable(routeTable);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			write.unlock();
		}
	}

	@Override
	public List<Replica> addRoutableKey(String routingKey, int replicationFactor) throws Exception {
		if (!connector.isCoordinator()) {
			throw new UnsupportedOperationException("This(" + getThisNodeKey() + ") is not a coordinator("
					+ getCoordinator().getNodeKey() + ") node, can't perform route modifications");
		}
		if (replicationFactor > nodes.size()) {
			throw new IllegalArgumentException("Fewer nodes(" + nodes.size()
					+ ") in the cluster than requested replication factor(" + replicationFactor + ")");
		}
		write.lock();
		try {
			logger.info("Replication factor:" + replicationFactor + " requested for new key:" + routingKey);
			List<Replica> list = routeTable.get(routingKey);
			if (list == null) {
				// logger.info("Nodes in the cluster:" + nodes);
				list = new ArrayList<>();
				routeTable.put(routingKey, list);
				Replica replica = new Replica();
				Node candidate = getNode();
				nodeToReplica(routingKey, replica, candidate, candidate);
				list.add(0, replica);

				for (int i = 1; i < replicationFactor; i++) {
					Node node = getNode();
					replica = new Replica();
					nodeToReplica(routingKey, replica, candidate, node);
					list.add(i, replica);
				}
				logger.info("Route key:" + routingKey + " has replicas:"
						+ list.stream().map(n -> n.getReplicaNodeKey()).collect(Collectors.toList()) + " leader:"
						+ list.get(0).getReplicaNodeKey());
				for (Replica r : list) {
					updateReplica(r);
				}
				// tell the cluster nodes about this
				connector.updateTable(routeTable);
				// offset the allocator by 1 so that leaders are assigned in a round-robin
				// fashion
				allocator++;
			} else {
				// routetable for key exists
			}
			return list;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to update route table in distributed metastore", e);
			throw e;
		} finally {
			write.unlock();
		}
	}

	private void updateReplica(Replica r) throws IOException, Exception {
		if (isLocal(r)) {
			logger.info(r.getReplicaNodeKey() + " is local on: " + getThisNodeKey());
			replicaUpdated(r);
		} else {
			logger.info(r.getReplicaNodeKey() + " is NOT local on: " + getThisNodeKey());
			connector.updateReplicaRoute(this, r, false);
		}
	}

	private Node getNode() {
		Node node = nodes.get((int) (allocator++ % nodes.size()));
		logger.info("NODE:" + node.getNodeKey());
		return node;
	}

	private void nodeToReplica(String routingKey, Replica replica, Node leader, Node follower) {
		replica.setLeaderAddress(leader.getAddress());
		replica.setLeaderPort(leader.getPort());
		replica.setRouteKey(routingKey);
		replica.setReplicaAddress(follower.getAddress());
		replica.setReplicaPort(follower.getPort());
	}

	@Override
	public void addNode(Node node) throws IOException {
		write.lock();
		if (getNodeMap().get(node.getNodeKey()) == null) {
			logger.info("Adding node(" + node.getNodeKey() + ") to WALManager");
			nodes.add(node);
			getNodeMap().put(node.getNodeKey(), node);
			logger.info("Node(" + node.getNodeKey() + ") added to WALManager");
		} else {
			logger.info("Node(" + node.getNodeKey() + ") is already present");
		}
		write.unlock();
	}

	@Override
	public void removeNode(String nodeId) throws Exception {
		write.lock();
		try {
			// if coordinator then fix the routes of all other followers
			if (connector.isCoordinator()) {
				for (Entry<String, List<Replica>> entry : routeTable.entrySet()) {
					List<Replica> value = entry.getValue();
					Replica leader = value.get(0);
					if (nodeId.equals(leader.getLeaderNodeKey())) {
						value.remove(0);
						// find next ISR
						leader = getFirstIsr(value);
						if (value.isEmpty() || leader == null) {
							// no suitable nodes left for this route key
							logger.info("No suitable replicas left for this route key:" + entry.getKey());
							continue;
						}
						leader.setLeaderAddress(leader.getReplicaAddress());
						leader.setLeaderPort(leader.getReplicaPort());
						updateReplica(leader);
						for (int i = 0; i < value.size(); i++) {
							Replica replica = value.get(i);
							replica.setLeaderAddress(leader.getLeaderAddress());
							replica.setLeaderPort(leader.getLeaderPort());
							updateReplica(replica);
						}
					}
				}
				connector.updateTable(routeTable);
			}
			logger.info("Removing node(" + nodeId + ") from WALManager");
			Node node2 = getNodeMap().remove(nodeId);
			nodes.remove(node2);
			if (node2 != null) {
				for (Iterator<Entry<String, Replica>> iterator = localReplicaTable.entrySet().iterator(); iterator
						.hasNext();) {
					Entry<String, Replica> entry = iterator.next();
					if (nodeId.equals(entry.getValue().getLeaderNodeKey())) {
						entry.getValue().getClient().stop();
						// TODO delete wal
						iterator.remove();
					}
				}
			}
			logger.info("Node(" + nodeId + ") removed from WALManager");
		} finally {
			write.unlock();
		}
	}

	private Replica getFirstIsr(List<Replica> replicas) {
		for (Replica r : replicas) {
			if (r.isIsr()) {
				return r;
			}
		}
		return null;
	}

	@Override
	public void replicaUpdated(Replica replica) throws IOException {
		write.lock();
		try {
			logger.info("Replica updated:" + replica.getLeaderNodeKey() + "\t" + "\t node:" + getThisNodeKey());
			Replica local = localReplicaTable.get(replica.getRouteKey());
			if (local == null) {
				local = replica;
				localReplicaTable.put(replica.getRouteKey(), replica);
				replica.setWal(super.initializeWAL(replica.getRouteKey()));
			} else {
				if (local.getClient() != null) {
					local.getClient().stop();
				}
			}
			// check if this node is the leader for this WAL
			if (!isLeader(local)) {
				// initialize a remote WAL client to copy data from the leader
				Node node = getNodeMap().get(replica.getLeaderNodeKey());
				logger.info("Node leader update:" + getThisNodeKey() + "\tReplica leader:"
						+ (node != null ? node.getNodeKey() : "null") + "\t" + replica.getLeaderNodeKey() + "\t"
						+ local.getWal() + "\t" + local.getRouteKey());
				local.setClient(new RemoteWALClient().configure(getConf(), getThisNodeKey(), node.getChannel(),
						local.getWal(), local.getRouteKey()));
				Thread th = new Thread(local.getClient());
				th.setDaemon(true);
				th.start();
				logger.info("Starting replication thread for:" + replica.getRouteKey() + " on replica => "
						+ local.getReplicaNodeKey());
			}
			// there's no local follower, create one
			if (local.getLocal() == null) {
				// local WAL client to replay this data on the local instance
				LocalWALClient client = walClientClass.newInstance();
				local.setLocal(client.configure(getConf(), getThisNodeKey(), local.getWal(), storageObject));
				Thread th = new Thread(local.getLocal());
				th.setDaemon(true);
				th.start();
				logger.info("Starting local follower thread for:" + replica.getRouteKey() + " on replica => "
						+ local.getReplicaNodeKey());
			}
		} catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(e);
		} finally {
			write.unlock();
		}
	}

	private boolean isLeader(Replica replica) {
		return replica.getLeaderNodeKey().equals(replica.getReplicaNodeKey());
	}

	private boolean isLocal(Replica replica) {
		return replica.getReplicaNodeKey().equals(this.getThisNodeKey());
	}

	@Override
	public void replicaRemoved(Replica replica) throws Exception {
		write.lock();
		logger.info("Removing replica " + replica.getRouteKey() + " cleaning up entries on " + this.getThisNodeKey());
		Replica replica2 = localReplicaTable.get(replica.getRouteKey());
		if (replica2 != null) {
			replica2.getClient().stop();
			logger.info("Replica removed " + replica.getRouteKey() + " on " + this.getThisNodeKey());
		} else {
			logger.info("Replica already removed, nothing to do");
		}
		write.unlock();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void makeCoordinator() throws Exception {
		write.lock();
		routeTable = (Map<String, List<Replica>>) connector.fetchRoutingTable(10);
		logger.info("Fetched latest route table from metastore:" + routeTable);
		if (routeTable == null) {
			connector.updateTable(this.routeTable = new HashMap<>());
			logger.info("No route table in metastore, created an empty one");
		} else {
			if (getCoordinator() != null && !getThisNodeKey().equals(getCoordinator().getNodeKey())) {
				removeNode(getCoordinator().getNodeKey());
			} else {
				logger.warning("Ignoring route table correction because of self last coordinator");
			}
		}
		write.unlock();
	}

	@Override
	public Object getRoutingTable() {
		return localReplicaTable;
	}

	@Override
	public void stop() throws InterruptedException {
		for (Entry<String, Replica> entry : localReplicaTable.entrySet()) {
			try {
				logger.info("Attempting to stop replica:" + entry.getKey() + " on node:" + getThisNodeKey());
				entry.getValue().getLocal().stop();
				if (entry.getValue().getClient() != null) {
					entry.getValue().getClient().stop();
				}
				entry.getValue().getWal().close();
				logger.info("Stopped replica:" + entry.getKey() + " on node:" + getThisNodeKey());
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Failed to stop wal:" + entry.getKey() + "\t on replica:"
						+ entry.getValue().getReplicaNodeKey() + "\tleader:" + entry.getValue().getLeaderNodeKey(), e);
			}
		}
		server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void resume() throws IOException {
		Map<String, List<Replica>> rt = (Map<String, List<Replica>>) connector.fetchRoutingTable(10);
		if (rt != null) {
			for (Entry<String, List<Replica>> entry : rt.entrySet()) {
				for (Replica replica : entry.getValue()) {
					if (replica.getReplicaNodeKey().equals(getThisNodeKey())) {
						// resume this replica
						logger.info("Found replica assignment for local node, resuming:" + replica.getReplicaNodeKey()
								+ "\t" + replica.getRouteKey());
						replicaUpdated(replica);
					}
				}
			}
		}
	}

	@Override
	public WAL getWAL(String key) throws IOException {
		Replica replica = localReplicaTable.get(key);
		if (replica == null) {
			return null;
		}
		return replica.getWal();
	}

	@Override
	public void setCoordinator(Node node) {
		write.lock();
		super.setCoordinator(node);
		write.unlock();
	}

	@Override
	public String getReplicaLeader(String routeKey) {
		List<Replica> replica = routeTable.get(routeKey);
		if (replica != null) {
			return replica.get(0).getLeaderNodeKey();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setRouteTable(Object newValue) {
		write.lock();
		routeTable = (Map<String, List<Replica>>) newValue;
		write.unlock();
	}

}
