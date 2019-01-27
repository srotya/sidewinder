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
package com.srotya.minuteman.connectors;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.Replica;
import com.srotya.minuteman.cluster.WALManager;

import io.atomix.AtomixReplica;
import io.atomix.AtomixReplica.Type;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.Storage.Builder;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.atomix.group.election.Term;
import io.atomix.variables.DistributedValue;

/**
 * @author ambud
 */
public class AtomixConnector extends ClusterConnector {

	public static final String FETCH_RETRY_INTERVAL = "cluster.atomix.fetch.retry.interval.ms";
	public static final String CLUSTER_ATOMIX_BOOTSTRAP_ADDRESSES = "cluster.atomix.bootstrap.addresses";
	public static final String CLUSTER_ATOMIX_BOOTSTRAP = "cluster.atomix.bootstrap";
	public static final String CLUSTER_ATOMIX_HEARTBEAT_INTERVAL = "cluster.atomix.heartbeat.interval";
	public static final String CLUSTER_ATOMIX_ELECTION_TIMEOUT = "cluster.atomix.election.timeout";
	public static final String CLUSTER_ATOMIX_STORAGE_DIRECTORY = "cluster.atomix.storage.directory";
	public static final String CLUSTER_ATOMIX_STORAGE_LEVEL = "cluster.atomix.storage.level";
	public static final String CLUSTER_ATOMIX_PORT = "cluster.atomix.port";
	public static final String CLUSTER_ATOMIX_HOST = "cluster.atomix.host";
	private static final String TABLE = "table";
	private static final String BROADCAST_GROUP = "controller";
	private static final Logger logger = Logger.getLogger(AtomixConnector.class.getName());
	private AtomixReplica atomix;
	private boolean isBootstrap;
	private String address;
	private int port;
	private volatile boolean isLeader;
	private DistributedGroup group;
	protected Node coordinator;
	private int fetchRetryInterval;

	@Override
	public void init(Map<String, String> conf) throws Exception {
		AtomixReplica.Builder builder = AtomixReplica
				.builder(new Address(conf.getOrDefault(CLUSTER_ATOMIX_HOST, "localhost"),
						Integer.parseInt(conf.getOrDefault(CLUSTER_ATOMIX_PORT, "8901"))));
		Builder storageBuilder = Storage.builder();

		storageBuilder
				.withStorageLevel(StorageLevel.valueOf(conf.getOrDefault(CLUSTER_ATOMIX_STORAGE_LEVEL, "MEMORY")));
		storageBuilder.withDirectory(conf.getOrDefault(CLUSTER_ATOMIX_STORAGE_DIRECTORY, "/tmp/sidewinder-atomix"));

		fetchRetryInterval = Integer.parseInt(conf.getOrDefault(FETCH_RETRY_INTERVAL, "1000"));
		atomix = builder.withStorage(storageBuilder.build()).withSessionTimeout(Duration.ofSeconds(10))
				.withGlobalSuspendTimeout(Duration.ofMinutes(2)).withType(Type.ACTIVE)
				.withElectionTimeout(
						Duration.ofSeconds(Integer.parseInt(conf.getOrDefault(CLUSTER_ATOMIX_ELECTION_TIMEOUT, "10"))))
				.withHeartbeatInterval(
						Duration.ofSeconds(Integer.parseInt(conf.getOrDefault(CLUSTER_ATOMIX_HEARTBEAT_INTERVAL, "5"))))
				.build();

		atomix.serializer().register(Node.class, NodeSerializer.class);
		atomix.serializer().register(Replica.class, ReplicaSerializer.class);

		this.isBootstrap = Boolean.parseBoolean(conf.getOrDefault(CLUSTER_ATOMIX_BOOTSTRAP, "true"));
		String[] bootstraps = conf.getOrDefault(CLUSTER_ATOMIX_BOOTSTRAP_ADDRESSES, "localhost:8901").split(",");
		List<Address> bootstrapNodes = new ArrayList<>();
		for (String addr : bootstraps) {
			bootstrapNodes.add(new Address(addr));
		}
		if (isBootstrap) {
			logger.info("Joining cluster as bootstrap node");
			atomix.bootstrap(bootstrapNodes).join();
			atomix.getValue(TABLE);
		} else {
			logger.info("Joining cluster as a member node");
			atomix.join(bootstrapNodes).get();
		}
		logger.info("Atomix clustering initialized");
	}

	@Override
	public void initializeRouterListener(final WALManager manager) throws IOException {
		port = manager.getPort();
		address = manager.getAddress();
		DistributedGroup.Config config = new DistributedGroup.Config().withMemberExpiration(Duration.ofSeconds(20));
		group = getAtomix().getGroup(BROADCAST_GROUP, config).join();
		group.election().onElection(new Consumer<Term>() {

			@Override
			public void accept(Term t) {
				logger.info("Completed leader election, new leader is => " + t.leader().id());
				if (isLocal(t.leader().id())) {
					isLeader = true;
					try {
						manager.makeCoordinator();
					} catch (Exception e) {
						e.printStackTrace();
						logger.severe("Error making corrdinator");
					}
				} else {
					isLeader = false;
					logger.info("This node is not the leader");
				}
				Node node = manager.getStrategy().get(t.leader().id().hashCode());
				if (node == null) {
					logger.info("Leader node is empty:" + t.leader().id());
					node = buildNode(t.leader().id());
				}
				manager.setCoordinator(node);
				coordinator = node;
				logger.info("Node:" + address + ":" + port + "\t leader status:" + isLeader);
			}
		});

		group.onJoin(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				logger.info("Node found:" + t.id());
				Node node = buildNode(t.id());
				try {
					manager.addNode(node);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		group.onLeave(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				logger.info("Node left:" + t.id().hashCode());
				try {
					manager.removeNode(t.id().hashCode());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		group.join(address + ":" + port).join();
		logger.info("Created cluster using Atomix connector");

		for (GroupMember groupMember : group.members()) {
			if (isLocal(groupMember.id())) {
				continue;
			}
			Node node = buildNode(groupMember.id());
			manager.addNode(node);
		}

		manager.resume();

		try {
			getAtomix().getValue(TABLE).get().onChange(event -> {
				// logger.info("Route table updated by leader:" + event.newValue());
				manager.setRouteTable(event.newValue());
			});
		} catch (InterruptedException | ExecutionException e) {
			logger.log(Level.SEVERE, "Error updating route table on node " + address + ":" + port, e);
		}
	}

	@Override
	public boolean isCoordinator() {
		return isLeader;
	}

	public AtomixReplica getAtomix() {
		return atomix;
	}

	public boolean isBootstrap() {
		return isBootstrap;
	}

	private boolean isLocal(String id) {
		String[] split = id.split(":");
		return split[0].equalsIgnoreCase(address) && Integer.parseInt(split[1]) == port;
	}

	@Override
	public int getClusterSize() throws Exception {
		return getAtomix().getGroup(BROADCAST_GROUP).join().members().size();
	}

	@Override
	public Object fetchRoutingTable(int retryCount) {
		try {
			logger.info("Fetching route table info from metastore");
			DistributedValue<Object> value = getAtomix().getValue(TABLE).get(2, TimeUnit.SECONDS);
			logger.info("Fetched route table info from metastore:" + value.get());
			return value.get().get();
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"Failed to fetch routing table on node " + address + ":" + port + " reason:" + e.getMessage());
			if (retryCount > 0) {
				logger.info("Failed to fetch routing table reason:" + e.getMessage() + " will retry in 1s");
				try {
					Thread.sleep(fetchRetryInterval);
				} catch (InterruptedException e1) {
					return null;
				}
				return fetchRoutingTable(retryCount - 1);
			}
		}
		return null;
	}

	@Override
	public void updateTable(Object table) throws Exception {
		getAtomix().getValue(TABLE).get().set(table);
		logger.fine("Updated route table in atomix");
	}

	@SuppressWarnings("rawtypes")
	public static class NodeSerializer implements TypeSerializer<Node> {

		@Override
		public Node read(Class<Node> arg0, BufferInput arg1, Serializer arg2) {
			String address = arg1.readUTF8();
			int port = arg1.readInt();
			return new Node((address + ":" + port).hashCode(), address, port);
		}

		@Override
		public void write(Node node, BufferOutput arg1, Serializer arg2) {
			arg1.writeUTF8(node.getAddress());
			arg1.writeInt(node.getPort());
		}

	}

	@SuppressWarnings("rawtypes")
	public static class ReplicaSerializer implements TypeSerializer<Replica> {

		@Override
		public Replica read(Class<Replica> arg0, BufferInput buf, Serializer arg2) {
			Replica replica = new Replica();
			replica.setLeaderAddress(buf.readString());
			replica.setLeaderPort(buf.readInt());
			replica.setReplicaAddress(buf.readString());
			replica.setReplicaPort(buf.readInt());
			replica.setRouteKey(buf.readInt());
			replica.setIsr(buf.readBoolean());
			return replica;
		}

		@Override
		public void write(Replica replica, BufferOutput buf, Serializer arg2) {
			buf.writeUTF8(replica.getLeaderAddress());
			buf.writeInt(replica.getLeaderPort());
			buf.writeUTF8(replica.getReplicaAddress());
			buf.writeInt(replica.getReplicaPort());
			buf.writeInt(replica.getRouteKey());
			buf.writeBoolean(replica.isIsr());
		}

	}

	@Override
	public Node getCoordinator() {
		return coordinator;
	}

	@Override
	public void stop() throws Exception {
		atomix.leave().get();
		atomix.shutdown();
	}

}
