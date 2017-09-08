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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.routing.GRPCEndpointService;
import com.srotya.sidewinder.cluster.routing.LocalEndpointService;
import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;

import io.atomix.AtomixReplica;
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
import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class AtomixConnector extends ClusterConnector {

	private static final String TABLE = "table";
	private static final String BROADCAST_GROUP = "controller";
	private static final Logger logger = Logger.getLogger(AtomixConnector.class.getName());
	private AtomixReplica atomix;
	private boolean isBootstrap;
	private String address;
	private int port;
	private volatile boolean leader;
	private Node localNode;

	@Override
	public void init(Map<String, String> conf) throws Exception {
		AtomixReplica.Builder builder = AtomixReplica
				.builder(new Address(conf.getOrDefault("cluster.atomix.host", "localhost"),
						Integer.parseInt(conf.getOrDefault("cluster.atomix.port", "8901"))));
		Builder storageBuilder = Storage.builder();

		storageBuilder
				.withStorageLevel(StorageLevel.valueOf(conf.getOrDefault("cluster.atomix.storage.level", "MEMORY")));
		storageBuilder.withDirectory(conf.getOrDefault("cluster.atomix.storage.directory", "/tmp/sidewinder-atomix"));

		atomix = builder.withStorage(storageBuilder.build())
				.withElectionTimeout(Duration
						.ofSeconds(Integer.parseInt(conf.getOrDefault("cluster.atomix.election.timeout", "10"))))
				.withHeartbeatInterval(Duration
						.ofSeconds(Integer.parseInt(conf.getOrDefault("cluster.atomix.heartbeat.interval", "5"))))
				.build();

		atomix.serializer().register(Node.class, NodeSerializer.class);

		this.isBootstrap = Boolean.parseBoolean(conf.getOrDefault("cluster.atomix.bootstrap", "true"));
		if (isBootstrap) {
			logger.info("Joining cluster as bootstrap node");
			atomix.bootstrap(new Address(conf.getOrDefault("cluster.atomix.bootstrap.host", "localhost"),
					Integer.parseInt(conf.getOrDefault("cluster.atomix.bootstrap.port", "8901")))).join();
			atomix.getValue(TABLE);
		} else {
			logger.info("Joining cluster as a member node");
			atomix.join(new Address(conf.getOrDefault("cluster.atomix.bootstrap.host", "localhost"),
					Integer.parseInt(conf.getOrDefault("cluster.atomix.bootstrap.port", "8901")))).get();
		}
		logger.info("Atomix clustering initialized");
	}

	@Override
	public void initializeRouterHooks(final RoutingEngine engine) {
		port = engine.getPort();
		address = engine.getAddress();
		final DistributedGroup group = getAtomix().getGroup(BROADCAST_GROUP).join();
		localNode = new Node(address, port, address + ":" + port);
		localNode.setEndpointService(new LocalEndpointService(engine.getEngine(), engine));
		// add local node to the node list so that requests can be routed to the local
		// writer instead of GRPC writer
		engine.nodeAdded(localNode);
		group.election().onElection(new Consumer<Term>() {

			@Override
			public void accept(Term t) {
				if (isLocal(t.leader().id())) {
					logger.info("Completed leader election:" + t.leader().id());
					leader = true;
					try {
						engine.makeCoordinator();
					} catch (Exception e) {
						e.printStackTrace();
						logger.severe("Error making corrdinator");
					}
				} else {
					logger.info("Leader election completed, " + t.leader().id() + " is the leader");
					leader = false;
				}
				Node node = engine.getNodeMap().get(t.leader().id());
				if (node == null) {
					logger.info("Leader node is empty:" + t.leader().id());
					node = buildNode(t.leader());
				}
				engine.setLeader(node);
			}
		});

		group.onJoin(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				if (!isLocal(t.id())) {
					logger.info("Node found:" + t.id());
					Node node = buildNode(t);
					engine.nodeAdded(node);
					engine.getNodeMap().put(t.id(), node);
				}
			}

		});

		group.onLeave(new Consumer<GroupMember>() {

			@Override
			public void accept(GroupMember t) {
				if (!isLocal(t.id())) {
					logger.info("Node found:" + t.id());
					String[] split = t.id().split(":");
					Node node = new Node(split[0], Integer.parseInt(split[1]), t.id());
					engine.nodeAdded(node);
					engine.getNodeMap().remove(t.id());
				}
			}
		});

		group.join(address + ":" + port).join();
		logger.info("Created cluster using Atomix connector");

		for (GroupMember groupMember : group.members()) {
			if (isLocal(groupMember.id())) {
				continue;
			}
			Node node = buildNode(groupMember);
			engine.nodeAdded(node);
		}

		try {
			getAtomix().getValue(TABLE).get().onChange(event -> {
				logger.info("Route table updated by leader");
				engine.updateLocalRouteTable(event.newValue());
			});
		} catch (InterruptedException | ExecutionException e) {
			logger.log(Level.SEVERE, "Error updating route table on node " + address + ":" + port, e);
		}
	}

	private Node buildNode(GroupMember t) {
		String[] split = t.id().split(":");
		ManagedChannel channel = ManagedChannelBuilder.forAddress(split[0], Integer.parseInt(split[1]))
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
		Node node = new Node(split[0], Integer.parseInt(split[1]), t.id());
		node.setEndpointService(new GRPCEndpointService(channel));
		return node;
	}

	@Override
	public boolean isLeader() {
		return leader;
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
	public Object fetchRoutingTable() {
		try {
			logger.info("Fetching route table info from metastore");
			DistributedValue<Object> value = getAtomix().getValue(TABLE).get(2, TimeUnit.SECONDS);
			logger.info("Fetched route table info from metastore:" + value.get());
			return value.get().get();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to fetch routing table" + e.getMessage());
		}
		return null;
	}

	@Override
	public void updateTable(Object table) throws Exception {
		getAtomix().getValue(TABLE).get().set(table);
	}

	@Override
	public Node getLocalNode() {
		return localNode;
	}

	@SuppressWarnings("rawtypes")
	public static class NodeSerializer implements TypeSerializer<Node> {

		@Override
		public Node read(Class<Node> arg0, BufferInput arg1, Serializer arg2) {
			String address = arg1.readUTF8();
			int port = arg1.readInt();
			return new Node(address, port, address + ":" + port);
		}

		@Override
		public void write(Node node, BufferOutput arg1, Serializer arg2) {
			arg1.writeUTF8(node.getAddress());
			arg1.writeInt(node.getPort());
		}

	}
}
