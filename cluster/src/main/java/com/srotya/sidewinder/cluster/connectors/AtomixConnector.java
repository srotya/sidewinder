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
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.routing.GRPCWriter;
import com.srotya.sidewinder.cluster.routing.LocalWriter;
import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.Storage.Builder;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;
import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class AtomixConnector extends ClusterConnector {

	private static final String BROADCAST_GROUP = "controller";
	private static final Logger logger = Logger.getLogger(AtomixConnector.class.getName());
	private AtomixReplica atomix;
	private boolean isBootstrap;
	private String address;
	private int port;

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

		logger.info("Constructed Atomix Replica");
		this.isBootstrap = Boolean.parseBoolean(conf.getOrDefault("cluster.atomix.bootstrap", "true"));
		if (isBootstrap) {
			logger.info("Joining cluster as bootstrap node");
			atomix.bootstrap().join();
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
		Node localNode = new Node(address, port, address + ":" + port);
		localNode.setWriter(new LocalWriter(engine.getEngine()));
		engine.nodeAdded(localNode);
		if (isBootstrap()) {
			// track nodes only if this node is the master
			group.onJoin(new Consumer<GroupMember>() {

				@Override
				public void accept(GroupMember t) {
					if (!isLocal(t.id())) {
						logger.info("Non-local node found:" + t.id());
						String[] split = t.id().split(":");
						ManagedChannel channel = ManagedChannelBuilder.forAddress(split[0], Integer.parseInt(split[1]))
								.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
						Node node = new Node(split[0], Integer.parseInt(split[1]), t.id());
						node.setWriter(new GRPCWriter(channel));
						engine.nodeAdded(node);
					}
				}
			});

			group.onLeave(new Consumer<GroupMember>() {

				@Override
				public void accept(GroupMember t) {
					if (!isLocal(t.id())) {
						logger.info("Non-local node found:" + t.id());
						String[] split = t.id().split(":");
						Node node = new Node(split[0], Integer.parseInt(split[1]), t.id());
						engine.nodeAdded(node);
					}
				}
			});
		}
		group.join(address + ":" + port).join();
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

}
