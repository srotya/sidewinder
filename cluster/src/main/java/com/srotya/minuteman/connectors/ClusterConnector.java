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
package com.srotya.minuteman.connectors;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.Replica;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.ReplicaRequest;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public abstract class ClusterConnector {

	private static final Logger logger = Logger.getLogger(ClusterConnector.class.getName());

	public abstract void init(Map<String, String> conf) throws Exception;

	public abstract void initializeRouterHooks(final WALManager manager) throws IOException;

	public abstract int getClusterSize() throws Exception;

	public abstract boolean isBootstrap();

	public abstract boolean isCoordinator();

	public abstract Object fetchRoutingTable(int retryCount);

	public abstract void updateTable(Object table) throws Exception;

	public abstract Node getCoordinator();
	
	public abstract void stop() throws Exception;

	public void updateReplicaRoute(WALManager mgr, Replica replica, boolean delete) throws Exception {
		logger.info(
				"Coordinator messaging replica:" + replica.getReplicaNodeKey() + " for key:" + replica.getRouteKey());
		if (!delete) {
			ReplicationServiceBlockingStub stub = ReplicationServiceGrpc
					.newBlockingStub(mgr.getNodeMap().get(replica.getReplicaNodeKey()).getChannel());
			ReplicaRequest build = ReplicaRequest.newBuilder().setLeaderAddress(replica.getLeaderAddress())
					.setLeaderNodeKey(replica.getLeaderNodeKey()).setLeaderPort(replica.getLeaderPort())
					.setReplicaAddress(replica.getReplicaAddress()).setReplicaNodeKey(replica.getReplicaNodeKey())
					.setRouteKey(replica.getRouteKey()).setReplicaPort(replica.getReplicaPort()).build();
			GenericResponse response = stub.addReplica(build);
			logger.info("Replica(" + replica.getReplicaNodeKey() + ") response:" + response.getResponseCode() + "\t"
					+ response.getResponseString());
		}
	}

	public String requestNewRoute(String routeKey, int replicationFactor) throws Exception {
		logger.info("Requesting route key:" + routeKey + " with replication factor:" + replicationFactor);
		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(getCoordinator().getChannel());
		RouteResponse response = stub.addRoute(
				RouteRequest.newBuilder().setRouteKey(routeKey).setReplicationFactor(replicationFactor).build());
		if (response.getResponseCode() == 200) {
			logger.info("Route key added with replicas:" + response.getReplicaidsList() + "\tLeader:"
					+ response.getLeaderid());
			return response.getLeaderid();
		} else {
			logger.severe("Failed to add replicas:" + response.getResponseString());
			return null;
		}
	}

	public Node buildNode(String id) {
		String[] split = id.split(":");
		Node node = new Node(id, split[0], Integer.parseInt(split[1]));
		node.setInBoundChannel(ManagedChannelBuilder.forAddress(node.getAddress(), node.getPort())
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build());
		logger.info("New connection to:" + id);
		return node;
	}

}
