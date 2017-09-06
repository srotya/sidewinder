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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.cluster.routing.GRPCWriter;
import com.srotya.sidewinder.cluster.routing.LocalWriter;
import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class ConfigConnector extends ClusterConnector {

	private List<Node> slavesList;
	private String master;
	private boolean isMaster;

	public ConfigConnector() {
		slavesList = new ArrayList<>();
	}

	@Override
	public void init(Map<String, String> conf) throws Exception {
		master = conf.getOrDefault("cluster.cc.master", "localhost:55021");
		isMaster = Boolean.parseBoolean(conf.getOrDefault("cluster.cc.ismaster", "false"));
		String slaves = conf.get("cluster.cc.slaves");
		if (slaves != null && !slaves.isEmpty()) {
			String[] splits = slaves.split(",");
			for (String slave : splits) {
				String[] split = slave.trim().split(":");
				slavesList.add(new Node(split[0], Integer.parseInt(split[1]), ""));
			}
		}
	}

	@Override
	public void initializeRouterHooks(RoutingEngine router) {
		String[] split = master.split(":");
		Node node = new Node(split[0], Integer.parseInt(split[1]), "");
		node.setWriter(new LocalWriter(router.getEngine()));
		router.nodeAdded(node);
		for (Node slave : slavesList) {
			ManagedChannel channel = ManagedChannelBuilder.forAddress(slave.getAddress(), slave.getPort())
					.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
			slave.setWriter(new GRPCWriter(channel));
			router.nodeAdded(slave);
		}
	}

	@Override
	public int getClusterSize() throws Exception {
		return slavesList.size() + 1;
	}

	@Override
	public boolean isBootstrap() {
		return isMaster;
	}

	public String getMaster() {
		return master;
	}

	public List<Node> getSlavesList() {
		return slavesList;
	}

	@Override
	public boolean isLeader() {
		return isMaster;
	}

	@Override
	public Object fetchRoutingTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateTable(Object table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Node getLocalNode() {
		// TODO Auto-generated method stub
		return null;
	}

}
