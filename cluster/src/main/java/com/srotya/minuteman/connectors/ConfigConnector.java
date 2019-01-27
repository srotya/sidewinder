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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;

/**
 * @author ambud
 */
public class ConfigConnector extends ClusterConnector {

	public static final String CLUSTER_CC_SLAVES = "cluster.cc.slaves";
	public static final String CLUSTER_CC_MASTER = "cluster.cc.master";
	private List<Node> slavesList;
	private boolean isMaster;
	private Node masterNode;

	public ConfigConnector() {
		slavesList = new ArrayList<>();
		isMaster = false;
	}

	@Override
	public void init(Map<String, String> conf) throws Exception {
		String master = conf.getOrDefault(CLUSTER_CC_MASTER, "localhost:55021");
		masterNode = buildNode(master);
		String[] slaves = conf.getOrDefault(CLUSTER_CC_SLAVES, "").split(",");
		for (String slave : slaves) {
			slave = slave.trim();
			if (!slave.isEmpty() && !slave.equals(masterNode.getNodeKey())) {
				slavesList.add(buildNode(slave));
			}
		}
	}

	@Override
	public void initializeRouterHooks(WALManager manager) throws IOException {
		String node = manager.getAddress() + ":" + manager.getPort();
		if (node.equals(masterNode.getNodeKey())) {
			isMaster = true;
		} else {
			isMaster = false;
		}
		System.out.println("Master:" + masterNode + "\t Local:" + node + "\t" + isMaster);
		manager.addNode(masterNode);
		for (Node slave : slavesList) {
			manager.addNode(slave);
		}
		manager.setCoordinator(masterNode);
	}

	@Override
	public int getClusterSize() throws Exception {
		return slavesList.size() + 1;
	}

	@Override
	public boolean isBootstrap() {
		return isMaster;
	}

	public List<Node> getSlavesList() {
		return slavesList;
	}

	@Override
	public boolean isCoordinator() {
		return isMaster;
	}

	@Override
	public Object fetchRoutingTable(int retryCount) {
		return getSlavesList();
	}

	@Override
	public void updateTable(Object table) {
	}

	@Override
	public Node getCoordinator() {
		return masterNode;
	}

	@Override
	public void stop() throws Exception {
	}

}
