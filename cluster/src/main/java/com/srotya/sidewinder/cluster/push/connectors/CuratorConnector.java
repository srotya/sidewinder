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
package com.srotya.sidewinder.cluster.push.connectors;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import com.srotya.sidewinder.cluster.push.routing.LocalEndpointService;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;

/**
 * @author ambud
 */
public class CuratorConnector extends ClusterConnector {

	private Logger logger = Logger.getLogger(CuratorConnector.class.getName());
	private LeaderLatch leaderLatch;
	private String id;
	private CuratorFramework curator;
	private int port;
	private String address;

	@Override
	public void init(Map<String, String> conf) throws Exception {
		curator = CuratorFrameworkFactory.newClient("localhost:2181",
				new BoundedExponentialBackoffRetry(1000, 5000, 60));
		curator.start();
		curator.getZookeeperClient().blockUntilConnectedOrTimedOut();
		id = conf.getOrDefault("cluster.curator.nodeid", "0");
	}

	@SuppressWarnings("resource")
	@Override
	public void initializeRouterHooks(RoutingEngine engine) throws Exception {
		port = engine.getPort();
		address = engine.getAddress();
		Node localNode = new Node(address, port, address + ":" + port);
		localNode.setEndpointService(new LocalEndpointService(engine.getEngine(), engine));
		engine.nodeAdded(localNode);

		PersistentNode node = new PersistentNode(curator, CreateMode.EPHEMERAL, true, "/sidewinder-nodes" + "/" + id,
				localNode.getNodeKey().getBytes());
		node.start();
		
		CuratorListener listener = new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client,
					CuratorEvent event) throws Exception {
				System.out.println("setDataAsync: " + event);
			}
		};
		curator.getCuratorListenable().addListener(listener);

		leaderLatch = new LeaderLatch(curator, "/sidewinder-leader", id);
		leaderLatch.addListener(new LeaderLatchListener() {

			@Override
			public void notLeader() {
				try {
					Participant leader = leaderLatch.getLeader();
					logger.info("Detected new leader:" + leader.getId());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			@Override
			public void isLeader() {
				try {
					engine.makeCoordinator();
				} catch (Exception e) {
					e.printStackTrace();
					logger.severe("Error making corrdinator");
				}
			}
		});
		leaderLatch.start();
	}

	@Override
	public int getClusterSize() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isBootstrap() {
		return true;
	}

	@Override
	public boolean isLeader() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object fetchRoutingTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateTable(Object table) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Node getLocalNode() {
		// TODO Auto-generated method stub
		return null;
	}

}
