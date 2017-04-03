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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;
import com.srotya.sidewinder.core.rpc.Point;

/**
 * @author ambud
 */
public class TestConfigConnector {

	@Test
	public void testDefaultInit() throws Exception {
		ConfigConnector connector = new ConfigConnector();
		connector.init(new HashMap<>());
		assertEquals("localhost:55021", connector.getMaster());
		assertEquals(0, connector.getSlavesList().size());
	}

	@Test
	public void testCustomInit() throws Exception {
		ConfigConnector connector = new ConfigConnector();
		Map<String, String> conf = new HashMap<>();
		conf.put("cluster.cc.slaves", "192.168.1.1:55021, 192.168.1.2:55021");
		connector.init(conf);
		assertEquals(2, connector.getSlavesList().size());
	}

	@Test
	public void testCallbacks() throws Exception {
		ConfigConnector connector = new ConfigConnector();
		Map<String, String> conf = new HashMap<>();
		conf.put("cluster.cc.slaves", "192.168.1.1:55021, 192.168.1.2:55021");
		connector.init(conf);
		final List<Node> nodes = new ArrayList<>();
		connector.initializeRouterHooks(new RoutingEngine() {

			@Override
			public void nodeDeleted(Node node) {
			}

			@Override
			public void nodeAdded(Node node) {
				nodes.add(node);
			}

			@Override
			public List<Node> routeData(Point point, int replicationFactor) {
				// TODO Auto-generated method stub
				return null;
			}

		});
		assertEquals(3, nodes.size());
		for (Node node : nodes) {
			assertNotNull(node.getWriter());
		}
	}

	@Test
	public void testBadInit() throws Exception {
		ConfigConnector connector = new ConfigConnector();
		Map<String, String> conf = new HashMap<>();
		conf.put("cluster.cc.slaves", "192.168.1.1:55a021, 192.168.1.2:55r021");
		try {
			connector.init(conf);
			fail("Must throw exception");
		} catch (Exception e) {
		}
	}
}
