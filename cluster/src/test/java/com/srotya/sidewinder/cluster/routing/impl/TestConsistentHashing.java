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
package com.srotya.sidewinder.cluster.routing.impl;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.Test;

import com.srotya.sidewinder.cluster.routing.Node;
import com.srotya.sidewinder.cluster.routing.impl.ConsistentHashRoutingStrategy;

/**
 * Unit tests for {@link ConsistentHashRoutingStrategy}
 * 
 * @author ambud
 */
public class TestConsistentHashing {

	@Test
	public void testConsistentHashing() {
		int virtualNodeAmplificationFactor = 10;
		ConsistentHashRoutingStrategy hash = new ConsistentHashRoutingStrategy();
		for (int i = 0; i < 5; i++) {
			List<Node> nodes = new ArrayList<>();
			for (int k = 0; k < virtualNodeAmplificationFactor; k++) {
				// System.out.println(k + (i * 5) + "\t" + i + "\t" + k);
				nodes.add(new Node("node", i + 10, String.valueOf(k + (i * virtualNodeAmplificationFactor))));
			}
			hash.addNodes(nodes);
		}
		Map<String, Integer> map = new TreeMap<>();
		for (int i = 0; i < 1_00_000; i++) {
			Node result = hash.getNode(String.valueOf(i));
			assertNotNull(result);
			Integer res = map.get(result.getNodeKey());
			if (res == null) {
				res = 1;
				map.put(result.getNodeKey(), res);
			}
			map.put(result.getNodeKey(), res + 1);
		}
		long ts = System.currentTimeMillis();

		for (int i = 0; i < 2; i++) {
			for (String key : hash.getRouteTable().keySet()) {
				hash.getNodes(key, 3);
			}
			List<Node> nodes = new ArrayList<>();
			for (int k = 0; k < virtualNodeAmplificationFactor; k++) {
				nodes.add(new Node("node", i, String.valueOf(k + (i * virtualNodeAmplificationFactor))));
			}
		}

		ts = System.currentTimeMillis() - ts;
		System.out.println("10M in:" + ts);

		map = new TreeMap<>();
		for (int i = 0; i < 1_00_000; i++) {
			Node result = hash.getNode(String.valueOf(i));
			assertNotNull(result);
			Integer res = map.get(result.getNodeKey());
			if (res == null) {
				res = 1;
				map.put(result.getNodeKey(), res);
			}
			map.put(result.getNodeKey(), res + 1);
		}
		for (Entry<String, Integer> entry : map.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}

}
