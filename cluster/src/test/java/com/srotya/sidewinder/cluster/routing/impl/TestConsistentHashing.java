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
package com.srotya.sidewinder.cluster.routing.impl;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.Test;

import com.srotya.sidewinder.cluster.pull.routing.impl.ConsistentHashRoutingStrategy;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.utils.MiscUtils;

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
			Node result = hash.getNode(i);
			assertNotNull(result);
			Integer res = map.get(result.getNodeKey());
			if (res == null) {
				res = 1;
				map.put(result.getNodeKey(), res);
			}
			map.put(result.getNodeKey(), res + 1);
		}

		map = new TreeMap<>();
		for (int i = 0; i < 1_00_000; i++) {
			Node result = hash.getNode(i);
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

	@Test
	public void testConsistentHashingPerformance() {
		int virtualNodeAmplificationFactor = 3;
		final ConsistentHashRoutingStrategy hash = new ConsistentHashRoutingStrategy();
		for (int i = 0; i < 5; i++) {
			List<Node> nodes = new ArrayList<>();
			for (int k = 0; k < virtualNodeAmplificationFactor; k++) {
				// System.out.println(k + (i * 5) + "\t" + i + "\t" + k);
				nodes.add(new Node("node" + i, 23123, String.valueOf(k + (i * virtualNodeAmplificationFactor))));
			}
			hash.addNodes(nodes);
		}
		long ts = System.currentTimeMillis();
		int j = 10_000_000;
		Map<String, Integer> distribution = new HashMap<>();
		for (int i = 0; i < j; i++) {
			List<Tag> tagList = Arrays.asList(Tag.newBuilder().setTagKey("host")
					.setTagValue("lvsets" + String.format("%05d", i % 10000) + ".xyz.srotya.com").build());
			int h = MiscUtils.tagHashCode(tagList);
			Node n = hash.getNode(h);
			Integer value = distribution.get(n.getAddress());
			if (value == null) {
				value = 0;
			} else {
				value = value + 1;
			}
			distribution.put(n.getAddress(), value);
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("10M @:" + ((double) j) * 1000 / ts + "/s");
		distribution.entrySet().stream().forEach(e -> System.out.println(e.getKey() + " " + e.getValue()));
	}

}
