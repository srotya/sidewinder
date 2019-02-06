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
package com.srotya.minuteman.cluster.router;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.srotya.minuteman.cluster.Node;

public class TestConsistentHashRoutingStrategy {

	@Test
	public void testConsistentHashingRouting() {
		RoutingStrategy strategy = new ConsistentHashRoutingStrategy();
		Node n1 = new Node("localhost", 2127, false);
		Node n2 = new Node("localhost", 2122, false);
		Node n3 = new Node("localhost", 2123, false);
		strategy.addNodes(Arrays.asList(n1, n2, n3));
		assertEquals(30, strategy.size());
		Map<Node, AtomicInteger> countMap = new HashMap<>();
		countMap.put(n1, new AtomicInteger(0));
		countMap.put(n2, new AtomicInteger(0));
		countMap.put(n3, new AtomicInteger(0));
		for (int i = 0; i < 10000; i++) {
			Node node = strategy.getRoute((int) UUID.randomUUID().getMostSignificantBits());
			countMap.get(node).incrementAndGet();
		}
		System.out.println(countMap);
		strategy.removeNode(n1.getNodeKey());
		assertEquals(20, strategy.size());
		strategy.removeNode(n2.getNodeKey());
		assertEquals(10, strategy.size());
		strategy.addNodes(Arrays.asList(n1, n2, n3));
		assertEquals(30, strategy.size());
	}

}