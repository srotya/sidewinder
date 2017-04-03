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
package com.srotya.sidewinder.cluster.storage;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.srotya.sidewinder.cluster.storage.ConsistentHash.Node;

public class TestConsistentHashing {

	@Test
	public void testConsistentHashing() {
		ConsistentHash hash = new ConsistentHash();
		for (int i = 0; i < 5; i++) {
			hash.addNode(new Node("node" + i));
		}
		Map<String, Integer> map = new HashMap<>();
		for (int i = 0; i < 1__000_000; i++) {
			Node result = hash.getNode(UUID.randomUUID().toString());
			assertNotNull(result);
			Integer res = map.get(result.getNodeKey());
			if (res == null) {
				res = 1;
				map.put(result.getNodeKey(), res);
			}
			map.put(result.getNodeKey(), res + 1);
		}
		System.out.println(map);
	}

}
