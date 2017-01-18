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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * @author ambud
 *
 */
public class TestClusteredStorageEngine {

	@Test
	public void testEncodeDecodProxyName() {
		String encoded = ClusteredMemStorageEngine.encodeDbAndProxyName("test", "1");
		List<String> proxies = new ArrayList<>();
		String dbName = ClusteredMemStorageEngine.decodeDbAndProxyNames(proxies, encoded);
		assertEquals("test", dbName);
		assertEquals("1", proxies.get(0));
	}

	@Test
	public void testMultiEncodeDecodProxyNames() {
		String encoded = ClusteredMemStorageEngine.encodeDbAndProxyName("test", "1");
		encoded = ClusteredMemStorageEngine.encodeDbAndProxyName(encoded, "2");
		encoded = ClusteredMemStorageEngine.encodeDbAndProxyName(encoded, "3");
		
		List<String> proxies = new ArrayList<>();
		String dbName = ClusteredMemStorageEngine.decodeDbAndProxyNames(proxies, encoded);
		assertEquals("test", dbName);
		assertEquals("1", proxies.get(0));
		assertEquals("2", proxies.get(1));
		assertEquals("3", proxies.get(2));
	}
	
}
