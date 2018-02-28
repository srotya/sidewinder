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
package com.srotya.sidewinder.core.storage.mem;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.storage.TagIndex;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MemTagIndex implements TagIndex {

	private Map<Integer, String> tagKeyMap;
	private Map<Integer, String> tagValueMap;
	private Map<String, Map<String, Set<String>>> rowKeyIndex;
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private XXHash32 hash;
	private Counter metricIndexTag;
	private Counter metricIndexRow;

	public MemTagIndex(MetricRegistry registry) {
		tagKeyMap = new ConcurrentHashMap<>();
		tagValueMap = new ConcurrentHashMap<>();
		rowKeyIndex = new ConcurrentHashMap<>();
		hash = factory.hash32();
		metricIndexTag = registry.counter("index-tag");
		metricIndexRow = registry.counter("index-row");
	}

	/**
	 * Hashes the tag to UI
	 * 
	 * @param tagKey
	 * @return uid
	 */
	public String mapTagKey(String tagKey) {
		int hash32 = hash.hash(tagKey.getBytes(), 0, tagKey.length(), 57);
		String val = tagKeyMap.get(hash32);
		if (val == null) {
			tagKeyMap.put(hash32, tagKey);
		}
		metricIndexTag.inc();
		return Integer.toHexString(hash32);
	}

	@Override
	public String mapTagValue(String tagValue) throws IOException {
		int hash32 = hash.hash(tagValue.getBytes(), 0, tagValue.length(), 57);
		String val = tagKeyMap.get(hash32);
		if (val == null) {
			tagValueMap.put(hash32, tagValue);
		}
		metricIndexTag.inc();
		return Integer.toHexString(hash32);
	}

	public String getTagKeyMapping(String hexString) {
		return tagKeyMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	@Override
	public String getTagValueMapping(String hexString) throws IOException {
		return tagValueMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	public Set<String> getTags() {
		return new HashSet<>(tagKeyMap.values());
	}

	/**
	 * Indexes tag in the row key, creating an adjacency list
	 * 
	 * @param tagKey
	 * @param tagValue
	 * @param rowKey
	 */
	public void index(String tagKey, String tagValue, String rowKey) {
		Map<String, Set<String>> map = rowKeyIndex.get(tagKey);

		if (map == null) {
			map = new ConcurrentHashMap<>();
			rowKeyIndex.put(tagKey, map);
		}

		Set<String> rowKeySet = map.get(tagValue);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = map.get(tagValue)) == null) {
					rowKeySet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
					map.put(tagValue, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowKey)) {
			rowKeySet.add(rowKey);
			metricIndexRow.inc();
		}
	}

	public Set<String> searchRowKeysForTag(String tagKey, String tagValue) {
		Map<String, Set<String>> map = rowKeyIndex.get(tagKey);
		if (map == null) {
			return null;
		} else {
			return map.get(tagValue);
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public int getSize() {
		return 0;
	}

	@Override
	public void index(String tag, String value, int rowIndex) throws IOException {
		// not implemented
	}

}