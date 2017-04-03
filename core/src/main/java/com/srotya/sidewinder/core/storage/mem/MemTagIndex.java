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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.jetty.util.ConcurrentHashSet;

import com.srotya.sidewinder.core.utils.MurmurHash;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MemTagIndex {

	private Map<Integer, String> tagMap;
	private Map<String, Set<String>> rowKeyIndex;

	public MemTagIndex() {
		tagMap = new ConcurrentHashMap<>();
		rowKeyIndex = new ConcurrentHashMap<>();
	}

	/**
	 * Hashes the tag to UI
	 * @param tag
	 * @return uid
	 */
	public String createEntry(String tag) {
		int hash32 = MurmurHash.hash32(tag);
		String val = tagMap.get(hash32);
		if (val == null) {
			tagMap.put(hash32, tag);
		}
		return Integer.toHexString(hash32);
	}

	public String getEntry(String hexString) {
		return tagMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	public Set<String> getTags() {
		return new HashSet<>(tagMap.values());
	}

	/**
	 * Indexes tag in the row key, creating an adjacency list
	 * @param tag
	 * @param rowKey
	 */
	public void index(String tag, String rowKey) {
		Set<String> rowKeySet = rowKeyIndex.get(tag);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = rowKeyIndex.get(tag)) == null) {
					rowKeySet = new ConcurrentHashSet<>();
					rowKeyIndex.put(tag, rowKeySet);
				}
			}
		}
		rowKeySet.add(rowKey);
	}

	public Set<String> searchRowKeysForTag(String tag) {
		return rowKeyIndex.get(tag);
	}

}