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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.utils.MiscUtils;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class DiskTagIndex implements TagIndex {

	private Map<Integer, String> tagMap;
	private Map<String, Set<String>> rowKeyIndex;
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private String indexPath;
	private XXHash32 hash;

	public DiskTagIndex(String baseIndexDirectory, String dbName, String measurementName) throws IOException {
		this.indexPath = baseIndexDirectory + "/" + dbName;
		new File(indexPath).mkdirs();
		this.indexPath += "/" + measurementName;
		tagMap = new ConcurrentHashMap<>();
		rowKeyIndex = new ConcurrentHashMap<>();
		hash = factory.hash32();
		loadTagIndex();
	}

	protected void loadTagIndex() throws IOException {
		File fwd = new File(indexPath + ".fwd");
		if (!fwd.exists()) {
			return;
		}
		List<String> tags = MiscUtils.readAllLines(fwd);
		// load forward lookup
		for (String tag : tags) {
			String[] split = tag.split("\t");
			String hash32 = split[0];
			tag = split[1];
			tagMap.put(Integer.parseInt(hash32), tag);
		}
		// load reverse lookup
		File rev = new File(indexPath + ".rev");
		if (!rev.exists()) {
			return;
		}
		List<String> revs = MiscUtils.readAllLines(rev);
		for (String r : revs) {
			String[] split = r.split("\t");
			String tag = split[0];
			Set<String> set = rowKeyIndex.get(tag);
			if (set == null) {
				set = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
				rowKeyIndex.put(split[0], set);
			}
			String rowKey = split[1];
			set.add(rowKey);
		}
	}

	@Override
	public String createEntry(String tag) throws IOException {
		int hash32 = hash.hash(tag.getBytes(), 0, tag.length(), 57);
		String val = tagMap.get(hash32);
		if (val == null) {
			synchronized (tagMap) {
				String out = tagMap.put(hash32, tag);
				if (out == null) {
					DiskStorageEngine.appendLineToFile(hash32 + "\t" + tag, indexPath + ".fwd");
				}
			}
		}
		return Integer.toHexString(hash32);
	}

	@Override
	public String getEntry(String hexString) {
		return tagMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	@Override
	public Set<String> getTags() {
		return new HashSet<>(tagMap.values());
	}

	@Override
	public void index(String tag, String rowKey) throws IOException {
		Set<String> rowKeySet = rowKeyIndex.get(tag);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = rowKeyIndex.get(tag)) == null) {
					rowKeySet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
					;
					rowKeyIndex.put(tag, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowKey)) {
			boolean add = rowKeySet.add(rowKey);
			if (add) {
				synchronized (tagMap) {
					DiskStorageEngine.appendLineToFile(tag + "\t" + rowKey, indexPath + ".rev");
				}
			}
		}
	}

	@Override
	public Set<String> searchRowKeysForTag(String tag) {
		return rowKeyIndex.get(tag);
	}

}