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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import com.srotya.sidewinder.core.storage.TagIndex;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MapDBTagIndex implements TagIndex {

	private Map<Integer, String> tagMap;
	private Map<String, Set<String>> rowKeyIndex;
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private XXHash32 hash;
	private DB db;

	@SuppressWarnings("unchecked")
	public MapDBTagIndex(String indexDir, String measurementName) throws IOException {
		String indexPath = indexDir + "/" + measurementName + "/idx";
		db = DBMaker.fileDB(indexPath).fileMmapEnableIfSupported().concurrencyScale(4)
				.allocateStartSize(1024 * 1024 * 10).allocateIncrement(1024 * 1024 * 10).make();
		tagMap = (Map<Integer, String>) db.hashMap("fwd").createOrOpen();
		rowKeyIndex = (Map<String, Set<String>>) db.hashMap("rev").valueSerializer(new ValueSerializer()).createOrOpen();
		hash = factory.hash32();
	}

	@Override
	public String mapTag(String tag) throws IOException {
		int hash32 = hash.hash(tag.getBytes(), 0, tag.length(), 57);
		String val = tagMap.get(hash32);
		if (val == null) {
			synchronized (tagMap) {
				tagMap.put(hash32, tag);
			}
		}
		return Integer.toHexString(hash32);
	}

	@Override
	public String getTagMapping(String hexString) {
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
					rowKeyIndex.put(tag, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowKey)) {
			rowKeySet.add(rowKey);
		}
	}

	@Override
	public Set<String> searchRowKeysForTag(String tag) {
		return rowKeyIndex.get(tag);
	}

	@Override
	public void close() throws IOException {
		db.close();
	}
	
	public static final class ValueSerializer implements Serializer<Set<String>> {
		@Override
		public void serialize(DataOutput2 out, Set<String> value) throws IOException {
			out.write(value.size());
			for (String val : value) {
				out.writeUTF(val);
			}
		}

		@Override
		public Set<String> deserialize(DataInput2 input, int available) throws IOException {
			int count = input.readInt();
			Set<String> set = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
			for (int i = 0; i < count; i++) {
				set.add(input.readUTF());
			}
			return set;
		}
	}

}