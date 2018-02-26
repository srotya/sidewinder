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
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.mapdb.DB;

import com.srotya.sidewinder.core.storage.TagIndex;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class SetIndex implements TagIndex {

	private static final String SEPERATOR = "~";
	private SortedSet<String> rowKeyIndex;
	private DB db;

	public SetIndex(String indexDir, String measurementName) throws IOException {
		rowKeyIndex = new ConcurrentSkipListSet<>();
	}

	@Override
	public String mapTag(String tag) throws IOException {
		return tag;
	}

	@Override
	public String getTagMapping(String hexString) {
		return hexString;
	}

	@Override
	public Set<String> getTags() {
		return new HashSet<>(rowKeyIndex);
	}

	@Override
	public void index(String tag, String rowKey) throws IOException {
		rowKey = tag + SEPERATOR + rowKey;
		rowKeyIndex.add(rowKey);
	}

	@Override
	public Set<String> searchRowKeysForTag(String tag) {
		Set<String> result = new HashSet<>();
		SortedSet<String> tailSet = rowKeyIndex.tailSet(tag);
		for (String entry : tailSet) {
			if (entry.startsWith(tag + SEPERATOR)) {
				result.add(entry.split(SEPERATOR)[1]);
			}else {
				break;
			}
		}
		return result;
	}

	@Override
	public void close() throws IOException {
		db.close();
	}

	@Override
	public void index(String tag, int rowIndex) throws IOException {
		// do nothing
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}