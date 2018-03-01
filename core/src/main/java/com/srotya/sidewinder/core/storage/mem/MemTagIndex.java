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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.TagFilter;
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
	private Map<String, SortedMap<String, Set<String>>> rowKeyIndex;
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private XXHash32 hash;
	private Counter metricIndexTag;
	private Counter metricIndexRow;

	public MemTagIndex(MetricRegistry registry) {
		tagKeyMap = new ConcurrentHashMap<>();
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
		return tagValue;
	}

	public String getTagKeyMapping(String hexString) {
		return tagKeyMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	@Override
	public String getTagValueMapping(String hexString) throws IOException {
		return hexString;
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
		SortedMap<String, Set<String>> map = rowKeyIndex.get(tagKey);

		if (map == null) {
			map = new ConcurrentSkipListMap<>();
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
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> searchRowKeysForTagFilter(TagFilter tagFilterTree) {
		return evalFilterForTags(tagFilterTree);
	}

	public Set<String> evalFilterForTags(TagFilter filterTree) {
		// either it's a simple tag filter or a complex tag filter
		if (filterTree instanceof SimpleTagFilter) {
			SimpleTagFilter simpleFilter = (SimpleTagFilter) filterTree;
			SortedMap<String, Set<String>> sortedMap = rowKeyIndex.get(simpleFilter.getTagKey());
			if (sortedMap == null) {
				return null;
			}
			return evalSimpleTagFilter(simpleFilter, sortedMap);
		} else {
			// if it's a complex tag filter then get individual units of return
			ComplexTagFilter complexFilter = (ComplexTagFilter) filterTree;
			List<TagFilter> filters = complexFilter.getFilters();
			Set<String> set = new HashSet<>();
			ComplexFilterType type = complexFilter.getType();
			for (TagFilter tagFilter : filters) {
				Set<String> r = evalFilterForTags(tagFilter);
				if (r == null) {
					// no match found from evaluation of this filter
					continue;
				} else if (set.isEmpty()) {
					set.addAll(r);
				}
				switch (type) {
				case AND:
					set.retainAll(r);
					break;
				case OR:
					set.addAll(r);
					break;
				}
			}
			return set;
		}
	}

	private Set<String> evalSimpleTagFilter(SimpleTagFilter simpleFilter, SortedMap<String, Set<String>> map) {
		switch (simpleFilter.getFilterType()) {
		case EQUALS:
			return map.get(simpleFilter.getComparedValue());
		case GREATER_THAN:
			SortedMap<String, Set<String>> tailMap = map.tailMap(simpleFilter.getComparedValue());
			if (tailMap == null || tailMap.isEmpty()) {
				return null;
			}
			Iterator<Set<String>> iterator = tailMap.values().iterator();
			// skip the first one since the condition is greater than
			iterator.next();
			return combineMaps(iterator);
		case LESS_THAN:
			SortedMap<String, Set<String>> headMap = map.headMap(simpleFilter.getComparedValue());
			if (headMap == null || headMap.isEmpty()) {
				return null;
			}
			return combineMaps(headMap.values().iterator());
		case GREATER_THAN_EQUALS:
			SortedMap<String, Set<String>> tailMap1 = map.tailMap(simpleFilter.getComparedValue());
			if (tailMap1 == null || tailMap1.isEmpty()) {
				return null;
			}
			Iterator<Set<String>> iterator1 = tailMap1.values().iterator();
			return combineMaps(iterator1);
		case LESS_THAN_EQUALS:
			SortedMap<String, Set<String>> headMap1 = map
					.headMap(simpleFilter.getComparedValue() + Character.MAX_VALUE);
			if (headMap1 == null || headMap1.isEmpty()) {
				return null;
			}
			return combineMaps(headMap1.values().iterator());
		}
		return null;
	}

	private Set<String> combineMaps(Iterator<Set<String>> itr) {
		Set<String> uberSet = new HashSet<>();
		while (itr.hasNext()) {
			uberSet.addAll(itr.next());
		}
		return uberSet;
	}

}