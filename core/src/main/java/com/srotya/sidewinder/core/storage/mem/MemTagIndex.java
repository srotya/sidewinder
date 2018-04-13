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
package com.srotya.sidewinder.core.storage.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.filters.ComplexTagFilter;
import com.srotya.sidewinder.core.filters.ComplexTagFilter.ComplexFilterType;
import com.srotya.sidewinder.core.filters.SimpleTagFilter;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.TagIndex;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MemTagIndex implements TagIndex {

	private Map<String, SortedMap<String, Set<Integer>>> rowKeyIndex;
	private Counter metricIndexRow;
	private Measurement m;
	@SuppressWarnings("unused")
	private boolean enableMetrics;

	@Override
	public void configure(Map<String, String> conf, String indexDir, Measurement measurement) throws IOException {
		m = measurement;
		rowKeyIndex = new ConcurrentHashMap<>();
		MetricsRegistryService instance = MetricsRegistryService.getInstance();
		if (instance != null) {
			MetricRegistry registry = instance.getInstance("requests");
			metricIndexRow = registry.counter("index-row");
			enableMetrics = true;
		}
	}

	public Set<String> getTagKeys() {
		return new HashSet<>(rowKeyIndex.keySet());
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
		SortedMap<String, Set<Integer>> map = rowKeyIndex.get(tag);

		if (map == null) {
			map = new ConcurrentSkipListMap<>();
			rowKeyIndex.put(tag, map);
		}

		Set<Integer> rowKeySet = map.get(value);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = map.get(value)) == null) {
					rowKeySet = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
					map.put(value, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowIndex)) {
			rowKeySet.add(rowIndex);
			if (metricIndexRow != null) {
				metricIndexRow.inc();
			}
		}
	}

	@Override
	public Set<ByteString> searchRowKeysForTagFilter(TagFilter tagFilterTree) {
		Set<Integer> e = evalFilterForTags(tagFilterTree);
		Set<ByteString> rowKeys = new HashSet<>();
		List<SeriesFieldMap> list = m.getSeriesList();
		if (e != null) {
			for (Integer val : e) {
				rowKeys.add(new ByteString(list.get(val).getSeriesId().toString()));
			}
		}
		return rowKeys;
	}

	public Set<Integer> evalFilterForTags(TagFilter filterTree) {
		// either it's a simple tag filter or a complex tag filter
		if (filterTree instanceof SimpleTagFilter) {
			SimpleTagFilter simpleFilter = (SimpleTagFilter) filterTree;
			SortedMap<String, Set<Integer>> sortedMap = rowKeyIndex.get(simpleFilter.getTagKey());
			if (sortedMap == null) {
				return null;
			}
			return evalSimpleTagFilter(simpleFilter, sortedMap);
		} else {
			// if it's a complex tag filter then get individual units of return
			ComplexTagFilter complexFilter = (ComplexTagFilter) filterTree;
			List<TagFilter> filters = complexFilter.getFilters();
			Set<Integer> set = new HashSet<>();
			ComplexFilterType type = complexFilter.getType();
			for (TagFilter tagFilter : filters) {
				Set<Integer> r = evalFilterForTags(tagFilter);
				if (r == null) {
					// no match found from evaluation of this filter
					if (type == ComplexFilterType.AND) {
						// if filter condition is AND then short circuit terminate the evaluation
						return set = new HashSet<>();
					} else {
						// if filter condition is OR then continue evaluation
						continue;
					}
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

	private Set<Integer> evalSimpleTagFilter(SimpleTagFilter simpleFilter, SortedMap<String, Set<Integer>> map) {
		switch (simpleFilter.getFilterType()) {
		case EQUALS:
			return map.get(simpleFilter.getComparedValue());
		case GREATER_THAN:
			SortedMap<String, Set<Integer>> tailMap = map.tailMap(simpleFilter.getComparedValue());
			if (tailMap == null || tailMap.isEmpty()) {
				return null;
			}
			Iterator<Set<Integer>> iterator = tailMap.values().iterator();
			// skip the first one since the condition is greater than
			iterator.next();
			return combineMaps(iterator);
		case LESS_THAN:
			SortedMap<String, Set<Integer>> headMap = map.headMap(simpleFilter.getComparedValue());
			if (headMap == null || headMap.isEmpty()) {
				return null;
			}
			return combineMaps(headMap.values().iterator());
		case GREATER_THAN_EQUALS:
			SortedMap<String, Set<Integer>> tailMap1 = map.tailMap(simpleFilter.getComparedValue());
			if (tailMap1 == null || tailMap1.isEmpty()) {
				return null;
			}
			Iterator<Set<Integer>> iterator1 = tailMap1.values().iterator();
			return combineMaps(iterator1);
		case LESS_THAN_EQUALS:
			SortedMap<String, Set<Integer>> headMap1 = map
					.headMap(simpleFilter.getComparedValue() + Character.MAX_VALUE);
			if (headMap1 == null || headMap1.isEmpty()) {
				return null;
			}
			return combineMaps(headMap1.values().iterator());
		case LIKE:
			List<Set<Integer>> filteredOutput = new ArrayList<>();
			Pattern p = Pattern.compile(simpleFilter.getComparedValue());
			for (Entry<String, Set<Integer>> v : map.entrySet()) {
				if (p.matcher(v.getKey()).matches()) {
					filteredOutput.add(v.getValue());
				}
			}
			return combineMaps(filteredOutput.iterator());
		}
		return null;
	}

	private Set<Integer> combineMaps(Iterator<Set<Integer>> itr) {
		Set<Integer> uberSet = new HashSet<>();
		while (itr.hasNext()) {
			uberSet.addAll(itr.next());
		}
		return uberSet;
	}

	@Override
	public Collection<String> getTagValues(String tagKey) {
		SortedMap<String, Set<Integer>> map = rowKeyIndex.get(tagKey);
		if (map != null) {
			return map.keySet();
		} else {
			return null;
		}
	}

}