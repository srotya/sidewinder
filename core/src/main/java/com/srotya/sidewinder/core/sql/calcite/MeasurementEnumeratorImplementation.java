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
package com.srotya.sidewinder.core.sql.calcite;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.calcite.linq4j.Enumerator;

import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.FieldReaderIterator;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;

final class MeasurementEnumeratorImplementation implements Enumerator<Object[]> {
	/**
	 * 
	 */
	private final MeasurementTable measurementTable;
	private final Entry<Long, Long> range;
	private final List<String> fields;
	private Map<ByteString, FieldReaderIterator[]> readers;
	private Iterator<Entry<ByteString, FieldReaderIterator[]>> iterator;
	private Entry<List<Tag>, FieldReaderIterator[]> next;
	private Object[] extracted;
	private Map<Integer, String> tagMapToFields;
	private int i;
	private boolean tagOnly = false;
	private List<Boolean> fTypes;
	private long queryTs;

	public MeasurementEnumeratorImplementation(MeasurementTable measurementTable, Entry<Long, Long> range,
			List<String> fields, List<Boolean> fTypes) {
		this.measurementTable = measurementTable;
		this.range = range;
		this.fields = fields;
		this.fTypes = fTypes;
	}

	@Override
	public void reset() {
		try {
			initializeIterator();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Map<String, String> getTagMapFromList(List<Tag> tags) {
		Map<String, String> tagMap = new HashMap<>();
		for (Tag tag : tags) {
			tagMap.put(tag.getTagKey(), tag.getTagValue());
		}
		return tagMap;
	}

	public Map<Integer, String> tagMapToFields(List<Tag> tags, List<String> fields) {
		Map<Integer, String> valueMap = new HashMap<>();
		Map<String, String> map = getTagMapFromList(tags);
		for (int i = 0; i < fields.size(); i++) {
			String field = fields.get(i);
			if (map.containsKey(field)) {
				valueMap.put(i, map.get(field));
			}
		}
		return valueMap;
	}

	@Override
	public boolean moveNext() {
		try {
			if (readers == null) {
				queryTs = System.currentTimeMillis();
				try {
					readers = this.measurementTable.getStorageEngine().queryReaders(this.measurementTable.dbName,
							this.measurementTable.measurementName, fields, false, range.getKey(), range.getValue());
					initializeIterator();
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
			i++;
			if (next != null) {
				if (!tagOnly) {
					return nonTagOnlyReads();
				} else {
					return tagOnlyReads();
				}
			} else {
				queryTs = System.currentTimeMillis() - queryTs;
				System.out.println("Row count:" + i + " in " + queryTs + "ms");
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private void initializeIterator() throws IOException {
		iterator = readers.entrySet().iterator();
		if (iterator.hasNext()) {
			Entry<ByteString, FieldReaderIterator[]> next2 = iterator.next();
			List<Tag> decodeTagsFromString = this.measurementTable.getStorageEngine().decodeTagsFromString(
					this.measurementTable.dbName, this.measurementTable.measurementName, next2.getKey());
			tagMapToFields = tagMapToFields(decodeTagsFromString, fields);
			next = new AbstractMap.SimpleEntry<List<Tag>, FieldReaderIterator[]>(decodeTagsFromString,
					next2.getValue());
			if (fields.size() == tagMapToFields.size()) {
				tagOnly = true;
			}
		}
	}

	private boolean tagOnlyReads() throws IOException {
		extracted = new Object[tagMapToFields.size()];
		for (Entry<Integer, String> entry : tagMapToFields.entrySet()) {
			extracted[entry.getKey()] = entry.getValue();
		}
		try {
			Entry<ByteString, FieldReaderIterator[]> next2 = iterator.next();
			List<Tag> decodeTagsFromString = this.measurementTable.getStorageEngine().decodeTagsFromString(
					this.measurementTable.dbName, this.measurementTable.measurementName, next2.getKey());
			tagMapToFields = tagMapToFields(decodeTagsFromString, fields);
			next = new AbstractMap.SimpleEntry<List<Tag>, FieldReaderIterator[]>(decodeTagsFromString,
					next2.getValue());
		} catch (NoSuchElementException e1) {
			next = null;
		}
		return true;
	}

	private boolean nonTagOnlyReads() {
		try {
			FieldReaderIterator[] iterators = next.getValue();
			try {
				extracted = FieldReaderIterator.extractedObject(iterators, i, fTypes);
				for (Entry<Integer, String> entry : tagMapToFields.entrySet()) {
					extracted[entry.getKey()] = entry.getValue();
				}
				return true;
			} catch (FilteredValueException e) {
				return moveNext();
			}
		} catch (IOException e) {
			if (iterator.hasNext()) {
				Entry<ByteString, FieldReaderIterator[]> next2 = iterator.next();
				List<Tag> decodeTagsFromString = null;
				try {
					decodeTagsFromString = this.measurementTable.getStorageEngine().decodeTagsFromString(
							this.measurementTable.dbName, this.measurementTable.measurementName, next2.getKey());
					next = new AbstractMap.SimpleEntry<List<Tag>, FieldReaderIterator[]>(decodeTagsFromString,
							next2.getValue());
					tagMapToFields = tagMapToFields(decodeTagsFromString, fields);
				} catch (IOException e1) {
					next = null;
					throw new RuntimeException(e);
				}
			} else {
				next = null;
			}
			if (next != null) {
				return true;
			} else {
				queryTs = System.currentTimeMillis() - queryTs;
				System.out.println("Row count:" + i + " in " + queryTs + "ms");
				return false;
			}
		}
	}

	@Override
	public Object[] current() {
		return extracted;
	}

	@Override
	public void close() {
		readers = null;
		iterator = null;
		next = null;
		extracted = null;
		tagMapToFields = null;
	}
}