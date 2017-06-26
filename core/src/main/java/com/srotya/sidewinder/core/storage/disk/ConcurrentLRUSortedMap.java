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

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A sorted map that evicts least recently used items and has a fixed capacity
 * 
 * @author ambud
 */
public class ConcurrentLRUSortedMap implements SortedMap<String, BucketEntry> {

	private ReentrantLock lock = new ReentrantLock();
	private SortedMap<String, BucketEntry> mapCore;
	private BucketEntry head;
	private BucketEntry tail;
	private volatile int maxSize;

	public ConcurrentLRUSortedMap(int maxSize) {
		if (maxSize <= 0) {
			throw new IllegalArgumentException("Invalid max size for LRU");
		}
		this.maxSize = maxSize;
		this.mapCore = new ConcurrentSkipListMap<String, BucketEntry>();
	}

	@Override
	public int size() {
		return mapCore.size();
	}

	@Override
	public boolean isEmpty() {
		return mapCore.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return mapCore.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return mapCore.containsValue(value);
	}

	@Override
	public void clear() {
		lock.lock();
		mapCore.clear();
		head = null;
		tail = null;
		lock.unlock();
	}

	@Override
	public Comparator<? super String> comparator() {
		return mapCore.comparator();
	}

	@Override
	public String firstKey() {
		lock.lock();
		String firstKey = mapCore.firstKey();
		lock.unlock();
		return firstKey;
	}

	@Override
	public String lastKey() {
		lock.lock();
		String lastKey = mapCore.lastKey();
		lock.unlock();
		return lastKey;
	}

	@Override
	public Set<String> keySet() {
		lock.lock();
		Set<String> keySet = mapCore.keySet();
		lock.unlock();
		return keySet;
	}

	@Override
	public BucketEntry get(Object key) {
		lock.lock();
		BucketEntry entry = mapCore.get(key);
		if (entry == null) {
			return null;
		}
		BucketEntry prev = entry.getPrev();
		BucketEntry next = entry.getNext();
		if (prev != null) {
			prev.setNext(next);
		}
		if (next != null) {
			next.setPrev(prev);
		}
		head.setPrev(entry);
		entry.setNext(head);
		head = entry;
		prune();
		lock.unlock();
		return entry;
	}

	@Override
	public BucketEntry put(String key, BucketEntry value) {
		lock.lock();
		prune();
		mapCore.put(key, value);
		if (head != null) {
			value.setNext(head);
			head.setPrev(value);
		}
		head = value;
		if (tail == null) {
			tail = value;
		}
		lock.unlock();
		return null;
	}

	private void prune() {
		if (mapCore.size() >= maxSize) {
			BucketEntry prev = tail.getPrev();
			if (prev == null) {
				System.err.println("Tail previous is null:" + head);
				throw new NullPointerException();
			}
			prev.setNext(null);
			tail.close();
			tail = prev;
		}
	}

	@Override
	public BucketEntry remove(Object key) {
		lock.lock();
		BucketEntry bucketEntry = mapCore.remove(key);
		if (bucketEntry != null) {
			BucketEntry next = bucketEntry.getNext();
			BucketEntry prev = bucketEntry.getPrev();
			if (prev != null) {
				prev.setNext(next);
			}
			if (next != null) {
				next.setPrev(prev);
			}
		}
		lock.unlock();
		return bucketEntry;
	}

	@Override
	public void putAll(Map<? extends String, ? extends BucketEntry> m) {
		for (Entry<? extends String, ? extends BucketEntry> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public SortedMap<String, BucketEntry> subMap(String fromKey, String toKey) {
		SortedMap<String, BucketEntry> subMap = mapCore.subMap(fromKey, toKey);
		for (java.util.Map.Entry<String, BucketEntry> entry : subMap.entrySet()) {
			get(entry.getKey());
		}
		return subMap;
	}

	@Override
	public SortedMap<String, BucketEntry> headMap(String toKey) {
		return mapCore.headMap(toKey);
	}

	@Override
	public SortedMap<String, BucketEntry> tailMap(String fromKey) {
		return mapCore.tailMap(fromKey);
	}

	@Override
	public Collection<BucketEntry> values() {
		return mapCore.values();
	}

	@Override
	public Set<java.util.Map.Entry<String, BucketEntry>> entrySet() {
		lock.lock();
		Set<java.util.Map.Entry<String, BucketEntry>> entrySet = mapCore.entrySet();
		lock.unlock();
		return entrySet;
	}

	@Override
	public String toString() {
		return mapCore.toString();
	}

}
