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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.facebook.util.digest.MurmurHash;

/**
 * @author ambud
 */
public class ConsistentHash {

	private SortedMap<Long, Node> map;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	private MurmurHash murmur;

	public ConsistentHash() {
		map = new TreeMap<>();
		murmur = new MurmurHash(MurmurHash.JCOMMON_SEED);
	}

	public long hash(String node) {
		return murmur.hashToLong(node.getBytes());
	}

	public void addNode(Node node) {
		long key = hash(node.getNodeKey());
		writeLock.lock();
		map.put(key, node);
		writeLock.unlock();
	}

	public void removeNode(Node node) {
		long key = hash(node.getNodeKey());
		writeLock.lock();
		map.remove(key);
		writeLock.unlock();
	}

	public Node getNode(String value) {
		long key = hash(value);
		readLock.lock();
		try {
			SortedMap<Long, Node> tailMap = map.tailMap(key);
			Long result = null;
			if (!tailMap.isEmpty()) {
				result = tailMap.firstKey();
			} else {
				result = map.firstKey();
			}
			return map.get(result);
		} finally {
			readLock.unlock();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		long ts = System.currentTimeMillis();
		ConsistentHash hash = new ConsistentHash();
		for (int i = 0; i < 5; i++) {
			hash.addNode(new Node("node" + i));
		}
		Map<String, Integer> map = new ConcurrentHashMap<>();
		ExecutorService es = Executors.newCachedThreadPool();
		for (int k = 0; k < 6; k++) {
			es.submit(() -> {
				ThreadLocalRandom rand = ThreadLocalRandom.current();
				for (int i = 0; i < 10_000_000; i++) {
					Node result = hash.getNode(String.valueOf(rand.nextLong()));
					if(result!=null) {
						
					}
//					Integer res = map.get(result.getNodeKey());
//					if (res == null) {
//						res = 1;
//						map.put(result.getNodeKey(), res);
//					}
//					map.put(result.getNodeKey(), res + 1);
				}
			});
		}
		es.shutdown();
		es.awaitTermination(100, TimeUnit.SECONDS);
		System.out.println(map + "\t" + (System.currentTimeMillis() - ts));
	}

	public static class Node {

		private String nodeKey;

		public Node(String nodeKey) {
			this.nodeKey = nodeKey;
		}

		/**
		 * @return the nodeKey
		 */
		public String getNodeKey() {
			return nodeKey;
		}

		/**
		 * @param nodeKey
		 *            the nodeKey to set
		 */
		public void setNodeKey(String nodeKey) {
			this.nodeKey = nodeKey;
		}

	}
}
