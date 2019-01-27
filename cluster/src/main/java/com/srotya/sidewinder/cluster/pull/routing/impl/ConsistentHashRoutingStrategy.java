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
package com.srotya.sidewinder.cluster.pull.routing.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingStrategy;

/**
 * @author ambud
 */
public class ConsistentHashRoutingStrategy implements RoutingStrategy {

	private static final Charset UTF8 = Charset.forName("utf-8");
	private SortedMap<Integer, Node> map;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	private HashFunction hf;

	public ConsistentHashRoutingStrategy() {
		map = new TreeMap<>();
		// murmur = new MurmurHash(MurmurHash.JCOMMON_SEED);
		hf = Hashing.sha1();
	}

	public int hash(String node) {
		// return murmur.hashToLong(node.getBytes(UTF8));
		return hf.hashString(node, UTF8).asInt();
	}

	public int hash(int node) {
		// return murmur.hashToLong(node.getBytes(UTF8));
		return hf.hashInt(node).asInt();
	}

	public void addNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			int key = hash(node.getNodeKey());
			map.put(key, node);
		}
		writeLock.unlock();
	}

	public void addNode(Node node) {
		int key = hash(node.getNodeKey());
		writeLock.lock();
		map.put(key, node);
		// List<Integer> keyMovements = recomputeKeydistribution();
		writeLock.unlock();
	}

	// private List<Integer> recomputeKeydistribution() {
	// List<Integer> keyMovements = new ArrayList<>();
	// ArrayList<Integer> keySet = new ArrayList<>(routeTable.keySet());
	// for (Integer keyEntry : keySet) {
	// List<Node> list = routeTable.get(keyEntry);
	// if (list == null) {
	// // TODO handle this weird situation and check if it's at all
	// // possible
	// continue;
	// }
	// List<Node> newList = computeNodePlacement(keyEntry, list.size());
	// if (!list.equals(newList)) {
	// keyMovements.add(keyEntry);
	// routeTable.put(keyEntry, newList);
	// }
	// }
	// return keyMovements;
	// }

	public void removeNode(Node node) {
		int key = hash(node.getNodeKey());
		writeLock.lock();
		map.remove(key);
		writeLock.unlock();
	}

	public void removeNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			int key = hash(node.getNodeKey());
			map.remove(key);
		}
		writeLock.unlock();
	}

	private List<Node> computeNodePlacement(Integer value, int replicas) {
		int key = hash(value);
		SortedMap<Integer, Node> tailMap = map.tailMap(key);
		List<Node> nodes = new ArrayList<>();
		Iterator<Entry<Integer, Node>> iterator = null;
		if (!tailMap.isEmpty()) {
			iterator = tailMap.entrySet().iterator();
		} else {
			iterator = map.entrySet().iterator();
		}
		int i = 0;
		while (i < replicas && iterator.hasNext()) {
			nodes.add(iterator.next().getValue());
			i++;
		}
		return nodes;
	}

	public Node getNode(Integer value) {
		readLock.lock();
		Node node = computeNodePlacement(value, 1).get(0);
		readLock.unlock();
		return node;
	}

	@Override
	public List<Node> getNodes(Integer value, int replicas) {
		readLock.lock();
		List<Node> nodes = computeNodePlacement(value, replicas);
		readLock.unlock();
		return nodes;
	}

	@Override
	public List<Node> getAllNodes() {
		return new ArrayList<>(map.values());
	}

	// @Override
	// public int getReplicationFactor(Integer key) {
	// int replica = 0;
	// writeLock.lock();
	// List<Node> list = routeTable.get(key);
	// if (list != null) {
	// replica = list.size();
	// }
	// writeLock.unlock();
	// return replica;
	// }

}
