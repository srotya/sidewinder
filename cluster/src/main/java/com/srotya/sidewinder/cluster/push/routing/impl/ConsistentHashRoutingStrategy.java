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
package com.srotya.sidewinder.cluster.push.routing.impl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	private SortedMap<Long, Node> map;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private Map<String, List<Node>> routeTable;
	private HashFunction hf;

	public ConsistentHashRoutingStrategy() {
		map = new TreeMap<>();
		// murmur = new MurmurHash(MurmurHash.JCOMMON_SEED);
		routeTable = new HashMap<>();
		hf = Hashing.sha1();
	}

	public long hash(String node) {
		// return murmur.hashToLong(node.getBytes(UTF8));
		return hf.hashString(node, UTF8).asLong();
	}

	public List<String> addNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			long key = hash(node.getNodeKey());
			map.put(key, node);
		}
		List<String> keyMovements = recomputeKeydistribution();
		writeLock.unlock();
		return keyMovements;
	}

	public List<String> addNode(Node node) {
		long key = hash(node.getNodeKey());
		writeLock.lock();
		map.put(key, node);
		List<String> keyMovements = recomputeKeydistribution();
		writeLock.unlock();
		return keyMovements;
	}

	private List<String> recomputeKeydistribution() {
		List<String> keyMovements = new ArrayList<>();
		ArrayList<String> keySet = new ArrayList<>(routeTable.keySet());
		for (String keyEntry : keySet) {
			List<Node> list = routeTable.get(keyEntry);
			if (list == null) {
				// TODO handle this weird situation and check if it's at all
				// possible
				continue;
			}
			List<Node> newList = computeNodePlacement(keyEntry, list.size());
			if (!list.equals(newList)) {
				keyMovements.add(keyEntry);
				routeTable.put(keyEntry, newList);
			}
		}
		return keyMovements;
	}

	public List<String> removeNode(Node node) {
		long key = hash(node.getNodeKey());
		writeLock.lock();
		map.remove(key);
		List<String> keyMovements = recomputeKeydistribution();
		writeLock.unlock();
		return keyMovements;
	}

	public List<String> removeNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			long key = hash(node.getNodeKey());
			map.remove(key);
		}
		List<String> keyMovements = recomputeKeydistribution();
		writeLock.unlock();
		return keyMovements;
	}

	private List<Node> computeNodePlacement(String value, int replicas) {
		long key = hash(value);
		SortedMap<Long, Node> tailMap = map.tailMap(key);
		List<Node> nodes = new ArrayList<>();
		Iterator<Entry<Long, Node>> iterator = null;
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

	public Node getNode(String value) {
		writeLock.lock();
		List<Node> nodes = routeTable.get(value);
		if (nodes == null) {
			nodes = computeNodePlacement(value, 1);
			routeTable.put(value, nodes);
		}
		writeLock.unlock();
		return nodes.get(0);
	}

	@Override
	public List<Node> getNodes(String value, int replicas) {
		writeLock.lock();
		List<Node> nodes = routeTable.get(value);
		if (nodes == null) {
			nodes = computeNodePlacement(value, replicas);
			routeTable.put(value, nodes);
		}
		writeLock.unlock();
		return nodes;
	}

	/**
	 * @return the routeTable
	 */
	public Map<String, List<Node>> getRouteTable() {
		return routeTable;
	}

	@Override
	public List<Node> getAllNodes() {
		return new ArrayList<>(map.values());
	}

	@Override
	public int getReplicationFactor(String key) {
		int replica = 0;
		writeLock.lock();
		List<Node> list = routeTable.get(key);
		if (list != null) {
			replica = list.size();
		}
		writeLock.unlock();
		return replica;
	}

}
