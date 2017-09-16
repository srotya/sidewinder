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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.facebook.util.digest.MurmurHash;
import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingStrategy;

/**
 * @author ambud
 */
public class ModHashRoutingStrategy implements RoutingStrategy {

	private static final Charset UTF8 = Charset.forName("utf-8");
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	private Map<Integer, Node> nodeSet;
	private MurmurHash murmur;

	public ModHashRoutingStrategy() {
		nodeSet = new ConcurrentHashMap<>();
		murmur = new MurmurHash(MurmurHash.JCOMMON_SEED);
	}

	@Override
	public Node getNode(String key) {
		readLock.lock();
		int size = (int) (murmur.hashToLong(key.getBytes(UTF8)) % nodeSet.size());
		Node node = nodeSet.get(size);
		readLock.unlock();
		return node;
	}

	@Override
	public List<Node> getNodes(String key, int replicas) {
		readLock.lock();
		List<Node> nodes = new ArrayList<>();
		for (int i = 0; i < replicas; i++) {
			int size = (int) ((murmur.hashToLong(key.getBytes(UTF8)) + i) % nodeSet.size());
			Node node = nodeSet.get(size);
			nodes.add(node);
		}
		readLock.unlock();
		return nodes;
	}

	@Override
	public List<String> addNode(Node node) {
		writeLock.lock();
		nodeSet.put(nodeSet.size(), node);
		writeLock.unlock();
		return new ArrayList<>();
	}

	@Override
	public List<String> removeNode(Node node) {
		return new ArrayList<>();
	}

	@Override
	public List<Node> getAllNodes() {
		return new ArrayList<>(nodeSet.values());
	}

	@Override
	public List<String> addNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			nodeSet.put(nodeSet.size(), node);
		}
		writeLock.unlock();
		return new ArrayList<>();
	}

	@Override
	public List<String> removeNodes(List<Node> nodes) {
		return new ArrayList<>();
	}

	@Override
	public int getReplicationFactor(String key) {
		return 1;
	}

}
