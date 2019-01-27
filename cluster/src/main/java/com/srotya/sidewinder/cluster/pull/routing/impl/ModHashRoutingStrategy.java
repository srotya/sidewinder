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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingStrategy;

/**
 * @author ambud
 */
public class ModHashRoutingStrategy implements RoutingStrategy {

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	private Map<Integer, Node> nodeSet;

	public ModHashRoutingStrategy() {
		nodeSet = new ConcurrentHashMap<>();
	}

	@Override
	public Node getNode(Integer key) {
		readLock.lock();
		int size = key % nodeSet.size();
		Node node = nodeSet.get(size);
		readLock.unlock();
		return node;
	}

	@Override
	public List<Node> getNodes(Integer key, int replicas) {
		readLock.lock();
		List<Node> nodes = new ArrayList<>();
		for (int i = 0; i < replicas; i++) {
			int size = key % nodeSet.size();
			Node node = nodeSet.get(size);
			nodes.add(node);
		}
		readLock.unlock();
		return nodes;
	}

	@Override
	public void addNode(Node node) {
		writeLock.lock();
		nodeSet.put(nodeSet.size(), node);
		writeLock.unlock();
	}

	@Override
	public void removeNode(Node node) {
	}

	@Override
	public List<Node> getAllNodes() {
		return new ArrayList<>(nodeSet.values());
	}

	@Override
	public void addNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			nodeSet.put(nodeSet.size(), node);
		}
		writeLock.unlock();
	}

	@Override
	public void removeNodes(List<Node> nodes) {
	}

}
