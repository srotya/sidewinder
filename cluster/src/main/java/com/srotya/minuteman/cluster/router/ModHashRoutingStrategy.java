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
package com.srotya.minuteman.cluster.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.srotya.minuteman.cluster.Node;

/**
 * @author ambud
 */
public class ModHashRoutingStrategy implements RoutingStrategy {

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
	private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
	private Map<Integer, Node> nodeSet;
	private Map<Integer, Node> nodeMap;

	public ModHashRoutingStrategy() {
		nodeSet = new ConcurrentHashMap<>();
		nodeMap = new ConcurrentHashMap<>();
	}

	@Override
	public Node getRoute(Integer key) {
		readLock.lock();
		int size = key % nodeSet.size();
		Node node = nodeSet.get(size);
		readLock.unlock();
		return node;
	}

	@Override
	public List<Node> getRoute(Integer key, int replicas) {
		readLock.lock();
		List<Node> nodes = new ArrayList<>();
		int id = key % nodeSet.size();
		Node node = nodeSet.get(id);
		for (int i = 0; i < replicas; i++) {
			node = nodeSet.get((id + i) % nodeSet.size());
			nodes.add(node);
		}
		readLock.unlock();
		return nodes;
	}

	@Override
	public void addNode(Node node) {
		writeLock.lock();
		nodeSet.put(nodeSet.size(), node);
		nodeMap.put(node.getNodeKey(), node);
		writeLock.unlock();
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
			nodeMap.put(node.getNodeKey(), node);
		}
		writeLock.unlock();
	}

	@Override
	public void removeNodes(List<Node> nodes) {
		writeLock.lock();
		for (Node node : nodes) {
			removeNode(node.getNodeKey());
		}
		writeLock.unlock();
	}

	@Override
	public int size() {
		return nodeSet.size();
	}

	@Override
	public Node removeNode(Integer nodeId) {
		writeLock.lock();
		Node remove = nodeMap.remove(nodeId);
		for (Entry<Integer, Node> entry : nodeSet.entrySet()) {
			if (entry.getValue().getNodeKey() == nodeId) {
				nodeId = entry.getKey();
				break;
			}
		}
		nodeMap.remove(nodeId);
		writeLock.unlock();
		return remove;
	}

	@Override
	public Node get(Integer nodeKey) {
		return nodeMap.get(nodeKey);
	}

}