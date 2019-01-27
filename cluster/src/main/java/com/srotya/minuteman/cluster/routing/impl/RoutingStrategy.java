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
package com.srotya.minuteman.cluster.routing.impl;

import java.util.List;

import com.srotya.minuteman.cluster.Node;

/**
 * A Strategy design pattern to compute data placement using different placement
 * algorithms depending on the clustering environment.
 * 
 * @author ambud
 */
public interface RoutingStrategy {

	/**
	 * Get a single placement node in case replication is completely disabled.
	 * 
	 * @param key
	 * @return placementNode
	 */
	public Node getRoute(Integer key);

	/**
	 * Get a list of placement nodes with n number of replicas.
	 * 
	 * @param key
	 * @param replicas
	 * @return placementNodes
	 */
	public List<Node> getRoute(Integer key, int replicas);

	/**
	 * 
	 * @param node
	 */
	public void addNode(Node node);

	public void addNodes(List<Node> nodes);

	public default Node removeNode(Node node) {
		return removeNode(node.getNodeKey());
	}

	public Node removeNode(Integer nodeId);

	public void removeNodes(List<Node> nodes);

	public List<Node> getAllNodes();

	public int size();

	public Node get(Integer nodeKey);

}