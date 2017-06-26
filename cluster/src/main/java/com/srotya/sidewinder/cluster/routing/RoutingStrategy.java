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
package com.srotya.sidewinder.cluster.routing;

import java.util.List;

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
	public Node getNode(String key);

	/**
	 * Get a list of placement nodes with n number of replicas.
	 * 
	 * @param key
	 * @param replicas
	 * @return placementNodes
	 */
	public List<Node> getNodes(String key, int replicas);

	/**
	 * 
	 * @param node
	 * @return
	 */
	public List<String> addNode(Node node);

	public List<String> addNodes(List<Node> nodes);

	public List<String> removeNode(Node node);

	public List<String> removeNodes(List<Node> nodes);

	public List<Node> getAllNodes();

	public int getReplicationFactor(String key);

}
