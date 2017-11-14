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
package com.srotya.sidewinder.cluster.push.routing;

/**
 * A node is a logical member of the Sidewinder cluster where one more more
 * nodes form a cluster.
 * 
 * The reason node is referred to as a logical member is because it may
 * represent a physical node or a virtual node that is created to for the
 * purpose of even distribution of data.
 * 
 * The {@link Node} object simply provides a mechanism for the
 * {@link RoutingStrategy} to compute placement location and provides a helper
 * {@link EndpointService} concept to avoid using yet another lookup table.
 * 
 * @author ambud
 */
public class Node {

	private String nodeKey;
	private transient EndpointService writer;
	private String address;
	private int port;

	public Node(String address, int port, String nodeKey) {
		this.address = address;
		this.port = port;
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

	/**
	 * @return the writer
	 */
	public EndpointService getEndpointService() {
		return writer;
	}

	/**
	 * @param writer
	 *            the writer to set
	 */
	public void setEndpointService(EndpointService writer) {
		this.writer = writer;
	}

	@Override
	public int hashCode() {
		return nodeKey.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Node)) {
			return false;
		} else {
			Node node = (Node) obj;
			return node.address.equals(address) && node.port == port;
		}
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Node [nodeKey=" + nodeKey + ", address=" + address + ", port=" + port + "]";
	}

}
