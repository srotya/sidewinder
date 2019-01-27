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
package com.srotya.minuteman.cluster;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Node {

	private int nodeKey;
	private String address;
	private int port;
	private transient ManagedChannel inBoundChannel;

	public Node(Integer nodeKey, String address, int port) {
		super();
		this.nodeKey = nodeKey;
		this.address = address;
		this.port = port;
		inBoundChannel = ManagedChannelBuilder.forAddress(address, port).usePlaintext(true)
				.maxInboundMessageSize(10 * 1024 * 1024).build();
	}
	
	@Override
	public int hashCode() {
		return nodeKey;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Node) {
			return nodeKey == ((Node)obj).nodeKey;
		}
		return false;
	}

	/**
	 * @return the nodeKey
	 */
	public Integer getNodeKey() {
		return nodeKey;
	}

	/**
	 * @param nodeKey
	 *            the nodeKey to set
	 */
	public void setNodeKey(Integer nodeKey) {
		this.nodeKey = nodeKey;
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @param address
	 *            the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port
	 *            the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the inBoundChannel
	 */
	public ManagedChannel getChannel() {
		return inBoundChannel;
	}

	/**
	 * @param inBoundChannel
	 *            the inBoundChannel to set
	 */
	public void setInBoundChannel(ManagedChannel inBoundChannel) {
		this.inBoundChannel = inBoundChannel;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Node [nodeKey=" + nodeKey + ", address=" + address + ", port=" + port + "]";
	}

}
