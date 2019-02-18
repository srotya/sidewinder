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

import com.google.common.hash.Hashing;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Node {

	private String address;
	private int port;
	private transient ManagedChannel inBoundChannel;
	
	public Node(String address, int port, boolean t) {
		this.address = address;
		this.port = port;
	}

	public Node(String address, int port) {
		this.address = address;
		this.port = port;
		inBoundChannel = ManagedChannelBuilder.forAddress(address, port).usePlaintext(true)
				.maxInboundMessageSize(10 * 1024 * 1024).build();
	}

	@Override
	public int hashCode() {
		return getNodeKey();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Node) {
			return getNodeKey() == ((Node) obj).getNodeKey();
		}
		return false;
	}

	/**
	 * @return the nodeKey
	 */
	public Integer getNodeKey() {
		return generateNodeKey(address, port);
	}
	
	public static Integer generateNodeKey(String address, int port) {
		return Hashing.sha1().hashUnencodedChars(address + ":" + port).asInt();
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Node [nodeKey=" + getNodeKey() + ", address=" + address + ", port=" + port + "]";
	}

}
