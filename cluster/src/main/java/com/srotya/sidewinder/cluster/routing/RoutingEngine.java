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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.srotya.sidewinder.cluster.connectors.ClusterConnector;
import com.srotya.sidewinder.core.rpc.Bucket;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.RawTimeSeriesBucket;
import com.srotya.sidewinder.core.rpc.RawTimeSeriesBucket.Builder;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;

/**
 * Routing Engine provides the abstraction to stitch together the
 * {@link RoutingStrategy}s, clustering subsystem like Atomix or Zookeeper and
 * methods to forward data to the appropriate positions.
 * 
 * @author ambud
 */
public abstract class RoutingEngine {

	public static final String DEFAULT_CLUSTER_GRPC_PORT = "55021";
	public static final String CLUSTER_GRPC_PORT = "cluster.grpc.port";
	public static final String DEFAULT_CLUSTER_HOST = "localhost";
	public static final String CLUSTER_HOST = "cluster.host";
	private int port;
	private String address;
	private StorageEngine engine;
	private volatile Node leader;
	private Map<String, Node> nodeMap;

	public void init(Map<String, String> conf, StorageEngine engine, ClusterConnector connector) throws Exception {
		this.engine = engine;
		this.port = Integer.parseInt(conf.getOrDefault(CLUSTER_GRPC_PORT, DEFAULT_CLUSTER_GRPC_PORT));
		this.address = conf.getOrDefault(CLUSTER_HOST, DEFAULT_CLUSTER_HOST);
		nodeMap = new ConcurrentHashMap<>();
	}

	public abstract List<Node> routeData(Point point) throws IOException, InterruptedException;

	public abstract void addRoutableKey(Point point, int replicationFactor);

	public abstract void nodeAdded(Node node);

	public abstract void nodeDeleted(Node node) throws Exception;

	public abstract void makeCoordinator() throws Exception;

	public abstract Object getRoutingTable();

	public abstract void updateLocalRouteTable(Object routingTable);

	public void setLeader(Node node) {
		this.leader = node;
	}
	
	public Node getLeader() {
		return leader;
	}
	
	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	/**
	 * @return the engine
	 */
	public StorageEngine getEngine() {
		return engine;
	}
	
	public Map<String, Node> getNodeMap() {
		return nodeMap;
	}
	
	public static RawTimeSeriesBucket seriesToRawBucket(StorageEngine engine, String dbName, String measurementName,
			String field, List<String> tags) throws Exception {
		Builder rawData = RawTimeSeriesBucket.newBuilder();
		TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, field, tags);
		if (timeSeries == null) {
			return null;
		}
		// create raw data payload
		rawData.setDbName(dbName);
		rawData.setMeasurementName(measurementName);
		rawData.addAllTags(tags);
		rawData.setValueFieldName(field);
		rawData.setFp(timeSeries.isFp());
		rawData.setBucketSize(timeSeries.getTimeBucketSize());
		for (Entry<String, com.srotya.sidewinder.core.storage.compression.Writer> bucket : timeSeries.getBucketMap()
				.entrySet()) {
			ByteBuffer rawBytes = bucket.getValue().getRawBytes();
			rawBytes.reset();
			Bucket rawBucket = Bucket.newBuilder().setData(ByteString.copyFrom(rawBytes)).setBucketId(bucket.getKey())
					.build();
			rawData.addBuckets(rawBucket);
		}
		return rawData.build();
	}
}
