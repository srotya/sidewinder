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
package com.srotya.minuteman.wal;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rpc.BatchDataRequest;
import com.srotya.minuteman.rpc.BatchDataResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;

import io.grpc.ManagedChannel;

/**
 * @author ambud
 */
public class RemoteWALClient extends WALClient {

	private static final Logger logger = Logger.getLogger(RemoteWALClient.class.getName());
	private AtomicInteger counter;
	private ReplicationServiceBlockingStub stub;
	private AtomicLong metricRequestTime;
	private AtomicLong metricWriteTime;
	private AtomicLong loopCounter;
	private Integer routeKey;

	public RemoteWALClient() {
		metricRequestTime = new AtomicLong();
		metricWriteTime = new AtomicLong();
		loopCounter = new AtomicLong();
	}

	public RemoteWALClient configure(Map<String, String> conf, Integer nodeId, ManagedChannel channel, WAL wal,
			Integer routeKey) throws IOException {
		super.configure(conf, nodeId, wal);
		this.routeKey = routeKey;
		this.counter = new AtomicInteger(0);
		this.stub = ReplicationServiceGrpc.newBlockingStub(channel).withCompression(
				conf.getOrDefault(WALManager.CLUSTER_GRPC_COMPRESSION, WALManager.DEFAULT_CLUSTER_GRPC_COMPRESSION));
		return this;
	}

	@Override
	public void iterate() {
		try {
			loopCounter.incrementAndGet();
			logger.fine("CLIENT: Requesting data:" + offset);
			long ts = System.currentTimeMillis();
			BatchDataRequest request = BatchDataRequest.newBuilder().setNodeId(nodeId)
					.setOffset(offset).setMaxBytes(maxFetchBytes).setRouteKey(routeKey).build();
			BatchDataResponse response = stub.requestBatchReplication(request);
			ts = System.currentTimeMillis() - ts;
			metricRequestTime.getAndAdd(ts);
			if (response.getDataList() == null || response.getDataList().isEmpty()) {
				logger.fine("CLIENT: No data to replicate, delaying poll, offset:" + offset);
				Thread.sleep(retryWait);
			} else {
				ts = System.currentTimeMillis();
				try {
					for (ByteString byteString : response.getDataList()) {
						wal.write(byteString.toByteArray(), false);
					}
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Failure to write to local WAL", e);
				}
				ts = System.currentTimeMillis() - ts;
				metricWriteTime.getAndAdd(ts);
				logger.fine("CLIENT: Client received:" + response.getDataList().size() + " messages \t fileId:"
						+ response.getNextOffset());
				counter.addAndGet(response.getDataList().size());
				wal.setCommitOffset(response.getCommitOffset());
			}
			offset = response.getNextOffset();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failure to replicate WAL", e);
			try {
				Thread.sleep(errorRetryWait);
			} catch (InterruptedException e1) {
				logger.severe("CLIENT: Remote client interrupt received, breaking loop");
				stop();
			}
		}
	}

	public long getPos() {
		return wal.getCurrentOffset();
	}

	public WAL getWal() {
		return wal;
	}

}
