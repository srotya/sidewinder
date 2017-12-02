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

import java.io.IOException;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.srotya.sidewinder.cluster.rpc.ClusterMetadataServiceGrpc;
import com.srotya.sidewinder.cluster.rpc.ClusterMetadataServiceGrpc.ClusterMetadataServiceBlockingStub;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.SingleData;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceFutureStub;

import io.grpc.ManagedChannel;

/**
 * @author ambud
 */
public class GRPCEndpointService implements EndpointService {

	private WriterServiceBlockingStub stub;
	private ClusterMetadataServiceBlockingStub mdService;
	private ManagedChannel channel;
	private WriterServiceFutureStub futureStub;

	public GRPCEndpointService(ManagedChannel channel) {
		this.channel = channel;
		stub = WriterServiceGrpc.newBlockingStub(channel);
		futureStub = WriterServiceGrpc.newFutureStub(channel);
		mdService = ClusterMetadataServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public void write(Point point) throws IOException {
		stub.writeSingleDataPoint(SingleData.newBuilder().setMessageId(point.getTimestamp()).setPoint(point).build());
	}

	@Override
	public ListenableFuture<Ack> writeAsync(Point point) throws InterruptedException {
		return futureStub.writeSingleDataPoint(
				SingleData.newBuilder().setMessageId(point.getTimestamp()).setPoint(point).build());
	}
	
	@Override
	public ListenableFuture<Ack> writeAsync(List<Point> points) throws InterruptedException {
		return futureStub.writeBatchDataPoint(
				BatchData.newBuilder().setMessageId(System.nanoTime()).addAllPoints(points).build());
	}
	
	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, List<Point> points) throws InterruptedException {
		return futureStub.writeBatchDataPoint(
				BatchData.newBuilder().setMessageId(messageId).addAllPoints(points).build());
	}
	
	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, Point point) throws InterruptedException {
		return futureStub.writeSingleDataPoint(
				SingleData.newBuilder().setMessageId(messageId).setPoint(point).build());
	}

	@Override
	public void close() throws IOException {
		if (!channel.isShutdown()) {
			channel.shutdownNow();
		}
	}

	@Override
	public void requestRouteEntry(Point point) throws IOException {
		SingleData request = SingleData.newBuilder().setMessageId(point.getTimestamp()).setPoint(point).build();
		Ack ack = mdService.requestRouteTableEntry(request);
		if (ack.getResponseCode() != 200) {
			throw new IOException("Failed to request route entry");
		}
	}

}
