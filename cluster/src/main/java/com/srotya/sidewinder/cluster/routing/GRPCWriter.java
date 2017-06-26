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

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.SingleData;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;

import io.grpc.ManagedChannel;

/**
 * @author ambud
 */
public class GRPCWriter implements Writer {

	private WriterServiceBlockingStub stub;
	private ManagedChannel channel;

	public GRPCWriter(ManagedChannel channel) {
		this.channel = channel;
		stub = WriterServiceGrpc.newBlockingStub(channel);
	}

	@Override
	public void write(Point point) throws IOException {
		stub.writeSingleDataPoint(SingleData.newBuilder().setMessageId(point.getTimestamp()).setPoint(point).build());
	}

	@Override
	public void close() throws IOException {
		if (!channel.isShutdown()) {
			channel.shutdownNow();
		}
	}

}
