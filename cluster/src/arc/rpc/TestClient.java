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
package com.srotya.sidewinder.cluster.rpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import com.srotya.sidewinder.cluster.rpc.BatchData.Builder;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.sidewinder.cluster.rpc.WriterServiceGrpc.WriterServiceFutureStub;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class TestClient {

	private static final String TEST = "test";
	private static final String VALUE2 = "value";
	private static final String COUNTER = "counter";

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		String dbname = "test22" + 2;
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
		WriterServiceFutureStub stub = WriterServiceGrpc.newFutureStub(channel);
		long ts = System.currentTimeMillis();
		Builder builder = BatchData.newBuilder();
		for (int k = 0; k <= 1_000_000; k++) {
			Point value = Point.newBuilder().setDbName(dbname).setFp(false).setValue(k).setMeasurementName(COUNTER)
					.setValueFieldName(VALUE2).addTags(TEST).setTimestamp(ts + (k * 5)).build();
			// System.out.println("Writing:"+value);
			builder.addPoints(value);
			if (k % 1000 == 0) {
				stub.writeBatchDataPoint(builder.build()).get();
				builder = BatchData.newBuilder();
			}
		}

		System.out.println("Completed writes");
		ReplicationServiceBlockingStub replica = ReplicationServiceGrpc.newBlockingStub(channel);
		RawTimeSeriesBucketRequest request = RawTimeSeriesBucketRequest.newBuilder().setDbName(dbname)
				.setMeasurementName(COUNTER).setValueFieldName(VALUE2).addTags(TEST).setBlockTimestamp(ts).build();
		RawTimeSeriesBucketResponse response = replica.readTimeseriesBucket(request);
		for (Bucket bucket : response.getBucketsList()) {
			System.out.println(bucket.getBucketId() + "\t" + bucket.getData().size() + "\t"
					+ bucket.getHeaderTimestamp() + "\t" + bucket.getCount());
			Writer writer = new ByzantineWriter();
			writer.configure(new HashMap<>());
			writer.bootstrap(bucket.getData().asReadOnlyByteBuffer());
			writer.setCounter(bucket.getCount());
			Reader reader = writer.getReader();
			reader.setIsFP(response.getFp());
			for (int i = 0; i < bucket.getCount(); i++) {
				DataPoint dp = reader.readPair();
				System.out.println(dp.getTimestamp() + "\t" + dp.getLongValue());
			}
		}
	}

}
