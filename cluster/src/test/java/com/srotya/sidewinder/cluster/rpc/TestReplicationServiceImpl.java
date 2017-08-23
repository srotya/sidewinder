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
package com.srotya.sidewinder.cluster.rpc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.cluster.rpc.ListTimeseriesOffsetResponse.OffsetEntry;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineWriter;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class TestReplicationServiceImpl {

	private static MemStorageEngine engine;
	private static Server server;
	private static ManagedChannel channel;

	@BeforeClass
	public static void beforeClass() throws IOException {
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1, new BackgrounThreadFactory("bgt"));
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTaskPool);
		int port = 50052;
		server = ServerBuilder.forPort(port).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new ReplicationServiceImpl(engine)).build().start();
		System.out.println("Server started, listening on " + port);
		channel = ManagedChannelBuilder.forAddress("localhost", port)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
	}

	@AfterClass
	public static void afterClass() throws InterruptedException {
		channel.shutdownNow().awaitTermination(100, TimeUnit.SECONDS);
		if (server != null) {
			server.shutdown().awaitTermination(100, TimeUnit.SECONDS);
		}
	}

	@Test
	public void testReplicationQuery() throws IOException {
		TimeSeries series = engine.getOrCreateTimeSeries("db1", "m1", "vf1", Arrays.asList("12", "14"), 4096, true);
		long ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		series = engine.getOrCreateTimeSeries("db1", "m1", "vf1", Arrays.asList("11", "16"), 4096, true);
		ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		ReplicationServiceBlockingStub replication = ReplicationServiceGrpc.newBlockingStub(channel);
		ListTimeseriesOffsetRequest.Builder request = ListTimeseriesOffsetRequest.newBuilder();
		request.setDbName("db1").setMeasurementName("m1");
		ListTimeseriesOffsetResponse response = replication.listBatchTimeseriesOffsetList(request.build());
		List<OffsetEntry> list = response.getEntriesList();
		assertEquals(2, list.size());
		OffsetEntry offsetEntry = list.get(0);
		assertEquals("12", offsetEntry.getTagsList().get(0));
		assertEquals("14", offsetEntry.getTagsList().get(1));
		assertEquals("vf1", offsetEntry.getValueFieldName());
		assertEquals(4, offsetEntry.getBucketsList().size());

		offsetEntry = list.get(1);
		assertEquals("11", offsetEntry.getTagsList().get(0));
		assertEquals("16", offsetEntry.getTagsList().get(1));
		assertEquals("vf1", offsetEntry.getValueFieldName());
		assertEquals(4, offsetEntry.getBucketsList().size());
	}

	@Test
	public void testFetchTimeseriesDataOffset() throws IOException {
		TimeSeries series = engine.getOrCreateTimeSeries("db2", "m1", "vf1", Arrays.asList("12", "14"), 4096, true);
		long ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		series = engine.getOrCreateTimeSeries("db2", "m1", "vf1", Arrays.asList("11", "16"), 4096, true);
		ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		ReplicationServiceBlockingStub replication = ReplicationServiceGrpc.newBlockingStub(channel);
		RawTimeSeriesOffsetRequest request = RawTimeSeriesOffsetRequest.newBuilder().setBlockTimestamp(ts)
				.setBucketSize(4096).setDbName("db2").setMeasurementName("m1").setValueFieldName("vf1").addTags("11")
				.addTags("16").setOffset(0).setIndex(0).build();
		RawTimeSeriesOffsetResponse response = replication.fetchTimeseriesDataAtOffset(request);
		ByteBuffer buf = response.getData().asReadOnlyByteBuffer();
		int count = response.getCount();
		System.out.println("\n\nCount:" + count);
		ByzantineWriter writer = new ByzantineWriter();
		writer.configure(new HashMap<>(), buf, false);
		assertEquals(count, writer.getCount());
	}

	@Test
	public void testBatchFetchTimeseriesDataOffset() throws IOException {
		TimeSeries series = engine.getOrCreateTimeSeries("db3", "m1", "vf1", Arrays.asList("12", "14"), 4096, true);
		long ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		series = engine.getOrCreateTimeSeries("db3", "m1", "vf1", Arrays.asList("11", "16"), 4096, true);
		ts = 1497720452566L;
		for (int i = 0; i < 1000; i++) {
			series.addDataPoint(TimeUnit.MILLISECONDS, ts, i);
		}

		ReplicationServiceBlockingStub replication = ReplicationServiceGrpc.newBlockingStub(channel);

		BatchRawTimeSeriesOffsetRequest.Builder request = BatchRawTimeSeriesOffsetRequest.newBuilder();

		request.addRequests(RawTimeSeriesOffsetRequest.newBuilder().setBlockTimestamp(ts).setBucketSize(4096)
				.setDbName("db3").setMeasurementName("m1").setValueFieldName("vf1").addTags("12").addTags("14")
				.setOffset(0).setIndex(0).build());

		request.addRequests(RawTimeSeriesOffsetRequest.newBuilder().setBlockTimestamp(ts).setBucketSize(4096)
				.setDbName("db3").setMeasurementName("m1").setValueFieldName("vf1").addTags("11").addTags("16")
				.setOffset(0).setIndex(0).build());

		BatchRawTimeSeriesOffsetResponse response = replication.batchFetchTimeseriesDataAtOffset(request.build());
		for (RawTimeSeriesOffsetResponse offsetResponse : response.getResponsesList()) {
			ByteBuffer buf = offsetResponse.getData().asReadOnlyByteBuffer();
			int count = offsetResponse.getCount();
			System.out.println("\n\nCount:" + count);
			ByzantineWriter writer = new ByzantineWriter();
			writer.configure(new HashMap<>(), buf, false);
			assertEquals(count, writer.getCount());
		}
	}

}
