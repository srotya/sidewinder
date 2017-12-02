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
package com.srotya.sidewinder.core.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceBlockingStub;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;

import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class TestGRPWriterServiceImpl {

	private static StorageEngine engine;
	private static Server server;
	private static ManagedChannel channel;

	@BeforeClass
	public static void beforeClass() throws Exception {
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), bgTaskPool);
		int port = 50051;
		server = ServerBuilder.forPort(port).decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new WriterServiceImpl(engine, new HashMap<>())).build().start();
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

//	@Test
	public void testSingleDataPointWrites() throws Exception {
		WriterServiceBlockingStub client = WriterServiceGrpc.newBlockingStub(channel);
		long sts = 1497720452566L;
		Point point = Point.newBuilder().setDbName("test").setFp(false).setMeasurementName("cpu").addTags("host1")
				.setTimestamp(sts).setValue(1L).setValueFieldName("usage").build();
		client.writeSingleDataPoint(SingleData.newBuilder().setMessageId(point.getTimestamp()).setPoint(point).build());
		assertTrue(engine.checkIfExists("test"));
		assertTrue(engine.checkIfExists("test", "cpu"));
		assertEquals("host1", engine.getTagsForMeasurement("test", "cpu").iterator().next());
		List<Series> result = engine.queryDataPoints("test", "cpu", "usage", sts, sts + 1,
				Arrays.asList("host1"), null);
		assertEquals(1, result.size());
		assertEquals(1, result.iterator().next().getDataPoints().size());
		assertEquals(1L, result.iterator().next().getDataPoints().iterator().next().getLongValue());
	}

//	@Test
	public void testMultiDataPointWrites() throws Exception {
		WriterServiceBlockingStub client = WriterServiceGrpc.newBlockingStub(channel);
		long sts = 1497720452566L;

		String dbName = "test2";
		String measurementName = "cpu";
		List<Point> points = Arrays.asList(
				Point.newBuilder().setDbName(dbName).setFp(false).setMeasurementName(measurementName).addTags("host1")
						.setTimestamp(sts).setValue(1L).setValueFieldName("usage").build(),
				Point.newBuilder().setDbName(dbName).setFp(false).setMeasurementName(measurementName).addTags("host1")
						.setTimestamp(sts + 1).setValue(2L).setValueFieldName("usage").build());
		client.writeBatchDataPoint(BatchData.newBuilder().setMessageId(sts).addAllPoints(points).build());
		assertTrue(engine.checkIfExists(dbName));
		assertTrue(engine.checkIfExists(dbName, measurementName));
		assertEquals("host1", engine.getTagsForMeasurement(dbName, measurementName).iterator().next());
		List<Series> result = engine.queryDataPoints(dbName, measurementName, "usage", sts, sts + 1,
				Arrays.asList("host1"), null);
		assertEquals(1, result.size());
		assertEquals(2, result.iterator().next().getDataPoints().size());
		assertEquals(1L, result.iterator().next().getDataPoints().iterator().next().getLongValue());
	}

	@Test
	public void testPointWritesRejects() throws Exception {
		WriterServiceBlockingStub client = WriterServiceGrpc.newBlockingStub(channel);
		long sts = 1497720452566L;

		String dbName = "test3";
		String measurementName = "cpu4";
		List<Point> points = Arrays.asList(
				Point.newBuilder().setDbName(dbName).setFp(false).setMeasurementName(measurementName).addTags("host1")
						.setTimestamp(sts).setValue(1L).setValueFieldName("usage").build(),
				Point.newBuilder().setDbName(dbName).setFp(true).setMeasurementName(measurementName).addTags("host1")
						.setTimestamp(sts + 1).setValue(2L).setValueFieldName("usage").build());
		try {
			Ack response = client
					.writeBatchDataPoint(BatchData.newBuilder().setMessageId(sts).addAllPoints(points).build());
			if (response.getResponseCode() == 200) {
				fail("Exception must be thrown");
			}
		} catch (Exception e) {
		}
		// second data point should have been rejected
		assertTrue(engine.checkIfExists(dbName));
		assertTrue(engine.checkIfExists(dbName, measurementName));
		assertEquals("host1", engine.getTagsForMeasurement(dbName, measurementName).iterator().next());
		List<Series> result = engine.queryDataPoints(dbName, measurementName, "usage", sts, sts + 1,
				Arrays.asList("host1"), null);
		assertEquals(1, result.size());
		assertEquals(1, result.iterator().next().getDataPoints().size());
		assertEquals(1L, result.iterator().next().getDataPoints().iterator().next().getLongValue());
	}

}
