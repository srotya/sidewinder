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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

import com.srotya.minuteman.connectors.AtomixConnector;
import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.connectors.ConfigConnector;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;
import com.srotya.minuteman.utils.FileUtils;
import com.srotya.minuteman.wal.WAL;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class TestWALManagerImpl {

	private static final ScheduledExecutorService es = Executors.newScheduledThreadPool(1, new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread th = new Thread(r);
			th.setDaemon(true);
			return th;
		}
	});

	@Test
	public void testInit() throws IOException, InterruptedException {
		FileUtils.delete(new File("target/mgrwal1"));
		Map<String, String> conf = new HashMap<>();
		conf.put(WAL.WAL_DIR, "target/mgrwal1");
		conf.put(WAL.WAL_SEGMENT_SIZE, String.valueOf(102400));
		WALManager mgr = new WALManagerImpl();
		try {
			mgr.init(conf, null, es, null);
			fail("Must fail since connector is null");
		} catch (Exception e) {
		}
		ClusterConnector connector = new ConfigConnector();
		try {
			connector.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr.init(conf, connector, es, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		assertNotNull(mgr.getCoordinator());
		try {
			mgr.addRoutableKey("key".hashCode(), 3);
			fail("Should throw insufficient nodes exception");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		try {
			List<Replica> replica = mgr.addRoutableKey("key".hashCode(), 1);
			assertEquals("localhost:55021".hashCode(), replica.get(0).getLeaderNodeKey().intValue());
			assertEquals("localhost:55021".hashCode(), replica.get(0).getReplicaNodeKey().intValue());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw insufficient nodes exception");
		}
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 55021)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(channel);
		RouteResponse response = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey("test".hashCode()).setReplicationFactor(1).build());
		assertEquals(200, response.getResponseCode());

		try {
			assertNotNull(mgr.getWAL("test".hashCode()));
		} catch (IOException e) {
			fail("Must not fail to get the WAL");
		}

		mgr.stop();
	}

	@Test
	public void testCluster() throws IOException, InterruptedException {
		FileUtils.delete(new File("target/mgrwal2-1"));
		FileUtils.delete(new File("target/mgrwal2-2"));
		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
		Map<String, String> conf = new HashMap<>();
		conf.put(WAL.WAL_DIR, "target/mgrwal2-1");
		conf.put(WAL.WAL_SEGMENT_SIZE, String.valueOf(102400));
		WALManagerImpl mgr = new WALManagerImpl();
		ClusterConnector connector = new ConfigConnector();
		conf.put(ConfigConnector.CLUSTER_CC_SLAVES, "localhost:55021,localhost:55022");
		try {
			connector.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr.init(conf, connector, es, null);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		assertNotNull(mgr.getCoordinator());
		try {
			mgr.addRoutableKey("key".hashCode(), 4);
			fail("Must throw insufficient nodes exception");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		conf.put(WALManager.CLUSTER_GRPC_PORT, "55022");
		conf.put(WAL.WAL_DIR, "target/mgrwal2-2");
		ClusterConnector connector2 = new ConfigConnector();
		WALManager mgr2 = new WALManagerImpl();
		try {
			connector2.init(conf);
			mgr2.init(conf, connector2, es, null);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}

		assertTrue(connector.isCoordinator());
		assertTrue(!connector2.isCoordinator());
		try {
			List<Replica> replica = mgr.addRoutableKey("key".hashCode(), 2);
			assertEquals("localhost:55022".hashCode(), replica.get(0).getLeaderNodeKey().intValue());
			assertEquals("localhost:55022".hashCode(), replica.get(0).getReplicaNodeKey().intValue());
			assertEquals("localhost:55022".hashCode(), replica.get(1).getLeaderNodeKey().intValue());
			assertEquals("localhost:55021".hashCode(), replica.get(1).getReplicaNodeKey().intValue());
			// validate mod hash leader assignment
			replica = mgr.addRoutableKey("key2".hashCode(), 2);
			assertEquals("localhost:55022".hashCode(), replica.get(0).getLeaderNodeKey().intValue());
			assertEquals("localhost:55022".hashCode(), replica.get(0).getReplicaNodeKey().intValue());
			assertEquals("localhost:55022".hashCode(), replica.get(1).getLeaderNodeKey().intValue());
			assertEquals("localhost:55021".hashCode(), replica.get(1).getReplicaNodeKey().intValue());
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw insufficient nodes exception");
		}
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 55022)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(channel);
		RouteResponse response = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey("test5".hashCode()).setReplicationFactor(1).build());
		assertEquals(500, response.getResponseCode());

		try {
			assertNull(mgr2.getWAL("test5".hashCode()));
		} catch (IOException e) {
			fail("Must not fail to get the WAL");
		}

		channel.shutdownNow();

		channel = ManagedChannelBuilder.forAddress("localhost", 55021)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();

		stub = ReplicationServiceGrpc.newBlockingStub(channel);
		response = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey("test5".hashCode()).setReplicationFactor(2).build());
		assertEquals(200, response.getResponseCode());

		mgr.stop();
		mgr2.stop();
	}

	@Test
	public void testWithAtomix() throws Exception {
		FileUtils.delete(new File("target/mgrwal3-1"));
		FileUtils.delete(new File("target/mgrwal3-2"));
		FileUtils.delete(new File("target/mgrwal3-3"));
		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);
		Map<String, String> conf = new HashMap<>();
		conf.put(WAL.WAL_DIR, "target/mgrwal3-1");
		conf.put(WAL.WAL_SEGMENT_SIZE, String.valueOf(102400));
		conf.put(WALManager.CLUSTER_GRPC_PORT, "55023");
		conf.put(AtomixConnector.CLUSTER_ATOMIX_BOOTSTRAP_ADDRESSES, "localhost:8901,localhost:8902");
		conf.put(AtomixConnector.CLUSTER_ATOMIX_ELECTION_TIMEOUT, "5");
		conf.put(AtomixConnector.CLUSTER_ATOMIX_HEARTBEAT_INTERVAL, "1");
		conf.put(WAL.WAL_ISRCHECK_FREQUENCY, "1");
		ClusterConnector connector = new AtomixConnector();
		Future<Boolean> result = es.submit(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				try {
					connector.init(conf);
					return true;
				} catch (Exception e) {
					e.printStackTrace();
				}
				return false;
			}
		});

		ClusterConnector connector2 = new AtomixConnector();
		Map<String, String> conf2 = new HashMap<>(conf);
		conf2.put(WAL.WAL_DIR, "target/mgrwal3-2");
		conf2.put(AtomixConnector.CLUSTER_ATOMIX_PORT, "8902");
		conf2.put(WALManager.CLUSTER_GRPC_PORT, "55024");
		try {
			connector2.init(conf2);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		Boolean response = result.get();
		assertTrue(response);

		WALManagerImpl mgr = new WALManagerImpl();
		try {
			mgr.init(conf, connector, es, null);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		WALManager mgr2 = new WALManagerImpl();
		try {
			mgr2.init(conf2, connector2, es, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw exception:" + e.getMessage());
		}

		// verify nodes are connected to the cluster
		assertEquals(2, mgr.getStrategy().size());
		assertEquals(2, mgr2.getStrategy().size());

		// add key to coordinator, invalid replication factor
		WALManager mg = mgr;
		if (connector2.isCoordinator()) {
			mg = mgr2;
		}
		try {
			mg.addRoutableKey("test1".hashCode(), 3);
			fail("Must throw illegal argument exception");
		} catch (IllegalArgumentException e) {
		}

		// add key to coordinator, invalid replication factor
		try {
			mg.addRoutableKey("test1".hashCode(), 2);
		} catch (IllegalArgumentException e) {
			fail("Must NOT throw illegal argument exception");
		}

		System.out.println("Routing tested");

		// join a node after keys are already created
		WALManager mgr3 = new WALManagerImpl();
		ClusterConnector connector3 = new AtomixConnector();
		conf.put(WAL.WAL_DIR, "target/mgrwal3-3");
		conf.put(AtomixConnector.CLUSTER_ATOMIX_PORT, "8903");
		conf.put(WALManager.CLUSTER_GRPC_PORT, "55025");
		try {
			connector3.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr3.init(conf, connector3, es, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			List<Replica> addRoutableKey = mg.addRoutableKey("test3".hashCode(), 3);
			System.out.println(
					"Leader:" + addRoutableKey.get(0).getLeaderAddress() + ":" + addRoutableKey.get(0).getLeaderPort());
		} catch (IllegalArgumentException e) {
			fail("Must NOT throw illegal argument exception");
		}

		mgr3.stop();
		connector3.stop();

		System.out.println("Reconnecting to cluster");

		// reconnect to cluster from machine 3, validate replicas are recovered
		mgr3 = new WALManagerImpl();
		connector3 = new AtomixConnector();
		conf.put(WAL.WAL_DIR, "target/mgrwal3-3");
		conf.put(AtomixConnector.CLUSTER_ATOMIX_PORT, "8903");
		conf.put(WALManager.CLUSTER_GRPC_PORT, "55025");
		try {
			connector3.init(conf);
		} catch (Exception e) {
			fail("Shouldn't throw exception:" + e.getMessage());
		}
		try {
			mgr3.init(conf, connector3, es, null);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Shouldn't throw exception:" + e.getMessage());
		}

		mgr2.getWAL("test3".hashCode()).write("test".getBytes(), false);

		int c = 0;
		while (mgr.getWAL("test3".hashCode()).getOffset() != 12) {
			Thread.sleep(100);
			c++;
			if (c % 100 == 0) {
				System.out.println("Offset:" + mgr.getWAL("test3".hashCode()).getOffset());
			}
		}

		System.out.println("Offset:" + mgr.getWAL("test3".hashCode()).getOffset());
		// fail the leader
		System.out.println("Primary has been stopped");

		System.out.println("Test completed, stopping managers");
	}
}
