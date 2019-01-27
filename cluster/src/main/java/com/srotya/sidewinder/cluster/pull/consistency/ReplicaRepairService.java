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
package com.srotya.sidewinder.cluster.pull.consistency;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc.ClusterReadServiceBlockingStub;
import com.srotya.sidewinder.cluster.rpc.DeltaObject;
import com.srotya.sidewinder.cluster.rpc.DeltaRequest;
import com.srotya.sidewinder.cluster.rpc.DeltaResponse;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.compression.Writer;

import io.dropwizard.lifecycle.Managed;
import io.netty.util.concurrent.ScheduledFuture;

public class ReplicaRepairService implements Managed {

	private static final Logger logger = Logger.getLogger(ReplicaRepairService.class.getName());
	private ScheduledExecutorService deltaExecutorPool;
	private StorageEngine engine;
	private ExecutorService replicaRepairExecutorPool;
	private int repairThreadCount = 2;
	private ArrayBlockingQueue<RepairTask> taskQueue;
	private Set<String> ignoreSet = new HashSet<>();
	private Map<String, ScheduledFuture<?>> scheduleMap;
	private WALManager mgr;

	public ReplicaRepairService(WALManager mgr, StorageEngine engine) {
		this.mgr = mgr;
		this.engine = engine;
		scheduleMap = new ConcurrentHashMap<>();
		ignoreSet.add("_internal");
	}

	@Override
	public void start() throws Exception {
		logger.info("Replica repair service starting");
		deltaExecutorPool = Executors.newScheduledThreadPool(1);
		replicaRepairExecutorPool = Executors.newFixedThreadPool(repairThreadCount);
		startReplicaRepairTasks();
		scheduleDeltaFetchAndCompare();
	}

	private void startReplicaRepairTasks() {
		for (int i = 0; i < repairThreadCount; i++) {
			replicaRepairExecutorPool.submit(new RepairWorker(taskQueue, engine));
		}
		logger.info("Deployed repair workers to pool");
	}

	private void scheduleDeltaFetchAndCompare() throws Exception {
		deltaExecutorPool.scheduleAtFixedRate(() -> {
			try {
				for (String db : engine.getDatabases()) {
					if (ignoreSet.contains(db)) {
						continue;
					}
					for (String measurement : engine.getAllMeasurementsForDb(db)) {
						if (scheduleMap.containsKey(db + "." + measurement)) {
							// don't schedule if this is already scheduled
							continue;
						}
						Set<String> fields = engine.getFieldsForMeasurement(db, measurement);
						for (String field : fields) {
							deltaExecutorPool.scheduleAtFixedRate(() -> {
								computeDeltaAndScheduleRepair(db, measurement, field);
							}, 2, 10, TimeUnit.SECONDS);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}, 1, 5, TimeUnit.SECONDS);
		logger.info("Started repair task schedule");
	}

	private void computeDeltaAndScheduleRepair(String db, String measurement, String valueFieldName) {
		try {
			// String entity = connectToLeaderAndFetchDeltas(db, measurement,
			// valueFieldName);
			// Gson gson = new Gson();
			// List<DeltaObject> deltas = gson.fromJson(entity, List<DeltaObject>.class);

			List<DeltaObject> deltas = getObjects(db, measurement, valueFieldName);
			logger.info("Checking deltas for:" + deltas.size());
			for (int i = 0; i < deltas.size(); i++) {
//				checkAndRepair(db, measurement, deltas, i);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private List<DeltaObject> getObjects(String db, String measurement, String valueFieldName) {
		Integer leader = null; 
//				mgr.getReplicaLeader(Utils.buildRouteKey(db, measurement, valueFieldName));
		Node node = mgr.getStrategy().get(leader);
		ClusterReadServiceBlockingStub stub = ClusterReadServiceGrpc.newBlockingStub(node.getChannel());
		DeltaResponse deltas = stub.getDeltas(DeltaRequest.newBuilder().setCompact(false).setDbName(db)
				.setMeasurementName(measurement).setValueFieldName(valueFieldName).build());
		return deltas.getObjectList();
	}

//	private void checkAndRepair(String db, String measurement, List<DeltaObject> deltas, int i)
//			throws IOException, InterruptedException {
//		DeltaObject obj = deltas.get(i);
//		String field = obj.getValueFieldName();
//		List<Tag> tags = obj.getTagsList();
//		int bucket = obj.getBucket();
//		int writerCount = obj.getWriterCount();
//		int bucketSize = obj.getWriterCount();
//		boolean fp = obj.getFp();
//		Series series = null; 
//				//engine.getOrCreateTimeSeries(db, measurement, field, tags, bucketSize, fp);
//		List<Writer> list = series.getBucketRawMap().get(bucket);
//		boolean repair = false;
//		repair = checkDeltaAndScheduleRepair(obj, writerCount, list, repair);
//		if (repair) {
//			logger.info("Submitting repair task for:" + obj);
//			taskQueue.put(new RepairTask(db, measurement, obj, "http://localhost:8080/rpc"));
//		} else {
//			logger.info("Verfied consistency of:" + obj);
//		}
//	}

	@SuppressWarnings("unused")
	private String connectToLeaderAndFetchDeltas(String db, String measurement, String valueFieldName)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
			ClientProtocolException {
		Integer leader = null;
//		mgr.getReplicaLeader(Utils.buildRouteKey(db, measurement, valueFieldName));
		Node node = mgr.getStrategy().get(leader);
		String baseUrl = "http://" + node.getAddress() + ":8080/rpc/delta/";
		HttpGet deltaRequest = new HttpGet(baseUrl + db + "/" + measurement);
		CloseableHttpClient client = buildClient(baseUrl, 5000, 5000);
		CloseableHttpResponse response = client.execute(deltaRequest);
		String entity = EntityUtils.toString(response.getEntity());
		return entity;
	}

	private boolean checkDeltaAndScheduleRepair(DeltaObject obj, int writerCount, List<Writer> list, boolean repair) {
		if (list == null) {
			// bucket doesn't exist, missing data should be backfilled as is
			repair = true;
		} else {
			// filter local list of writers to remove open writers
			list = list.stream().filter(v -> v.isFull() | v.isReadOnly()).collect(Collectors.toList());
			if (list.size() == writerCount) {// same buffer count
				// let's check if they have the same buffer elements
				for (int k = 0; k < writerCount; k++) {
					if (obj.getWriterDataPointCountList().get(k) != list.get(k).getCount()) {
						// mismatch in points, buffer needs to be repaired
						// submit a repair task
						repair = true;
						break;
					}
				}
			}
			// different count of buffers means either there is newer data coming in to the
			// master or there is there is compaction gap
			else if (list.size() > writerCount) {
				// means somehow this node didn't compact this buffer
				// or it has more data than the master
			} else {
				// verify if master indeed has newer data
				int diff = list.size();
				for (int k = 0; k < diff; k++) {
					if (obj.getWriterDataPointCountList().get(k) != list.get(k).getCount()) {
						// mismatch in points, buffer needs to be repaired
						repair = true;
						break;
					}
				}

			}
		}
		return repair;
	}

	@Override
	public void stop() throws Exception {
		// cleanup queue to ensure new tasks are not picked up
		taskQueue.clear();
		// shutdown executors
		replicaRepairExecutorPool.shutdown();
		deltaExecutorPool.shutdown();
		// let executors finish any pending tasks
		replicaRepairExecutorPool.awaitTermination(100, TimeUnit.SECONDS);
		deltaExecutorPool.awaitTermination(100, TimeUnit.SECONDS);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();
		return clientBuilder.setDefaultRequestConfig(config).build();
	}

}
