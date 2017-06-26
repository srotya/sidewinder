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
package com.srotya.sidewinder.cluster.replication;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.srotya.sidewinder.cluster.rpc.RawTimeSeriesOffsetRequest;
import com.srotya.sidewinder.cluster.rpc.RawTimeSeriesOffsetResponse;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc.ReplicationServiceFutureStub;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;

import io.grpc.CompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class ReplicationService {

	private String masterAddress;
	private int port;
	private ManagedChannel channel;
	private StorageEngine engine;
	private ExecutorService replicationThreadPool;

	public ReplicationService(String masterAddress, int port, StorageEngine engine, ExecutorService replicationThreadPool) {
		this.masterAddress = masterAddress;
		this.port = port;
		this.engine = engine;
		this.replicationThreadPool = replicationThreadPool;
	}

	public void run() {
		channel = ManagedChannelBuilder.forAddress(masterAddress, port)
				.compressorRegistry(CompressorRegistry.getDefaultInstance()).usePlaintext(true).build();
		while (true) {
			try {
				for (String dbName : engine.getDatabases()) {
					for (String measurementName : engine.getAllMeasurementsForDb(dbName)) {
						for (String valueFieldName : engine.getFieldsForMeasurement(dbName, measurementName)) {
							List<List<String>> tagList = engine.getTagsForMeasurement(dbName, measurementName, valueFieldName);
							for (List<String> tags : tagList) {
								TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, valueFieldName, tags);
//								timeSeries.get
								ReplicationServiceFutureStub rpc = ReplicationServiceGrpc.newFutureStub(channel);
								RawTimeSeriesOffsetRequest request = RawTimeSeriesOffsetRequest.newBuilder()
										.setBlockTimestamp(0).setDbName(dbName).setMeasurementName(measurementName)
										.setValueFieldName(valueFieldName).addAllTags(tags).build();
								final ListenableFuture<RawTimeSeriesOffsetResponse> future = rpc.fetchTimeseriesDataAtOffset(request);
								future.addListener(new Runnable() {
									
									@Override
									public void run() {
										try {
											RawTimeSeriesOffsetResponse response = future.get();
											TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, valueFieldName, tags);
//											timeSeries.getSeriesBucket(unit, timestamp);
										} catch (InterruptedException | ExecutionException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										} catch (Exception e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
									}
								}, replicationThreadPool);
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @return the masterAddress
	 */
	public String getMasterAddress() {
		return masterAddress;
	}

	/**
	 * @param masterAddress
	 *            the masterAddress to set
	 */
	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
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

}
