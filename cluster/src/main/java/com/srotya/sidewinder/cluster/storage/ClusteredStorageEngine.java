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
package com.srotya.sidewinder.cluster.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.srotya.sidewinder.cluster.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;

/**
 * @author ambud
 */
public class ClusteredStorageEngine implements StorageEngine {

	private StorageEngine localStorageEngine;
	private ClusterConnector connector;
	private RoutingEngine router;

	@Override
	public void configure(Map<String, String> conf, ScheduledExecutorService bgTaskPool) throws IOException {
		String storageEngineClass = conf.getOrDefault("storage.engine",
				"com.srotya.sidewinder.core.storage.mem.MemStorageEngine");
		try {
			localStorageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
			localStorageEngine.configure(conf, bgTaskPool);
			localStorageEngine.connect();
		} catch (Exception e) {
			throw new IOException(e);
		}

		try {
			connector = (ClusterConnector) Class.forName(
					conf.getOrDefault("cluster.connector", "com.srotya.sidewinder.cluster.connectors.ConfigConnector"))
					.newInstance();
			connector.init(conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		try {
			router = (RoutingEngine) Class.forName(conf.getOrDefault("cluster.routing.engine",
					"com.srotya.sidewinder.cluster.routing.impl.MasterSlaveRoutingEngine")).newInstance();
			router.init(conf, localStorageEngine, connector);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void connect() throws IOException {
	}

	@Override
	public void disconnect() throws IOException {
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteAllData() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean checkIfExists(String dbName) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours)
			throws ItemNotFoundException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Measurement> getOrCreateDatabase(String dbName, int retentionPolicy) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Measurement getOrCreateMeasurement(String dbName, String measurementName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws RejectException, IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, DBMetadata> getDbMetadataMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, Measurement>> getMeasurementMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, Measurement>> getDatabaseMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getDefaultTimebucketSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AtomicInteger getCounter() {
		// TODO Auto-generated method stub
		return null;
	}

}
