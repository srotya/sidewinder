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
package com.srotya.sidewinder.cluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.connectors.ClusterConnector;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class ClusterResourceMonitor {
	
	private static final String DB = "_internal";
	private static Logger logger = Logger.getLogger(ClusterResourceMonitor.class.getName());
	private static final ClusterResourceMonitor INSTANCE = new ClusterResourceMonitor();
	private StorageEngine storageEngine;
	private ClusterConnector connector;
	
	private ClusterResourceMonitor() {
	}
	
	public static ClusterResourceMonitor getInstance() {
		return INSTANCE;
	}
	
	public void init(StorageEngine storageEngine, ClusterConnector connector, ScheduledExecutorService bgTasks) {
		this.storageEngine = storageEngine;
		this.connector = connector;
		if (bgTasks != null) {
			try {
				storageEngine.getOrCreateDatabase(DB, 28);
			} catch (IOException e) {
				throw new RuntimeException("Unable to create internal database", e);
			}
			bgTasks.scheduleAtFixedRate(() -> clusterMonitor(), 0, 2, TimeUnit.SECONDS);
		}
	}

	private void clusterMonitor() {
		try {
			DataPoint dp = new DataPoint();
			dp.setDbName(DB);
			dp.setMeasurementName("cluster");
			dp.setFp(false);
			dp.setLongValue(connector.getClusterSize());
			dp.setTimestamp(System.currentTimeMillis());
			dp.setValueFieldName("size");
			dp.setTags(Arrays.asList("controller"));
			storageEngine.writeDataPoint(dp);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to monitor cluster", e);
		}
	}

}
