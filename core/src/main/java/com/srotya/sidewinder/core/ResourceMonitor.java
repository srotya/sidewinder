/**
 * Copyright 2016 Ambud Sharma
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
package com.srotya.sidewinder.core;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class ResourceMonitor {

	private static final String DB = "_internal";
	private static Logger logger = Logger.getLogger(ResourceMonitor.class.getName());
	private static final ResourceMonitor INSTANCE = new ResourceMonitor();
	private AtomicBoolean reject = new AtomicBoolean(false);
	private StorageEngine storageEngine;

	private ResourceMonitor() {
	}

	public static ResourceMonitor getInstance() {
		return INSTANCE;
	}

	public void init(StorageEngine storageEngine, ScheduledExecutorService bgTasks) {
		this.storageEngine = storageEngine;
		storageEngine.getOrCreateDatabase(DB, 28);
		bgTasks.scheduleAtFixedRate(() -> memAndCPUMonitor(), 0, 2, TimeUnit.SECONDS);
	}

	public void memAndCPUMonitor() {
		MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
		MemoryUsage heap = mem.getHeapMemoryUsage();
		MemoryUsage nonheap = mem.getNonHeapMemoryUsage();

		validateCPUUsage();

		validateMemoryUsage("heap", heap, 10_485_760);
		validateMemoryUsage("nonheap", nonheap, 1_073_741_824);
	}

	private void validateCPUUsage() {
		double systemLoadAverage = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
		DataPoint dp = new DataPoint(DB, "cpu", "load", Arrays.asList(), System.currentTimeMillis(), systemLoadAverage);
		try {
			storageEngine.writeDataPoint(dp);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Unable to write internal metrics", e);
		}
	}

	public void validateMemoryUsage(String type, MemoryUsage mem, int min) {
		long max = mem.getMax();
		if (max == -1) {
			max = Integer.MAX_VALUE;
		}
		long used = mem.getUsed();
		DataPoint dp = new DataPoint(DB, "memory", "used", Arrays.asList(type), System.currentTimeMillis(), used);
		try {
			storageEngine.writeDataPoint(dp);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Unable to write internal metrics", e);
		}

		dp = new DataPoint(DB, "memory", "max", Arrays.asList(type), System.currentTimeMillis(), used);
		try {
			storageEngine.writeDataPoint(dp);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Unable to write internal metrics", e);
		}

		logger.log(Level.FINE, "Used:" + used + ",Max:" + max);
		if ((max - used) < min) {
			logger.warning("Insufficient memory(" + used + "/" + max + "), new metrics can't be created");
		}
	}

	public boolean isReject() {
		return reject.get();
	}

}
