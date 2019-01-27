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
package com.srotya.sidewinder.cluster.consistency;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.minuteman.wal.LocalWALClient;
import com.srotya.minuteman.wal.WAL;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class SidewinderWALClient extends LocalWALClient {

	private static final Logger logger = Logger.getLogger(SidewinderWALClient.class.getName());
	private StorageEngine storageEngine;
	private static AtomicInteger counter = new AtomicInteger();

	@Override
	public LocalWALClient configure(Map<String, String> conf, Integer nodeId, WAL localWAL, Object storageObject)
			throws IOException {
		conf.put("wal.local.read.mode", "uncommitted");
		super.configure(conf, nodeId, localWAL, storageObject);
		storageEngine = (StorageEngine) storageObject;
		logger.info("Starting local wal follower thread");
		return this;
	}

	@Override
	public void processData(List<byte[]> data) {
		try {
			logger.finer("Reading data:" + data.size() + "\tCounter Before:" + counter.get());
			for (byte[] d : data) {
//				d = Snappy.uncompress(d);
				BatchData points = BatchData.parseFrom(d);
				for (Point point : points.getPointsList()) {
					storageEngine.writeDataPointWithoutLock(point, false);
					counter.incrementAndGet();
				}
			}
			logger.finer(
					"Committed data to Sidewinder storage engine:" + data.size() + "\tCounter After:" + counter.get());
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to write datapoints", e);
		}
	}

}
