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
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.srotya.sidewinder.cluster.ThreadLocalCaches;
import com.srotya.sidewinder.cluster.rpc.DataObject;
import com.srotya.sidewinder.cluster.rpc.DeltaObject;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;

public class RepairWorker implements Runnable {

	private static Logger logger = Logger.getLogger(RepairWorker.class.getName());
	private ArrayBlockingQueue<RepairTask> repairQueue;
	private StorageEngine engine;

	public RepairWorker(ArrayBlockingQueue<RepairTask> repairQueue, StorageEngine engine) {
		this.repairQueue = repairQueue;
		this.engine = engine;
	}

	@Override
	public void run() {
		while (true) {
			try {
				// fetch new task from the queue; block wait if nothing is available
				RepairTask task = repairQueue.take();
				DeltaObject obj = task.getObject();
				DataObject data = httpGetData(task, obj);
				if (data != null) {
					try {
						Series series = null;
//						engine.getOrCreateTimeSeries(data.getDbName(), data.getMeasurementName(),
//								data.getValueFieldName(), data.getTagsList(), obj.getBucketSize(), obj.getFp());
						List<Entry<Long, byte[]>> bufList = data.getBufList().stream().map(s -> {
							String[] split = s.split("_");
							return new AbstractMap.SimpleEntry<Long, byte[]>(Long.parseLong(split[0]),
									Base64.decodeBase64(split[1]));
						}).collect(Collectors.toList());
						try {
//							series.replaceFirstBuckets(data.getBucket(), bufList);
						} catch (Exception e) {
							// TODO Fix repair failure
							e.printStackTrace();
						}
						System.out.println("Repairing data fetched:" + data + " engine:" + engine.hashCode());
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Error executing repair request:" + " for replica repair", e);
					}
				}
			} catch (InterruptedException e) {
				break;
			}
		}

	}

	private DataObject httpGetData(RepairTask task, DeltaObject obj) {
		String url = task.getUrl() + "/data/";
		try {
			Gson gson = ThreadLocalCaches.getGsonInstance();
			CloseableHttpClient client = ReplicaRepairService.buildClient(url, 5000, 5000);
			HttpPost req = new HttpPost(url);
			req.setEntity(new StringEntity(gson.toJson(obj)));
			CloseableHttpResponse resp = client.execute(req);
			String dataString = EntityUtils.toString(resp.getEntity());
			DataObject data = gson.fromJson(dataString, DataObject.class);
			return data;
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			logger.log(Level.SEVERE, "Error connecting to:" + url + " for replica repair", e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error executing repair fetch request:" + url + " for replica repair", e);
		}
		return null;
	}

}