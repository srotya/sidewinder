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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.Gson;
import com.srotya.linea.MurmurHash;
import com.srotya.linea.Topology;
import com.srotya.linea.clustering.Columbus;
import com.srotya.linea.clustering.WorkerEntry;
import com.srotya.sidewinder.cluster.Utils;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Callback;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.gorilla.MemStorageEngine;
import com.srotya.sidewinder.core.storage.gorilla.Reader;
import com.srotya.sidewinder.core.storage.gorilla.TimeSeries;

/**
 * @author ambud
 */
public class ClusteredMemStorageEngine implements StorageEngine {

	private static final Logger logger = Logger.getLogger(ClusteredMemStorageEngine.class.getName());
	public static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};
	private Columbus columbus;
	private int clusterSize;
	private Map<Integer, TCPClient> clients;
	private StorageEngine local;

	public ClusteredMemStorageEngine() {
	}

	@Override
	public void configure(Map<String, String> conf) throws IOException {
		this.local = new MemStorageEngine();
		this.local.configure(conf);
		this.clusterSize = Integer.parseInt(conf.getOrDefault("cluster.size", "1"));
		conf.put(Topology.WORKER_DATA_PORT, "9927");
		conf.put("linea.zk.root", "/sidewinder");
		this.columbus = new Columbus(conf);
	}

	@Override
	public void connect() throws IOException {
		this.local.connect();
		Executors.newSingleThreadExecutor().submit(columbus);
		while (columbus.getWorkerCount() < clusterSize) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				break;
			}
			logger.info("Waiting for worker discovery");
		}
		this.clients = new HashMap<>();
		for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
			if (entry.getKey() != columbus.getSelfWorkerId()) {
				TCPClient client = new TCPClient(entry.getValue());
				clients.put(entry.getKey(), client);
				client.connect();
				logger.info("Connected to " + entry.getValue().getWorkerAddress());
			}
		}
		logger.info("All worker connections initialized");
	}

	@Override
	public void disconnect() throws IOException {
		for (Entry<Integer, TCPClient> entry : clients.entrySet()) {
			entry.getValue().disconnect();
		}
	}

	@Override
	public void writeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags,
			TimeUnit unit, long timestamp, long value, Callback callback) throws IOException {
		int workerId = computeWorkerId(dbName, measurementName);
		if (workerId == columbus.getSelfWorkerId()) {
			local.writeSeries(dbName, measurementName, valueFieldName, tags, unit, timestamp, value, callback);
		} else {
			TCPClient tcpClient = clients.get(workerId);
			tcpClient.write(dbName, measurementName, tags, unit, timestamp, value);
		}
	}

	@Override
	public void writeSeries(String dbName, String measurementName, String valueFieldName, List<String> tags,
			TimeUnit unit, long timestamp, double value, Callback callback) throws IOException {
		int workerId = computeWorkerId(dbName, measurementName);
		if (workerId == columbus.getSelfWorkerId()) {
			local.writeSeries(dbName, measurementName, valueFieldName, tags, unit, timestamp, value, callback);
		} else {
			TCPClient tcpClient = clients.get(workerId);
			tcpClient.write(dbName, measurementName, tags, unit, timestamp, value);
		}
	}

	@Override
	public void writeDataPoint(String dbName, DataPoint dp) throws IOException {
		if (dp.isFp()) {
			writeSeries(dbName, dp.getMeasurementName(), dp.getValueFieldName(), dp.getTags(), TimeUnit.MILLISECONDS,
					dp.getTimestamp(), dp.getValue(), null);
		} else {
			writeSeries(dbName, dp.getMeasurementName(), dp.getValueFieldName(), dp.getTags(), TimeUnit.MILLISECONDS,
					dp.getTimestamp(), dp.getLongValue(), null);
		}
	}

	/**
	 * Mod-hash the measurement name to get which node this measurement should
	 * be sent to
	 * 
	 * @param dbName
	 * @param measurementName
	 * @return nodeId
	 */
	public int computeWorkerId(String dbName, String measurementName) {
		int workerCount = columbus.getWorkerCount();
		return Math.abs(MurmurHash.hash32(dbName + "_" + measurementName) % workerCount);
	}

	@Override
	public Map<String, List<DataPoint>> queryDataPoints(String dbName, String measurementName, String valueFieldName,
			long startTime, long endTime, List<String> tags, Predicate valuePredicate) throws ItemNotFoundException {
		int workerId = computeWorkerId(dbName, measurementName);
		if (workerId == columbus.getSelfWorkerId()) {
			return local.queryDataPoints(dbName, measurementName, valueFieldName, startTime, endTime, tags,
					valuePredicate);
		} else {
			WorkerEntry entry = columbus.getWorkerMap().get(workerId);
			entry.getWorkerAddress();
		}
		return null;
	}

	/**
	 * Encode proxy hops to the dbname so that recursion information can be
	 * communicated to the smaller caller.
	 * 
	 * This approach is used break recursion cycles when sending proxied
	 * commands through the cluster.
	 * 
	 * @param dbName
	 * @param selfWorker
	 * @return encodedDbName
	 */
	public static String encodeDbAndProxyName(String dbName, String selfWorker) {
		List<String> proxies = new ArrayList<>();
		dbName = decodeDbAndProxyNames(proxies, dbName);
		proxies.add(selfWorker);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Output output = new Output(os);
		kryoThreadLocal.get().writeObject(output, proxies);
		output.close();

		String proxyString = Base64.getEncoder().encodeToString(os.toByteArray());
		StringBuilder builder = new StringBuilder(proxyString.length() + 1 + dbName.length());
		builder.append(proxyString).append("|").append(dbName);
		return builder.toString();
	}

	/**
	 * Decode proxy hops to the dbname so that recursion information can be
	 * communicated to the smaller caller.
	 * 
	 * This approach is used break recursion cycles when sending proxied
	 * commands through the cluster.
	 * 
	 * @param proxies
	 * @param dbName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static String decodeDbAndProxyNames(List<String> proxies, String dbName) {
		String[] split = dbName.split("\\|");
		if (split.length > 1 && proxies != null) {
			dbName = split[1];
			byte[] decode = Base64.getDecoder().decode(split[0]);
			Input input = new Input(decode);
			proxies.addAll(kryoThreadLocal.get().readObject(input, ArrayList.class));
		}
		return dbName;
	}

	@Override
	public Set<String> getMeasurementsLike(String dbName, String partialMeasurementName) throws IOException {
		List<String> proxies = new ArrayList<>();
		dbName = decodeDbAndProxyNames(proxies, dbName);
		if (proxies.size() > 0) {
			return local.getMeasurementsLike(dbName, partialMeasurementName);
		} else {
			Set<String> localResult = local.getMeasurementsLike(dbName, partialMeasurementName);
			for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
				if (entry.getKey() != columbus.getSelfWorkerId()) {
					String newDbName = encodeDbAndProxyName(dbName, String.valueOf(columbus.getSelfWorkerId()));
					// http call
					CloseableHttpClient client = Utils.getClient(
							"http://" + entry.getValue().getWorkerAddress().getHostAddress() + ":8080/", 5000, 5000);
					// Grafana API
					HttpPost post = new HttpPost("http://" + entry.getValue().getWorkerAddress().getHostAddress()
							+ ":8080/" + newDbName + "/query/search");
					CloseableHttpResponse result = client.execute(post);
					if (result.getStatusLine().getStatusCode() == 200) {
						result.close();
						client.close();
						Gson gson = new Gson();
						@SuppressWarnings("unchecked")
						Set<String> fromJson = gson.fromJson(EntityUtils.toString(result.getEntity()), Set.class);
						localResult.addAll(fromJson);
					}
				}
			}
			return localResult;
		}
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		return local.getDatabases();
	}

	@Override
	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception {
		return getMeasurementsLike(dbName, "");
	}

	@Override
	public void deleteAllData() throws Exception {
		local.deleteAllData();
	}

	@Override
	public boolean checkIfExists(String dbName) throws Exception {
		List<String> proxies = new ArrayList<>();
		dbName = decodeDbAndProxyNames(proxies, dbName);
		if (proxies.size() > 0) {
			return local.checkIfExists(dbName);
		} else {
			boolean localResult = local.checkIfExists(dbName);
			for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
				if (entry.getKey() != columbus.getSelfWorkerId()) {
					String newDbName = encodeDbAndProxyName(dbName, String.valueOf(columbus.getSelfWorkerId()));
					// http call
					CloseableHttpClient client = Utils.getClient(
							"http://" + entry.getValue().getWorkerAddress().getHostAddress() + ":8080/", 5000, 5000);
					// Grafan API
					HttpGet post = new HttpGet("http://" + entry.getValue().getWorkerAddress().getHostAddress()
							+ ":8080/" + newDbName + "/hc");
					CloseableHttpResponse result = client.execute(post);
					if (result.getStatusLine().getStatusCode() == 200) {
						localResult = true;
						result.close();
						client.close();
						break;
					}
				}
			}
			return localResult;
		}
	}

	@Override
	public boolean checkIfExists(String dbName, String measurement) throws Exception {
		List<String> proxies = new ArrayList<>();
		dbName = decodeDbAndProxyNames(proxies, dbName);
		if (proxies.size() > 0) {
			return local.checkIfExists(dbName);
		} else {
			boolean localResult = local.checkIfExists(dbName);
			for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
				if (entry.getKey() != columbus.getSelfWorkerId()) {
					String newDbName = encodeDbAndProxyName(dbName, String.valueOf(columbus.getSelfWorkerId()));
					// http call
					CloseableHttpClient client = Utils.getClient(
							"http://" + entry.getValue().getWorkerAddress().getHostAddress() + ":8080/", 5000, 5000);

					// MeasurementOpsApi
					HttpGet post = new HttpGet("http://" + entry.getValue().getWorkerAddress().getHostAddress()
							+ ":8080/database/" + newDbName + "/" + measurement + "/check");
					CloseableHttpResponse result = client.execute(post);
					if (result.getStatusLine().getStatusCode() == 200) {
						localResult = true;
						result.close();
						client.close();
						break;
					}
				}
			}
			return localResult;
		}
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		List<String> proxies = new ArrayList<>();
		dbName = decodeDbAndProxyNames(proxies, dbName);
		if (proxies.size() > 0) {
			local.dropDatabase(dbName);
		} else {
			local.dropDatabase(dbName);
			for (Entry<Integer, WorkerEntry> entry : columbus.getWorkerMap().entrySet()) {
				if (entry.getKey() != columbus.getSelfWorkerId()) {
					String newDbName = encodeDbAndProxyName(dbName, String.valueOf(columbus.getSelfWorkerId()));
					// http call
					CloseableHttpClient client = Utils.getClient(
							"http://" + entry.getValue().getWorkerAddress().getHostAddress() + ":8080/", 5000, 5000);
					HttpDelete post = new HttpDelete("http://" + entry.getValue().getWorkerAddress().getHostAddress()
							+ ":8080/database/" + newDbName);
					CloseableHttpResponse result = client.execute(post);
					result.close();
					client.close();
				}
			}
		}
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		local.dropMeasurement(dbName, measurementName);
	}

	@Override
	public Set<String> getTagsForMeasurement(String dbname, String measurementName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getFieldsForMeasurement(String dbName, String measurementName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int retentionHours) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, String measurementName, int retentionHours) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateDefaultTimeSeriesRetentionPolicy(String dbName, int retentionHours) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTimeSeriesRetentionPolicy(String dbName, int retentionHours) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SortedMap<String, TimeSeries> getOrCreateMeasurement(String dbName, String measurementName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String dbName, String measurementName, String valueFieldName,
			List<String> tags, int timeBucketSize, boolean fp) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, SortedMap<String, TimeSeries>> getOrCreateDatabase(String dbName, int retentionPolicy) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isMeasurementFieldFP(String dbName, String measurementName, String valueFieldName)
			throws RejectException {
		// TODO Auto-generated method stub
		return false;
	}

}
