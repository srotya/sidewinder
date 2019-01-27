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
package com.srotya.sidewinder.cluster.pull.rpc;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.srotya.minuteman.utils.FileUtils;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc.ClusterReadServiceBlockingStub;
import com.srotya.sidewinder.cluster.rpc.DeltaObject;
import com.srotya.sidewinder.cluster.rpc.DeltaRequest;
import com.srotya.sidewinder.cluster.rpc.DeltaResponse;
import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.disk.DiskStorageEngine;
import com.srotya.sidewinder.core.storage.disk.PersistentMeasurement;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@SuppressWarnings("unused")
public class TestDataApi {

	public static void main(String[] args) throws Exception {
		rpcDeltaReads();
		// gorillaTest();
		// compressionTest();
		// extracted();
		// localEncodingTest();
	}

	private static void rpcDeltaReads() throws InvalidProtocolBufferException {
		DeltaRequest build = DeltaRequest.newBuilder().setDbName("cc").setMeasurementName("cpu")
				.setValueFieldName("usage_user").build();
		System.out.println(JsonFormat.printer().print(build));
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9882).usePlaintext(true).build();
		ClusterReadServiceBlockingStub stub = ClusterReadServiceGrpc.newBlockingStub(channel);
		DeltaResponse deltas = stub.getDeltas(build);
		for (DeltaObject deltaObject : deltas.getObjectList()) {
			System.out.println(JsonFormat.printer().print(deltaObject));
		}
		channel.shutdown();
	}

	private static void gorillaTest() throws IOException {
		FileUtils.delete(new File("target/m/"));
		int hours = 100;
		long ts = System.currentTimeMillis();
		StorageEngine engine = new DiskStorageEngine();
		Map<String, String> conf = new HashMap<>();
		conf.put("compaction.enabled", "true");
		conf.put("compaction.codec", "gorilla");
		// conf.put("compaction.codec", "byzantine");
		conf.put("compaction.ratio", "0.8");
		engine.configure(conf, null);

		Measurement m = new PersistentMeasurement();
		m.configure(conf, engine, 4096, "test", "m", "target/m/i", "target/m/d", new DBMetadata(), null);

		List<String> tags = new ArrayList<>();
		tags.add("t1");
		int timeBucketSize = 7200 * 2;
		boolean fp = false;
		for (int i = 0; i < 3600 * hours; i++) {
//			TimeSeries t = m.getOrCreateTimeSeries("v1", tags, timeBucketSize, fp, conf);
//			t.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, 1221);
		}
		m.compact();
//		TimeSeries t = null;
//		m.getOrCreateTimeSeries("v1", tags, timeBucketSize, fp, conf);
//		int sum = t.getBucketRawMap().entrySet().stream().map(e -> {
//			List<String> collect = e.getValue().stream().map(w -> "c:" + w.getCount() + ":s:" + w.getPosition())
//					.collect(Collectors.toList());
//			// return e.getKey() + " " + e.getValue().size() + " " + collect;
//			return e.getValue().stream()
//					// .filter(p -> p.isReadOnly() || p.isFull())
//					.mapToInt(w -> w.getPosition()).sum();
//		})
				// .forEach(System.out::println);
//				.mapToInt(e -> e).sum();
//		System.out.println(sum + ":" + 3600 * hours);
	}

	// private static void compressionTest() throws IOException {
	// String data = "f,masdnfnafadfasdfkaksdjfkasitupwq4jtnq4gj39utg903jgnalfm,sa
	// dv tnigionfk kfemwef";
	// byte[] compress = Utils.compress(data.getBytes());
	// byte[] decompress = Utils.uncompress(compress);
	// System.out.println(new String(decompress));
	// }
	//
	// private static void localEncodingTest() throws IOException {
	// ByteBuffer buf = ByteBuffer.allocate(1024);
	// ByzantineWriter wr = new ByzantineWriter();
	// wr.configure(new HashMap<>(), buf, true, 2, true);
	//
	// long ts = System.currentTimeMillis();
	// for (int i = 0; i < 400; i++) {
	// try {
	// wr.addValue(ts + i * 100, i);
	// } catch (Exception e) {
	// break;
	// }
	// }
	// String str = Base64.encodeBase64String(buf.array());
	// byte[] decodeBase64 = Base64.decodeBase64(str.getBytes());
	// buf = ByteBuffer.wrap(decodeBase64);
	// wr = new ByzantineWriter();
	// wr.configure(new HashMap<>(), buf, false, 2, true);
	// ByzantineReader reader = wr.getReader();
	// for (int i = 0; i < 400; i++) {
	// try {
	// System.out.println(reader.readPair());
	// } catch (Exception e) {
	// break;
	// }
	// }
	// }

	// private static void extracted() throws NoSuchAlgorithmException,
	// KeyStoreException, KeyManagementException,
	// IOException, ClientProtocolException, InstantiationException,
	// IllegalAccessException {
	// CloseableHttpClient client =
	// ReplicaRepairService.buildClient("http://localhost:8080/rpc/data", 5000,
	// 5000);
	// HttpPost req = new HttpPost("http://localhost:8080/rpc/data");
	// DataObject obj = new DataObject();
	// obj.setBucket("5a550000");
	// obj.setDbName("cc");
	// obj.setMeasurementName("cpu");
	// obj.setTags(Arrays.asList("hostname=host_232", "os=ubunut10.1",
	// "region=us-west-1"));
	// obj.setValueFieldName("usage_user");
	// Gson gson = new Gson();
	// String json = gson.toJson(obj);
	// req.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
	// CloseableHttpResponse resp = client.execute(req);
	// String data = EntityUtils.toString(resp.getEntity());
	// obj = gson.fromJson(data, DataObject.class);
	// for (String dat : obj.getBuf()) {
	// String decompress = dat.split("_")[1];
	// byte[] bytes = Base64.decodeBase64(decompress);
	// ByteBuffer wrap = ByteBuffer.wrap(bytes);
	// byte type = wrap.get(0);
	// System.out.println("Compression type:" + type + " size:" +
	// decompress.length() + " " + bytes.length);
	// Class<Writer> classById = CompressionFactory.getClassById(type);
	// Writer writer = classById.newInstance();
	// writer.configure(new HashMap<>(), wrap, false, 2, true);
	// System.out.println("Points:" + writer.getCount());
	// }
	// }

}
