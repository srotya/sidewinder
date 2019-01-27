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
package com.srotya.sidewinder.cluster.qa.masterslave;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.cluster.ClusterConfiguration;
import com.srotya.sidewinder.cluster.pull.SidewinderClusteredServer;

import io.dropwizard.testing.junit.DropwizardAppRule;

/**
 * @author ambud
 */
public class TestInMemoryCluster {

	// @ClassRule
	public static final DropwizardAppRule<ClusterConfiguration> MASTER = new DropwizardAppRule<>(
			SidewinderClusteredServer.class, "src/test/resources/master-slave-simple/master.yaml");
	// @ClassRule
	public static final DropwizardAppRule<ClusterConfiguration> SLAVE = new DropwizardAppRule<>(
			SidewinderClusteredServer.class, "src/test/resources/master-slave-simple/slave.yaml");

	// @Test
	public void test() throws KeyManagementException, Exception {
		long sts = 1497720452566L;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setTimeZone(TimeZone.getTimeZone("utc"));
		String payload = "{\"panelId\":2,\"range\":{\"from\":\"%s\",\"to\":\"%s\",\"raw\":{\"from\":\"now-5m\",\"to\":\"now\"}},\"rangeRaw\":{\"from\":\"now-5m\",\"to\":\"now\"},\"interval\":\"200ms\",\"intervalMs\":200,\"targets\":[{\"target\":\"cpu\",\"filters\":[],\"aggregator\":{\"args\":[{\"index\":0,\"type\":\"int\",\"value\":\"20\"}],\"name\":\"none\",\"unit\":\"secs\"},\"field\":\"value\",\"refId\":\"A\",\"type\":\"timeserie\"}],\"format\":\"json\",\"maxDataPoints\":1280}";
		HttpPost post = new HttpPost("http://localhost:8080/influx?db=qaSingleSeries");
		CloseableHttpResponse response = makeRequest(post);
		assertEquals(400, response.getStatusLine().getStatusCode());

		post = new HttpPost("http://localhost:8080/influx?db=qaSingleSeries");
		post.setEntity(new StringEntity("cpu,host=server01,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server01,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720453566000000"));
		response = makeRequest(post);
		assertEquals(204, response.getStatusLine().getStatusCode());

		post = new HttpPost("http://localhost:8080/qaSingleSeries/query/measurements");
		post.setHeader("Content-Type", "application/json");
		response = makeRequest(post);
		Gson gson = new Gson();
		JsonArray ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals("cpu", ary.get(0).getAsString());

		post = new HttpPost("http://localhost:8080/qaSingleSeries/query/tags");
		post.setHeader("Content-Type", "application/json");
		post.setEntity(new StringEntity("{ \"target\":\"cpu\" }"));
		response = makeRequest(post);
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals(4, ary.size());

		post = new HttpPost("http://localhost:8080/qaSingleSeries/query");
		post.setHeader("Content-Type", "application/json");
		post.setEntity(new StringEntity(
				String.format(payload, format.format(new Date(sts - 60_000)), format.format(new Date(sts + 60_000)))));
		response = makeRequest(post);
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals(3, ary.size());
		int i = 0;
		for (JsonElement ele : ary) {
			i += ele.getAsJsonObject().get("datapoints").getAsJsonArray().size();
		}
		assertEquals(6, i);
		response = makeRequest(
				new HttpGet("http://localhost:8080/databases/qaSingleSeries/measurements/cpu/fields/value?startTime="
						+ (sts - 2000) + "&endTime=" + (sts + 2000)));
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		Set<String> tag = new HashSet<>(
				Arrays.asList("host=server01", "host=server02", "host=server03", "region=uswest"));
		Iterator<JsonElement> itr = ary.iterator();
		i = 0;
		while (itr.hasNext()) {
			JsonObject obj = itr.next().getAsJsonObject();
			assertEquals("cpu", obj.get("measurementName").getAsString());
			assertEquals("value", obj.get("valueFieldName").getAsString());
			ary = obj.get("tags").getAsJsonArray();
			for (JsonElement ele : ary) {
				assertTrue(ele.getAsString(), tag.contains(ele.getAsString()));
			}
			i += obj.get("dataPoints").getAsJsonArray().size();
			JsonArray ary2 = obj.get("dataPoints").getAsJsonArray();
			for (int j = 0; j < ary2.size(); j++) {
				JsonElement ele = ary2.get(j);
				assertEquals(1, ele.getAsJsonObject().get("value").getAsInt());
				long ts = sts + j * 1000;
				assertEquals(ts, ele.getAsJsonObject().get("timestamp").getAsLong());
			}
		}
		assertEquals(6, i);

		// check slave node for replicated data
		response = makeRequest(
				new HttpGet("http://localhost:8082/databases/qaSingleSeries/measurements/cpu/fields/value?startTime="
						+ (sts - 2000) + "&endTime=" + (sts + 2000)));
		assertEquals(200, response.getStatusLine().getStatusCode());
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		tag = new HashSet<>(Arrays.asList("host=server01", "host=server02", "host=server03", "region=uswest"));
		itr = ary.iterator();
		i = 0;
		while (itr.hasNext()) {
			JsonObject obj = itr.next().getAsJsonObject();
			assertEquals("cpu", obj.get("measurementName").getAsString());
			assertEquals("value", obj.get("valueFieldName").getAsString());
			ary = obj.get("tags").getAsJsonArray();
			for (JsonElement ele : ary) {
				assertTrue(ele.getAsString(), tag.contains(ele.getAsString()));
			}
			i += obj.get("dataPoints").getAsJsonArray().size();
			JsonArray ary2 = obj.get("dataPoints").getAsJsonArray();
			for (int j = 0; j < ary2.size(); j++) {
				JsonElement ele = ary2.get(j);
				assertEquals(1, ele.getAsJsonObject().get("value").getAsInt());
				long ts = sts + j * 1000;
				assertEquals(ts, ele.getAsJsonObject().get("timestamp").getAsLong());
			}
		}
		assertEquals(6, i);
	}

	public static CloseableHttpResponse makeRequest(HttpRequestBase request) throws KeyManagementException,
			ClientProtocolException, NoSuchAlgorithmException, KeyStoreException, MalformedURLException, IOException {
		return buildClient(request.getURI().toURL().toString(), 1000, 1000).execute(request);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();
		return clientBuilder.setDefaultRequestConfig(config).build();
	}

}
