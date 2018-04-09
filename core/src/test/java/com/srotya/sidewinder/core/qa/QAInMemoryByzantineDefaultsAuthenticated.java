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
package com.srotya.sidewinder.core.qa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.SidewinderConfig;
import com.srotya.sidewinder.core.SidewinderServer;
import com.srotya.sidewinder.core.rpc.Tag;

import io.dropwizard.testing.junit.DropwizardAppRule;

/**
 * @author ambud
 */
public class QAInMemoryByzantineDefaultsAuthenticated {

	private static final String PORT = "55442";

	@ClassRule
	public static final DropwizardAppRule<SidewinderConfig> RULE = new DropwizardAppRule<SidewinderConfig>(
			SidewinderServer.class, "src/test/resources/auth.yaml");

	private static CredentialsProvider provider;

	@BeforeClass
	public static void beforeClass() {
		provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
		provider.setCredentials(AuthScope.ANY, credentials);
	}

	@Test
	public void testUnauthenticatedRequests() throws Exception {
		CloseableHttpResponse response = TestUtils
				.makeRequest(new HttpGet("http://localhost:" + PORT + "/databases/_internal"));
		assertEquals(401, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequest(new HttpGet("http://localhost:" + PORT + "/databases/_internal2"));
		assertEquals(401, response.getStatusLine().getStatusCode());
	}

	@Test
	public void testRestApi() throws Exception {
		CloseableHttpResponse response = TestUtils
				.makeRequestAuthenticated(new HttpGet("http://localhost:" + PORT + "/databases/_internal"), provider);
		assertEquals(200, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(new HttpGet("http://localhost:" + PORT + "/databases/_internal2"),
				provider);
		assertEquals(404, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal/measurements/cpu"), provider);
		assertEquals(200, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal/measurements/memory"), provider);
		assertEquals(200, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal2/measurements/memory"), provider);
		assertEquals(404, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal/measurements/memory2"), provider);
		assertEquals(404, response.getStatusLine().getStatusCode());
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal/measurements/memory/check"), provider);
		assertEquals(200, response.getStatusLine().getStatusCode());
		assertEquals("true", EntityUtils.toString(response.getEntity()));
		response = TestUtils.makeRequestAuthenticated(
				new HttpGet("http://localhost:" + PORT + "/databases/_internal/measurements/memory/fields/value"),
				provider);
		assertEquals(200, response.getStatusLine().getStatusCode());
	}

	@Test
	public void testTSQLApi() throws Exception, ClientProtocolException, NoSuchAlgorithmException, KeyStoreException,
			MalformedURLException, IOException {
		HttpPost post = new HttpPost("http://localhost:" + PORT + "/influx?db=qaTSQL");
		post.setEntity(new StringEntity("cpu,host=server01,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server01,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720453566000000"));
		CloseableHttpResponse response = TestUtils.makeRequestAuthenticated(post, provider);
		assertEquals(204, response.getStatusLine().getStatusCode());
		HttpPost get = new HttpPost("http://localhost:" + PORT + "/databases/qaTSQL/query");
		get.setEntity(new StringEntity("1497720442566<cpu.value<1497720652566"));
		response = TestUtils.makeRequestAuthenticated(get, provider);
		String entity = EntityUtils.toString(response.getEntity());
		System.out.println("RESULT:" + entity);
		JsonArray ary = new Gson().fromJson(entity, JsonArray.class);
		assertEquals(3, ary.size());
		for (int i = 0; i < ary.size(); i++) {
			assertEquals(2, ary.get(i).getAsJsonObject().get("dataPoints").getAsJsonArray().size());
		}
	}

	@Test
	public void testSingleSeriesWrites()
			throws KeyManagementException, ClientProtocolException, NoSuchAlgorithmException, KeyStoreException,
			MalformedURLException, IOException, ParseException, InterruptedException {
		long sts = 1497720452566L;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setTimeZone(TimeZone.getTimeZone("utc"));
		String payload = "{\"panelId\":2,\"range\":{\"from\":\"%s\",\"to\":\"%s\",\"raw\":{\"from\":\"now-5m\",\"to\":\"now\"}},\"rangeRaw\":{\"from\":\"now-5m\",\"to\":\"now\"},\"interval\":\"200ms\",\"intervalMs\":200,\"targets\":[{\"target\":\"cpu\",\"filters\":[],\"aggregator\":{\"args\":[{\"index\":0,\"type\":\"int\",\"value\":\"20\"}],\"name\":\"none\",\"unit\":\"secs\"},\"field\":\"value\",\"refId\":\"A\",\"type\":\"timeserie\"}],\"format\":\"json\",\"maxDataPoints\":1280}";
		HttpPost post = new HttpPost("http://localhost:" + PORT + "/influx?db=qaSingleSeries");
		CloseableHttpResponse response = TestUtils.makeRequestAuthenticated(post, provider);
		assertEquals(400, response.getStatusLine().getStatusCode());

		post = new HttpPost("http://localhost:" + PORT + "/influx?db=qaSingleSeries");
		post.setEntity(new StringEntity("cpu,host=server01,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720452566000000\n"
				+ "cpu,host=server01,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server02,region=uswest value=1i 1497720453566000000\n"
				+ "cpu,host=server03,region=uswest value=1i 1497720453566000000"));
		response = TestUtils.makeRequestAuthenticated(post, provider);
		assertEquals(204, response.getStatusLine().getStatusCode());

		post = new HttpPost("http://localhost:" + PORT + "/qaSingleSeries/query/measurements");
		post.setHeader("Content-Type", "application/json");
		response = TestUtils.makeRequestAuthenticated(post, provider);
		Gson gson = new Gson();
		JsonArray ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals("cpu", ary.get(0).getAsString());

		post = new HttpPost("http://localhost:" + PORT + "/qaSingleSeries/query/tags");
		post.setHeader("Content-Type", "application/json");
		post.setEntity(new StringEntity("{ \"target\":\"cpu\" }"));
		response = TestUtils.makeRequestAuthenticated(post, provider);
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals(2, ary.size());

		post = new HttpPost("http://localhost:" + PORT + "/qaSingleSeries/query");
		post.setHeader("Content-Type", "application/json");
		post.setEntity(new StringEntity(
				String.format(payload, format.format(new Date(sts - 60_000)), format.format(new Date(sts + 60_000)))));
		response = TestUtils.makeRequestAuthenticated(post, provider);
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		assertEquals(3, ary.size());
		int i = 0;
		for (JsonElement ele : ary) {
			i += ele.getAsJsonObject().get("datapoints").getAsJsonArray().size();
		}
		assertEquals(6, i);
		response = TestUtils.makeRequestAuthenticated(new HttpGet(
				"http://localhost:" + PORT + "/databases/qaSingleSeries/measurements/cpu/fields/value?startTime="
						+ (sts - 2000) + "&endTime=" + (sts + 2000)),
				provider);
		ary = gson.fromJson(EntityUtils.toString(response.getEntity()), JsonArray.class);
		Set<Tag> tag = new HashSet<>(Arrays.asList(Tag.newBuilder().setTagKey("host").setTagValue("server02").build(),
				Tag.newBuilder().setTagKey("host").setTagValue("server01").build(),
				Tag.newBuilder().setTagKey("host").setTagValue("server03").build(),
				Tag.newBuilder().setTagKey("region").setTagValue("uswest").build()));
		Iterator<JsonElement> itr = ary.iterator();
		i = 0;
		while (itr.hasNext()) {
			JsonObject obj = itr.next().getAsJsonObject();
			assertEquals("cpu", obj.get("measurementName").getAsString());
			assertEquals("value", obj.get("valueFieldName").getAsString());
			ary = obj.get("tags").getAsJsonArray();
			for (JsonElement ele : ary) {
				Tag tagObj = gson.fromJson(ele, Tag.class);
				assertTrue(ele + " ", tag.contains(tagObj));
			}
			i += obj.get("dataPoints").getAsJsonArray().size();
			JsonArray ary2 = obj.get("dataPoints").getAsJsonArray();
			for (int j = 0; j < ary2.size(); j++) {
				JsonElement ele = ary2.get(j);
				assertEquals(1, ele.getAsJsonObject().get("value").getAsInt());
				long ts = 1497720452566L + j * 1000;
				assertEquals(ts, ele.getAsJsonObject().get("timestamp").getAsLong());
			}
		}
		assertEquals(6, i);
	}

}
