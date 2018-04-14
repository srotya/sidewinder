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
package com.srotya.sidewinder.core.monitoring;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.codahale.metrics.health.HealthCheck;

/**
 * @author ambud
 */
public class RestAPIHealthCheck extends HealthCheck {

	@Override
	protected Result check() throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date ets = new Date();
		Date sts = new Date(ets.getTime() - 60 * 1000);
		HttpPost hcPost = new HttpPost("http://localhost:8080/_internal/query/measurements");
		String payload = "{\"panelId\":2,\"range\":{\"from\":\"" + format.format(sts) + "\",\"to\":\""
				+ format.format(ets)
				+ "\",\"raw\":{\"from\":\"now-5m\",\"to\":\"now\"}},\"rangeRaw\":{\"from\":\"now-5m\",\"to\":\"now\"},\"interval\":\"200ms\",\"intervalMs\":200,\"targets\":[{\"target\":\"memory\",\"filters\":[],\"aggregator\":{\"args\":[{\"index\":0,\"type\":\"int\",\"value\":\"20\"}],\"name\":\"none\",\"unit\":\"secs\"},\"field\":\"max\",\"refId\":\"A\",\"type\":\"timeserie\"},{\"target\":\"memory\",\"filters\":[],\"aggregator\":{\"args\":[{\"index\":0,\"type\":\"int\",\"value\":\"20\"}],\"name\":\"none\",\"unit\":\"secs\"},\"correlate\":true,\"field\":\"used\",\"refId\":\"B\",\"type\":\"timeserie\"}],\"format\":\"json\",\"maxDataPoints\":1280}";
		hcPost.setEntity(new StringEntity(payload));
		hcPost.setHeader("Content-Type", "application/json");
		CloseableHttpClient dbC = buildClient("http://localhost:8080/", 5000, 5000);
		CloseableHttpResponse response = dbC.execute(hcPost);
		if (response.getStatusLine().getStatusCode() == 200) {
			return Result.healthy();
		} else {
			return Result.unhealthy(
					response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());
		}
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}

}
