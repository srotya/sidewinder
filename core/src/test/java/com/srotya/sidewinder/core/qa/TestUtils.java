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
package com.srotya.sidewinder.core.qa;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * @author ambud
 */
public class TestUtils {

	private TestUtils() {
	}

	public static CloseableHttpResponse makeRequest(HttpRequestBase request) throws KeyManagementException,
			ClientProtocolException, NoSuchAlgorithmException, KeyStoreException, MalformedURLException, IOException {
		return buildClient(request.getURI().toURL().toString(), 1000, 1000, null).execute(request);
	}

	public static CloseableHttpResponse makeRequestAuthenticated(HttpRequestBase request, CredentialsProvider provider)
			throws KeyManagementException, ClientProtocolException, NoSuchAlgorithmException, KeyStoreException,
			MalformedURLException, IOException {
		return buildClient(request.getURI().toURL().toString(), 1000, 1000, provider).execute(request);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout,
			CredentialsProvider provider) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		if (provider != null) {
			clientBuilder.setDefaultCredentialsProvider(provider);
		}
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).setAuthenticationEnabled(true).build();
		return clientBuilder.setDefaultRequestConfig(config).build();
	}

}
