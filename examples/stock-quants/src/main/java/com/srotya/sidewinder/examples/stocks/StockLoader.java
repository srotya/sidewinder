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
package com.srotya.sidewinder.examples.stocks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Sidewinder example to load last 20 years of Stock Quants into Sidewinder.
 * <br>
 * <br>
 * Data source: QuantQuote, https://quantquote.com/historical-stock-data
 * 
 * @author ambud
 */
public class StockLoader {

	private static final String BASE_URL = "http://localhost:8080/database/stocks/measurement/ticker/series/ticker";

	public static void main(String[] args) throws IOException, KeyManagementException, NoSuchAlgorithmException,
			KeyStoreException, ParseException, InterruptedException {

		downloadAndUnpackQuants();
		createDBWithRetectionPolicy();

		int counter = 0;
		for (File file : new File("target/quantquote_daily_sp500_83986/daily").listFiles()) {
			CloseableHttpClient client = buildClient(BASE_URL, 5000, 5000);
			String ticker = file.getName().replace(".csv", "").split("_")[1];
			for (String field : Arrays.asList("open", "high", "low", "close", "volume")) {
				createTagsForTicker(client, ticker, field);
			}
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String temp = null;
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			StringBuilder req = new StringBuilder();
			while ((temp = reader.readLine()) != null) {
				buildDataPointEntry(ticker, temp, format, req);
				counter++;
			}
			uploadDataPointsForTicker(client, req);
			reader.close();
		}
		System.out.println(counter);
	}

	private static void uploadDataPointsForTicker(CloseableHttpClient client, StringBuilder req)
			throws UnsupportedEncodingException {
		HttpPost post = new HttpPost("http://localhost:8080/http?db=stocks");
		post.setEntity(new StringEntity(req.toString()));
		try {
			CloseableHttpResponse execute = client.execute(post);
			execute.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void buildDataPointEntry(String ticker, String temp, SimpleDateFormat format, StringBuilder req)
			throws ParseException {
		String[] splits = temp.split(",");
		Date timestamp = format.parse(splits[0]);

		String metric = "ticker," + ticker + " open=" + splits[2] + ",high=" + splits[3] + ",low=" + splits[4]
				+ ",close=" + splits[5] + ",volume=" + splits[6] + "i " + timestamp.getTime() * 1000 * 1000;
		req.append(metric + "\n");
	}

	private static void createTagsForTicker(CloseableHttpClient client, String ticker, String field)
			throws UnsupportedEncodingException, IOException, ClientProtocolException {
		HttpPut put = new HttpPut(BASE_URL);
		put.setHeader("Content-Type", "application/json");
		Gson gson = new Gson();
		JsonObject object = new JsonObject();
		object.addProperty("valueField", field);
		object.addProperty("floatingPoint", true);
		object.addProperty("timeBucket", 3600 * 24 * 365);
		JsonArray tags = new JsonArray();
		tags.add(ticker);
		object.add("tags", tags);
		StringEntity entity = new StringEntity(gson.toJson(object));
		put.setEntity(entity);
		CloseableHttpResponse execute = client.execute(put);
		execute.close();
	}

	private static void createDBWithRetectionPolicy() throws NoSuchAlgorithmException, KeyStoreException,
			KeyManagementException, IOException, ClientProtocolException {
		// build a stocks database with 20 year retention policy for series
		HttpPut db = new HttpPut("http://localhost:8080/database/stocks?retentionPolicy=" + (24 * 365 * 20));
		CloseableHttpClient dbC = buildClient(BASE_URL, 5000, 5000);
		dbC.execute(db).close();
	}

	public static void downloadAndUnpackQuants() throws IOException, MalformedURLException {
		// Download quant data from QuantQuote
		File output = new File("target/output.zip");

		if (!output.exists()) {
			FileUtils.copyURLToFile(new URL("http://quantquote.com/files/quantquote_daily_sp500_83986.zip"), output);
		}

		ZipFile zipFile = new ZipFile(output);
		try {
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				File entryDestination = new File("target", entry.getName());
				if (entry.isDirectory()) {
					entryDestination.mkdirs();
				} else {
					entryDestination.getParentFile().mkdirs();
					InputStream in = zipFile.getInputStream(entry);
					OutputStream out = new FileOutputStream(entryDestination);
					IOUtils.copy(in, out);
					IOUtils.closeQuietly(in);
					out.close();
				}
			}
		} finally {
			zipFile.close();
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
