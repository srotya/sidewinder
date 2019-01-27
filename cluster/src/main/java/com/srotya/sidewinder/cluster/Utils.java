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
package com.srotya.sidewinder.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.MiscUtils;
import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * @author ambud
 */
public class Utils {

	private Utils() {
	}

	public static SortedMap<Integer, List<Writer>> checkAndScopeTimeRange(long startTime, long endTime,
			Series series, SortedMap<Integer, List<Writer>> bucketRawMap, int timeBucketSize) {
		Integer startBucket = bucketRawMap.firstKey();
		Integer endBucket = bucketRawMap.lastKey();
		if (startTime > 0 && TimeUtils.getTimeFromBucketString(startBucket) > startTime) {
			startBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, startTime, timeBucketSize)
					- timeBucketSize;
		}
		if (endTime > 0 && endTime < TimeUtils.getTimeFromBucketString(endBucket)) {
			endBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, endTime, timeBucketSize);
		}

		// swap timestamps
		if (startBucket.compareTo(endBucket) > 0) {
			Integer tmp = endBucket;
			endBucket = startBucket;
			startBucket = tmp;
		}

		if (bucketRawMap.size() > 1) {
			bucketRawMap = bucketRawMap.subMap(startBucket, endBucket);
		}
		return bucketRawMap;
	}

	/**
	 * Build a {@link CloseableHttpClient}
	 * 
	 * @param baseURL
	 * @param connectTimeout
	 * @param requestTimeout
	 * @return http client
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws KeyManagementException
	 */
	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}

	/**
	 * Get client
	 * 
	 * @param baseURL
	 * @param connectTimeout
	 * @param requestTimeout
	 * @return client
	 */
	public static CloseableHttpClient getClient(String baseURL, int connectTimeout, int requestTimeout) {
		try {
			return buildClient(baseURL, connectTimeout, requestTimeout);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			return null;
		}
	}

	public static byte[] compress(byte[] data) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
		GZIPOutputStream gzip = new GZIPOutputStream(bos);
		gzip.write(data);
		gzip.close();
		byte[] compressed = bos.toByteArray();
		bos.close();
		return compressed;
	}

	public static byte[] uncompress(byte[] compressed) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
		GZIPInputStream gis = new GZIPInputStream(bis);
		int read = 0;
		while ((read = gis.read()) != -1) {
			bos.write(read);
		}
		gis.close();
		bis.close();
		return bos.toByteArray();
	}

	public static String buildRouteKey(String dbName, String measurementName, String valueFieldName) {
		return dbName + "#" + measurementName + "#" + valueFieldName;
	}

	public static Integer pointToRouteKey(Point dp) {
		return MiscUtils.tagHashCode(dp.getTagsList());
	}

}
