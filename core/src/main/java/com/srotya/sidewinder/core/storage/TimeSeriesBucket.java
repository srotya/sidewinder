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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.predicates.Predicate;

/**
 * In-memory representation of a time series based on Facebook's Gorilla
 * compression. This class wraps the compressed time series byte representation
 * of Gorilla and adds read-write concurrency and thread-safety using re-entrant
 * locks.
 * 
 * @author ambud
 */
public class TimeSeriesBucket implements Serializable {

	private static final long serialVersionUID = 1L;
	// private static final RejectException OLD_DATA_POINT = new
	// RejectException("Rejected older datapoint");
	private Writer writer;
	private long headerTimestamp;

	public TimeSeriesBucket(String seriesId, String compressionFQCN, long headerTimestamp, boolean disk,
			Map<String, String> conf) {
		this.headerTimestamp = headerTimestamp;
//		try {
//			writer = (Writer) Class.forName(compressionFQCN).newInstance();
//			writer.setSeriesId(seriesId);
//			writer.configure(conf);
//		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}
		writer.setHeaderTimestamp(headerTimestamp);
	}

	/**
	 * Add data point with a double value and timestamp.<br>
	 * <br>
	 * Throws {@link RejectException} if the caller tries to add data older than
	 * the current timestamp.
	 * 
	 * @param timestamp
	 * @param value
	 * @throws RejectException
	 */
	public void addDataPoint(long timestamp, double value) throws IOException {
		writer.addValue(timestamp, value);
	}

	/**
	 * Add data point with a long value and timestamp.<br>
	 * <br>
	 * Throws {@link RejectException} if the caller tries to add data older than
	 * the current timestamp.
	 *
	 * @param timestamp
	 * @param value
	 * @throws RejectException
	 */
	public void addDataPoint(long timestamp, long value) throws IOException {
		writer.addValue(timestamp, value);
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to
	 * it. Along with {@link DataPoint} enrichments pushed to it.
	 * 
	 * @param timePredicate
	 * @param valuePredicate
	 * @param isFp
	 * @param appendFieldValueName
	 * @param appendTags
	 * @return point in time instance of reader
	 * @throws IOException 
	 */
	public Reader getReader(Predicate timePredicate, Predicate valuePredicate, boolean isFp,
			String appendFieldValueName, List<String> appendTags) throws IOException {
		Reader reader = writer.getReader();
		reader.setTimePredicate(timePredicate);
		reader.setValuePredicate(valuePredicate);
		reader.setFieldName(appendFieldValueName);
		reader.setIsFP(isFp);
		reader.setTags(appendTags);
		return reader;
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to
	 * it.
	 * 
	 * @param timePredicate
	 * @param valuePredicate
	 * @return point in time instance of reader
	 * @throws IOException 
	 */
	public Reader getReader(Predicate timePredicate, Predicate valuePredicate) throws IOException {
		Reader reader = writer.getReader();
		reader.setTimePredicate(timePredicate);
		reader.setValuePredicate(valuePredicate);
		return reader;
	}

	/**
	 * @return the headerTimestamp
	 */
	public long getHeaderTimestamp() {
		return headerTimestamp;
	}

	/**
	 * Get count of data points currently hosted in this bucket
	 * 
	 * @return
	 * @throws IOException 
	 */
	public int getCount() throws IOException {
		return getReader(null, null).getPairCount();
	}

	/**
	 * Analytical method used for monitoring compression ratios for a given
	 * timeseries. Ratio = expected number of bytes / actual number of bytes.
	 * 
	 * @return compression ratio
	 */
	public double getCompressionRatio() {
		return writer.getCompressionRatio();
	}
	
	public Writer getWriter() {
		return writer;
	}

	public void close() throws IOException {
	}
	
	public void delete() throws IOException {
	}
}