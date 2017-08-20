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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.RollOverException;
import com.srotya.sidewinder.core.storage.compression.Writer;

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
	private TimeSeriesBucket prev, next;
	private long headerTimestamp;
	private volatile boolean full;

	public TimeSeriesBucket(String compressionFQCN, long headerTimestamp, Map<String, String> conf, ByteBuffer buf,
			boolean isNew) {
		this.headerTimestamp = headerTimestamp;
		try {
			writer = (Writer) Class.forName(compressionFQCN).newInstance();
			writer.configure(conf, buf, isNew);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		if (isNew) {
			writer.setHeaderTimestamp(headerTimestamp);
		}
	}

	/**
	 * Add data point with a double value and timestamp.<br>
	 * <br>
	 * Throws {@link RejectException} if the caller tries to add data older than the
	 * current timestamp.
	 * 
	 * @param timestamp
	 * @param value
	 * @throws RollOverException
	 * @throws IOException
	 */
	public void addDataPoint(long timestamp, double value) throws IOException, RollOverException {
		writer.addValue(timestamp, value);
	}

	/**
	 * Add data point with a long value and timestamp.<br>
	 * <br>
	 * Throws {@link RejectException} if the caller tries to add data older than the
	 * current timestamp.
	 *
	 * @param timestamp
	 * @param value
	 * @throws RollOverException
	 * @throws IOException
	 */
	public void addDataPoint(long timestamp, long value) throws IOException, RollOverException {
		writer.addValue(timestamp, value);
	}

	/**
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
	 * Along with {@link DataPoint} enrichments pushed to it.
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
	 * Get {@link Reader} with time and value filter predicates pushed-down to it.
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
	 * Get count of data points currently hosted in this bucket
	 * 
	 * @return
	 * @throws IOException
	 */
	public int getCount() throws IOException {
		return writer.getCount();
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

	/**
	 * @return the prev
	 */
	public TimeSeriesBucket getPrev() {
		return prev;
	}

	/**
	 * @return the next
	 */
	public TimeSeriesBucket getNext() {
		return next;
	}

	/**
	 * @return the headerTimestamp
	 */
	public long getHeaderTimestamp() {
		return headerTimestamp;
	}

	/**
	 * @return the full
	 */
	public boolean isFull() {
		return full;
	}

	/**
	 * @param full
	 *            the full to set
	 */
	public void setFull(boolean full) {
		this.full = full;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimeSeriesBucket [headerTimestamp=" + headerTimestamp + "]";
	}
}