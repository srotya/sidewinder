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
package com.srotya.sidewinder.core.storage.compression.dod;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.Writer;

/**
 * A simple delta-of-delta timeseries compression with no value compression
 * 
 * @author ambud
 */
public class DodWriter implements Writer {

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadLock read = lock.readLock();
	private WriteLock write = lock.writeLock();
	private BitWriter writer;
	private long prevTs;
	private long delta;
	private int count;
	private long lastTs;
	private boolean readOnly;

	public DodWriter() {
	}

	public DodWriter(long headTs, byte[] buf) {
		prevTs = headTs;
		writer = new BitWriter(buf);
	}
	
	@Override
	public void configure(Map<String, String> conf, ByteBuffer buf, boolean isNew) throws IOException {
		writer = new BitWriter(buf);
	}
	
	public void write(DataPoint dp) throws RejectException {
		if(readOnly) {
			throw WRITE_REJECT_EXCEPTION;
		}
		write.lock();
		writeDataPoint(dp.getTimestamp(), dp.getLongValue());
		write.unlock();
	}

	public void write(List<DataPoint> dps) {
		write.lock();
		for (Iterator<DataPoint> itr = dps.iterator(); itr.hasNext();) {
			DataPoint dp = itr.next();
			writeDataPoint(dp.getTimestamp(), dp.getLongValue());
		}
		write.unlock();
	}

	/**
	 * (a) Calculate the delta of delta: D = (tn - tn1) - (tn1 - tn2) (b) If D
	 * is zero, then store a single `0' bit (c) If D is between [-63, 64], store
	 * `10' followed by the value (7 bits) (d) If D is between [-255, 256],
	 * store `110' followed by the value (9 bits) (e) if D is between [-2047,
	 * 2048], store `1110' followed by the value (12 bits) (f) Otherwise store
	 * `1111' followed by D using 32 bits
	 * 
	 * @param dp
	 */
	private void writeDataPoint(long timestamp, long value) {
		lastTs = timestamp;
		long ts = timestamp;
		long newDelta = (ts - prevTs);
		int deltaOfDelta = (int) (newDelta - delta);

		writer.writeBits(deltaOfDelta, 32);
		writer.writeBits(value, 64);

		count++;
		prevTs = ts;
		delta = newDelta;
	}
	
	@Override
	public void addValue(long timestamp, double value) {
		addValue(timestamp, Double.doubleToLongBits(value));
	}

	public DoDReader getReader() {
		DoDReader reader = null;
		read.lock();
		ByteBuffer rbuf = writer.getBuffer().duplicate();
		reader = new DoDReader(rbuf, count, lastTs);
		read.unlock();
		return reader;
	}

	@Override
	public void addValue(long timestamp, long value) {
		write.lock();
		writeDataPoint(timestamp, value);
		write.unlock();
	}

	@Override
	public double getCompressionRatio() {
		return 0.1;
	}

	@Override
	public void setHeaderTimestamp(long timestamp) {
		prevTs = timestamp;
		writer.writeBits(timestamp, 64);
	}

	public BitWriter getWriter() {
		return writer;
	}

	@Override
	public ByteBuffer getRawBytes() {
		read.lock();
		ByteBuffer b = writer.getBuffer().duplicate();
		b.rewind();
		read.unlock();
		return b;
	}

	@Override
	public void bootstrap(ByteBuffer buf) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCounter(int counter) {
		this.count = counter;
	}

	@Override
	public void makeReadOnly() {
		write.lock();
		readOnly = true;
		write.unlock();
	}

}
