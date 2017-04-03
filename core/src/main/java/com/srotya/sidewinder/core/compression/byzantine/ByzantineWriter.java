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
package com.srotya.sidewinder.core.compression.byzantine;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.mem.Writer;

/**
 * A simple delta-of-delta timeseries compression with XOR value compression
 * 
 * @author ambud
 */
public class ByzantineWriter implements Writer {

	public static final int DEFAULT_BUFFER_INIT_SIZE = 4096;
	private static final int BYTES_PER_DATAPOINT = 16;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadLock read = lock.readLock();
	private WriteLock write = lock.writeLock();
	private long prevTs;
	private long delta;
	private int count;
	private long lastTs;
	private ByteBuffer buf;
	private long prevValue;

	public ByzantineWriter() {
		buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_INIT_SIZE);
	}

	public ByzantineWriter(long headerTimestamp, byte[] buf) {
		this.buf = ByteBuffer.allocateDirect(buf.length);
		setHeaderTimestamp(headerTimestamp);
	}

	public void write(DataPoint dp) {
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
	 * 
	 * @param dp
	 */
	private void writeDataPoint(long timestamp, long value) {
		lastTs = timestamp;
		checkAndExpandBuffer();
		compressAndWriteTimestamp(timestamp);
		compressAndWriteValue(value);
		count++;
	}

	private void compressAndWriteValue(long value) {
		long xor = prevValue ^ value;
		if (xor == 0) {
			buf.put((byte) 0);
		} else if (xor >= Byte.MIN_VALUE && xor <= Byte.MAX_VALUE) {
			buf.put((byte) 1);
			buf.put((byte) xor);
		} else if (xor >= Short.MIN_VALUE && xor <= Short.MAX_VALUE) {
			buf.put((byte) 2);
			buf.putShort((short) xor);
		} else if (xor >= Integer.MIN_VALUE && xor <= Integer.MAX_VALUE) {
			buf.put((byte) 3);
			buf.putInt((int) xor);
		} else {
			buf.put((byte) 4);
			buf.putLong(xor);
		}
		prevValue = value;
	}

	private void checkAndExpandBuffer() {
		if (buf.remaining() < 13) {
			ByteBuffer temp = ByteBuffer.allocateDirect((int) (buf.capacity() * 2));
			buf.flip();
			temp.put(buf);
			buf = temp;
		}
	}

	private void compressAndWriteTimestamp(long timestamp) {
		long ts = timestamp;
		long newDelta = (ts - prevTs);
		int deltaOfDelta = (int) (newDelta - delta);
		if (deltaOfDelta == 0) {
			buf.put((byte) 0);
		} else if (deltaOfDelta >= Byte.MIN_VALUE && deltaOfDelta <= Byte.MAX_VALUE) {
			buf.put((byte) 1);
			buf.put((byte) deltaOfDelta);
		} else if (deltaOfDelta >= -32767 && deltaOfDelta <= 32768) {
			buf.put((byte) 10);
			buf.putShort((short) deltaOfDelta);
		} else {
			buf.put((byte) 11);
			buf.putInt(deltaOfDelta);
		}
		prevTs = ts;
		delta = newDelta;
	}

	@Override
	public void addValue(long timestamp, double value) {
		addValue(timestamp, Double.doubleToLongBits(value));
	}

	public ByzantineReader getReader() {
		ByzantineReader reader = null;
		read.lock();
		ByteBuffer rbuf = buf.duplicate();
		rbuf.rewind();
		reader = new ByzantineReader(rbuf, count, lastTs);
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
		double ratio = 0;
		read.lock();
		ratio = ((double) count * BYTES_PER_DATAPOINT) / buf.position();
		read.unlock();
		return ratio;
	}

	@Override
	public void setHeaderTimestamp(long timestamp) {
		prevTs = timestamp;
		buf.putLong(timestamp);
	}

	/**
	 * @return the lock
	 */
	protected ReentrantReadWriteLock getLock() {
		return lock;
	}

	/**
	 * @return the read
	 */
	protected ReadLock getRead() {
		return read;
	}

	/**
	 * @return the write
	 */
	protected WriteLock getWrite() {
		return write;
	}

	/**
	 * @return the prevTs
	 */
	protected long getPrevTs() {
		return prevTs;
	}

	/**
	 * @return the delta
	 */
	protected long getDelta() {
		return delta;
	}

	/**
	 * @return the count
	 */
	protected int getCount() {
		return count;
	}

	/**
	 * @return the lastTs
	 */
	protected long getLastTs() {
		return lastTs;
	}

	/**
	 * @return the buf
	 */
	protected ByteBuffer getBuf() {
		return buf;
	}

	/**
	 * @return the prevValue
	 */
	protected long getPrevValue() {
		return prevValue;
	}

}
