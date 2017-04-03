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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.Writer;

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
	private String seriesId;
	private boolean onDisk;
	private String outputFile;
	private RandomAccessFile file;

	public ByzantineWriter() {
	}

	public ByzantineWriter(long headerTimestamp, byte[] buf) {
		this.buf = ByteBuffer.allocateDirect(buf.length);
		setHeaderTimestamp(headerTimestamp);
	}

	@Override
	public void configure(Map<String, String> conf) throws IOException {
		onDisk = Boolean.parseBoolean(conf.getOrDefault(StorageEngine.PERSISTENCE_DISK, "false"));
		if (onDisk) {
			String outputDirectory = conf.getOrDefault("data.dir", "/tmp/sidewinder/data");
			outputFile = outputDirectory + "/" + seriesId;
//			System.out.println("\n\n" + outputFile + "\t" + outputFile + "\n\n");
			File fileTmp = new File(outputFile);
			if (fileTmp.exists()) {
				file = new RandomAccessFile(outputFile, "rwd");
				MappedByteBuffer map = file.getChannel().map(MapMode.READ_WRITE, 0, file.length());
				map.load();
				buf = map;
				count = buf.getInt();
				forwardCursorToEnd();
			} else {
				fileTmp.createNewFile();
				file = new RandomAccessFile(outputFile, "rwd");
				buf = file.getChannel().map(MapMode.READ_WRITE, 0, DEFAULT_BUFFER_INIT_SIZE);
				buf.putInt(0);
			}
		} else {
			buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_INIT_SIZE);
		}
	}

	private void forwardCursorToEnd() throws IOException {
		ByzantineReader reader = new ByzantineReader(buf, count);
		for (int i = 1; i < count; i++) {
			reader.readPair();
		}
		delta = reader.getDelta();
		prevTs = reader.getPrevTs();
		prevValue = reader.getPrevValue();
	}

	public void write(DataPoint dp) throws IOException {
		write.lock();
		writeDataPoint(dp.getTimestamp(), dp.getLongValue());
		write.unlock();
	}

	public void write(List<DataPoint> dps) throws IOException {
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
	 * @throws IOException
	 */
	private void writeDataPoint(long timestamp, long value) throws IOException {
		lastTs = timestamp;
		checkAndExpandBuffer();
		compressAndWriteTimestamp(buf, timestamp);
		compressAndWriteValue(buf, value);
		count++;
		if (onDisk) {
			updateCount();
		}
	}

	private void compressAndWriteValue(ByteBuffer tBuf, long value) {
		long xor = prevValue ^ value;
		if (xor == 0) {
			tBuf.put((byte) 0);
		} else if (xor >= Byte.MIN_VALUE && xor <= Byte.MAX_VALUE) {
			tBuf.put((byte) 1);
			tBuf.put((byte) xor);
		} else if (xor >= Short.MIN_VALUE && xor <= Short.MAX_VALUE) {
			tBuf.put((byte) 2);
			tBuf.putShort((short) xor);
		} else if (xor >= Integer.MIN_VALUE && xor <= Integer.MAX_VALUE) {
			tBuf.put((byte) 3);
			tBuf.putInt((int) xor);
		} else {
			tBuf.put((byte) 4);
			tBuf.putLong(xor);
		}
		prevValue = value;
	}

	private void checkAndExpandBuffer() throws IOException {
		if (buf.remaining() < 20) {
			ByteBuffer temp = null;
			if (onDisk) {
				temp = file.getChannel().map(MapMode.READ_WRITE, 0, (long) (file.length() * 1.2));
			} else {
				temp = ByteBuffer.allocateDirect((int) (buf.capacity() * 1.2));
			}
			buf.flip();
			temp.put(buf);
			buf = temp;
		}
	}

	private void compressAndWriteTimestamp(ByteBuffer tBuf, long timestamp) {
		long ts = timestamp;
		long newDelta = (ts - prevTs);
		int deltaOfDelta = (int) (newDelta - delta);
		if (deltaOfDelta == 0) {
			tBuf.put((byte) 0);
		} else if (deltaOfDelta >= Byte.MIN_VALUE && deltaOfDelta <= Byte.MAX_VALUE) {
			tBuf.put((byte) 1);
			tBuf.put((byte) deltaOfDelta);
		} else if (deltaOfDelta >= -32767 && deltaOfDelta <= 32768) {
			tBuf.put((byte) 10);
			tBuf.putShort((short) deltaOfDelta);
		} else {
			tBuf.put((byte) 11);
			tBuf.putInt(deltaOfDelta);
		}
		prevTs = ts;
		delta = newDelta;
	}

	@Override
	public void addValue(long timestamp, double value) throws IOException {
		addValue(timestamp, Double.doubleToLongBits(value));
	}

	private void updateCount() {
		buf.putInt(0, count);
	}

	public ByzantineReader getReader() {
		ByzantineReader reader = null;
		read.lock();
		ByteBuffer rbuf = buf.duplicate();
		rbuf.rewind();
		if (onDisk) {
			rbuf.getInt();
		}
		reader = new ByzantineReader(rbuf, count);
		read.unlock();
		return reader;
	}

	@Override
	public void addValue(long timestamp, long value) throws IOException {
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
		if (prevTs == 0) {
			prevTs = timestamp;
			buf.putLong(timestamp);
		}
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

	@Override
	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

}
