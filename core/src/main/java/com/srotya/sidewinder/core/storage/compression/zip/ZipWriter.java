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
package com.srotya.sidewinder.core.storage.compression.zip;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.NoLock;

/**
 * @author ambud
 */
public abstract class ZipWriter implements Writer {

	private Lock read;
	private Lock write;
	private int count;
	private OutputStream zip;
	protected ByteBuffer buf;
	private DataOutputStream dos;
	private int startOffset;
	private String tsBucket;
	private int blockSize;
	private String bufferId;
	private ByteArrayOutputStream bas;

	@Override
	public void configure(Map<String, String> conf, ByteBuffer buf, boolean isNew, int startOffset, boolean isLocking)
			throws IOException {
		this.startOffset = startOffset;
		if (isLocking) {
			ReentrantReadWriteLock lck = new ReentrantReadWriteLock();
			read = lck.readLock();
			write = lck.writeLock();
		} else {
			read = new NoLock();
			write = new NoLock();
		}
		this.buf = buf;
		this.blockSize = Integer.parseInt(conf.getOrDefault("zip.block.size", "128"));
		buf.position(startOffset);
		if (isNew) {
			buf.putInt(0);
			bas = new ByteArrayOutputStream();
			zip = getOutputStream(bas, blockSize);
			dos = new DataOutputStream(zip);
		} else {
			count = buf.getInt();
		}
	}

	public abstract OutputStream getOutputStream(ByteBufferOutputStream stream, int blockSize) throws IOException;

	public abstract OutputStream getOutputStream(OutputStream stream, int blockSize) throws IOException;

	@Override
	public void addValue(long timestamp, long value) throws IOException {
		write.lock();
		try {
			dos.writeLong(timestamp);
			dos.writeLong(value);
			count++;
			buf.putInt(startOffset, count);
		} catch (Exception e) {
			throw new IOException("Read only buffer, writes can't be accepted:" + count + "\t" + buf.capacity());
		} finally {
			write.unlock();
		}
	}

	@Override
	public void addValue(long timestamp, double value) throws IOException {
		addValue(timestamp, Double.doubleToLongBits(value));
	}

	@Override
	public void write(DataPoint dp) throws IOException {
		addValue(dp.getTimestamp(), dp.getLongValue());
	}

	@Override
	public void write(List<DataPoint> dps) throws IOException {
		write.lock();
		try {
			for (DataPoint dp : dps) {
				dos.writeLong(dp.getTimestamp());
				dos.writeLong(dp.getLongValue());
			}
			dos.flush();
		} catch (Exception e) {
			throw new IOException("Read only buffer, writes can't be accepted");
		} finally {
			write.unlock();
		}
	}

	@Override
	public double getCompressionRatio() {
		read.lock();
		int position = buf.position();
		read.unlock();
		return ((double) Long.BYTES * 2 * count) / position;
	}

	@Override
	public void bootstrap(ByteBuffer buf) throws IOException {
	}

	@Override
	public ByteBuffer getRawBytes() {
		read.lock();
		ByteBuffer raw = buf.duplicate();
		read.unlock();
		return raw;
	}

	@Override
	public void setCounter(int counter) {
		count = counter;
	}

	@Override
	public void makeReadOnly() throws IOException {
		try {
			if (dos != null) {
				dos.flush();
				dos.close();
				buf.put(bas.toByteArray());
				// dereference buffers to free-up memory
				bas = null;
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public int currentOffset() {
		return buf.position();
	}

	@Override
	public int getCount() {
		return count;
	}

	@Override
	public boolean isFull() {
		return buf.remaining() == 0;
	}

	@Override
	public long getHeaderTimestamp() {
		return 0;
	}

	@Override
	public void setHeaderTimestamp(long timestamp) {
	}

	public static class ByteBufferInputStream extends InputStream {

		private ByteBuffer buffer = null;

		public ByteBufferInputStream(ByteBuffer b) {
			this.buffer = b;
		}

		@Override
		public int read() throws IOException {
			return (buffer.get() & 0xFF);
		}

	}

	public static class ByteBufferOutputStream extends OutputStream {

		private ByteBuffer buffer = null;

		public ByteBufferOutputStream(ByteBuffer b) {
			this.buffer = b;
		}

		@Override
		public void write(int b) throws IOException {
			buffer.put((byte) (b & 0xFF));
		}

	}

	@Override
	public void setTsBucket(String tsBucket) {
		this.tsBucket = tsBucket;
	}

	@Override
	public String getTsBucket() {
		return tsBucket;
	}

	protected Lock getRead() {
		return read;
	}

	protected Lock getWrite() {
		return write;
	}

	protected int getStartOffset() {
		return startOffset;
	}

	@Override
	public int getPosition() {
		return buf.position();
	}

	public int getBlockSize() {
		return blockSize;
	}

	/**
	 * @return the bufferId
	 */
	public String getBufferId() {
		return bufferId;
	}

	/**
	 * @param bufferId
	 *            the bufferId to set
	 */
	public void setBufferId(String bufferId) {
		this.bufferId = bufferId;
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}
	
}
