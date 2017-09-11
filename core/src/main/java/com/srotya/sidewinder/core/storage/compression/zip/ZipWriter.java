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
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * @author ambud
 */
public class ZipWriter implements Writer {

	private Lock read;
	private Lock write;
	private int count;
	private OutputStream zip;
	private ByteBuffer buf;
	private DataOutputStream dos;

	@Override
	public void configure(Map<String, String> conf, ByteBuffer buf, boolean isNew) throws IOException {
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		read = lock.readLock();
		write = lock.writeLock();
		this.buf = buf;
		buf.putInt(0);
		zip = new LZ4BlockOutputStream(new ByteBufferOutputStream(buf), 2048);
		dos = new DataOutputStream(zip);
	}

	@Override
	public void addValue(long timestamp, long value) throws IOException {
		write.lock();
		dos.writeLong(timestamp);
		dos.writeLong(value);
		dos.flush();
		count++;
		buf.putInt(0, count);
		write.unlock();
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
		for (DataPoint dp : dps) {
			dos.writeLong(dp.getTimestamp());
			dos.writeLong(dp.getLongValue());
		}
		dos.flush();
	}

	@Override
	public Reader getReader() throws IOException {
		read.lock();
		ByteBuffer readBuf = buf.duplicate();
		dos.flush();
		read.unlock();
		return new ZipReader(readBuf);
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
	public void makeReadOnly() {
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

	public class ByteBufferInputStream extends InputStream {

		private ByteBuffer buffer = null;

		public ByteBufferInputStream(ByteBuffer b) {
			this.buffer = b;
		}

		@Override
		public int read() throws IOException {
			return (buffer.get() & 0xFF);
		}
	}

	public class ByteBufferOutputStream extends OutputStream {

		private ByteBuffer buffer = null;

		public ByteBufferOutputStream(ByteBuffer b) {
			this.buffer = b;
		}

		@Override
		public void write(int b) throws IOException {
			buffer.put((byte) (b & 0xFF));
		}

	}

}
