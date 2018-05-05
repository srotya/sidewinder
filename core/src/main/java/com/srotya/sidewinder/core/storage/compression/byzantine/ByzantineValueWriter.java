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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.compression.ValueCodec;
import com.srotya.sidewinder.core.storage.compression.ValueWriter;

/**
 * A simple delta-of-delta timeseries compression with XOR value compression
 * 
 * @author ambud
 */
@ValueCodec(id = 1, name = "byzantine")
public class ByzantineValueWriter implements ValueWriter {

	private int count;
	private ByteBuffer buf;
	private long prevValue;
	private boolean readOnly;
	private volatile boolean full;
	private int startOffset;
	private LinkedByteString bufferId;

	public ByzantineValueWriter() {
	}

	/**
	 * For unit testing only
	 * 
	 * @param buf
	 */
	protected ByzantineValueWriter(byte[] buf) {
		this();
		this.buf = ByteBuffer.allocateDirect(buf.length);
	}

	@Override
	public void configure(ByteBuffer buf, boolean isNew, int startOffset) throws IOException {
		this.startOffset = startOffset;
		this.buf = buf;
		this.buf.position(startOffset);
		if (isNew) {
			this.buf.putInt(0);
		} else {
			forwardCursorToEnd();
		}
	}

	private void forwardCursorToEnd() throws IOException {
		ByzantineValueReader reader = new ByzantineValueReader(buf, startOffset);
		count = reader.getCount();
		for (int i = 0; i < count; i++) {
			reader.read();
		}
		prevValue = reader.getPrevValue();
	}

	/**
	 * @param value
	 * @throws IOException
	 */
	protected void writeDataPoint(long value) throws IOException {
		if (readOnly) {
			throw WRITE_REJECT_EXCEPTION;
		}
		checkAndExpandBuffer();
		compressAndWriteValue(buf, value);
		count++;
		updateCount();
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
		if (buf.remaining() < 9 || buf.isReadOnly()) {
			full = true;
			throw BUF_ROLLOVER_EXCEPTION;
		}
	}

	private void updateCount() {
		buf.putInt(startOffset, count);
	}

	public ByzantineValueReader getReader() throws IOException {
		ByzantineValueReader reader = null;
		ByteBuffer rbuf = buf.duplicate();
		rbuf.rewind();
		reader = new ByzantineValueReader(rbuf, startOffset);
		return reader;
	}

	@Override
	public void add(long value) throws IOException {
		writeDataPoint(value);
	}

	@Override
	public double getCompressionRatio() {
		double ratio = 0;
		ratio = ((double) count * Long.BYTES * 2) / buf.position();
		return ratio;
	}

	@Override
	public int getPosition() {
		return buf.position();
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
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
	public ByteBuffer getRawBytes() {
		ByteBuffer b = buf.duplicate();
		return b;
	}

	@Override
	public void bootstrap(ByteBuffer buf) throws IOException {
		this.buf.rewind();
		buf.rewind();
		if (this.buf.limit() < buf.limit()) {
			throw BUF_ROLLOVER_EXCEPTION;
		}
		this.buf.put(buf);
		this.buf.rewind();
		forwardCursorToEnd();
	}

	@Override
	public void setCounter(int count) {
		this.count = count;
	}

	@Override
	public void makeReadOnly(boolean recovery) {
		readOnly = true;
	}

	@Override
	public int currentOffset() {
		int offset = 0;
		offset = buf.position();
		return offset;
	}

	@Override
	public boolean isFull() {
		return full;
	}

	/**
	 * @return the bufferId
	 */
	public LinkedByteString getBufferId() {
		return bufferId;
	}

	/**
	 * @param bufferId
	 *            the bufferId to set
	 */
	public void setBufferId(LinkedByteString bufferId) {
		this.bufferId = bufferId;
	}

	@Override
	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public int getStartOffset() {
		return startOffset;
	}

}
