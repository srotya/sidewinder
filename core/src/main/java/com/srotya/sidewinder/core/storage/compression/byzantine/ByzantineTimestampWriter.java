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

import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.compression.TimeCodec;
import com.srotya.sidewinder.core.storage.compression.TimeWriter;

/**
 * A simple delta-of-delta timeseries compression with XOR value compression
 * 
 * @author ambud
 */
@TimeCodec(id = 1, name = "byzantine")
public class ByzantineTimestampWriter implements TimeWriter {

	private long prevTs;
	private long tsDelta;
	private int count;
	private Buffer buf;
	private boolean readOnly;
	private volatile boolean full;
	private int startOffset;
	private LinkedByteString bufferId;

	public ByzantineTimestampWriter() {
	}

	/**
	 * For unit testing only
	 * 
	 * @param headerTimestamp
	 * @param buf
	 */
	protected ByzantineTimestampWriter(long headerTimestamp, Buffer buf) {
		this();
		this.buf = buf;
		setHeaderTimestamp(headerTimestamp);
	}

	@Override
	public void configure(Buffer buf, boolean isNew, int startOffset) throws IOException {
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
		ByzantineTimestampReader reader = new ByzantineTimestampReader(buf, startOffset);
		count = reader.getCount();
		for (int i = 0; i < count; i++) {
			reader.read();
		}
		tsDelta = reader.getDelta();
		prevTs = reader.getPrevTs();
	}

	/**
	 * @param timestamp
	 * @throws IOException
	 */
	protected void writeDataPoint(long timestamp) throws IOException {
		if (readOnly) {
			throw WRITE_REJECT_EXCEPTION;
		}
		checkAndExpandBuffer();
		compressAndWriteTimestamp(buf, timestamp);
		count++;
		updateCount();
	}

	private void checkAndExpandBuffer() throws IOException {
		if (buf.remaining() < 5 || buf.isReadOnly()) {
			full = true;
			throw BUF_ROLLOVER_EXCEPTION;
		}
	}

	private void compressAndWriteTimestamp(Buffer tBuf, long timestamp) {
		long ts = timestamp;
		long newDelta = (ts - prevTs);
		int deltaOfDelta = (int) (newDelta - tsDelta);
		if (deltaOfDelta == 0) {
			tBuf.put((byte) 0);
		} else if (deltaOfDelta >= Byte.MIN_VALUE && deltaOfDelta <= Byte.MAX_VALUE) {
			tBuf.put((byte) 1);
			tBuf.put((byte) deltaOfDelta);
		} else if (deltaOfDelta >= Short.MIN_VALUE && deltaOfDelta <= Short.MAX_VALUE) {
			tBuf.put((byte) 2);
			tBuf.putShort((short) deltaOfDelta);
		} else {
			tBuf.put((byte) 3);
			tBuf.putInt(deltaOfDelta);
		}
		prevTs = ts;
		tsDelta = newDelta;
	}

	@Override
	public void add(long timestamp) throws IOException {
		writeDataPoint(timestamp);
	}

	private void updateCount() {
		buf.putInt(startOffset, count);
	}

	@Override
	public ByzantineTimestampReader getReader() throws IOException {
		ByzantineTimestampReader reader = null;
		Buffer rbuf = buf.duplicate();
		rbuf.rewind();
		reader = new ByzantineTimestampReader(rbuf, startOffset);
		return reader;
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

	@Override
	public void setHeaderTimestamp(long timestamp) {
		if (prevTs == 0) {
			prevTs = timestamp;
			buf.putLong(timestamp);
		}
	}

	@Override
	public long getHeaderTimestamp() {
		return buf.getLong(4 + startOffset);
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
		return tsDelta;
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
	protected Buffer getBuf() {
		return buf;
	}

	@Override
	public Buffer getRawBytes() {
		Buffer b = buf.duplicate();
		return b;
	}

	@Override
	public void bootstrap(Buffer buf) throws IOException {
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

}
