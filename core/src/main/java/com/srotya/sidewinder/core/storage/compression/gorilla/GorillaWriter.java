/**
 * Copyright 2018 Ambud Sharma
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
package com.srotya.sidewinder.core.storage.compression.gorilla;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.compression.Codec;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.utils.ByteUtils;

@Codec(id = 4, name = "gorilla")
public class GorillaWriter implements Writer {

	public static final int MD5_PADDING = 32;
	private boolean full;
	private LinkedByteString bufferId;
	private ByteBuffer buf;
	private GorillaCompressor compressor;
	private int counter;
	private long timestamp;
	private ByteBufferBitOutput output;
	private int startOffset;
	private int position;
	private int checkSumLocaltion;

	@Override
	public void configure(ByteBuffer buf, boolean isNew, int startOffset, boolean isLocking)
			throws IOException {
		this.buf = buf;
		this.checkSumLocaltion = startOffset;
		this.startOffset = startOffset + MD5_PADDING;
		buf.position(this.startOffset);
		if (!isNew) {
			counter = buf.getInt();
			// forward to the end
			position = buf.getInt();
			// forwardToEnd();
		} else {
			buf.putInt(0);
			buf.putInt(0);
		}
	}

	@SuppressWarnings("unused")
	private void forwardToEnd() throws IOException {
		GorillaReader reader = new GorillaReader(buf, startOffset, MD5_PADDING);
		for (int i = 0; i < counter; i++) {
			reader.readPair();
		}
	}

	@Override
	public void addValueLocked(long timestamp, long value) throws IOException {
		compressor.addValue(timestamp, value);
		counter++;
	}

	private void updateCount() {
		buf.putInt(startOffset, counter);
		position = buf.position();
		buf.putInt(startOffset + 4, position);
	}

	@Override
	public void addValueLocked(long timestamp, double value) throws IOException {
		compressor.addValue(timestamp, value);
		counter++;
	}

	@Override
	public void write(DataPoint dp) throws IOException {
		compressor.addValue(dp.getTimestamp(), dp.getLongValue());
		counter++;
	}

	@Override
	public void write(List<DataPoint> dp) throws IOException {
		for (DataPoint d : dp) {
			write(d);
			counter++;
		}
	}

	@Override
	public Reader getReader() throws IOException {
		ByteBuffer duplicate = buf.duplicate();
		duplicate.rewind();
		GorillaReader reader = new GorillaReader(duplicate, startOffset, checkSumLocaltion);
		return reader;
	}

	@Override
	public double getCompressionRatio() {
		return (double) position / counter;
	}

	@Override
	public void bootstrap(ByteBuffer buf) throws IOException {
	}

	@Override
	public ByteBuffer getRawBytes() {
		ByteBuffer duplicate = buf.duplicate();
		return duplicate;
	}

	@Override
	public void setCounter(int counter) {
		this.counter = counter;
	}

	@Override
	public void makeReadOnly() throws IOException {
		// this writer is always readonly
		if (compressor != null) {
			compressor.close();
			updateCount();
			// compute md5 and store
			try {
				// cache old position so the buf position can be reset
				int oldPosition = buf.position();
				byte[] bufferToHash = bufferToHash();
				buf.position(checkSumLocaltion);
				// save hash
				buf.put(bufferToHash);
				buf.position(oldPosition);
			} catch (NoSuchAlgorithmException e) {
				throw new IOException(e);
			}
		}
	}

	private byte[] bufferToHash() throws NoSuchAlgorithmException {
		ByteBuffer duplicate = buf.duplicate();
		duplicate.rewind();
		ByteBuffer copy = ByteBuffer.allocate(duplicate.capacity());
		copy.put(duplicate);
		byte[] array = copy.array();
		return ByteUtils.md5(array);
	}

	@Override
	public int currentOffset() {
		return position;
	}

	@Override
	public int getCount() {
		return counter;
	}

	@Override
	public boolean isFull() {
		return full;
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public long getHeaderTimestamp() {
		return timestamp;
	}

	@Override
	public void setHeaderTimestamp(long timestamp) throws IOException {
		this.timestamp = timestamp;
		this.output = new ByteBufferBitOutput(buf);
		compressor = new GorillaCompressor(timestamp, output);
	}

	@Override
	public int getPosition() {
		return position;
	}

	@Override
	public void setBufferId(LinkedByteString key) {
		bufferId = key;
	}

	@Override
	public LinkedByteString getBufferId() {
		return bufferId;
	}

}
