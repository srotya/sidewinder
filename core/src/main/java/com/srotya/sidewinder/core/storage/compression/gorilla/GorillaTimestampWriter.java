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
package com.srotya.sidewinder.core.storage.compression.gorilla;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.TimeCodec;
import com.srotya.sidewinder.core.storage.compression.TimeWriter;
import com.srotya.sidewinder.core.utils.ByteUtils;

@TimeCodec(id = 4, name = "gorilla")
public class GorillaTimestampWriter implements TimeWriter {

	public static final int MD5_PADDING = 32;
	private boolean full;
	private LinkedByteString bufferId;
	private Buffer buf;
	private GorillaTimestampCompressor compressor;
	private int counter;
	private long timestamp;
	private ByteBufferBitOutput output;
	private int startOffset;
	private int position;
	private int checkSumLocaltion;

	@Override
	public void configure(Buffer buf, boolean isNew, int startOffset) throws IOException {
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
		GorillaValueReader reader = new GorillaValueReader(buf, startOffset, MD5_PADDING);
		for (int i = 0; i < counter; i++) {
			reader.read();
		}
	}

	@Override
	public void add(long timestamp) throws IOException {
		compressor.addValue(timestamp);
		counter++;
	}

	private void updateCount() {
		buf.putInt(startOffset, counter);
		position = buf.position();
		buf.putInt(startOffset + 4, position);
	}

	@Override
	public Reader getReader() throws IOException {
		Buffer duplicate = buf.duplicate(true);
		duplicate.rewind();
		GorillaTimestampReader reader = new GorillaTimestampReader(duplicate, startOffset, checkSumLocaltion);
		return reader;
	}

	@Override
	public double getCompressionRatio() {
		return (double) position / counter;
	}

	@Override
	public void bootstrap(Buffer buf) throws IOException {
	}

	@Override
	public Buffer getRawBytes() {
		Buffer duplicate = buf.duplicate(false);
		return duplicate;
	}

	@Override
	public void setCounter(int counter) {
		this.counter = counter;
	}

	@Override
	public void makeReadOnly(boolean recovery) throws IOException {
		// this writer is always readonly
		full = true;
		if (compressor != null && !recovery) {
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
		Buffer duplicate = buf.duplicate(true);
		duplicate.rewind();
		Buffer copy = duplicate.newInstance(duplicate.capacity());
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
		compressor = new GorillaTimestampCompressor(timestamp, output);
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
