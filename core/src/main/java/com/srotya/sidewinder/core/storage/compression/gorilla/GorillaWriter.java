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
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Codec;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.storage.compression.Writer;

@Codec(id = 3, name = "gorilla")
public class GorillaWriter implements Writer {

	private boolean full;
	private String bufferId;
	private String tsBucket;
	private ByteBuffer buf;
	private GorillaCompressor compressor;
	private int counter;
	private long timestamp;
	private ByteBufferBitOutput output;
	private int startOffset;

	@Override
	public void configure(Map<String, String> conf, ByteBuffer buf, boolean isNew, int startOffset, boolean isLocking)
			throws IOException {
		this.buf = buf;
		this.startOffset = startOffset;
		buf.position(startOffset);
		if (!isNew) {
			counter = buf.getInt();
		} else {
			buf.putInt(0);
		}
	}

	@Override
	public void addValue(long timestamp, long value) throws IOException {
		compressor.addValue(timestamp, value);
		updateCount();
	}

	private void updateCount() {
		counter++;
		buf.putInt(startOffset, counter);
	}

	@Override
	public void addValue(long timestamp, double value) throws IOException {
		compressor.addValue(timestamp, value);
		updateCount();
	}

	@Override
	public void write(DataPoint dp) throws IOException {
		compressor.addValue(dp.getTimestamp(), dp.getLongValue());
		updateCount();
	}

	@Override
	public void write(List<DataPoint> dp) throws IOException {
		for (DataPoint d : dp) {
			write(d);
			updateCount();
		}
	}

	@Override
	public Reader getReader() throws IOException {
		ByteBuffer duplicate = buf.duplicate();
		duplicate.rewind();
		GorillaReader reader = new GorillaReader(duplicate, startOffset);
		return reader;
	}

	@Override
	public double getCompressionRatio() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void bootstrap(ByteBuffer buf) throws IOException {
		// TODO Auto-generated method stub
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
		}
	}

	@Override
	public int currentOffset() {
		return buf.position();
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
	public void setTsBucket(String tsBucket) {
		this.tsBucket = tsBucket;
	}

	@Override
	public String getTsBucket() {
		return tsBucket;
	}

	@Override
	public int getPosition() {
		return buf.position();
	}

	@Override
	public void setBufferId(String key) {
		bufferId = key;
	}

	@Override
	public String getBufferId() {
		return bufferId;
	}

}
