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
package com.srotya.sidewinder.core.storage.compression;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * @author ambud
 */
public interface Writer {
	
	public static final RollOverException BUF_ROLLOVER_EXCEPTION = new RollOverException();
	public static final RejectException WRITE_REJECT_EXCEPTION = new RejectException();

	public void addValue(long timestamp, long value) throws IOException;

	public void addValue(long timestamp, double value) throws IOException;

	public void write(DataPoint dp) throws IOException;
	
	public void write(List<DataPoint> dp) throws IOException;
	
	public Reader getReader() throws IOException;

	public double getCompressionRatio();

	public void configure(Map<String, String> conf, ByteBuffer buf, boolean isNew) throws IOException;

	public void bootstrap(ByteBuffer buf) throws IOException;

	public ByteBuffer getRawBytes();
	
	public void setCounter(int counter);
	
	public void makeReadOnly();
	
	public int currentOffset();
	
	public int getCount();

	public boolean isFull();

	public long getHeaderTimestamp();

	public void setHeaderTimestamp(long timestamp);

}