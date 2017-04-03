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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author ambud
 */
public interface Writer {

	public void addValue(long timestamp, long value) throws IOException;

	public void addValue(long timestamp, double value) throws IOException;

	public Reader getReader() throws IOException;

	public double getCompressionRatio();

	public void setHeaderTimestamp(long timestamp);
	
	public void configure(Map<String, String> conf) throws IOException;

	public void bootstrap(ByteBuffer buf) throws IOException;

	public void setSeriesId(String seriesId);

	public ByteBuffer getRawBytes();
	
	public void setCounter(int counter);

	public void close() throws IOException;

	public void setConf(Map<String, String> conf);
	
}