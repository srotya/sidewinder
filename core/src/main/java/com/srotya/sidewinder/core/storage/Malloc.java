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
package com.srotya.sidewinder.core.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public interface Malloc {
	
	public static final int DEFAULT_INCREMENT_SIZE = 32768;
	public static final String CONF_MEASUREMENT_BUF_INCREMENT_SIZE = "malloc.buf.increment";

	public BufferObject createNewBuffer(LinkedByteString fieldId, Integer tsBucket, int size) throws IOException;

	public BufferObject createNewBuffer(LinkedByteString fieldId, Integer tsBucket) throws IOException;

	public void cleanupBufferIds(Set<String> cleanupList) throws IOException;

	public Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBufferMap() throws FileNotFoundException, IOException;

	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine,
			ScheduledExecutorService bgTaskPool, ReentrantLock lock) throws IOException;

	public void close() throws IOException;

	public LinkedByteString repairBufferId(LinkedByteString fieldId, LinkedByteString bufferId);

}
