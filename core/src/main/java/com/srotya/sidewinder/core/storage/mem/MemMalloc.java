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
package com.srotya.sidewinder.core.storage.mem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.StorageEngine;

public class MemMalloc implements Malloc {

	private static final ByteString STR2 = new ByteString("\t");
	private int size;
	private List<String> cleanupCallback;

	public MemMalloc() {
	}

	public MemMalloc(List<String> cleanupCallback) {
		this.cleanupCallback = cleanupCallback;
	}

	public BufferObject createNewBuffer(ByteString seriesId, Integer tsBucket) throws IOException {
		return createNewBuffer(seriesId, tsBucket, size);
	}

	public BufferObject createNewBuffer(ByteString seriesId, Integer tsBucket, int newSize) throws IOException {
		ByteBuffer allocateDirect = ByteBuffer.allocateDirect(newSize);
		LinkedByteString str = new LinkedByteString(seriesId);
		str.concat(STR2).concat(String.valueOf(tsBucket));
		return new BufferObject(str, allocateDirect);
	}

	@Override
	public void cleanupBufferIds(Set<String> cleanupSet) throws IOException {
		if (cleanupCallback != null) {
			cleanupCallback.addAll(cleanupSet);
		}
	}

	@Override
	public Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBufferMap() throws FileNotFoundException, IOException {
		return null;
	}

	@Override
	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine,
			ScheduledExecutorService bgTaskPool, ReentrantLock lock) {
		this.size = Integer.parseInt(conf.getOrDefault("buffer.size", "1024"));
	}

	@Override
	public void close() throws IOException {
	}

	public List<String> getCleanupCallback() {
		return cleanupCallback;
	}

}
