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

	public BufferObject createNewBuffer(ByteString seriesId, Integer tsBucket, int size) throws IOException;

	public BufferObject createNewBuffer(ByteString seriesId, Integer tsBucket) throws IOException;

	public void cleanupBufferIds(Set<String> cleanupList) throws IOException;

	public Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBufferMap() throws FileNotFoundException, IOException;

	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine,
			ScheduledExecutorService bgTaskPool, ReentrantLock lock);

	public void close() throws IOException;

}
