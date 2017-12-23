package com.srotya.sidewinder.core.storage.malloc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.StorageEngine;

public interface Malloc {

	public BufferObject createNewBuffer(String seriesId, String tsBucket, int newSize) throws IOException;

	public BufferObject createNewBuffer(String seriesId, String tsBucket) throws IOException;

	public void cleanupBufferIds(Set<String> cleanupList) throws IOException;

	public Map<String, List<Entry<String, BufferObject>>> seriesBufferMap() throws FileNotFoundException, IOException;

	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine, ScheduledExecutorService bgTaskPool);

}
