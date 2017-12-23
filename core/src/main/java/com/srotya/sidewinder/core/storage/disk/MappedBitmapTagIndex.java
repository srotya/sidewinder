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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.TagIndex;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MappedBitmapTagIndex implements TagIndex {

	private static final Logger logger = Logger.getLogger(MappedBitmapTagIndex.class.getName());
	private static final int INCREMENT_SIZE = 1024 * 1024 * 1;
	private Map<String, Set<String>> rowKeyIndex;
	private String indexPath;
	private File revIndex;
	private Counter metricIndexRow;
	private boolean enableMetrics;
	private RandomAccessFile revRaf;
	private MappedByteBuffer rev;

	public MappedBitmapTagIndex(String indexDir, String measurementName) throws IOException {
		this.indexPath = indexDir + "/" + measurementName;
		rowKeyIndex = new ConcurrentHashMap<>(10000);
		revIndex = new File(indexPath + ".rev");
		MetricsRegistryService instance = MetricsRegistryService.getInstance();
		if (instance != null) {
			MetricRegistry registry = instance.getInstance("requests");
			metricIndexRow = registry.counter("index-row");
			enableMetrics = true;
		}
		loadTagIndex();
	}

	protected void loadTagIndex() throws IOException {
		if (!revIndex.exists()) {
			revRaf = new RandomAccessFile(revIndex, "rwd");
			rev = revRaf.getChannel().map(MapMode.READ_WRITE, 0, INCREMENT_SIZE);
			rev.putInt(0);
			logger.fine("Tag index is missing; initializing new index");
		} else {
			revRaf = new RandomAccessFile(revIndex, "rwd");
			logger.info("Tag index is present; loading:" + revIndex.getAbsolutePath());
			rev = revRaf.getChannel().map(MapMode.READ_WRITE, 0, revIndex.length());

			// load reverse lookup
			int offsetLimit = rev.getInt();
			while (rev.position() < offsetLimit) {
				int length = rev.getInt();
				byte[] b = new byte[length];
				rev.get(b);
				String r = new String(b);
				String[] split = r.split("\t");
				String tag = split[0];
				Set<String> set = rowKeyIndex.get(tag);
				if (set == null) {
					set = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>(100));
					rowKeyIndex.put(split[0], set);
				}
				String rowKey = split[1];
				set.add(rowKey);
			}
		}
	}

	@Override
	public String mapTag(String tagKey) throws IOException {
		return tagKey;
	}

	@Override
	public String getTagMapping(String hexString) {
		return hexString;
	}

	@Override
	public Set<String> getTags() {
		return new HashSet<>(rowKeyIndex.keySet());
	}

	@Override
	public void index(String tagKey, String rowKey) throws IOException {
		Set<String> rowKeySet = rowKeyIndex.get(tagKey);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = rowKeyIndex.get(tagKey)) == null) {
					rowKeySet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
					rowKeyIndex.put(tagKey, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowKey)) {
			boolean add = rowKeySet.add(rowKey);
			if (add) {
				if (enableMetrics) {
					metricIndexRow.inc();
				}
				synchronized (rowKeyIndex) {
					byte[] str = (tagKey + "\t" + rowKey).getBytes();
					if (rev.remaining() < str.length + Integer.BYTES) {
						// resize buffer
						int temp = rev.position();
						rev = revRaf.getChannel().map(MapMode.READ_WRITE, 0, rev.capacity() + INCREMENT_SIZE);
						rev.position(temp);
					}
					rev.putInt(str.length);
					rev.put(str);
					rev.putInt(0, rev.position());
				}
			}
		}
	}

	@Override
	public Collection<String> searchRowKeysForTag(String tagKey) {
		return rowKeyIndex.get(tagKey);
	}

	@Override
	public void close() throws IOException {
		rev.force();
		revRaf.close();
	}

}