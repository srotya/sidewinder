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
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
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
public class MappedSetTagIndex implements TagIndex {

	private static final String SEPERATOR = " ";
	private static final Logger logger = Logger.getLogger(MappedSetTagIndex.class.getName());
	private static final int INCREMENT_SIZE = 1024 * 1024 * 1;
	private SortedSet<String> rowKeyIndex;
	private String indexPath;
	private File revIndex;
	private Counter metricIndexRow;
	private boolean enableMetrics;
	private RandomAccessFile revRaf;
	private MappedByteBuffer rev;

	public MappedSetTagIndex(String indexDir, String measurementName) throws IOException {
		this.indexPath = indexDir + "/" + measurementName;
		rowKeyIndex = new ConcurrentSkipListSet<>();
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
				rowKeyIndex.add(r);
			}
		}
	}

	@Override
	public String mapTagKey(String tagKey) throws IOException {
		return tagKey;
	}

	@Override
	public String getTagKeyMapping(String hexString) {
		return hexString;
	}

	@Override
	public Set<String> getTags() {
		Set<String> tags = new HashSet<>();
		for (String string : rowKeyIndex) {
			String[] split = string.split(SEPERATOR);
			tags.add(split[0]);
		}
		return tags;
	}

	@Override
	public void index(String tagKey, String tagValue, String rowKey) throws IOException {
		rowKey = new StringBuilder(tagKey.length() + 1 + tagValue.length() + 1 + rowKey.length()).append(tagKey)
				.append(SEPERATOR).append(tagValue).append(SEPERATOR).append(rowKey).toString();
		if (rowKeyIndex.add(rowKey)) {
			boolean add = true;
			if (add) {
				if (enableMetrics) {
					metricIndexRow.inc();
				}
				synchronized (rowKeyIndex) {
					byte[] str = rowKey.getBytes();
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
	public Collection<String> searchRowKeysForTag(String tagKey, String tagValue) {
		Set<String> result = new HashSet<>();
		String tag = tagKey + SEPERATOR + tagValue;
		SortedSet<String> tailSet = rowKeyIndex.tailSet(tag);
		for (String entry : tailSet) {
			if (entry.startsWith(tag + SEPERATOR)) {
				result.add(entry.split(SEPERATOR)[1]);
			} else {
				break;
			}
		}
		return result;
	}

	@Override
	public void close() throws IOException {
		rev.force();
		revRaf.close();
	}

	@Override
	public void index(String tagKey, String tagValue, int rowIndex) throws IOException {
		// do nothing
	}

	@Override
	public int getSize() {
		int total = 0;
		for (String string : rowKeyIndex) {
			total += string.length();
		}
		return total;
	}

	@Override
	public String mapTagValue(String tagValue) throws IOException {
		return tagValue;
	}

	@Override
	public String getTagValueMapping(String tagValue) throws IOException {
		return tagValue;
	}

}