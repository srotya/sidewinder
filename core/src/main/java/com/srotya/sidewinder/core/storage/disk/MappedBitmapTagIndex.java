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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.SeriesFieldMap;
import com.srotya.sidewinder.core.storage.TagIndex;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MappedBitmapTagIndex implements TagIndex {

	private static final Logger logger = Logger.getLogger(MappedBitmapTagIndex.class.getName());
	private static final int INCREMENT_SIZE = 1024 * 1024 * 1;
	private Map<String, MutableRoaringBitmap> rowKeyIndex;
	private String indexPath;
	private File revIndex;
	private Counter metricIndexRow;
	private boolean enableMetrics;
	private RandomAccessFile revRaf;
	private MappedByteBuffer rev;
	private PersistentMeasurement measurement;

	public MappedBitmapTagIndex(String indexDir, String measurementName, PersistentMeasurement measurement)
			throws IOException {
		this.measurement = measurement;
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
				String[] split = r.split(" ");
				String tag = split[0];
				MutableRoaringBitmap set = rowKeyIndex.get(tag);
				if (set == null) {
					set = new MutableRoaringBitmap();
					rowKeyIndex.put(split[0], set);
				}
				String rowKeyIndex = split[1];
				set.add(Integer.parseInt(rowKeyIndex));
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
	}

	@Override
	public Collection<String> searchRowKeysForTag(String tagKey) {
		MutableRoaringBitmap mutableRoaringBitmap = rowKeyIndex.get(tagKey);
		List<SeriesFieldMap> ref = measurement.getSeriesListAsList();
		List<String> rowKeys = new ArrayList<>();
		for (Iterator<Integer> iterator = mutableRoaringBitmap.iterator(); iterator.hasNext();) {
			Integer idx = iterator.next();
			rowKeys.add(ref.get(idx).getSeriesId());
		}
		return rowKeys;
	}

	@Override
	public void close() throws IOException {
		rev.force();
		revRaf.close();
	}

	public MutableRoaringBitmap getBitMapForTag(String tagKey) {
		return rowKeyIndex.get(tagKey);
	}

	@Override
	public void index(String tagKey, int rowIndex) throws IOException {
		MutableRoaringBitmap rowKeySet = rowKeyIndex.get(tagKey);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = rowKeyIndex.get(tagKey)) == null) {
					rowKeySet = new MutableRoaringBitmap();
					rowKeyIndex.put(tagKey, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowIndex)) {
			boolean add = rowKeySet.checkedAdd(rowIndex);
			if (add) {
				if (enableMetrics) {
					metricIndexRow.inc();
				}
				synchronized (rowKeyIndex) {
					byte[] str = (tagKey + " " + rowIndex).getBytes();
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
	public int getSize() {
		int total = 0;
		for (Entry<String, MutableRoaringBitmap> entry : rowKeyIndex.entrySet()) {
			total += entry.getValue().getSizeInBytes() + entry.getKey().length();
		}
		return total;
	}

}