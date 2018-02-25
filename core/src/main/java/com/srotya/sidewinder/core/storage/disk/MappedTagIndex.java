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

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Tag hash lookup table + Tag inverted index
 * 
 * @author ambud
 */
public class MappedTagIndex implements TagIndex {

	private static final String SEPARATOR = " ";
	private static final Logger logger = Logger.getLogger(MappedTagIndex.class.getName());
	private static final int INCREMENT_SIZE = 1024 * 1024 * 1;
	private Map<Integer, String> tagMap;
	private Map<String, Set<String>> rowKeyIndex;
	private XXHashFactory factory = XXHashFactory.fastestInstance();
	private String indexPath;
	private XXHash32 hash;
	private File fwdIndex;
	private File revIndex;
	private Counter metricIndexTag;
	private Counter metricIndexRow;
	private boolean enableMetrics;
	private RandomAccessFile fwdRaf;
	private RandomAccessFile revRaf;
	private MappedByteBuffer fwd;
	private MappedByteBuffer rev;

	public MappedTagIndex(String indexDir, String measurementName) throws IOException {
		this.indexPath = indexDir + "/" + measurementName;
		tagMap = new ConcurrentHashMap<>(10000);
		rowKeyIndex = new ConcurrentHashMap<>(10000);
		fwdIndex = new File(indexPath + ".fwd");
		revIndex = new File(indexPath + ".rev");
		hash = factory.hash32();
		MetricsRegistryService instance = MetricsRegistryService.getInstance();
		if (instance != null) {
			MetricRegistry registry = instance.getInstance("requests");
			metricIndexTag = registry.counter("index-tag");
			metricIndexRow = registry.counter("index-row");
			enableMetrics = true;
		}
		loadTagIndex();
	}

	protected void loadTagIndex() throws IOException {
		if (!fwdIndex.exists() || !revIndex.exists()) {
			fwdRaf = new RandomAccessFile(fwdIndex, "rwd");
			revRaf = new RandomAccessFile(revIndex, "rwd");
			fwd = fwdRaf.getChannel().map(MapMode.READ_WRITE, 0, INCREMENT_SIZE);
			rev = revRaf.getChannel().map(MapMode.READ_WRITE, 0, INCREMENT_SIZE);
			fwd.putInt(0);
			rev.putInt(0);
			logger.info("Tag index is missing; initializing new index");
		} else {
			fwdRaf = new RandomAccessFile(fwdIndex, "rwd");
			revRaf = new RandomAccessFile(revIndex, "rwd");
			logger.info("Tag index is present; loading:" + fwdIndex.getAbsolutePath());
			fwd = fwdRaf.getChannel().map(MapMode.READ_WRITE, 0, fwdIndex.length());
			rev = revRaf.getChannel().map(MapMode.READ_WRITE, 0, revIndex.length());
			// load forward lookup
			int offsetLimit = fwd.getInt();
			while (fwd.position() < offsetLimit) {
				int length = fwd.getInt();
				byte[] b = new byte[length];
				fwd.get(b);
				String tag = new String(b);
				String[] split = tag.split(SEPARATOR);
				String hash32 = split[0];
				tag = split[1];
				tagMap.put(Integer.parseInt(hash32), tag);
			}

			// load reverse lookup
			offsetLimit = rev.getInt();
			while (rev.position() < offsetLimit) {
				int length = rev.getInt();
				byte[] b = new byte[length];
				rev.get(b);
				String r = new String(b);
				String[] split = r.split(SEPARATOR);
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
		int hash32 = hash.hash(tagKey.getBytes(), 0, tagKey.length(), 57);
		String val = tagMap.get(hash32);
		if (val == null) {
			synchronized (tagMap) {
				String out = tagMap.put(hash32, tagKey);
				if (out == null) {
					byte[] str = (hash32 + SEPARATOR + tagKey).getBytes();
					if (fwd.remaining() < str.length + Integer.BYTES) {
						// resize buffer
						int temp = fwd.position();
						fwd = fwdRaf.getChannel().map(MapMode.READ_WRITE, 0, fwd.capacity() + INCREMENT_SIZE);
						fwd.position(temp);
					}
					fwd.putInt(str.length);
					fwd.put(str);
					fwd.putInt(0, fwd.position());
				}
			}
		}
		if (enableMetrics) {
			metricIndexTag.inc();
		}
		return Integer.toHexString(hash32);
	}

	@Override
	public String getTagMapping(String hexString) {
		return tagMap.get(Integer.parseUnsignedInt(hexString, 16));
	}

	@Override
	public Set<String> getTags() {
		return new HashSet<>(tagMap.values());
	}

	@Override
	public void index(String tag, String rowKey) throws IOException {
		Set<String> rowKeySet = rowKeyIndex.get(tag);
		if (rowKeySet == null) {
			synchronized (rowKeyIndex) {
				if ((rowKeySet = rowKeyIndex.get(tag)) == null) {
					rowKeySet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
					rowKeyIndex.put(tag, rowKeySet);
				}
			}
		}
		if (!rowKeySet.contains(rowKey)) {
			boolean add = rowKeySet.add(rowKey);
			if (add) {
				if (enableMetrics) {
					metricIndexRow.inc();
				}
				synchronized (tagMap) {
					byte[] str = (tag + SEPARATOR + rowKey).getBytes();
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
	public Set<String> searchRowKeysForTag(String tag) {
		return rowKeyIndex.get(tag);
	}

	@Override
	public void close() throws IOException {
		fwd.force();
		rev.force();
		fwdRaf.close();
		revRaf.close();
	}

	@Override
	public void index(String tag, int rowIndex) throws IOException {
		// do nothing
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}