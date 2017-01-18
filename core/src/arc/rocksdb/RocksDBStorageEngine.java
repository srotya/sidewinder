/**
 * Copyright 2016 Ambud Sharma
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
package com.srotya.sidewinder.core.storage.rocksdb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.rocksdb.CompactionStyle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.AbstractStorageEngine;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.WriteTask;

/**
 * @author ambudsharma
 */
public class RocksDBStorageEngine extends AbstractStorageEngine {

	private static final Charset CHARSET = Charset.forName("utf-8");
	private LoadingCache<String, TreeMap<Long, byte[]>> seriesLookup;
	private RocksDB indexdb;
	private Options indexdbOptions;
	private RocksDB tsdb;
	private Options tsdbOptions;
	private WriteOptions writeOptions;
	private String tsdbWalDirectory;
	private String tsdbMemDirectory;
	private String indexdbMemDirectory;
	private String indexdbWalDirectory;
	private FlushOptions flushOptions;
	private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			return kryo;
		}
	};

	static {
		RocksDB.loadLibrary();
	}

	@SuppressWarnings("resource")
	@Override
	public void configure(Map<String, String> conf) throws IOException {
		tsdbWalDirectory = conf.getOrDefault("tsdb.wal.directory", "target/tsdbw");
		tsdbMemDirectory = conf.getOrDefault("tsdb.mem.directory", "target/tsdbm");

		indexdbWalDirectory = conf.getOrDefault("idxdb.wal.directory", "target/idxdbw");
		indexdbMemDirectory = conf.getOrDefault("idxdb.mem.directory", "target/idxdbm");

		if (true) {
			wipeDirectory(tsdbWalDirectory);
			wipeDirectory(tsdbMemDirectory);
			wipeDirectory(indexdbWalDirectory);
			wipeDirectory(indexdbMemDirectory);
		}
		tsdbOptions = new Options().setCreateIfMissing(true)
				.setAllowMmapReads(true).setAllowMmapWrites(true)
				.setIncreaseParallelism(2).setFilterDeletes(true).setMaxBackgroundCompactions(10)
				.setMaxBackgroundFlushes(10).setDisableDataSync(false).setUseFsync(false).setUseAdaptiveMutex(false)
				.setWriteBufferSize(1 * SizeUnit.MB).setCompactionStyle(CompactionStyle.UNIVERSAL)
				.setMaxWriteBufferNumber(6).setWalTtlSeconds(60).setWalSizeLimitMB(512)
				.setMaxTotalWalSize(1024 * SizeUnit.MB).setErrorIfExists(false).setAllowOsBuffer(true)
				.setWalDir(tsdbWalDirectory).setOptimizeFiltersForHits(false);

		indexdbOptions = new Options().setCreateIfMissing(true)
				.setAllowMmapReads(true).setAllowMmapWrites(true)
				.setIncreaseParallelism(2).setFilterDeletes(true).setMaxBackgroundCompactions(10)
				.setMaxBackgroundFlushes(10).setDisableDataSync(false).setUseFsync(false).setUseAdaptiveMutex(false)
				.setWriteBufferSize(1 * SizeUnit.MB).setCompactionStyle(CompactionStyle.UNIVERSAL)
				.setMaxWriteBufferNumber(6).setWalTtlSeconds(60).setWalSizeLimitMB(512)
				.setMaxTotalWalSize(1024 * SizeUnit.MB).setErrorIfExists(false).setAllowOsBuffer(true)
				.setWalDir(indexdbWalDirectory).setOptimizeFiltersForHits(false);
		writeOptions = new WriteOptions().setDisableWAL(false).setSync(false);
		
		flushOptions = new FlushOptions().setWaitForFlush(true);
	}

	private void wipeDirectory(String directory) {
		File file = new File(directory);
		if (file.isDirectory() && file.exists()) {
			Arrays.asList(file.listFiles()).forEach((f) -> {
				f.delete();
			});
			file.delete();
			file.mkdirs();
		}
	}

	@Override
	public void connect() throws IOException {
		try {
			tsdb = RocksDB.open(tsdbOptions, tsdbMemDirectory);
			indexdb = RocksDB.open(indexdbOptions, indexdbMemDirectory);
			seriesLookup = CacheBuilder.newBuilder().maximumSize(1000)
					.build(new CacheLoader<String, TreeMap<Long, byte[]>>() {

						@Override
						public TreeMap<Long, byte[]> load(String key) throws Exception {
							return getTreeFromDS(key);
						}

					});
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void disconnect() throws IOException {
		if(flushOptions!=null) {
			flushOptions.close();
		}
		if (tsdb != null) {
			tsdb.close();
		}
		if (indexdb != null) {
			indexdb.close();
		}
		if (writeOptions != null) {
			writeOptions.close();
		}
		if (indexdbOptions != null) {
			indexdbOptions.close();
		}
		if (tsdbOptions != null) {
			tsdbOptions.close();
		}
	}

	@Override
	public byte[] indexIdentifier(String identifier) throws IOException {
		try {
			byte[] key = identifier.getBytes(CHARSET);
			byte[] val = indexdb.get(key);
			if (val == null) {
//				val = ByteUtils.intToByteMSBTruncated((identifier));
				indexdb.put(key, val);
			}
			return val;
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public TreeMap<Long, byte[]> getTreeFromDS(String rowKey) throws Exception {
		return getTreeFromDS(rowKey.getBytes());
	}

	@SuppressWarnings("unchecked")
	public TreeMap<Long, byte[]> getTreeFromDS(byte[] rowKey) throws RocksDBException {
		byte[] ds = tsdb.get(rowKey);
		TreeMap<Long, byte[]> map;
		if (ds != null) {
			map = kryoThreadLocal.get().readObject(new Input(ds), TreeMap.class);
		} else {
			map = new TreeMap<>();
		}
		return map;
	}

	@Override
	public void writeSeriesPoint(WriteTask point) throws IOException {
		String encodedKey = new String(point.getRowKey());
		try {
			TreeMap<Long, byte[]> map = seriesLookup.get(encodedKey);
			indexdb.put(point.getSeriesName(), point.getSeriesName());
			map.put(point.getTimestamp(), point.getValue());
			ByteArrayOutputStream stream = new ByteArrayOutputStream((map.size() + 1) * 20);
			Output output = new Output(stream);
			kryoThreadLocal.get().writeObject(output, map);
			output.close();
			tsdb.put(point.getRowKey(), stream.toByteArray());
		} catch (RocksDBException | ExecutionException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	@Override
	public Set<String> getAllMeasurementsForDb(String dbName) throws Exception {
		Set<String> series = new HashSet<>();
		RocksIterator itr = indexdb.newIterator();
		itr.seekToFirst();
		while (itr.isValid()) {
			String seriesName = new String(itr.key());
			if (seriesName.startsWith(dbName+"_series_")) {
				series.add(seriesName);
			}
			itr.next();
		}
		return series;
	}
	
	@Override
	public void flush() throws IOException {
		try {
			tsdb.flush(flushOptions);
			System.err.println("Flushed");
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	public void print() throws Exception {
		RocksIterator itr = tsdb.newIterator();
		itr.seekToFirst();
		do {
			System.out.println(new String(itr.key()));
			itr.next();
		}while(itr.isValid());
	}

	@Override
	public Set<String> getDatabases() throws Exception {
		Set<String> databases = new HashSet<>();
		RocksIterator itr = indexdb.newIterator();
		itr.seekToFirst();
		while (itr.isValid()) {
			String seriesName = new String(itr.key());
			if (seriesName.startsWith("db_")) {
				String dbName = seriesName.split("_")[1];
				databases.add(dbName);
			}
			itr.next();
		}
		return databases;
	}

	@Override
	public void deleteAllData() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkIfExists(String dbName) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void dropDatabase(String dbName) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeDataPoint(String dbName, DataPoint dp) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<String> getMeasurementsLike(String dbName, String seriesNames) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DataPoint> queryDataPoints(String dbName, String measurementName, long startTime, long endTime,
			List<String> tags, Predicate valuePredicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dropMeasurement(String dbName, String measurementName) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean checkIfExists(String dbName, String measurement) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
}