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
package com.srotya.sidewinder.core.storage.malloc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.BufferObject;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;

public class DiskMalloc implements Malloc {

	private static final String SEPARATOR = ")";
	private static final int INCREMENT = 1048576;
	private static final Logger logger = Logger.getLogger(DiskMalloc.class.getName());
	// 100MB default buffer increment size
	private static final int DEFAULT_BUF_INCREMENT = 1048576;
	private static final int DEFAULT_MAX_FILE_SIZE = Integer.MAX_VALUE;
	private static final int DEFAULT_INCREMENT_SIZE = 32768;
	public static final String CONF_MEASUREMENT_FILE_MAX = "measurement.file.max";
	public static final String CONF_MEASUREMENT_INCREMENT_SIZE = "measurement.buf.increment";
	public static final String CONF_MEASUREMENT_FILE_INCREMENT = "measurement.file.increment";
	private ReentrantLock lock = new ReentrantLock(false);
	private int itr;
	private int fileMapIncrement;
	private int increment;
	private int curr;
	private int fcnt;
	private MappedByteBuffer memoryMappedBuffer;
	private String measurementName;
	private long maxFileSize;
	private String filename;
	private RandomAccessFile rafActiveFile;
	private String dataDirectory;
	private long offset;
	private MappedByteBuffer ptrBuf;
	private RandomAccessFile rafPtr;
	private File ptrFile;
	private int ptrCounter;
	private boolean enableMetricsCapture;
	private Counter metricsBufferSize;
	private Counter metricsBufferResize;
	private Counter metricsFileRotation;
	private Counter metricsBufferCounter;

	@Override
	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine,
			ScheduledExecutorService bgTaskPool) {
		this.measurementName = measurementName;
		this.dataDirectory = dataDirectory + "/" + measurementName;
		this.fileMapIncrement = Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_INCREMENT, String.valueOf(DEFAULT_BUF_INCREMENT)));
		this.maxFileSize = Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_MAX, String.valueOf(DEFAULT_MAX_FILE_SIZE)));
		this.increment = Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_INCREMENT_SIZE, String.valueOf(DEFAULT_INCREMENT_SIZE)));
		if (maxFileSize < 0) {
			throw new IllegalArgumentException("File size can't be negative or greater than:" + Integer.MAX_VALUE);
		}
		if (fileMapIncrement >= maxFileSize) {
			throw new IllegalArgumentException("File increment can't be greater than or equal to file size");
		}
		this.ptrFile = new File(getPtrPath());
		if (engine != null) {
			enableMetricsCapture = true;
			MetricsRegistryService reg = MetricsRegistryService.getInstance(engine, bgTaskPool);
			MetricRegistry r = reg.getInstance("memoryops");
			metricsBufferSize = r.counter("buffer-size");
			metricsBufferResize = r.counter("buffer-resize");
			metricsFileRotation = r.counter("file-rotation");
			metricsBufferCounter = r.counter("buffer-counter");
		}
	}

	@Override
	public BufferObject createNewBuffer(String seriesId, String tsBucket) throws IOException {
		return createNewBuffer(seriesId, tsBucket, increment);
	}

	@Override
	public BufferObject createNewBuffer(String seriesId, String tsBucket, int newSize) throws IOException {
		logger.fine("Seriesid:" + seriesId + " requesting buffer of size:" + newSize);
		if (rafActiveFile == null) {
			lock.lock();
			if (rafActiveFile == null) {
				filename = dataDirectory + "/data-" + String.format("%012d", fcnt) + ".dat";
				rafActiveFile = new RandomAccessFile(filename, "rwd");
				offset = 0;
				logger.info("Creating new datafile for measurement:" + filename);
				memoryMappedBuffer = rafActiveFile.getChannel().map(MapMode.READ_WRITE, 0, fileMapIncrement);
				fcnt++;
				if (enableMetricsCapture) {
					metricsFileRotation.inc();
				}
			}
			lock.unlock();
		}
		lock.lock();
		try {
			if (curr + newSize < 0 || curr + newSize > memoryMappedBuffer.remaining() + 1) {
				curr = 0;
				itr++;
				offset = (((long) (fileMapIncrement)) * itr);
				// close the current data file, increment the filename by 1 so
				// that
				// a new data file will be created next time a buffer is
				// requested
				if (offset >= maxFileSize) {
					itr = 0;
					logger.info("Rotating datafile for measurement:" + measurementName + " closing active file:"
							+ filename);
					rafActiveFile.close();
					rafActiveFile = null;
					return createNewBuffer(seriesId, tsBucket);
				}
				memoryMappedBuffer = rafActiveFile.getChannel().map(MapMode.READ_WRITE, offset, fileMapIncrement);
				logger.fine("Buffer expansion:" + offset + "\t\t" + curr);
				if (enableMetricsCapture) {
					metricsBufferResize.inc();
					metricsBufferSize.inc(fileMapIncrement);
				}
			}
			String ptrKey = appendBufferPointersToDisk(seriesId, filename, curr, offset);
			TimeSeries.writeStringToBuffer(tsBucket, memoryMappedBuffer);
			ByteBuffer buf = memoryMappedBuffer.slice();
			buf.limit(newSize);
			curr = curr + newSize;
			memoryMappedBuffer.position(curr);
			logger.fine("Position:" + buf.position() + "\t" + buf.limit() + "\t" + buf.capacity());
			if (enableMetricsCapture) {
				metricsBufferCounter.inc();
			}
			return new BufferObject(ptrKey, buf);
		} finally {
			lock.unlock();
		}

	}

	@Override
	public Map<String, List<Entry<String, BufferObject>>> seriesBufferMap() throws FileNotFoundException, IOException {
		Map<String, MappedByteBuffer> bufferMap = new ConcurrentHashMap<>();
		File[] listFiles = new File(dataDirectory).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dat");
			}
		});

		for (File dataFile : listFiles) {
			try {
				RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
				MappedByteBuffer map = raf.getChannel().map(MapMode.READ_ONLY, 0, dataFile.length());
				bufferMap.put(dataFile.getName(), map);
				logger.info("Recovering data file:" + dataDirectory + "/" + dataFile.getName());
				raf.close();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Failed to recover data files for measurement:" + measurementName, e);
			}
		}
		Map<String, List<Entry<String, BufferObject>>> seriesBuffers = new HashMap<>();
		sliceMappedBuffersForBuckets(bufferMap, seriesBuffers);
		return seriesBuffers;
	}

	private void sliceMappedBuffersForBuckets(Map<String, MappedByteBuffer> bufferMap,
			Map<String, List<Entry<String, BufferObject>>> seriesBuffers) throws IOException {
		ptrCounter = 0;
		if (ptrFile.exists()) {
			rafPtr = new RandomAccessFile(ptrFile, "rwd");
			ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, ptrFile.length());
			ptrCounter = ptrBuf.getInt();
			logger.info(
					"Ptr file exists, will load " + ptrCounter + " series entries, file length:" + ptrFile.length());
		} else {
			rafPtr = new RandomAccessFile(ptrFile, "rwd");
			ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, INCREMENT);
			ptrBuf.putInt(0);
			logger.fine("Ptr file is missing, creating one");
		}
		for (int i = 0; i < ptrCounter; i++) {
			String line = TimeSeries.getStringFromBuffer(ptrBuf);
			String[] splits = line.split("\\" + SEPARATOR);
			logger.finest("reading line:"+Arrays.toString(splits));
			String fileName = splits[1];
			int positionOffset = Integer.parseInt(splits[3]);
			String seriesId = splits[0];
			int pointer = Integer.parseInt(splits[2]);
			MappedByteBuffer buf = bufferMap.get(fileName);
			int position = positionOffset + pointer;
			buf.position(position);
			String tsBucket = TimeSeries.getStringFromBuffer(buf);
			ByteBuffer slice = buf.slice();
			slice.limit(increment);
			List<Entry<String, BufferObject>> list = seriesBuffers.get(seriesId);
			if (list == null) {
				list = new ArrayList<>();
				seriesBuffers.put(seriesId, list);
			}
			list.add(new AbstractMap.SimpleEntry<>(tsBucket, new BufferObject(line, slice)));
		}
	}

	protected String appendBufferPointersToDisk(String seriesId, String filename, int curr, long offset)
			throws IOException {
		String[] split = filename.split("/");
		String line = seriesId + SEPARATOR + split[split.length - 1] + SEPARATOR + curr + SEPARATOR + offset;
		// resize
		byte[] bytes = line.getBytes();
		if (ptrBuf.remaining() < bytes.length + Integer.BYTES) {
			int newSize = ptrBuf.position() + INCREMENT;
			ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, newSize);
		}
		TimeSeries.writeStringToBuffer(line, ptrBuf);
		ptrBuf.putInt(0, ++ptrCounter);
		return line;
	}

	public void close() throws IOException {
		lock.lock();
		try {
			if (memoryMappedBuffer != null) {
				memoryMappedBuffer.force();
			}
			if (rafActiveFile != null) {
				rafActiveFile.close();
			}
			ptrBuf.force();
			rafPtr.close();
			logger.info("Closing measurement:" + measurementName);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void cleanupBufferIds(Set<String> cleanupList) throws IOException {
		lock.lock();
		try {
			Set<String> fileSet = new HashSet<>();
			ByteBuffer duplicate = ptrBuf.duplicate();
			duplicate.rewind();
			ByteBuffer temp = ByteBuffer.allocate(duplicate.capacity());
			temp.putInt(0);
			int tmpCounter = 0;
			duplicate.getInt();
			for (int i = 0; i < ptrCounter; i++) {
				String line = TimeSeries.getStringFromBuffer(duplicate);
				if (!cleanupList.contains(line)) {
					byte[] bytes = line.getBytes();
					temp.putInt(bytes.length);
					temp.put(bytes);
					fileSet.add(line.split("\\" + SEPARATOR)[1]);
					tmpCounter++;
					logger.fine("Rewriting line:" + line + " to ptr file for measurement");
				} else {
					logger.fine("Removing line:" + line + " from ptr file due to garbage collection for measurement:");
					if (enableMetricsCapture) {
						metricsBufferCounter.dec();
					}
				}
			}
			temp.putInt(0, tmpCounter);
			// swap files
			ptrBuf.rewind();
			temp.rewind();
			ptrBuf.put(temp);
			logger.fine("Renaming tmp ptr file to main for measurement");
			// check and delete data files
			deleteFilesExcept(fileSet);
		} finally {
			lock.unlock();
		}
	}

	private void deleteFilesExcept(Set<String> fileSet) throws IOException {
		File[] files = new File(dataDirectory).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dat");
			}
		});
		logger.fine("GC: Currently there are:" + files.length + " data files");
		int deleteCounter = 0;
		for (File file : files) {
			if (!fileSet.contains(file.getName())) {
				logger.info("GC: Deleting data file:" + file.getName());
				if (enableMetricsCapture) {
					metricsFileRotation.dec();
				}
				file.delete();
				deleteCounter++;
			}
		}
		logger.fine("GC: Remaining files:" + fileSet.size() + "; deleted:" + deleteCounter + " files");
	}

	private String getPtrPath() {
		return dataDirectory + "/.ptr";
	}
}
