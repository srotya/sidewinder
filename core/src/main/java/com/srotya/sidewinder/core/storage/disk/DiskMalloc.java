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
package com.srotya.sidewinder.core.storage.disk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
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
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.ByteString.ByteStringCache;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.Malloc;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.buffer.GenericBuffer;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class DiskMalloc implements Malloc {

	public static final int BUF_PARTS_LENGTH = 9;
	public static boolean debug = false;
	private static final int PTR_INCREMENT = 1048576;
	public static final ByteString SEPARATOR = new ByteString(")");
	private static final Logger logger = Logger.getLogger(DiskMalloc.class.getName());
	// 100MB default buffer increment size
	private static final int DEFAULT_FILE_INCREMENT = 1048576;
	private static final int DEFAULT_MAX_FILE_SIZE = Integer.MAX_VALUE;
	public static final String CONF_MALLOC_PTRFILE_INCREMENT = "malloc.ptrfile.increment";
	public static final String CONF_MEASUREMENT_FILE_MAX = "malloc.file.max";
	public static final String CONF_MEASUREMENT_FILE_INCREMENT = "malloc.file.increment";
	private ReentrantLock lock;
	private int ptrFileIncrement;
	private int itr;
	private int fileMapIncrement;
	private int increment;
	private int curr;
	private int fcnt;
	private MappedByteBuffer memoryMappedBuffer;
	private String measurementName;
	private long maxFileSize;
	private ByteString filename;
	private RandomAccessFile rafActiveFile;
	private String dataDirectory;
	private long offset;
	private MappedByteBuffer ptrBuf;
	private RandomAccessFile rafPtr;
	private File ptrFile;
	private ByteStringCache cache;

	private volatile int ptrCounter;
	private boolean enableMetricsCapture;
	private Counter metricsBufferSize;
	private Counter metricsBufferResize;
	private Counter metricsFileRotation;
	private Counter metricsBufferCounter;
	private Map<ByteString, WeakReference<MappedByteBuffer>> oldBufferReferences;

	@Override
	public void configure(Map<String, String> conf, String dataDirectory, String measurementName, StorageEngine engine,
			ScheduledExecutorService bgTaskPool, ReentrantLock lock) {
		this.measurementName = measurementName;
		this.lock = lock;
		this.dataDirectory = dataDirectory + "/" + measurementName;
		this.fileMapIncrement = getFileIncrement(conf);
		this.maxFileSize = getMaxFileSize(conf);
		this.increment = getBufIncrement(conf);
		if (maxFileSize < 0) {
			throw new IllegalArgumentException("File size can't be negative or greater than:" + Integer.MAX_VALUE);
		}
		if (fileMapIncrement >= maxFileSize) {
			throw new IllegalArgumentException("File increment can't be greater than or equal to file size");
		}
		this.ptrFile = new File(getPtrPath());
		this.ptrFileIncrement = Integer
				.parseInt(conf.getOrDefault(CONF_MALLOC_PTRFILE_INCREMENT, String.valueOf(PTR_INCREMENT)));
		cache = ByteStringCache.instance();
		if (engine != null) {
			enableMetricsCapture = true;
			MetricsRegistryService reg = MetricsRegistryService.getInstance(engine, bgTaskPool);
			MetricRegistry r = reg.getInstance("memoryops");
			metricsBufferSize = r.counter("buffer-size");
			metricsBufferResize = r.counter("buffer-resize");
			metricsFileRotation = r.counter("file-rotation");
			metricsBufferCounter = r.counter("buffer-counter");
		}
		if (debug) {
			oldBufferReferences = new ConcurrentHashMap<>();
		}
	}

	public static int getBufIncrement(Map<String, String> conf) {
		return Integer.parseInt(
				conf.getOrDefault(CONF_MEASUREMENT_BUF_INCREMENT_SIZE, String.valueOf(DEFAULT_INCREMENT_SIZE)));
	}

	public static int getMaxFileSize(Map<String, String> conf) {
		return Integer.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_MAX, String.valueOf(DEFAULT_MAX_FILE_SIZE)));
	}

	public static int getFileIncrement(Map<String, String> conf) {
		return Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_INCREMENT, String.valueOf(DEFAULT_FILE_INCREMENT)));
	}

	@Override
	public BufferObject createNewBuffer(LinkedByteString fieldId, Integer tsBucket) throws IOException {
		return createNewBuffer(fieldId, tsBucket, increment);
	}

	@Override
	public BufferObject createNewBuffer(LinkedByteString fieldId, Integer tsBucket, int newSize) throws IOException {
		logger.fine(() -> "Seriesid:" + fieldId + " requesting buffer of size:" + newSize);
		if (rafActiveFile == null) {
			lock.lock();
			if (rafActiveFile == null) {
				filename = new ByteString(dataDirectory + "/data-" + String.format("%012d", fcnt) + ".dat");
				rafActiveFile = new RandomAccessFile(filename.toString(), "rwd");
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
				// that a new data file will be created next time a buffer is
				// requested
				if (offset >= maxFileSize) {
					itr = 0;
					logger.info("Rotating datafile for measurement:" + measurementName + " closing active file:"
							+ filename);
					memoryMappedBuffer.force();
					rafActiveFile.close();
					rafActiveFile = null;
					return createNewBuffer(fieldId, tsBucket, newSize);
				}
				// used for GC testing and debugging
				if (oldBufferReferences != null) {
					oldBufferReferences.put(filename, new WeakReference<MappedByteBuffer>(memoryMappedBuffer));
				}
				memoryMappedBuffer = rafActiveFile.getChannel().map(MapMode.READ_WRITE, offset, fileMapIncrement);
				logger.fine("Buffer expansion:" + offset + "\t\t" + curr);
				if (enableMetricsCapture) {
					metricsBufferResize.inc();
					metricsBufferSize.inc(fileMapIncrement);
				}
			}
			LinkedByteString ptrKey = appendBufferPointersToDisk(fieldId, filename, curr, offset, newSize, tsBucket);
			ByteBuffer buf = memoryMappedBuffer.slice();
			buf.limit(newSize);
			curr = curr + newSize;
			memoryMappedBuffer.position(curr);
			logger.fine(() -> "Position:" + buf.position() + "\t" + buf.limit() + "\t" + buf.capacity());
			if (enableMetricsCapture) {
				metricsBufferCounter.inc();
			}
			return new BufferObject(ptrKey, new GenericBuffer(buf));
		} finally {
			lock.unlock();
		}

	}

	@Override
	public Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBufferMap()
			throws FileNotFoundException, IOException {
		Map<String, MappedByteBuffer> bufferMap = new ConcurrentHashMap<>();
		File[] listFiles = new File(dataDirectory).listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dat");
			}
		});

		Arrays.sort(listFiles, new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		for (File dataFile : listFiles) {
			try {
				RandomAccessFile raf = new RandomAccessFile(dataFile, "rw");
				MappedByteBuffer map = raf.getChannel().map(MapMode.READ_WRITE, 0, dataFile.length());
				bufferMap.put(dataFile.getName(), map);
				logger.info("Recovering data file:" + dataDirectory + "/" + dataFile.getName());
				raf.close();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Failed to recover data files for measurement:" + measurementName, e);
			}
		}
		// fix file sequencing since compaction & garbage collection will delete old
		// files and this will prevent them from being overwritten
		if (listFiles.length > 0) {
			fcnt = Integer.parseInt(listFiles[listFiles.length - 1].getName().replace("data-", "").replace(".dat", ""))
					+ 1;
		}
		Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBuffers = new HashMap<>();
		sliceMappedBuffersForBuckets(bufferMap, seriesBuffers);
		return seriesBuffers;
	}

	private void sliceMappedBuffersForBuckets(Map<String, MappedByteBuffer> bufferMap,
			Map<ByteString, List<Entry<Integer, BufferObject>>> seriesBuffers) throws IOException {
		ptrCounter = 0;
		initializePtrFile();
		for (int i = 0; i < ptrCounter; i++) {
			String line = MiscUtils.getStringFromBuffer(ptrBuf).trim();
			String[] splits = line.split("\\" + SEPARATOR);
			logger.finer("Reading line:" + Arrays.toString(splits));
			String fileName = splits[1];
			int positionOffset = Integer.parseInt(splits[3]);
			String seriesIdStr = splits[0];
			int pointer = Integer.parseInt(splits[2]);
			int size = Integer.parseInt(splits[4]);
			MappedByteBuffer buf = bufferMap.get(fileName);
			int position = positionOffset + pointer;
			buf.position(position);
			String tsBucket = splits[5];
			ByteBuffer slice = buf.slice();
			slice.limit(size);

			ByteString seriesId = new ByteString(seriesIdStr);
			LinkedByteString bsLine = new LinkedByteString(BUF_PARTS_LENGTH);
			bsLine.concat(seriesId).concat(SEPARATOR).concat(cache.get(new ByteString(splits[1]))).concat(SEPARATOR)
					.concat(new ByteString(splits[2])).concat(SEPARATOR).concat(new ByteString(splits[3]))
					.concat(SEPARATOR).concat(new ByteString(splits[4]));

			List<Entry<Integer, BufferObject>> list = seriesBuffers.get(seriesId);
			if (list == null) {
				list = new ArrayList<>();
				seriesBuffers.put(seriesId, list);
			}
			list.add(new AbstractMap.SimpleEntry<>(Integer.parseInt(tsBucket, 16),
					new BufferObject(bsLine, new GenericBuffer(slice))));
		}
	}

	@Override
	public LinkedByteString repairBufferId(LinkedByteString fieldId, LinkedByteString bufferId) {
		List<ByteString> stringList = bufferId.getStringList();
		stringList.remove(0);
		stringList.addAll(0, fieldId.getStringList());
		return bufferId;
	}

	private void initializePtrFile() throws FileNotFoundException, IOException {
		if (ptrFile.exists()) {
			rafPtr = new RandomAccessFile(ptrFile, "rwd");
			ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, ptrFile.length());
			ptrCounter = ptrBuf.getInt();
			logger.fine(
					"Ptr file exists, will load " + ptrCounter + " series entries, file length:" + ptrFile.length());
		} else {
			rafPtr = new RandomAccessFile(ptrFile, "rwd");
			ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, ptrFileIncrement);
			ptrBuf.putInt(ptrCounter);
			logger.info("Ptr file is missing, creating one");
		}
	}

	protected LinkedByteString appendBufferPointersToDisk(LinkedByteString seriesId, ByteString filename, int curr,
			long offset, int size, Integer tsBucket) throws IOException {
		lock.lock();
		try {
			ByteString[] split = filename.split("/");
			LinkedByteString line = new LinkedByteString(BUF_PARTS_LENGTH);
			line.concat(seriesId).concat(SEPARATOR).concat(cache.get(split[split.length - 1])).concat(SEPARATOR)
					.concat(String.valueOf(curr)).concat(SEPARATOR).concat(String.valueOf(offset)).concat(SEPARATOR)
					.concat(String.valueOf(size)).concat(SEPARATOR).concat(Integer.toHexString(tsBucket)).concat("\n");
			logger.fine("Measurement(" + measurementName + ")Appending pointer information to ptr file:" + line);
			// resize
			String strLine = line.toString();
			byte[] bytes = strLine.getBytes();
			if (ptrBuf.remaining() < bytes.length + Short.BYTES) {
				logger.fine("Need to resize ptrbuf because ptrBufRem:" + ptrBuf.remaining() + " line:" + bytes.length);
				int pos = ptrBuf.position();
				int newSize = pos + ptrFileIncrement;
				ptrBuf.force();
				ptrBuf = rafPtr.getChannel().map(MapMode.READ_WRITE, 0, newSize);
				// BUGFIX: missing pointer reset caused PTR file corruption
				ptrBuf.position(pos);
				logger.info("Resizing ptr file:" + ptrBuf.getInt(0) + " ptrcount:" + ptrCounter + " inc:"
						+ ptrFileIncrement + " position:" + pos);
			}
			MiscUtils.writeStringToBuffer(strLine, ptrBuf);
			ptrBuf.putInt(0, ++ptrCounter);
			logger.fine(() -> "Measurement(" + measurementName + ") appending pointer information to ptr file:" + line
					+ " pos:" + ptrBuf.position());
			return line;
		} finally {
			lock.unlock();
		}
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
			if (cleanupList.isEmpty()) {
				return;
			}
			Map<String, Integer> fileSet = new HashMap<>();
			ByteBuffer duplicate = ptrBuf.duplicate();
			duplicate.rewind();
			ByteBuffer temp = ByteBuffer.allocate(duplicate.capacity());
			temp.putInt(0);
			int tmpCounter = 0;
			duplicate.getInt();
			for (int i = 0; i < ptrCounter; i++) {
				String line = MiscUtils.getStringFromBuffer(duplicate);
				if (line.isEmpty()) {
					throw new IOException("Empty line in ptrbuffer, ptr buffer is likely corrupt");
				}
				if (!cleanupList.contains(line)) {
					MiscUtils.writeStringToBuffer(line, temp);
					try {
						String filename = line.split("\\" + SEPARATOR)[1];
						Integer count = fileSet.get(filename);
						if (count == null) {
							count = 0;
						}
						count = count + 1;
						fileSet.put(filename, count);
					} catch (ArrayIndexOutOfBoundsException e) {
						logger.severe("AOBException for buffer-cleanup:" + line + "=" + i + "=" + ptrCounter);
						throw e;
					}
					tmpCounter++;
					logger.fine("Rewriting line:" + line + " to ptr file for measurement:" + measurementName);
				} else {
					logger.fine("Removing line:" + line + " from ptr file due to garbage collection for measurement:"
							+ measurementName);
					if (enableMetricsCapture) {
						metricsBufferCounter.dec();
					}
				}
			}
			// rewrite buffers

			// BUGFIX: without the buffer marker; the position was moved to the end of the
			// buffer causing buffer to contain empty lines which errored out since this
			// file is suppose to contain sequential binary data
			int mark = temp.position();
			temp.putInt(0, tmpCounter);
			ptrBuf.rewind();
			temp.rewind();
			temp.limit(mark);
			ptrBuf.put(temp);
			logger.fine("Swapped ptr buffers: " + "Counter:" + tmpCounter + " total:" + ptrBuf.getInt(0) + " before:"
					+ ptrCounter + " pos:" + ptrBuf.position());
			ptrCounter = tmpCounter;

			for (Entry<String, Integer> entry : fileSet.entrySet()) {
				logger.info("file stats:" + entry.getKey() + " bufs:" + entry.getValue());
			}

			// check and delete data files
			deleteFilesExcept(fileSet.keySet());
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
		if (files == null) {
			logger.warning("Empty data directory:" + dataDirectory);
			return;
		}
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
		logger.info("GC: Remaining files:" + fileSet.size() + "; deleted:" + deleteCounter + " files");
	}

	private String getPtrPath() {
		return dataDirectory + "/.ptr";
	}

	public Map<ByteString, WeakReference<MappedByteBuffer>> getOldBufferReferences() {
		return oldBufferReferences;
	}
}
