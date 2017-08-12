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
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TagIndex;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class PersistentMeasurement implements Measurement {

	private static final int DEFAULT_BUF_INCREMENT = 1048576;
	private static final int DEFAULT_MAX_FILE_SIZE = Integer.MAX_VALUE;
	private static final String CONF_MEASUREMENT_FILE_MAX = "measurement.file.max";
	private static final String CONF_MEASUREMENT_FILE_INCREMENT = "measurement.file.increment";
	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private Map<String, TimeSeries> seriesMap;
	private DiskTagIndex tagIndex;
	private String compressionClass;
	private String dataDirectory;
	private DBMetadata metadata;
	private Map<String, String> conf;
	private ScheduledExecutorService bgTaskPool;
	private RandomAccessFile activeFile;
	private List<ByteBuffer> bufTracker;
	private int itr;
	private int fileMapIncrement;
	private int increment = 4096;
	private int curr;
	private int base;
	private int fcnt;
	private MappedByteBuffer memoryMappedBuffer;
	private String measurementName;
	private long maxFileSize;
	private String filename;
	private PrintWriter prBufPointers;
	private PrintWriter prMetadata;
	private String indexDirectory;

	@Override
	public void configure(Map<String, String> conf, String measurementName, String indexDirectory, String dataDirectory,
			DBMetadata metadata, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.dataDirectory = dataDirectory + "/" + measurementName;
		this.indexDirectory = indexDirectory + "/" + measurementName;
		createMeasurementDirectory();
		this.metadata = metadata;
		this.bgTaskPool = bgTaskPool;
		this.seriesMap = new ConcurrentHashMap<>(10000);
		this.tagIndex = new DiskTagIndex(this.indexDirectory, measurementName);
		this.compressionClass = conf.getOrDefault(StorageEngine.COMPRESSION_CLASS,
				StorageEngine.DEFAULT_COMPRESSION_CLASS);
		this.measurementName = measurementName;
		this.bufTracker = new ArrayList<>();
		this.prBufPointers = new PrintWriter(new FileOutputStream(new File(getPtrPath()), true));
		this.prMetadata = new PrintWriter(new FileOutputStream(new File(getMetadataPath()), true));
		this.fileMapIncrement = Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_INCREMENT, String.valueOf(DEFAULT_BUF_INCREMENT)));
		this.maxFileSize = Integer
				.parseInt(conf.getOrDefault(CONF_MEASUREMENT_FILE_MAX, String.valueOf(DEFAULT_MAX_FILE_SIZE)));
		if (fileMapIncrement >= maxFileSize) {
			throw new IllegalArgumentException("File increment can't be greater than or equal to file size");
		}
	}

	private String getPtrPath() {
		return dataDirectory + "/.ptr";
	}

	private String getMetadataPath() {
		return dataDirectory + "/.md";
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String seriesId = constructSeriesId(valueFieldName, tags, tagIndex);
		TimeSeries timeSeries = getTimeSeries(seriesId);
		if (timeSeries == null) {
			synchronized (this) {
				if ((timeSeries = getTimeSeries(seriesId)) == null) {
					timeSeries = new TimeSeries(this, compressionClass, seriesId, timeBucketSize, metadata, fp, conf,
							bgTaskPool);
					seriesMap.put(seriesId, timeSeries);
					appendTimeseriesToMeasurementMetadata(seriesId, fp, timeBucketSize);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ seriesId + "\t" + metadata);
				}
			}
		}
		return timeSeries;
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
		logger.info("Loading measurement:" + measurementName);
		String mdFilePath = getMetadataPath();
		File file = new File(mdFilePath);
		if (!file.exists()) {
			logger.warning("Metadata file missing for measurement:" + measurementName);
			return;
		}

		String[] dataFiles = new File(dataDirectory).list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dat");
			}
		});
		Arrays.sort(dataFiles);
		for (int i = 0; i < dataFiles.length - 1; i++) {
			String dataFile = dataFiles[i];
			activeFile = new RandomAccessFile(dataDirectory + "/" + dataFile, "");
			System.err.println("DF:" + dataFile);
		}

		List<String> seriesEntries = MiscUtils.readAllLines(file);
		List<Future<?>> futures = new ArrayList<>();
		for (String entry : seriesEntries) {
			String[] split = entry.split("\t");
			futures.add(bgTaskPool.submit(() -> {
				String seriesId = split[0];
				logger.fine("Loading Timeseries:" + seriesId);
				try {
					String timeBucketSize = split[2];
					String isFp = split[1];
					seriesMap.put(seriesId, new TimeSeries(this, compressionClass, seriesId,
							Integer.parseInt(timeBucketSize), metadata, Boolean.parseBoolean(isFp), conf, bgTaskPool));
					logger.fine("Loaded Timeseries:" + seriesId);
				} catch (NumberFormatException | IOException e) {
					logger.log(Level.SEVERE, "Failed to load series:" + entry, e);
				}
			}));
		}
		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.log(Level.SEVERE, "Failed to load series", e);
			}
		}
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
		return seriesMap.values();
	}

	@Override
	public Map<String, TimeSeries> getTimeSeriesMap() {
		return seriesMap;
	}

	@Override
	public TagIndex getTagIndex() {
		return tagIndex;
	}

	protected TimeSeries getTimeSeries(String entry) {
		return seriesMap.get(entry);
	}

	protected void createMeasurementDirectory() throws IOException {
		new File(dataDirectory).mkdirs();
		new File(indexDirectory).mkdirs();
	}

	protected void appendTimeseriesToMeasurementMetadata(String seriesId, boolean fp, int timeBucketSize)
			throws IOException {
		DiskStorageEngine.appendLineToFile(seriesId + "\t" + fp + "\t" + timeBucketSize, prMetadata);
	}

	@Override
	public ByteBuffer createNewBuffer(String seriesId) throws IOException {
		if (activeFile == null) {
			synchronized (this) {
				if (activeFile == null) {
					filename = dataDirectory + "/data-" + fcnt + ".dat";
					activeFile = new RandomAccessFile(filename, "rwd");
					logger.info("Creating new datafile for measurement:" + filename);
					memoryMappedBuffer = activeFile.getChannel().map(MapMode.READ_WRITE, 0, fileMapIncrement);
					bufTracker.add(memoryMappedBuffer);
					fcnt++;
				}
			}
		}
		synchronized (activeFile) {
			if (curr + increment < 0 || curr + increment > memoryMappedBuffer.remaining()) {
				base = 0;
				long position = (((long) (fileMapIncrement)) * itr) + 1;
				// close the current data file, increment the filename by 1 so
				// that
				// a new data file will be created next time a buffer is
				// requested
				if (position >= maxFileSize) {
					itr = 0;
					logger.info("Rotating datafile for measurement:" + measurementName + " closing active file"
							+ activeFile);
					activeFile.close();
					activeFile = null;
					return createNewBuffer(seriesId);
				}
				memoryMappedBuffer = activeFile.getChannel().map(MapMode.READ_WRITE, position, fileMapIncrement);
				logger.fine("Buffer expansion:" + position + "\t" + curr);
				// bufTracker.add(memoryMappedBuffer);
				itr++;
			}
			curr = base * increment;
			memoryMappedBuffer.position(curr);
			appendBufferPointersToDisk(seriesId, filename, curr);
			base++;
			return memoryMappedBuffer.slice();
		}
	}

	protected void appendBufferPointersToDisk(String seriesId, String filename, int curr) throws IOException {
		DiskStorageEngine.appendLineToFile(seriesId + "\t" + filename + "\t" + curr, prBufPointers);
	}

	@Override
	public String getMeasurementName() {
		return measurementName;
	}

	@Override
	public Set<String> getTags() {
		return tagIndex.getTags();
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public void delete() throws IOException {
		MiscUtils.delete(new File(dataDirectory));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "PersistentMeasurement [seriesMap=" + seriesMap + ", measurementName=" + measurementName + "]";
	}

}
