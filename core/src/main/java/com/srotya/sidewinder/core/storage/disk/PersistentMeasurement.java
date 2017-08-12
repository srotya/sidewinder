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
import java.io.FilenameFilter;
import java.io.IOException;
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

	private static final Logger logger = Logger.getLogger(PersistentMeasurement.class.getName());
	private Map<String, TimeSeries> seriesMap;
	private DiskTagIndex tagIndex;
	private String compressionClass;
	private String dbDataDirectory;
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

	@Override
	public void configure(Map<String, String> conf, String measurementName, String indexDirectory, String dataDirectory,
			DBMetadata metadata, ScheduledExecutorService bgTaskPool) throws IOException {
		this.conf = conf;
		this.dbDataDirectory = dataDirectory;
		this.metadata = metadata;
		this.bgTaskPool = bgTaskPool;
		this.seriesMap = new ConcurrentHashMap<>(10000);
		this.tagIndex = new DiskTagIndex(indexDirectory, measurementName);
		this.compressionClass = conf.getOrDefault(StorageEngine.COMPRESSION_CLASS,
				StorageEngine.DEFAULT_COMPRESSION_CLASS);
		this.measurementName = measurementName;
		this.bufTracker = new ArrayList<>();
		this.fileMapIncrement = Integer
				.parseInt(conf.getOrDefault("measurement.file.increment", String.valueOf(1024 * 1024 * 1)));
		this.maxFileSize = Integer
				.parseInt(conf.getOrDefault("measurement.file.max", String.valueOf(Integer.MAX_VALUE)));
		if(fileMapIncrement>=maxFileSize) {
			throw new IllegalArgumentException("File increment can't be greater than or equal to file size");
		}
		createMeasurementDirectory();
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize, boolean fp,
			Map<String, String> conf) throws IOException {
		Collections.sort(tags);
		String rowKey = constructRowKey(valueFieldName, tags, tagIndex);
		TimeSeries timeSeries = getTimeSeries(rowKey);
		if (timeSeries == null) {
			synchronized (this) {
				if ((timeSeries = getTimeSeries(rowKey)) == null) {
					timeSeries = new TimeSeries(this, compressionClass, seriesId(rowKey), timeBucketSize, metadata, fp,
							conf, bgTaskPool);
					seriesMap.put(rowKey, timeSeries);
					appendTimeseriesToMeasurementMetadata(measurementMetadataFilePath(), seriesId(rowKey), fp,
							timeBucketSize);
					logger.fine("Created new timeseries:" + timeSeries + " for measurement:" + measurementName + "\t"
							+ rowKey + "\t" + metadata);
				}
			}
		}
		return timeSeries;
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
		logger.info("Loading measurement:" + measurementName);
		String mdFilePath = measurementMetadataFilePath();
		File file = new File(mdFilePath);
		if (!file.exists()) {
			logger.warning("Metadata file missing for measurement:" + measurementName);
			return;
		}

		String[] dataFiles = new File(getMeasurementDataDirectory()).list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dat");
			}
		});
		Arrays.sort(dataFiles);
		for (int i = 0; i < dataFiles.length - 1; i++) {
			String dataFile = dataFiles[i];
			activeFile = new RandomAccessFile(getMeasurementDataDirectory() + "/" + dataFile, "");
			System.err.println("DF:" + dataFile);
		}

		List<String> seriesEntries = MiscUtils.readAllLines(file);
		List<Future<?>> futures = new ArrayList<>();
		for (String entry : seriesEntries) {
			String[] split = entry.split("\t");
			futures.add(bgTaskPool.submit(() -> {
				String rowKey = split[0];
				logger.fine("Loading Timeseries:" + seriesId(rowKey));
				try {
					String timeBucketSize = split[2];
					seriesMap.put(rowKey,
							new TimeSeries(this, compressionClass, seriesId(rowKey), Integer.parseInt(timeBucketSize),
									metadata, Boolean.parseBoolean(split[1]), conf, bgTaskPool));
					logger.fine("Loaded Timeseries:" + seriesId(rowKey) + "\tkey:" + rowKey);
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

	private String measurementDataDirectoryPath() {
		return dbDataDirectory + "/" + measurementName;
	}

	private String measurementMetadataFilePath() {
		return measurementDataDirectoryPath() + "/.md";
	}

	public static String seriesId(String rowKey) {
		return rowKey;
	}

	public String getMeasurementDataDirectory() {
		return measurementDataDirectoryPath();
	}

	protected void createMeasurementDirectory() throws IOException {
		String measurementDirectory = measurementDataDirectoryPath();
		new File(measurementDirectory).mkdirs();
	}

	protected void appendTimeseriesToMeasurementMetadata(String measurementFilePath, String rowKey, boolean fp,
			int timeBucketSize) throws IOException {
		DiskStorageEngine.appendLineToFile(rowKey + "\t" + fp + "\t" + timeBucketSize, measurementFilePath);
	}

	@Override
	public ByteBuffer createNewBuffer() throws IOException {
		if (activeFile == null) {
			synchronized (this) {
				if (activeFile == null) {
					String filename = getMeasurementDataDirectory() + "/data-" + fcnt + ".dat";
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
					return createNewBuffer();
				}
				memoryMappedBuffer = activeFile.getChannel().map(MapMode.READ_WRITE, position, fileMapIncrement);
				logger.fine("Buffer expansion:" + position + "\t" + curr);
				// bufTracker.add(memoryMappedBuffer);
				itr++;
			}
			curr = base * increment;
			memoryMappedBuffer.position(curr);
			base++;
			return memoryMappedBuffer.slice();
		}
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
		MiscUtils.delete(new File(measurementDataDirectoryPath()));
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
