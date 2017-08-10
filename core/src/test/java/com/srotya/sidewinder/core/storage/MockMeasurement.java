package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class MockMeasurement implements Measurement {

	private int bufferRenewCounter = 0;
	private List<ByteBuffer> list;
	private int bufSize;

	public MockMeasurement(int bufSize) {
		this.bufSize = bufSize;
		list = new ArrayList<>();
	}

	@Override
	public Collection<TimeSeries> getTimeSeries() {
		return null;
	}

	@Override
	public Map<String, TimeSeries> getTimeSeriesMap() {
		return null;
	}

	@Override
	public TagIndex getTagIndex() {
		return null;
	}

	@Override
	public void delete() throws IOException {
	}

	@Override
	public void garbageCollector() throws IOException {
	}

	@Override
	public ByteBuffer createNewBuffer() throws IOException {
		bufferRenewCounter++;
		ByteBuffer allocate = ByteBuffer.allocate(bufSize);
		list.add(allocate);
		return allocate;
	}

	public int getBufferRenewCounter() {
		return bufferRenewCounter;
	}

	@Override
	public List<ByteBuffer> getBufTracker() {
		return list;
	}

	@Override
	public void loadTimeseriesFromMeasurements() throws IOException {
	}

	@Override
	public TimeSeries getOrCreateTimeSeries(String valueFieldName, List<String> tags, int timeBucketSize,
			boolean fp, Map<String, String> conf) throws IOException {
		return null;
	}

	@Override
	public void configure(Map<String, String> conf, String measurementName, String baseIndexDirectory,
			String dataDirectory, DBMetadata metadata, ScheduledExecutorService bgTaskPool) throws IOException {
	}

	@Override
	public String getMeasurementName() {
		return null;
	}

	@Override
	public Logger getLogger() {
		return null;
	}



}