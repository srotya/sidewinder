package com.srotya.sidewinder.core.storage.mem;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.storage.DBMetadata;
import com.srotya.sidewinder.core.storage.Database;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.disk.DiskMalloc;

public class MemoryDatabase implements Database {

	private static Logger logger = Logger.getLogger(MemoryDatabase.class.getName());
	private Map<String, Measurement> measurementMap;
	private DBMetadata metadata;
	private MemStorageEngine engine;
	private String dbName;

	public MemoryDatabase(String dbName, MemStorageEngine engine) {
		this.dbName = dbName;
		this.engine = engine;
		this.measurementMap = new ConcurrentHashMap<>();
		this.metadata = new DBMetadata();
	}

	@Override
	public Map<String, Measurement> getMeasurementMap() {
		return measurementMap;
	}

	@Override
	public Collection<Measurement> getMeasurements() {
		return measurementMap.values();
	}

	@Override
	public Measurement getMeasurement(String measurementName) {
		return measurementMap.get(measurementName);
	}

	@Override
	public DBMetadata getDbMetadata() {
		return metadata;
	}

	@Override
	public Set<String> keySet() {
		return measurementMap.keySet();
	}

	@Override
	public boolean containsMeasurement(String measurement) {
		return measurementMap.containsKey(measurement);
	}

	@Override
	public void updateRetention(int retentionHours) {
		synchronized (metadata) {
			metadata.setRetentionHours(retentionHours);
		}
	}

	@Override
	public void init(int retentionHours, Map<String, String> conf) throws IOException {
		metadata.setRetentionHours(retentionHours);
		metadata.setBufIncrementSize(DiskMalloc.getBufIncrement(conf));
	}

	@Override
	public Measurement getOrCreateMeasurement(String measurementName) throws IOException {
		Measurement measurement = measurementMap.get(measurementName);
		if (measurement == null) {
			synchronized (measurementMap) {
				if ((measurement = measurementMap.get(measurementName)) == null) {
					measurement = new MemoryMeasurement();
					measurement.configure(engine.getConf(), engine, engine.getDefaultTimebucketSize(), dbName,
							measurementName, "", "", metadata, engine.getBgTaskPool());
					measurementMap.put(measurementName, measurement);
					logger.info("Created new measurement:" + measurementName);
					// metricsMeasurementCounter.inc();
				}
			}
		}
		return measurement;
	}

	@Override
	public synchronized void dropDatabase() throws IOException, Exception {
		Iterator<Entry<String, Measurement>> iterator = measurementMap.entrySet().iterator();
		while (iterator.hasNext()) {
			iterator.remove();
		}
	}

	@Override
	public synchronized void dropMeasurement(String measurementName) {
		measurementMap.remove(measurementName);
	}

	@Override
	public void load() throws IOException {
	}

	@Override
	public int size() {
		return measurementMap.size();
	}

}
