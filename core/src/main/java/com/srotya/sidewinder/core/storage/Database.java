package com.srotya.sidewinder.core.storage;

import java.util.Collection;
import java.util.Map;

public interface Database {

	public Map<String, Measurement> getMeasurementMap();

	public Collection<Measurement> getMeasurements();

	public Measurement getMeasurement(String measurementName);

	public DBMetadata getDbMetadata();
	
}
