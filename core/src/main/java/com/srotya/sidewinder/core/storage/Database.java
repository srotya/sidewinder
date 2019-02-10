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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface Database {
	
	public static final String BUCKET_SIZE = "bucket.size";

	public Map<String, Measurement> getMeasurementMap();

	public Collection<Measurement> getMeasurements();

	public Measurement getMeasurement(String measurementName);

	public DBMetadata getDbMetadata();

	public Set<String> keySet();

	public boolean containsMeasurement(String measurement);

	public void updateRetention(int retentionHours);

	public void init(int retentionHours, Map<String, String> conf) throws IOException;

	public Measurement getOrCreateMeasurement(String measurementName) throws IOException;

	public void dropDatabase() throws Exception;

	public void dropMeasurement(String measurementName) throws IOException;

	public void load() throws IOException;

	public int size();
	
}
