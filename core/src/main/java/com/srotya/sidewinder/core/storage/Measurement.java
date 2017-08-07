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
package com.srotya.sidewinder.core.storage;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import com.srotya.sidewinder.core.storage.mem.TimeSeries;

/**
 * @author ambud
 *
 */
public interface Measurement {

	public void configure(Map<String, String> conf, String compressionClass, String baseIndexDirectory,
			String dataDirectory, String dbName, String measurementName, DBMetadata metadata,
			ScheduledExecutorService bgTaskPool) throws IOException;

	public Collection<TimeSeries> getTimeSeries();

	public Map<String, TimeSeries> getTimeSeriesMap();

	public TagIndex getTagIndex();

	public TimeSeries get(String entry);

	public void loadTimeseriesFromMeasurementMetadata() throws IOException;

	public TimeSeries getOrCreateTimeSeries(String rowKey, int timeBucketSize, boolean fp, Map<String, String> conf) throws IOException;

	public void delete() throws IOException;

	public void garbageCollector() throws IOException;

}
