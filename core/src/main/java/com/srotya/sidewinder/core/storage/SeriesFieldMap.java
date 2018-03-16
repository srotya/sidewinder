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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SeriesFieldMap {
	
	private Object seriesId;
	private Map<String, TimeSeries> seriesMap;
	
	public SeriesFieldMap(Object seriesId) {
		this.seriesId = seriesId;
		seriesMap = new ConcurrentHashMap<>();
	}
	
	public Set<String> getFields() {
		return seriesMap.keySet();
	}

	public void addSeries(String valueFieldName, TimeSeries series) {
		seriesMap.put(valueFieldName, series);
	}

	public TimeSeries get(String valueFieldName) {
		return seriesMap.get(valueFieldName);
	}

	/**
	 * Must close before modifying this via iterator
	 * @return
	 */
	public Collection<? extends TimeSeries> values() {
		return seriesMap.values();
	}

	/**
	 * @return the seriesId
	 */
	public Object getSeriesId() {
		return seriesId;
	}

	/**
	 * @param seriesId the seriesId to set
	 */
	public void setSeriesId(String seriesId) {
		this.seriesId = seriesId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SeriesFieldMap [seriesMap=" + seriesMap + "]";
	}

}
