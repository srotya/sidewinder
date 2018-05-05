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

import java.util.List;

import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class SeriesOutput implements Comparable<SeriesOutput> {

	private String measurementName;
	private String valueFieldName;
	private boolean isFp;
	private List<Tag> tags;
	private List<DataPoint> dataPoints;

	public SeriesOutput() {
	}

	public SeriesOutput(String measurementName, String valueFieldName, List<Tag> tags) {
		this.measurementName = measurementName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
	}

	public SeriesOutput(String measurementName, String valueFieldName, List<Tag> tags,
			List<DataPoint> dataPoints) {
		this.measurementName = measurementName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
		this.dataPoints = dataPoints;
	}

	public SeriesOutput(List<DataPoint> value) {
		dataPoints = value;
	}

	@Override
	public int hashCode() {
		return (measurementName + "-" + valueFieldName + MiscUtils.tagsToString(tags)).hashCode();
	}

	/**
	 * @return the measurementName
	 */
	public String getMeasurementName() {
		return measurementName;
	}

	/**
	 * @param measurementName
	 *            the measurementName to set
	 */
	public void setMeasurementName(String measurementName) {
		this.measurementName = measurementName;
	}

	/**
	 * @return the valueFieldName
	 */
	public String getValueFieldName() {
		return valueFieldName;
	}

	/**
	 * @param valueFieldName
	 *            the valueFieldName to set
	 */
	public void setValueFieldName(String valueFieldName) {
		this.valueFieldName = valueFieldName;
	}

	/**
	 * @return the tags
	 */
	public List<Tag> getTags() {
		return tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(List<Tag> tags) {
		this.tags = tags;
	}

	/**
	 * @return the dataPoints
	 */
	public List<DataPoint> getDataPoints() {
		return dataPoints;
	}

	/**
	 * @param dataPoints
	 *            the dataPoints to set
	 */
	public void setDataPoints(List<DataPoint> dataPoints) {
		this.dataPoints = dataPoints;
	}

	/**
	 * @return the isFp
	 */
	public boolean isFp() {
		return isFp;
	}

	/**
	 * @param isFp
	 *            the isFp to set
	 */
	public void setFp(boolean isFp) {
		this.isFp = isFp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return measurementName + "-" + valueFieldName + MiscUtils.tagsToString(tags);
	}

	@Override
	public int compareTo(SeriesOutput o) {
		return toString().compareTo(o.toString());
	}

}
