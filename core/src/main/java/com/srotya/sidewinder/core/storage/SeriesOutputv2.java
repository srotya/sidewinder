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
public class SeriesOutputv2 implements Comparable<SeriesOutputv2> {

	private String measurementName;
	private String valueFieldName;
	private boolean isFp;
	private List<Tag> tags;
	private DataPointIterator iterator;

	public SeriesOutputv2() {
	}

	public SeriesOutputv2(String measurementName, String valueFieldName, List<Tag> tags) {
		this.measurementName = measurementName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
	}

	public SeriesOutputv2(String measurementName, String valueFieldName, List<Tag> tags,
			DataPointIterator dataPoints) {
		this.measurementName = measurementName;
		this.valueFieldName = valueFieldName;
		this.tags = tags;
		this.iterator = dataPoints;
	}

	public SeriesOutputv2(DataPointIterator value) {
		this.iterator = value;
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
	
	public DataPointIterator getIterator() {
		return iterator;
	}
	
	public void setIterator(DataPointIterator iterator) {
		this.iterator = iterator;
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
	public int compareTo(SeriesOutputv2 o) {
		return toString().compareTo(o.toString());
	}

}
