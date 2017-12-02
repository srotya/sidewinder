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
package com.srotya.sidewinder.core.api.grafana;

import java.util.List;

import com.srotya.sidewinder.core.filters.Filter;
import com.srotya.sidewinder.core.functions.Function;

public class TargetSeries {

	private String measurementName;
	private String fieldName;
	private List<String> tagList;
	private boolean autoCorrelate;
	private Filter<List<String>> tagFilter;
	private Function function;

	public TargetSeries(String measurementName, String fieldName, List<String> tagList, Filter<List<String>> tagFilter,
			Function function, boolean autoCorrelate) {
		this.measurementName = measurementName;
		this.fieldName = fieldName;
		this.tagList = tagList;
		this.tagFilter = tagFilter;
		this.function = function;
		this.autoCorrelate = autoCorrelate;
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
	 * @return the fieldName
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * @param fieldName
	 *            the fieldName to set
	 */
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	/**
	 * @return the tagList
	 */
	public List<String> getTagList() {
		return tagList;
	}

	/**
	 * @return the tagFilter
	 */
	public Filter<List<String>> getTagFilter() {
		return tagFilter;
	}

	/**
	 * @return the autoCorrelate
	 */
	public boolean isAutoCorrelate() {
		return autoCorrelate;
	}

	/**
	 * @param autoCorrelate
	 *            the autoCorrelate to set
	 */
	public void setAutoCorrelate(boolean autoCorrelate) {
		this.autoCorrelate = autoCorrelate;
	}

	/**
	 * @return the aggregationFunction
	 */
	public Function getAggregationFunction() {
		return function;
	}
}