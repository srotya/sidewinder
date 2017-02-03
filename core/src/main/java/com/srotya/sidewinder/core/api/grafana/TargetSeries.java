package com.srotya.sidewinder.core.api.grafana;

import java.util.List;

import com.srotya.sidewinder.core.filters.Filter;

public class TargetSeries {

	private String measurementName;
	private String fieldName;
	private List<String> tagList;
	private boolean autoCorrelate;
	private Filter<List<String>> tagFilter;

	public TargetSeries(String measurementName, String fieldName, List<String> tagList, Filter<List<String>> tagFilter, boolean autoCorrelate) {
		this.measurementName = measurementName;
		this.fieldName = fieldName;
		this.tagList = tagList;
		this.tagFilter = tagFilter;
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
	 * @param autoCorrelate the autoCorrelate to set
	 */
	public void setAutoCorrelate(boolean autoCorrelate) {
		this.autoCorrelate = autoCorrelate;
	}
}