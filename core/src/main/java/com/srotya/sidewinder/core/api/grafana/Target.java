package com.srotya.sidewinder.core.api.grafana;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Target implements Serializable {

	private static final long serialVersionUID = 1L;

	private String target;
	private List<Number[]> datapoints;

	public Target(String target) {
		this.target = target;
		datapoints = new ArrayList<>();
	}

	/**
	 * @return the target
	 */
	public String getTarget() {
		return target;
	}

	/**
	 * @param target
	 *            the target to set
	 */
	public void setTarget(String target) {
		this.target = target;
	}

	/**
	 * @return the datapoints
	 */
	public List<Number[]> getDatapoints() {
		return datapoints;
	}

	/**
	 * @param datapoints
	 *            the datapoints to set
	 */
	public void setDatapoints(List<Number[]> datapoints) {
		this.datapoints = datapoints;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Target [target=" + target + ", datapoints=" + datapoints + "]";
	}

}