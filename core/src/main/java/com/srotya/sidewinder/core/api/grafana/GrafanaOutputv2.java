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
package com.srotya.sidewinder.core.api.grafana;

import java.io.Serializable;

import com.srotya.sidewinder.core.storage.DataPointIterator;

public class GrafanaOutputv2 implements Serializable, Comparable<GrafanaOutputv2> {

	private static final long serialVersionUID = 1L;

	private String target;
	private transient DataPointIterator pointsIterator;
	private transient boolean isFp;

	public GrafanaOutputv2(String target, boolean isFp) {
		this.target = target;
		this.isFp = isFp;
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

	public DataPointIterator getPointsIterator() {
		return pointsIterator;
	}

	public void setPointsIterator(DataPointIterator pointsIterator) {
		this.pointsIterator = pointsIterator;
	}
	
	public boolean isFp() {
		return isFp;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Target [target=" + target + ", datapoints=" + pointsIterator + "]";
	}

	@Override
	public int compareTo(GrafanaOutputv2 o) {
		return this.target.compareTo(o.target);
	}

}