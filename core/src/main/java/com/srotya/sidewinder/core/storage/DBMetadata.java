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

public class DBMetadata {
	
	private int retentionHours;
	private int bufIncrementSize;
	private int fileIncrementSize;
	private int timeBucketSize;
	
	public DBMetadata() {
	}

	public DBMetadata(int retentionHours, int bufIncrementSize, int fileIncrementSize, int timeBucketSize) {
		this.retentionHours = retentionHours;
		this.bufIncrementSize = bufIncrementSize;
		this.fileIncrementSize = fileIncrementSize;
		this.timeBucketSize = timeBucketSize;
	}

	/**
	 * @return the retentionHours
	 */
	public int getRetentionHours() {
		return retentionHours;
	}

	/**
	 * @param retentionHours the retentionHours to set
	 */
	public void setRetentionHours(int retentionHours) {
		this.retentionHours = retentionHours;
	}

	public int getBufIncrementSize() {
		return bufIncrementSize;
	}

	public void setBufIncrementSize(int bufIncrementSize) {
		this.bufIncrementSize = bufIncrementSize;
	}

	public int getFileIncrementSize() {
		return fileIncrementSize;
	}

	public void setFileIncrementSize(int fileIncrementSize) {
		this.fileIncrementSize = fileIncrementSize;
	}
	
	public int getTimeBucketSize() {
		return timeBucketSize;
	}
	
	public void setTimeBucketSize(int timeBucketSize) {
		this.timeBucketSize = timeBucketSize;
	}

}