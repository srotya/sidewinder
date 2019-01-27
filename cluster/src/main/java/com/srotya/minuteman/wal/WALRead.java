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
package com.srotya.minuteman.wal;

import java.util.List;

/**
 * @author ambud
 */
public class WALRead {

	private long nextOffset;
	private long commitOffset;
	private List<byte[]> data;
	
	public WALRead() {
	}

	/**
	 * @return the nextOffset
	 */
	public long getNextOffset() {
		return nextOffset;
	}

	/**
	 * @param nextOffset
	 *            the nextOffset to set
	 */
	public WALRead setNextOffset(long nextOffset) {
		this.nextOffset = nextOffset;
		return this;
	}

	/**
	 * @return the data
	 */
	public List<byte[]> getData() {
		return data;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public WALRead setData(List<byte[]> data) {
		this.data = data;
		return this;
	}

	/**
	 * @return the commitOffset
	 */
	public long getCommitOffset() {
		return commitOffset;
	}

	/**
	 * @param commitOffset the commitOffset to set
	 */
	public void setCommitOffset(long commitOffset) {
		this.commitOffset = commitOffset;
	}


}