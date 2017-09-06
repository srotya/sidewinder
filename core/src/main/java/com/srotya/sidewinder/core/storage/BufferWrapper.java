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

import java.nio.ByteBuffer;

/**
 * 
 * @author ambud
 */
public class BufferWrapper {
	
	private int startOffset;
	private int limit;
	private ByteBuffer buf;
	
	public BufferWrapper() {
	}
	
	public BufferWrapper(int startOffset, int limit, ByteBuffer buf) {
		this.startOffset = startOffset;
		this.limit = limit;
		this.buf = buf;
	}
	/**
	 * @return the limit
	 */
	public int getLimit() {
		return limit;
	}
	/**
	 * @param limit the limit to set
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}
	/**
	 * @return the startOffset
	 */
	public int getStartOffset() {
		return startOffset;
	}
	/**
	 * @param startOffset the startOffset to set
	 */
	public void setStartOffset(int startOffset) {
		this.startOffset = startOffset;
	}
	/**
	 * @return the buf
	 */
	public ByteBuffer getBuf() {
		return buf;
	}
	/**
	 * @param buf the buf to set
	 */
	public void setBuf(ByteBuffer buf) {
		this.buf = buf;
	}

}
