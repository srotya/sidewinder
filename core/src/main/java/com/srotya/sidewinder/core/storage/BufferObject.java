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

public class BufferObject {

	private LinkedByteString bufferId;
	private Buffer buf;
	
	public BufferObject(LinkedByteString bufferId, Buffer buf) {
		this.bufferId = bufferId;
		this.buf = buf;
	}
	/**
	 * @return the bufferId
	 */
	public LinkedByteString getBufferId() {
		return bufferId;
	}
	/**
	 * @param bufferId the bufferId to set
	 */
	public void setBufferId(LinkedByteString bufferId) {
		this.bufferId = bufferId;
	}
	/**
	 * @return the buf
	 */
	public Buffer getBuf() {
		return buf;
	}
	/**
	 * @param buf the buf to set
	 */
	public void setBuf(Buffer buf) {
		this.buf = buf;
	}

}
