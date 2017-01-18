/**
 * Copyright 2016 Michael Burman
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
package com.srotya.sidewinder.core.storage.gorilla;

/**
 * This interface is used for reading a compressed time series.
 *
 * @author Michael Burman
 */
public interface BitInput {

	/**
	 * Reads the next bit and returns true if bit is set and false if not.
	 *
	 * @return true == 1, false == 0
	 */
	boolean readBit();

	/**
	 * Returns a long that was stored in the next X bits in the stream.
	 *
	 * @param bits
	 *            Amount of least significant bits to read from the stream.
	 * @return reads the next long in the series using bits meaningful bits
	 */
	long getLong(int bits);
}
