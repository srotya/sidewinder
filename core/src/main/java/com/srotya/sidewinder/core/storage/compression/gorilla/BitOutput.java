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
package com.srotya.sidewinder.core.storage.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * This interface is used to write a compressed timeseries.
 *
 * @author Michael Burman
 */
public interface BitOutput {

	/**
	 * Stores a single bit, set if true, false otherwise.
	 * 
	 * @param bit
	 *            false == 0, true == 1
	 */
	void writeBit(boolean bit);

	/**
	 * Write the given long value using the defined amount of least significant
	 * bits.
	 *
	 * @param value
	 *            The long value to be written
	 * @param bits
	 *            How many bits are stored to the stream
	 */
	void writeBits(long value, int bits);

	/**
	 * Flushes the current byte to the underlying stream
	 */
	void flush();
	
	ByteBuffer getBuffer();
}
