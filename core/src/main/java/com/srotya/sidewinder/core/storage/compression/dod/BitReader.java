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
package com.srotya.sidewinder.core.storage.compression.dod;

import java.nio.ByteBuffer;

/**
 * @author ambud
 */
public class BitReader {

	private ByteBuffer buffer;
	private byte b = 0;
	private int bitsLeft = 0;

	public BitReader(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	public boolean readBit() {
		if (bitsLeft == 0) {
			b = buffer.get();
			bitsLeft = Byte.SIZE;
		}
		return ((b >> --bitsLeft) & 1) == 1;
	}

	public long readBits(int bits) {
		long value = 0;
		while (bits > 0) {
			boolean bit = readBit();
			if (bit) {
				value |= (1L << bits - 1);
			}
			bits--;
		}
		return value & Long.MAX_VALUE;
	}
	
	/**if (reader.readBit()) {
		if (!reader.readBit()) {
			deltaOfDelta = (int) reader.readBits(16);
		} else {
			deltaOfDelta = (int) reader.readBits(32);
		}
	}*/
	/**
	 * if (!reader.readBit()) { if (!reader.readBit()) { deltaOfDelta =
	 * (int) reader.readBits(7); } else { if (!reader.readBit()) {
	 * deltaOfDelta = (int) reader.readBits(9); } else { if
	 * (!reader.readBit()) { deltaOfDelta = (int) reader.readBits(12); }
	 * else { deltaOfDelta = (int) reader.readBits(32); } } } }else {
	 * deltaOfDelta = 0; } System.out.println("Reader DOD:" + deltaOfDelta);
	 **/

}
