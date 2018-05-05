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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.utils.ByteUtils;

/**
 * @author ambud
 */
public class ByzantineTimestampReader implements Reader {

	private long prevTs;
	private long delta;
	private int counter;
	private int count;
	private Predicate timePredicate;
	private ByteBuffer buf;

	public ByzantineTimestampReader(ByteBuffer buf, int startOffset) {
		buf.position(startOffset);
		this.buf = buf;
		this.count = buf.getInt();
		prevTs = buf.getLong();
	}

	public int getCounter() {
		return counter;
	}

	@Override
	public long read() throws FilteredValueException, RejectException {
		if (counter < count) {
			uncompressAndReadTimestamp();
			counter++;
			if (timePredicate != null && !timePredicate.test(prevTs)) {
				throw FILTERED_VALUE_EXCEPTION;
			}
			return prevTs;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	public void uncompressAndReadTimestamp() {
		int deltaOfDelta;
		byte b = buf.get();
		if (b == 0) {
			deltaOfDelta = 0;
		} else if (b == (byte) 1) {
			deltaOfDelta = buf.get();
		} else if (b == (byte) 2) {
			deltaOfDelta = buf.getShort();
		} else {
			deltaOfDelta = buf.getInt();
		}
		delta = delta + deltaOfDelta;
		prevTs += delta;
	}

	@Override
	public int getCount() {
		return count;
	}

	@Override
	public void setPredicate(Predicate valuePredicate) {
		this.timePredicate = valuePredicate;
	}

	/**
	 * @return the prevTs
	 */
	public long getPrevTs() {
		return prevTs;
	}

	/**
	 * @return the delta
	 */
	public long getDelta() {
		return delta;
	}

	@Override
	public byte[] getDataHash() throws NoSuchAlgorithmException {
		ByteBuffer duplicate = buf.duplicate();
		duplicate.rewind();
		ByteBuffer copy = ByteBuffer.allocate(duplicate.capacity());
		copy.put(duplicate);
		byte[] array = copy.array();
		return ByteUtils.md5(array);
	}
}
