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

import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Reader;
import com.srotya.sidewinder.core.utils.ByteUtils;

/**
 * @author ambud
 */
public class ByzantineValueReader implements Reader {

	private int counter;
	private int count;
	private Predicate predicate;
	private Buffer buf;
	private long prevValue;

	public ByzantineValueReader(Buffer buf, int startOffset) {
		buf.position(startOffset);
		this.buf = buf;
		this.count = buf.getInt();
	}

	public int getCounter() {
		return counter;
	}

	@Override
	public long read() throws FilteredValueException, RejectException {
		if (counter < count) {
			uncompressAndReadValue();
			counter++;
			if (predicate != null && !predicate.test(prevValue)) {
				throw FILTERED_VALUE_EXCEPTION;
			}
			return prevValue;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	public void uncompressAndReadValue() {
		byte flag = buf.get();
		if (flag == (byte) 0) {
			prevValue = prevValue ^ 0L;
		} else if (flag == (byte) 1) {
			prevValue = prevValue ^ (long) buf.get();
		} else if (flag == (byte) 2) {
			prevValue = prevValue ^ (long) buf.getShort();
		} else if (flag == (byte) 3) {
			prevValue = prevValue ^ (long) buf.getInt();
		} else {
			prevValue = prevValue ^ (long) buf.getLong();
		}
	}

	@Override
	public int getCount() {
		return count;
	}

	@Override
	public void setPredicate(Predicate valuePredicate) {
		this.predicate = valuePredicate;
	}

	/**
	 * @return the prevValue
	 */
	public long getPrevValue() {
		return prevValue;
	}

	@Override
	public byte[] getDataHash() throws NoSuchAlgorithmException {
		Buffer duplicate = buf.duplicate(true);
		duplicate.rewind();
		Buffer copy = duplicate.newInstance(duplicate.capacity());
		copy.put(duplicate);
		byte[] array = copy.array();
		return ByteUtils.md5(array);
	}
}
