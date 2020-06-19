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
package com.srotya.sidewinder.core.storage.compression.gorilla;

import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.Reader;

public class GorillaTimestampReader implements Reader {

	private int count;
	private int counter;
	private Predicate valuePredicate;
	private GorillaTimestampDecompressor decompressor;
	private Buffer buf;
	private int checkSumLocation;

	public GorillaTimestampReader(Buffer buf, int startOffset, int checkSumLocation) {
		this.buf = buf;
		this.checkSumLocation = checkSumLocation;
		buf.position(startOffset);
		this.count = buf.getInt();
		buf.getInt();
		this.decompressor = new GorillaTimestampDecompressor(new ByteBufferBitInput(buf));
	}

	@Override
	public long read() throws FilteredValueException, RejectException {
		if (counter < count) {
			long value = decompressor.read();
			if (valuePredicate != null && !valuePredicate.test(value)) {
				throw FILTERED_VALUE_EXCEPTION;
			}
			counter++;
			return value;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	@Override
	public int getCounter() {
		return counter;
	}

	@Override
	public int getCount() {
		return count;
	}

	@Override
	public void setPredicate(Predicate predicate) {
		valuePredicate = predicate;
	}

	@Override
	public byte[] getDataHash() throws NoSuchAlgorithmException {
		Buffer duplicate = buf.duplicate(true);
		duplicate.rewind();
		duplicate.position(checkSumLocation);
		byte[] ary = new byte[GorillaTimestampWriter.MD5_PADDING];
		duplicate.get(ary);
		return ary;
	}

}
