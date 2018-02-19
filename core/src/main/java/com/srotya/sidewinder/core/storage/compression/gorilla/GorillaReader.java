/**
 * Copyright 2018 Ambud Sharma
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;

public class GorillaReader implements Reader {

	private int count;
	private int counter;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private GorillaDecompressor decompressor;
	private ByteBuffer buf;
	private int checkSumLocation;

	public GorillaReader(ByteBuffer buf, int startOffset, int checkSumLocation) {
		this.buf = buf;
		this.checkSumLocation = checkSumLocation;
		buf.position(startOffset);
		count = buf.getInt();
		buf.getInt();
		decompressor = new GorillaDecompressor(new ByteBufferBitInput(buf));
	}

	@Override
	public DataPoint readPair() throws IOException {
		if (counter < count) {
			DataPoint pair = decompressor.readPair();
			if (timePredicate != null && !timePredicate.test(pair.getTimestamp())) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.test(pair.getLongValue())) {
				return null;
			}
			counter++;
			return pair;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	@Override
	public long[] read() throws IOException {
		if (counter < count) {
			long[] pair = decompressor.read();
			if (timePredicate != null && !timePredicate.test(pair[0])) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.test(pair[1])) {
				return null;
			}
			return pair;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	@Override
	public int getCounter() {
		return counter;
	}

	@Override
	public int getPairCount() {
		return count;
	}

	@Override
	public void setTimePredicate(Predicate timePredicate) {
		this.timePredicate = timePredicate;
	}

	@Override
	public void setValuePredicate(Predicate valuePredicate) {
		this.valuePredicate = valuePredicate;
	}

	@Override
	public byte[] getDataHash() throws NoSuchAlgorithmException {
		ByteBuffer duplicate = buf.duplicate();
		duplicate.rewind();
		duplicate.position(checkSumLocation);
		byte[] ary = new byte[GorillaWriter.MD5_PADDING];
		duplicate.get(ary);
		return ary;
	}

}
