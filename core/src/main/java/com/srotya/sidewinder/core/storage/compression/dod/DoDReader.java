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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.Reader;

/**
 * @author ambud
 */
public class DoDReader implements Reader {

	private static final RejectException EOS_EXCEPTION = new RejectException("End of stream reached");
	private long prevTs;
	private long delta;
	private int counter;
	private int count;
	private long lastTs;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private BitReader reader;
	private long prevValue;
	private long prevXor;

	public DoDReader(ByteBuffer buf, int count, long lastTs) {
		// this.buf = buf;
		this.lastTs = lastTs;
		buf.rewind();
		this.count = count;
		reader = new BitReader(buf);
	}

	public int getCounter() {
		return counter;
	}

	public ByteBuffer getBuf() {
		// return buf;
		return null;
	}

	public long getLastTs() {
		return lastTs;
	}

	/**
	 * (a) Calculate the delta of delta: D = (tn - tn1) - (tn1 - tn2) (b) If D is
	 * zero, then store a single `0' bit (c) If D is between [-63, 64], store `10'
	 * followed by the value (7 bits) (d) If D is between [-255, 256], store `110'
	 * followed by the value (9 bits) (e) if D is between [-2047, 2048], store
	 * `1110' followed by the value (12 bits) (f) Otherwise store `1111' followed by
	 * D using 32 bits
	 */
	@Override
	public DataPoint readPair() throws IOException {
		DataPoint dp = null;
		if (counter < count) {
			uncompressTimestamp();
			uncompressValue();
			counter++;

			if (timePredicate != null && !timePredicate.test(prevTs)) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.test(prevValue)) {
				return null;
			}
			dp = new DataPoint();
			dp.setTimestamp(prevTs);
			dp.setLongValue(prevValue);
			return dp;
		} else {
			throw EOS_EXCEPTION;
		}
	}

	@Override
	public long[] read() throws IOException {
		if (counter < count) {
			long[] dp = new long[2];
			uncompressTimestamp();
			uncompressValue();
			counter++;

			if (timePredicate != null && !timePredicate.test(prevTs)) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.test(prevValue)) {
				return null;
			}
			dp[0] = prevTs;
			dp[1] = prevValue;
			return dp;
		} else {
			throw EOS_EXCEPTION;
		}
	}
	
	private void uncompressValue() {
		if (counter == 0) {
			prevValue = reader.readBits(64);
		} else {
			long xor = 0;
			if (reader.readBit()) {
				if (!reader.readBit()) {
					int prevNumberOfLeadingZeros = Long.numberOfLeadingZeros(prevXor);
					int prevNumberOfTrailingZeros = Long.numberOfTrailingZeros(prevXor);
					int length = Long.SIZE - prevNumberOfTrailingZeros - prevNumberOfLeadingZeros;
					long value = reader.readBits(length);
					xor = value << prevNumberOfTrailingZeros;
				} else {
					int leadingZeros = (int) (reader.readBits(6)); // & ((1 << 6) - 1)
					int length = (int) (reader.readBits(6));
					long v = reader.readBits(length);
					xor = (v << (Long.SIZE - leadingZeros - length));
				}
			}
			prevXor = xor;
			prevValue = prevValue ^ prevXor;
		}
	}

	private void uncompressTimestamp() {
		if (counter == 0) {
			prevTs = reader.readBits(64);
		} else {
			int temp = 0;
			if (reader.readBit()) {
				if (reader.readBit()) {
					if (reader.readBit()) {
						if (reader.readBit()) {
							temp = (int) reader.readBits(32);
						} else {
							temp = (int) reader.readBits(12);
						}
					} else {
						temp = (int) reader.readBits(9);
					}
				} else {
					temp = (int) reader.readBits(7);
				}
			}
			delta = delta + temp;
			prevTs += delta;
		}
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

}
