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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;

/**
 * @author ambud
 */
public class ByzantineReader implements Reader {

	private long prevTs;
	private long delta;
	private int counter;
	private int count;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private ByteBuffer buf;
	private long prevValue;

	public ByzantineReader(ByteBuffer buf, int startOffset) {
		buf.position(startOffset);
		this.buf = buf;
		this.count = buf.getInt();
		prevTs = buf.getLong();
	}

	public int getCounter() {
		return counter;
	}

	@Override
	public DataPoint readPair() throws IOException {
		DataPoint dp = null;
		if (counter < count) {
			uncompressAndReadTimestamp();
			uncompressAndReadValue();
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
			uncompressAndReadTimestamp();
			uncompressAndReadValue();
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

	/**
	 * @return the prevValue
	 */
	public long getPrevValue() {
		return prevValue;
	}
}
