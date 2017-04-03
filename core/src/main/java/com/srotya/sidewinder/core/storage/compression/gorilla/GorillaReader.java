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

import java.io.IOException;
import java.util.List;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * Decompresses a compressed stream done created by the Compressor. Returns
 * pairs of timestamp and flaoting point value.
 *
 * @author Michael Burman
 * 
 *         Modified by @author Ambud to remove EOF markers and count based
 *         multi-threaded readers
 */
public class GorillaReader implements Reader {

	private static final RejectException EOS_EXCEPTION = new RejectException("End of stream reached");
	private int storedLeadingZeros = Integer.MAX_VALUE;
	private int storedTrailingZeros = 0;
	private long storedVal = 0;
	private long storedTimestamp = 0;
	private long storedDelta = 0;
	private long blockTimestamp = 0;
	private volatile boolean endOfStream = false;
	private BitInput in;
	private final int pairCount;
	private volatile int counter;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private boolean isFp;
	private String appendFieldValueName;
	private List<String> appendTags;

	public GorillaReader(BitInput input, int pairCount) {
		in = input;
		this.pairCount = pairCount;
		readHeader();
	}

	public GorillaReader(BitInput input, int pairCount, Predicate timePredicate, Predicate valuePredicate, boolean isFp,
			String appendFieldValueName, List<String> appendTags) {
		in = input;
		this.pairCount = pairCount;
		this.timePredicate = timePredicate;
		this.valuePredicate = valuePredicate;
		this.isFp = isFp;
		this.appendFieldValueName = appendFieldValueName;
		this.appendTags = appendTags;
		readHeader();
	}

	private void readHeader() {
		blockTimestamp = in.getLong(64);
	}

	/**
	 * Returns the next pair in the time series, if available.
	 *
	 * @return Pair if there's next value, null if series is done.
	 */
	public DataPoint readPair() throws IOException {
		next();
		if (endOfStream) {
			throw EOS_EXCEPTION;
		}
		if (timePredicate != null && !timePredicate.apply(storedTimestamp)) {
			System.out.println("Predicate mismatch:" + storedTimestamp + "\t" + storedVal);
			return null;
		}
		if (valuePredicate != null && !valuePredicate.apply(storedVal)) {
			return null;
		}
		if (appendFieldValueName == null || appendTags == null) {
			DataPoint dp = new DataPoint();
			dp.setTimestamp(storedTimestamp);
			dp.setLongValue(storedVal);
			return dp;
		} else {
			DataPoint dataPoint = new DataPoint();
			dataPoint.setTimestamp(storedTimestamp);
			dataPoint.setLongValue(storedVal);
			dataPoint.setFp(isFp);
			dataPoint.setTags(appendTags);
			dataPoint.setValueFieldName(appendFieldValueName);
			return dataPoint;
		}
	}

	private void next() {
		if (counter < pairCount) {
			if (storedTimestamp == 0) {
				// First item to read
				storedDelta = in.getLong(GorillaWriter.FIRST_DELTA_BITS);
				storedVal = in.getLong(64);
				storedTimestamp = blockTimestamp + storedDelta;
			} else {
				nextTimestamp();
				nextValue();
			}
			counter++;
		} else {
			endOfStream = true;
		}
	}

	private int bitsToRead() {
		int val = 0x00;

		for (int i = 0; i < 4; i++) {
			val <<= 1;
			boolean bit = in.readBit();
			if (bit) {
				val |= 0x01;
			} else {
				break;
			}
		}

		int toRead = 0;

		switch (val) {
		case 0x00:
			break;
		case 0x02:
			toRead = 7; // '10'
			break;
		case 0x06:
			toRead = 9; // '110'
			break;
		case 0x0e:
			toRead = 12;
			break;
		case 0x0F:
			toRead = 32;
			break;
		}

		return toRead;
	}

	private void nextTimestamp() {
		// Next, read timestamp
		long deltaDelta = 0;
		int toRead = bitsToRead();
		if (toRead > 0) {
			deltaDelta = in.getLong(toRead);

			if (toRead != 32) {
				// Turn "unsigned" long value back to signed one
				if (deltaDelta > (1 << (toRead - 1))) {
					deltaDelta -= (1 << toRead);
				}
			}

			deltaDelta = (int) deltaDelta;
		}

		storedDelta = storedDelta + deltaDelta;
		storedTimestamp = storedDelta + storedTimestamp;
	}

	private void nextValue() {
		// Read value
		if (in.readBit()) {
			// else -> same value as before
			if (in.readBit()) {
				// New leading and trailing zeros
				storedLeadingZeros = (int) in.getLong(5);

				byte significantBits = (byte) in.getLong(6);
				if (significantBits == 0) {
					significantBits = 64;
				}
				storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
			}
			long value = in.getLong(64 - storedLeadingZeros - storedTrailingZeros);
			value <<= storedTrailingZeros;
			value = storedVal ^ value;
			storedVal = value;
		}
	}

	public byte[] toByteArray() {
		return in.toByteArray();
	}

	/**
	 * @return the pairCount
	 */
	public int getPairCount() {
		return pairCount;
	}

	/**
	 * @return the counter
	 */
	public int getCounter() {
		return counter;
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
	public void setIsFP(boolean fp) {
		this.isFp = fp;
	}

	@Override
	public void setFieldName(String fieldName) {
		this.appendFieldValueName = fieldName;
	}

	@Override
	public void setTags(List<String> tags) {
		this.appendTags = tags;
	}

}