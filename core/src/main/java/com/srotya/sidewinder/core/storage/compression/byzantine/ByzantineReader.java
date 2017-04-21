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
import java.util.List;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;

/**
 * @author ambud
 */
public class ByzantineReader implements Reader {

	private static final RejectException EOS_EXCEPTION = new RejectException("End of stream reached");
	private long prevTs;
	private long delta;
	private int counter;
	private int count;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private boolean fp;
	private String fieldName;
	private List<String> tags;
	private ByteBuffer buf;
	private long prevValue;

	public ByzantineReader(ByteBuffer buf, int count) {
		this.buf = buf;
		this.count = count;
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
			if (timePredicate != null && !timePredicate.apply(prevTs)) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.apply(prevValue)) {
				return null;
			}
			dp = new DataPoint(prevTs, prevValue);
			dp.setFp(fp);
			if (tags != null) {
				dp.setTags(tags);
			}

			if (fieldName != null) {
				dp.setValueFieldName(fieldName);
			}

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
		} else if (b == (byte) 10) {
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

	@Override
	public void setIsFP(boolean fp) {
		this.fp = fp;
	}

	@Override
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public void setTags(List<String> tags) {
		this.tags = tags;
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
