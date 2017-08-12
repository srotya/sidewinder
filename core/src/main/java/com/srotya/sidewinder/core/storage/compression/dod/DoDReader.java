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
import java.util.List;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.Reader;

/**
 * @author ambud
 */
public class DoDReader implements Reader {

	private static final RejectException EOS_EXCEPTION = new RejectException("End of stream reached");
	private volatile ByteBuffer buf;
	private long prevTs;
	private long delta;
	private int counter;
	private int count;
	private long lastTs;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private boolean fp;
	private String fieldName;
	private List<String> tags;

	public DoDReader(ByteBuffer buf, int count, long lastTs) {
		this.buf = buf;
		this.lastTs = lastTs;
		buf.rewind();
		this.count = count;
		prevTs = buf.getLong();
	}

	public int getCounter() {
		return counter;
	}

	public ByteBuffer getBuf() {
		return buf;
	}

	public long getLastTs() {
		return lastTs;
	}

	@Override
	public DataPoint readPair() throws IOException {
		DataPoint dp = null;
		if (counter < count) {
			long val = 0;
			int temp = buf.getInt();
			val = buf.getLong();
			delta = delta + temp;
			prevTs += delta;
			counter++;

			if (timePredicate != null && !timePredicate.apply(prevTs)) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.apply(val)) {
				return null;
			}
			dp = new DataPoint();
			dp.setTimestamp(prevTs);
			dp.setValue(val);
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
}
