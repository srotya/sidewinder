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
package com.srotya.sidewinder.core.storage.compression.zip;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.srotya.sidewinder.core.predicates.Predicate;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.compression.Reader;

public abstract class ZipReader implements Reader {

	private InputStream zip;
	private DataInputStream dis;
	private int count;
	private int counter;
	private List<String> tags;
	private Predicate timePredicate;
	private Predicate valuePredicate;
	private String fieldName;
	private boolean fp;

	public ZipReader(ByteBuffer readBuf, int startOffset, int blockSize) throws IOException {
		readBuf.position(startOffset);
		this.count = readBuf.getInt();
		zip = getInputStream(new ZipWriter.ByteBufferInputStream(readBuf), blockSize);
		dis = new DataInputStream(zip);
	}

	public abstract InputStream getInputStream(InputStream stream, int blockSize) throws IOException;

	@Override
	public DataPoint readPair() throws IOException {
		if (counter < count) {
			long ts, value;
			try {
				ts = dis.readLong();
				value = dis.readLong();
			} catch (IOException e) {
				throw EOS_EXCEPTION;
			}

			if (timePredicate != null && !timePredicate.apply(ts)) {
				return null;
			}
			if (valuePredicate != null && !valuePredicate.apply(value)) {
				return null;
			}
			DataPoint dp = new DataPoint();
			dp.setTimestamp(ts);
			dp.setLongValue(value);
			dp.setFp(fp);
			if (tags != null) {
				dp.setTags(tags);
			}
			if (fieldName != null) {
				dp.setValueFieldName(fieldName);
			}
			// System.out.println("Reading:" + dp);
			return dp;
		} else {
			zip.close();
			throw EOS_EXCEPTION;
		}
	}

	@Override
	public long[] read() throws IOException {
		long[] dp = new long[2];
		try {
			dp[0] = dis.readLong();
			dp[1] = dis.readLong();
		} catch (IOException e) {
			return null;
		}

		if (timePredicate != null && !timePredicate.apply(dp[0])) {
			return null;
		}
		if (valuePredicate != null && !valuePredicate.apply(dp[1])) {
			return null;
		}
		return dp;
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

	@Override
	public void setTimePredicate(Predicate timePredicate) {
		this.timePredicate = timePredicate;
	}

	@Override
	public void setValuePredicate(Predicate valuePredicate) {
		this.valuePredicate = valuePredicate;
	}

}
