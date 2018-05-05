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
package com.srotya.sidewinder.core.sql.calcite;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.time.Instant;

import org.junit.Test;

import com.srotya.sidewinder.core.sql.calcite.SqlFunctions.DateDiff;
import com.srotya.sidewinder.core.sql.calcite.SqlFunctions.Now;
import com.srotya.sidewinder.core.sql.calcite.SqlFunctions.ToMilli;
import com.srotya.sidewinder.core.sql.calcite.SqlFunctions.ToTimestamp;

/**
 * Unit tests for: {@link ToTimestamp} {@link ToMilliseconds}
 * {@link NowFunction} {@link DateDiffFunction}
 * 
 * @author ambud
 */
public class TestSqlFunctions {

	@Test
	public void testToTimestamp() {
		long ts = System.currentTimeMillis();
		ToTimestamp function = new ToTimestamp();
		Timestamp value = function.eval(ts);
		assertEquals(ts, value.getTime());
	}

	@Test
	public void testToMilliseconds() {
		ToMilli function = new ToMilli();
		int ts = (int) (System.currentTimeMillis() / 1000);
		Long apply = function.eval(ts, "SECONDS");
		assertEquals(((long) ts) * 1000, apply.longValue());
	}

	@Test
	public void testNowFunction() {
		Now function = new Now();
		long currentTimeMillis = System.currentTimeMillis();
		Timestamp ts = function.eval();
		assertEquals(currentTimeMillis, ts.getTime(), 5);
	}

	@Test
	public void testDateDiffFunction() {
		DateDiff function = new DateDiff();
		long ts1 = System.currentTimeMillis();
		long ts2 = ts1 - 100;
		Long apply = function.eval(Timestamp.from(Instant.ofEpochMilli(ts1)),
				Timestamp.from(Instant.ofEpochMilli(ts2)));
		assertEquals(100, apply.longValue());
		apply = function.eval(Timestamp.from(Instant.ofEpochMilli(ts2)), Timestamp.from(Instant.ofEpochMilli(ts1)));
		assertEquals(100, apply.longValue());
	}
}
