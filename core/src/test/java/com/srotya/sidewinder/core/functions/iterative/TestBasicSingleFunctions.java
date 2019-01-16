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
package com.srotya.sidewinder.core.functions.iterative;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.DataPointIterator;
import com.srotya.sidewinder.core.storage.Field;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.MockMeasurement;
import com.srotya.sidewinder.core.storage.NoLock;
import com.srotya.sidewinder.core.storage.TimeField;
import com.srotya.sidewinder.core.storage.ValueField;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.functions.iterative.BasicSingleFunctions.*;

public class TestBasicSingleFunctions {

	private MockMeasurement measurement;
	private DataPointIterator itr;

	@Before
	public void before() throws IOException {
		measurement = new MockMeasurement(32768, 100);
		TimeField.compressionClass = CompressionFactory.getTimeClassByName("byzantine");
		Field tField = new TimeField(measurement, new LinkedByteString().concat(new ByteString("time")), 121213,
				new HashMap<>());
		long ts = 1546755991280L;
		for (int i = 0; i < 200; i++) {
			tField.addDataPoint(measurement, ts + i * 1000);
		}

		Field vField = new ValueField(measurement, new LinkedByteString().concat(new ByteString("field1")), 121213,
				new HashMap<>());
		for (int i = 0; i < 100; i++) {
			vField.addDataPoint(measurement, i * 1L);
		}
		for (int i = 100; i > 0; i--) {
			vField.addDataPoint(measurement, i * 1L);
		}
		itr = new DataPointIterator(tField.queryReader(null, new NoLock()), vField.queryReader(null, new NoLock()));
	}

	@Test
	public void testFirst() {
		FunctionIterator f = new FirstFunction(itr, false);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(0, next.getLongValue());
	}

	@Test
	public void testLast() {
		FunctionIterator f = new LastFunction(itr, false);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(1, next.getLongValue());
	}
	
	@Test
	public void testMax() {
		FunctionIterator f = new MaxFunction(itr, false);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(100, next.getLongValue());
	}
	
	@Test
	public void testMin() {
		FunctionIterator f = new MinFunction(itr, false);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(0, next.getLongValue());
	}
	
	@Test
	public void testSum() {
		FunctionIterator f = new SumFunction(itr, false);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(10000, next.getLongValue());
	}
	
	@Test
	public void testMean() {
		FunctionIterator f = new MeanFunction(itr, true);
		assertTrue(f.hasNext());
		DataPoint next = f.next();
		assertEquals(50, next.getLongValue());
	}

}