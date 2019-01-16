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

import java.io.IOException;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.DataPointIterator;
import com.srotya.sidewinder.core.storage.Field;
import com.srotya.sidewinder.core.storage.LinkedByteString;
import com.srotya.sidewinder.core.storage.MockMeasurement;
import com.srotya.sidewinder.core.storage.NoLock;
import com.srotya.sidewinder.core.storage.TimeField;
import com.srotya.sidewinder.core.storage.ValueField;
import com.srotya.sidewinder.core.storage.compression.CompressionFactory;

public class TestTumblingWindowFunction {

	private MockMeasurement measurement;

	@Before
	public void before() throws IOException {
		measurement = new MockMeasurement(32768, 100);
		TimeField.compressionClass = CompressionFactory.getTimeClassByName("byzantine");
	}

	@Test
	public void testBasicSumDownSampling() throws Exception {
		Field tField = new TimeField(measurement, new LinkedByteString().concat(new ByteString("time")), 121213,
				new HashMap<>());
		long ts = 1546755991280L;
		for (int i = 0; i < 100; i++) {
			tField.addDataPoint(measurement, ts + i * 1000);
		}

		Field vField = new ValueField(measurement, new LinkedByteString().concat(new ByteString("field1")), 121213,
				new HashMap<>());
		for (int i = 0; i < 100; i++) {
			vField.addDataPoint(measurement, 1L);
		}
		DataPointIterator itr = new DataPointIterator(tField.queryReader(null, new NoLock()),
				vField.queryReader(null, new NoLock()));

		TumblingWindowFunction tumblingWindowFunction = new TumblingWindowFunction(itr, false);
		tumblingWindowFunction.init(new Object[] { 10, "ssum" });
		int c = 0;
		while (tumblingWindowFunction.hasNext()) {
			tumblingWindowFunction.next();
			c++;
		}
		assertEquals(11, c);
	}
	
	@Test
	public void testBasicMinDownSampling() throws Exception {
		Field tField = new TimeField(measurement, new LinkedByteString().concat(new ByteString("time")), 121213,
				new HashMap<>());
		long ts = 1546755991280L;
		for (int i = 0; i < 100; i++) {
			tField.addDataPoint(measurement, ts + i * 1000);
		}

		Field vField = new ValueField(measurement, new LinkedByteString().concat(new ByteString("field1")), 121213,
				new HashMap<>());
		for (int i = 0; i < 100; i++) {
			vField.addDataPoint(measurement, i);
		}
		DataPointIterator itr = new DataPointIterator(tField.queryReader(null, new NoLock()),
				vField.queryReader(null, new NoLock()));

		TumblingWindowFunction tumblingWindowFunction = new TumblingWindowFunction(itr, false);
		tumblingWindowFunction.init(new Object[] { 10, "smin" });
		int c = 0;
		while (tumblingWindowFunction.hasNext()) {
			tumblingWindowFunction.next();
			c++;
		}
		assertEquals(11, c);
	}
}