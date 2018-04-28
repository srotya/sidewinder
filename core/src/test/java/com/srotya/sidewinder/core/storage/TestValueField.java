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
package com.srotya.sidewinder.core.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.srotya.sidewinder.core.storage.compression.CompressionFactory;
import com.srotya.sidewinder.core.storage.compression.Writer;
import com.srotya.sidewinder.core.storage.compression.byzantine.NoLock;

public class TestValueField {

	private MockMeasurement measurement;

	@Before
	public void before() {
		measurement = new MockMeasurement(32768, 100);
		ValueField.compressionClass = CompressionFactory.getValueClassByName("byzantine");
	}

	@Test
	public void testReadWrite() throws IOException {
		Field field = new ValueField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		for (int i = 0; i < 100; i++) {
			field.addDataPoint(measurement, i);
		}
		FieldReaderIterator itr = field.queryReader(null, new NoLock());
		for (int i = 0; i < 100; i++) {
			assertEquals(i, itr.next());
		}
		try {
			itr.next();
			fail("Must fail with rejectedexception");
		} catch (Exception e) {
		}
		for (int i = 0; i < 100; i++) {
			field.addDataPoint(measurement, i);
		}
		// existing iterator must fail
		try {
			itr.next();
			fail("Must fail with rejectedexception");
		} catch (Exception e) {
		}
		// new iterator must succeed
		itr = field.queryReader(null, new NoLock());
		for (int i = 0; i < 200; i++) {
			assertEquals(i % 100, itr.next());
		}
	}

	@Test
	public void testExpandBufferError() throws IOException {
		measurement = new MockMeasurement(149, 100);
		Field field = new ValueField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		try {
			for (double i = 100; i > 0; i--) {
				long v = Double.doubleToLongBits(3.1417 * i % 7);
				field.addDataPoint(measurement, v);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("No exception must be thrown");
		}
	}

	@Test
	public void testReadWriteResize() throws IOException {
		ValueField field = new ValueField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		for (int i = 0; i < 20000; i++) {
			field.addDataPoint(measurement, i);
		}
		assertEquals(2, field.getRawWriterList().size());
		FieldReaderIterator itr = field.queryReader(null, new NoLock());
		for (int i = 0; i < 20000; i++) {
			assertEquals(i, itr.next());
		}
	}

	@Test
	public void testCompactionByzantine() throws IOException {
		ValueField.compactionClass = CompressionFactory.getValueClassByName("byzantine");
		ValueField.compactionRatio = 2.2;
		ValueField field = new ValueField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		long ts = 1497720652566L;
		for (int i = 0; i < 30000; i++) {
			field.addDataPoint(measurement, ts + i * 1000);
		}
		assertEquals(3, field.getRawWriterList().size());
		List<Writer> compact = field.compact(measurement, new NoLock(), t -> {
		});
		assertEquals(2, compact.size());
		assertEquals(2, field.getRawWriterList().size());
	}

	@Test
	public void testCompactionGorilla() throws IOException {
		ValueField.compactionClass = CompressionFactory.getValueClassByName("gorilla");
		ValueField.compactionRatio = 1.2;
		ValueField field = new ValueField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		for (int i = 0; i < 10000; i++) {
			field.addDataPoint(measurement, Double.doubleToLongBits(i * 1.1));
		}
		assertEquals(3, field.getRawWriterList().size());
		List<Writer> compact = field.compact(measurement, new NoLock(), t -> {
		});
		assertEquals(2, compact.size());
		assertEquals(2, field.getRawWriterList().size());
	}

}
