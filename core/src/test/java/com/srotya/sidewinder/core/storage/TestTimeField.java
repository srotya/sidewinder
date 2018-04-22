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
import com.srotya.sidewinder.core.storage.compression.TimeWriter;
import com.srotya.sidewinder.core.storage.compression.byzantine.NoLock;

public class TestTimeField {

	private MockMeasurement measurement;

	@Before
	public void before() {
		measurement = new MockMeasurement(1024, 100);
		TimeField.compressionClass = CompressionFactory.getTimeClassByName("byzantine");
	}

	@Test
	public void testReadWrite() throws IOException {
		Field field = new TimeField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			field.addDataPoint(measurement, ts + i * 1000);
		}
		FieldReaderIterator itr = field.queryReader(null, new NoLock());
		for (int i = 0; i < 100; i++) {
			assertEquals(ts + i * 1000, itr.next());
		}
		try {
			itr.next();
			fail("Must fail with rejectedexception");
		} catch (Exception e) {
		}
		for (int i = 0; i < 100; i++) {
			field.addDataPoint(measurement, ts + i * 1000);
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
			assertEquals(ts + (i % 100) * 1000, itr.next());
		}
	}

	@Test
	public void testReadWriteResize() throws IOException {
		TimeField field = new TimeField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		for (int i = 0; i < 40000; i++) {
			field.addDataPoint(measurement, i);
		}
		assertEquals(2, field.getRawWriterList().size());
		FieldReaderIterator itr = field.queryReader(null, new NoLock());
		for (int i = 0; i < 40000; i++) {
			assertEquals(i, itr.next());
		}
	}

	@Test
	public void testCompactionByzantine() throws IOException {
		TimeField.compactionClass = CompressionFactory.getTimeClassByName("byzantine");
		TimeField.compactionRatio = 2.2;
		TimeField field = new TimeField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		long ts = 1497720652566L;
		for (int i = 0; i < 80000; i++) {
			field.addDataPoint(measurement, ts + i * 1000);
		}
		assertEquals(3, field.getRawWriterList().size());
		List<TimeWriter> compact = field.compact(measurement, new NoLock(), t -> {
		});
		assertEquals(2, compact.size());
		assertEquals(2, field.getRawWriterList().size());
	}

	@Test
	public void testCompactionGorilla() throws IOException {
		TimeField.compactionClass = CompressionFactory.getTimeClassByName("gorilla");
		TimeField.compactionRatio = 0.6;
		TimeField field = new TimeField(measurement, new ByteString("field1"), 121213, new HashMap<>());
		long ts = 1497720652566L;
		for (int i = 0; i < 80000; i++) {
			field.addDataPoint(measurement, ts + i * 1000);
		}
		assertEquals(3, field.getRawWriterList().size());
		List<TimeWriter> compact = field.compact(measurement, new NoLock(), t -> {
		});
		assertEquals(2, compact.size());
		assertEquals(2, field.getRawWriterList().size());
	}

}
