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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.compression.gorilla.ByteBufferBitOutput;
import com.srotya.sidewinder.core.storage.compression.gorilla.GorillaReader;
import com.srotya.sidewinder.core.storage.compression.gorilla.GorillaWriter;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * These are generic tests to test that input matches the output after
 * compression + decompression cycle, using both the timestamp and value
 * compression.
 *
 * @author Michael Burman
 * 
 *         Modified by @author Ambud to switch to JUnit4
 */
public class TestGorillaCompression {

	@Test
	public void simpleEncodeAndDecodeTest() throws Exception {
		long now = System.currentTimeMillis();
		GorillaWriter c = new GorillaWriter();
		DataPoint[] pairs = { MiscUtils.buildDataPoint(now + 10, Double.doubleToRawLongBits(1.0)),
				MiscUtils.buildDataPoint(now + 20, Double.doubleToRawLongBits(-2.0)),
				MiscUtils.buildDataPoint(now + 28, Double.doubleToRawLongBits(-2.5)),
				MiscUtils.buildDataPoint(now + 84, Double.doubleToRawLongBits(65537)),
				MiscUtils.buildDataPoint(now + 400, Double.doubleToRawLongBits(2147483650.0)),
				MiscUtils.buildDataPoint(now + 2300, Double.doubleToRawLongBits(-16384)),
				MiscUtils.buildDataPoint(now + 16384, Double.doubleToRawLongBits(2.8)),
				MiscUtils.buildDataPoint(now + 16500, Double.doubleToRawLongBits(-38.0)) };

		Arrays.stream(pairs).forEach(p -> c.addValue(p.getTimestamp(), p.getValue()));
		c.flush();

		GorillaReader d = c.getReader();

		// Replace with stream once decompressor supports it
		for (int i = 0; i < pairs.length; i++) {
			DataPoint pair = d.readPair();
			assertEquals("Timestamp did not match", pairs[i].getTimestamp(), pair.getTimestamp());
			assertEquals("Value did not match", pairs[i].getValue(), pair.getValue(), 0);
		}

		try {
			assertNull(d.readPair());
			fail("End of stream, shouldn't be able to read any more");
		} catch (RejectException e) {
		}
	}

	/**
	 * Tests encoding of similar floats, see
	 * https://github.com/dgryski/go-tsz/issues/4 for more information.
	 */
	@Test
	public void testEncodeSimilarFloats() throws Exception {
		long now = LocalDateTime.of(2015, Month.MARCH, 02, 00, 00).toInstant(ZoneOffset.UTC).toEpochMilli();

		ByteBufferBitOutput output = new ByteBufferBitOutput();
		GorillaWriter c = new GorillaWriter();

		ByteBuffer bb = ByteBuffer.allocate(5 * 2 * Long.BYTES);

		bb.putLong(now + 1);
		bb.putDouble(6.00065e+06);
		bb.putLong(now + 2);
		bb.putDouble(6.000656e+06);
		bb.putLong(now + 3);
		bb.putDouble(6.000657e+06);
		bb.putLong(now + 4);
		bb.putDouble(6.000659e+06);
		bb.putLong(now + 5);
		bb.putDouble(6.000661e+06);

		bb.flip();

		for (int j = 0; j < 5; j++) {
			c.addValue(bb.getLong(), bb.getDouble());
		}

		c.flush();

		bb.flip();

		ByteBuffer byteBuffer = output.getBuffer();
		byteBuffer.flip();

		GorillaReader d = c.getReader();

		// Replace with stream once decompressor supports it
		for (int i = 0; i < 5; i++) {
			DataPoint pair = d.readPair();
			assertEquals("Timestamp did not match", bb.getLong(), pair.getTimestamp());
			assertEquals("Value did not match", bb.getDouble(), pair.getValue(), 0);
		}
		try {
			assertNull(d.readPair());
			fail("End of stream, shouldn't be able to read any more");
		} catch (RejectException e) {
		}
	}

	/**
	 * Tests writing enough large amount of datapoints that causes the included
	 * ByteBufferBitOutput to do internal byte array expansion.
	 */
	@Test
	public void testEncodeLargeAmountOfData() throws Exception {
		// This test should trigger ByteBuffer reallocation
		int amountOfPoints = 100000;
		long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();
		ByteBufferBitOutput output = new ByteBufferBitOutput();

		long now = blockStart + 60;
		ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2 * Long.BYTES);

		for (int i = 0; i < amountOfPoints; i++) {
			bb.putLong(now + i * 60);
			bb.putDouble(i * Math.random());
		}

		GorillaWriter c = new GorillaWriter();

		bb.flip();

		for (int j = 0; j < amountOfPoints; j++) {
			c.addValue(bb.getLong(), bb.getDouble());
		}

		c.flush();

		bb.flip();

		ByteBuffer byteBuffer = output.getBuffer();
		byteBuffer.flip();

		GorillaReader d = c.getReader();

		for (int i = 0; i < amountOfPoints; i++) {
			long tStamp = bb.getLong();
			double val = bb.getDouble();
			DataPoint pair = d.readPair();
			assertEquals("Expected timestamp did not match at point " + i, tStamp, pair.getTimestamp());
			assertEquals(val, pair.getValue(), 0);
		}
		try {
			assertNull(d.readPair());
			fail("End of stream, shouldn't be able to read any more");
		} catch (RejectException e) {
		}
	}

	/**
	 * Although not intended usage, an empty block should not cause errors
	 */
	@Test
	public void testEmptyBlock() throws Exception {
		GorillaWriter c = new GorillaWriter();
		c.flush();

		GorillaReader d = c.getReader();
		try {
			assertNull(d.readPair());
			fail("End of stream, shouldn't be able to read any more");
		} catch (RejectException e) {
		}
	}

	/**
	 * Long values should be compressable and decompressable in the stream
	 */
	@Test
	public void testLongEncoding() throws Exception {
		// This test should trigger ByteBuffer reallocation
		int amountOfPoints = 10000;
		long blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS).toInstant(ZoneOffset.UTC).toEpochMilli();
		long now = blockStart + 60;
		ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2 * Long.BYTES);

		for (int i = 0; i < amountOfPoints; i++) {
			bb.putLong(now + i * 60);
			bb.putLong(ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE));
		}

		GorillaWriter c = new GorillaWriter();

		bb.flip();

		for (int j = 0; j < amountOfPoints; j++) {
			c.addValue(bb.getLong(), bb.getLong());
		}

		c.flush();

		bb.flip();

		GorillaReader d = c.getReader();

		for (int i = 0; i < amountOfPoints; i++) {
			long tStamp = bb.getLong();
			long val = bb.getLong();
			DataPoint pair = d.readPair();
			assertEquals("Expected timestamp did not match at point " + i, tStamp, pair.getTimestamp());
			assertEquals(val, pair.getLongValue());
		}
		try {
			assertNull(d.readPair());
			fail("End of stream, shouldn't be able to read any more");
		} catch (RejectException e) {
		}
	}

	@Test
	public void testWritePerformance() throws IOException {
		GorillaWriter writer = new GorillaWriter();
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		for (int i = 0; i < 1_000_000; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Gorilla Write time:" + ts + " data size:" + writer.getOut().getBuffer().position());
		ts = System.currentTimeMillis();
		Reader reader = writer.getReader();
		for (int i = 0; i < 1_000_000; i++) {
			reader.readPair();
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Gorilla Read time:" + ts);
	}
}