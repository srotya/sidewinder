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
package com.srotya.minuteman.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

import com.srotya.minuteman.utils.FileUtils;

/**
 * @author ambud
 */
public class TestMappedWAL {

	private ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

	@Test
	public void testWALConfiguration() throws IOException {
		String walDir = "target/wal1";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		wal.configure(conf, es);
		File[] listFiles = new File(walDir).listFiles();
		Arrays.sort(listFiles);
		assertEquals(2, listFiles.length);
		assertEquals(MappedWAL.getSegmentFileName(walDir, 0), listFiles[1].getPath().replace("\\", "/"));
		assertEquals(1024 * 1024 * 2, listFiles[1].length());
	}

	@Test
	public void testWALWrites() throws IOException {
		String walDir = "target/wal2";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(512 * 1024));
		wal.configure(conf, es);
		int LIMIT = 10000;
		for (int i = 0; i < LIMIT; i++) {
			String str = ("test" + String.format("%04d", i));
			wal.write(str.getBytes(), false);
		}
		wal.flush();
		int expectedBytes = 12 * LIMIT + 4;
		RandomAccessFile raf = new RandomAccessFile(MappedWAL.getSegmentFileName(walDir, 0), "r");
		MappedByteBuffer map = raf.getChannel().map(MapMode.READ_ONLY, 0, expectedBytes);
		raf.close();
		map.getInt();
		for (int i = 0; i < LIMIT; i++) {
			try {
				byte[] dst = new byte[8];
				map.getInt();
				map.get(dst);
				assertEquals("test" + String.format("%04d", i), new String(dst));
			} catch (Exception e) {
				System.out.println("Marker:" + i);
				throw e;
			}
		}
	}

	@Test
	public void testWALReads() throws IOException {
		String walDir = "target/wal3";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 1));
		wal.configure(conf, es);
		int LIMIT = 30000;
		for (int i = 0; i < LIMIT; i++) {
			String str = ("test" + String.format("%05d", i));
			wal.write(str.getBytes(), false);
		}
		WALRead read = wal.read("local".hashCode(), 4, 10_000_000, false);
		List<byte[]> data = read.getData();
		for (int i = 0; i < LIMIT; i++) {
			ByteBuffer buf = ByteBuffer.wrap(data.get(i));
			byte[] dst = new byte[9];
			try {
				buf.get(dst);
			} catch (Exception e) {
				fail("Shouldn't throw exception:" + e.getMessage());
				throw e;
			}
			assertEquals("test" + String.format("%05d", i), new String(dst));
		}
	}

	@Test
	public void testFollowers() throws IOException, InterruptedException {
		String walDir = "target/wal4";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(1024 * 1024 * 2));
		conf.put(WAL.WAL_ISRCHECK_FREQUENCY, "1");
		conf.put(WAL.WAL_ISR_THRESHOLD, "1024");
		conf.put(WAL.WAL_SEGMENT_FLUSH_COUNT, "100");
		assertEquals(-1, wal.getOffset());
		wal.configure(conf, es);
		int total = 4;
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
			total += str.length() + Integer.BYTES;
		}
		wal.flush();
		assertEquals(total, wal.getOffset());
		// let ISR check thread mark this follower as not ISR
		WALRead read = wal.read("f1".hashCode(), 4L, 1024 * 1024, false);
		assertEquals(1000, read.getData().size());
		assertEquals(false, wal.isIsr("f1".hashCode()));
		assertEquals(4, wal.getFollowerOffset("f1".hashCode()));
		assertEquals(0, read.getCommitOffset());
		assertEquals(11004, read.getNextOffset());
		total = 0;
		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
			total += str.length();
		}
		wal.flush();
		Thread.sleep(1000);
		assertEquals(false, wal.isIsr("f1".hashCode()));
		read = wal.read("f1".hashCode(), read.getNextOffset(), 1024 * 1024, false);
		assertEquals(1000, read.getData().size());
		assertEquals(11000 + 4, wal.getFollowerOffset("f1".hashCode()));
		total = 11000 * 2;
		total += 4;
		read = wal.read("f1".hashCode(), read.getNextOffset(), 1024 * 1024, false);
		// let ISR check thread run and mark this follower as ISR
		Thread.sleep(1000);
		assertTrue(read.getData() == null);
		assertEquals(total, wal.getFollowerOffset("f1".hashCode()));
		read = wal.read("f1".hashCode(), read.getNextOffset(), 1024 * 1024, false);
		assertTrue(read.getData() == null);
		assertEquals(total, wal.getFollowerOffset("f1".hashCode()));
		assertEquals(total, read.getCommitOffset());

		for (int i = 0; i < 1000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		read = wal.read("f1".hashCode(), read.getNextOffset(), 1024 * 1024, false);
		assertEquals(total, read.getCommitOffset());
	}

	@Test
	public void testWALSegmentRotations() throws IOException {
		String walDir = "target/wal5";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		wal.configure(conf, es);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		assertEquals(5, wal.getSegmentCounter());
		WALRead read = wal.read("local".hashCode(), 4, 10000, false);
		List<byte[]> data = read.getData();
		// buf.getInt();
		assertEquals(454, data.size());
		for (int i = 0; i < 454; i++) {
			try {
				ByteBuffer buf = ByteBuffer.wrap(data.get(i));
				byte[] dst = new byte[7];
				buf.get(dst);
				assertEquals("test" + String.format("%03d", i), new String(dst));
			} catch (Exception e) {
				e.printStackTrace();
				fail("Shouldn't throw exception:" + e.getMessage() + "\t" + i);
				throw e;
			}
		}
		assertEquals(-1, wal.getFollowerOffset("f1".hashCode()));
		assertEquals(5004, read.getNextOffset());
		read = wal.read("local".hashCode(), read.getNextOffset(), 10000, false);
		assertEquals(10004, read.getNextOffset());
		data = read.getData();
		assertEquals(454, read.getData().size());
		for (int i = 0; i < 454; i++) {
			ByteBuffer buf = ByteBuffer.wrap(data.get(i));
			try {
				byte[] dst = new byte[7];
				buf.get(dst);
				assertEquals("test" + String.format("%03d", i + 454), new String(dst));
			} catch (Exception e) {
				fail("Shouldn't throw exception:" + e.getMessage());
				throw e;
			}
		}
		read = wal.read("local".hashCode(), read.getNextOffset(), 10000, false);
		assertEquals(15004, read.getNextOffset());
		read = wal.read("local".hashCode(), read.getNextOffset(), 10000, false);
		assertEquals(20004, read.getNextOffset());
		// assertEquals(5, read.getSegmentId());
		read = wal.read("local".hashCode(), 4, 10000, false);
		assertEquals(5004, read.getNextOffset());
		// assertEquals(1, read.getSegmentId());
	}

	@Test
	public void testWALSegmentRecovery() throws IOException {
		ScheduledExecutorService es1 = Executors.newScheduledThreadPool(1);
		String walDir = "target/wal6";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		wal.configure(conf, es1);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%03d", i));
			wal.write(str.getBytes(), false);
		}
		wal.read("f2".hashCode(), 4, 100, false);
		wal.close();
		es1.shutdownNow();
		es1 = Executors.newScheduledThreadPool(1);
		wal = new MappedWAL();
		wal.configure(conf, es1);
		assertEquals("Files:" + Arrays.toString(new File(walDir).list()), 5, wal.getSegmentCounter());
		wal.read("f2".hashCode(), 4, 100, false);
		assertEquals(1, wal.getFollowers().size());
	}

	@Test
	public void testWALOperationsUncommitted() throws Exception {
		String walDir = "target/wal6";
		FileUtils.delete(new File(walDir));
		WAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		wal.configure(conf, es);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%04d", i));
			wal.write(str.getBytes(), false);
		}
		assertEquals(5, wal.getSegmentCounter());

		int totalRead = 0;
		long inOffset = 4;
		for (int k = 0; k < 30; k++) {
			WALRead read = wal.read("local".hashCode(), inOffset, 1000, false);
			if (read.getData() != null) {
				List<byte[]> data = read.getData();
				for (int i = 0; i < data.size(); i++) {
					try {
						ByteBuffer buf = ByteBuffer.wrap(data.get(i));
						byte[] dst = new byte[8];
						buf.get(dst);
						assertEquals("test" + String.format("%04d", totalRead++), new String(dst));
					} catch (Exception e) {
						e.printStackTrace();
						fail("Shouldn't throw exception:" + e.getMessage() + "\t" + i);
						throw e;
					}
				}
			}
			inOffset = read.getNextOffset();
		}
		assertEquals(2000, totalRead);
	}

	@Test
	public void testWALOperationsCommitted() throws Exception {
		String walDir = "target/wal7";
		FileUtils.delete(new File(walDir));
		MappedWAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(5000));
		conf.put(WAL.WAL_ISRCHECK_DELAY, String.valueOf(10));
		conf.put(WAL.WAL_ISRCHECK_FREQUENCY, String.valueOf(10));
		conf.put(WAL.WAL_ISR_THRESHOLD, String.valueOf(1024));
		wal.configure(conf, es);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%04d", i));
			wal.write(str.getBytes(), false);
		}
		assertEquals(5, wal.getSegmentCounter());

		int totalRead = 0;
		long inOffset = 4;
		wal.read(("local" + 2).hashCode(), inOffset, 10, false);
		for (int k = 0; k < 25; k++) {
			WALRead read = wal.read("local".hashCode(), inOffset, 1000, false);
			if (read.getData() != null) {
				List<byte[]> data = read.getData();
				for (int i = 0; i < data.size(); i++) {
					try {
						ByteBuffer buf = ByteBuffer.wrap(data.get(i));
						byte[] dst = new byte[8];
						buf.get(dst);
						assertEquals("test" + String.format("%04d", totalRead++), new String(dst));
					} catch (Exception e) {
						e.printStackTrace();
						fail("Shouldn't throw exception:" + e.getMessage() + "\t" + i);
						throw e;
					}
				}
			}
			inOffset = read.getNextOffset();
			wal.updateISR();
		}

		wal.updateISR();
		inOffset = 4;
		totalRead = 0;

		for (int k = 0; k < 24; k++) {
			WALRead read = wal.read(("local" + 2).hashCode(), inOffset, 1000, true);
			if (read.getData() != null) {
				System.out.println("Read data not empty:" + read.getData().size() + "\t" + k + "\t NextReturn:"
						+ read.getNextOffset() + "\t Current:" + inOffset + "\t Commit:" + read.getCommitOffset());
				List<byte[]> data = read.getData();
				for (int i = 0; i < data.size(); i++) {
					try {
						ByteBuffer buf = ByteBuffer.wrap(data.get(i));
						byte[] dst = new byte[8];
						buf.get(dst);
						assertEquals(bufferToString(data), "test" + String.format("%04d", totalRead++),
								new String(dst));
					} catch (Exception e) {
						e.printStackTrace();
						fail("Shouldn't throw exception:" + e.getMessage() + "\t" + i);
						throw e;
					}
				}
			}
			inOffset = read.getNextOffset();
		}
		assertEquals(2000, totalRead);
	}

	@Test
	public void testWALCommittedReads() throws IOException {
		String walDir = "target/wal8";
		FileUtils.delete(new File(walDir));
		MappedWAL wal = new MappedWAL();
		Map<String, String> conf = new HashMap<>();
		conf.put(MappedWAL.WAL_DIR, walDir);
		conf.put(MappedWAL.WAL_SEGMENT_SIZE, String.valueOf(20000));
		conf.put(WAL.WAL_ISRCHECK_DELAY, String.valueOf(10));
		conf.put(WAL.WAL_ISRCHECK_FREQUENCY, String.valueOf(10));
		conf.put(WAL.WAL_ISR_THRESHOLD, String.valueOf(4096));
		wal.configure(conf, es);
		for (int i = 0; i < 2000; i++) {
			String str = ("test" + String.format("%04d", i));
			wal.write(str.getBytes(), false);
		}
		assertEquals(2, wal.getSegmentCounter());
		long inOffset = 4;
		WALRead read = wal.read("local2".hashCode(), inOffset, 24000, false);
		assertEquals(1666, read.getData().size());
		read = wal.read("local2".hashCode(), read.getNextOffset(), 10, false);
		wal.updateISR();
		WALRead read2 = wal.read("local3".hashCode(), inOffset, 1000, true);
		assertTrue(read2.getData() != null);
	}

	private String bufferToString(List<byte[]> data) {
		StringBuilder builder = new StringBuilder();
		for (byte[] b : data) {
			ByteBuffer buf = ByteBuffer.wrap(b);
			byte[] dst = new byte[8];
			buf.get(dst);
			builder.append(new String(dst) + "\n");
		}
		return builder.toString();
	}
}
