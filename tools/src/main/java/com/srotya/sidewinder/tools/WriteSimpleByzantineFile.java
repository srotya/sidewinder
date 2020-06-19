package com.srotya.sidewinder.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import com.srotya.sidewinder.core.storage.Buffer;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.buffer.GenericBuffer;
import com.srotya.sidewinder.core.storage.compression.FilteredValueException;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineTimestampReader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineTimestampWriter;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineValueReader;
import com.srotya.sidewinder.core.storage.compression.byzantine.ByzantineValueWriter;

import io.netty.util.internal.ThreadLocalRandom;

public class WriteSimpleByzantineFile {

	private static final int BUF_SIZE = 2048;
	private static final int POINT_COUNT = 1000;
	private static final int FILE_MB = 60;
	private static final int NUM_SERIES = 30000;
	private static final int SKIP = 0;

	public static void main(String[] args) throws Exception {
		// writeValue("/Users/ambud.sharma/tmp/byzantine-val.dat", 80);
		// readValue();
		// write("/Users/ambud.sharma/tmp/byzantine-ts.dat");
		readTs("/Users/ambud.sharma/tmp/byzantine-ts.dat");
		// readRaw();
	}

	private static void readRaw(String file) throws Exception {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1024 * 1024 * FILE_MB);
		raf.close();

		for (int k = 0; k < NUM_SERIES; k++) {
			for (int i = 0; i < BUF_SIZE; i++) {
				System.out.print(map.get());
			}
			System.out.println();
		}
	}

	private static void readTs(String file)
			throws FileNotFoundException, IOException, FilteredValueException, RejectException {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1024 * 1024 * FILE_MB);
		raf.close();

		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		long ts = System.currentTimeMillis();
		int next = 0;
		for (int k = 0; k < NUM_SERIES; k++) {
			map.position(next);
			ByteBuffer d = map.slice();
			d.limit(BUF_SIZE);
			next += BUF_SIZE;
			Buffer buf = new GenericBuffer(d);
			ByzantineTimestampReader reader = new ByzantineTimestampReader(buf, SKIP);
			// System.out.println(reader.read());
			for (int i = 0; i < reader.getCount(); i++) {
				long read = reader.read();
				if (read < min) {
					min = read;
				}
				if (read > max) {
					max = read;
				}
			}
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println("min:" + min + " ts:" + ts + "  max:" + max);
	}

	private static void readValue(String file)
			throws FileNotFoundException, IOException, FilteredValueException, RejectException {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1024 * 1024 * FILE_MB);
		raf.close();

		long ts = System.currentTimeMillis();
		int next = 0;
		for (int k = 0; k < 1; k++) {
			map.position(next);
			ByteBuffer d = map.slice();
			d.limit(BUF_SIZE);
			next += BUF_SIZE;
			Buffer buf = new GenericBuffer(d);
			ByzantineValueReader reader = new ByzantineValueReader(buf, SKIP);
			// System.out.println(reader.getCount() + " " + reader.read());
			for (int i = 0; i < reader.getCount(); i++) {
				System.out.println(reader.read());
			}
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println(ts + "ms");
	}

	public static void write(String file) throws Exception {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1024 * 1024 * FILE_MB);
		raf.close();

		int count = 0;
		int next = 0;
		for (int k = 0; k < NUM_SERIES; k++) {
			map.position(next);
			ByteBuffer d = map.slice();
			d.limit(BUF_SIZE);
			next += BUF_SIZE;
			ByzantineTimestampWriter writer = new ByzantineTimestampWriter();
			Buffer buf = new GenericBuffer(d);
			writer.configure(buf, true, SKIP);
			long ts = System.currentTimeMillis() + k;
			writer.setHeaderTimestamp(ts);
			for (int i = 0; i < POINT_COUNT; i++) {
				writer.add(ts + i * 1000);
			}
			count += POINT_COUNT;
		}
		System.out.println(count);
		map.force();
	}

	public static void writeValue(String file, int fileMB) throws Exception {
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel ch = raf.getChannel();
		MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1024 * 1024 * fileMB);
		raf.close();

		int next = 0;
		for (int k = 0; k < NUM_SERIES; k++) {
			map.position(next);
			ByteBuffer d = map.slice();
			d.limit(BUF_SIZE);
			next += BUF_SIZE;
			ByzantineValueWriter writer = new ByzantineValueWriter();
			Buffer buf = new GenericBuffer(d);
			writer.configure(buf, true, SKIP);
			for (int i = 0; i < POINT_COUNT; i++) {
				writer.add(ThreadLocalRandom.current().nextInt(100));
			}
		}
		map.force();
	}

}
