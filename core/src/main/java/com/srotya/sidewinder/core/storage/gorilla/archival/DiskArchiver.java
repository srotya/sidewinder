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
package com.srotya.sidewinder.core.storage.gorilla.archival;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.srotya.sidewinder.core.storage.gorilla.ArchiveException;
import com.srotya.sidewinder.core.storage.gorilla.Archiver;
import com.srotya.sidewinder.core.storage.gorilla.ByteBufferBitOutput;
import com.srotya.sidewinder.core.storage.gorilla.Reader;
import com.srotya.sidewinder.core.storage.gorilla.TimeSeriesBucket;

/**
 * Archives the timeseries buckets to a disk backed location.
 * 
 * @author ambud
 */
public class DiskArchiver implements Archiver {

	private static final String MAX_FILE_SIZE = "max.file.size";
	public static final String ARCHIVAL_DISK_DIRECTORY = "archival.disk.directory";
	private File archivalDirectory;
	private File outputFile;
	private DataOutputStream bos;
	private long maxFileSize;

	@Override
	public void init(Map<String, String> conf) throws IOException {
		maxFileSize = Long.parseLong(conf.getOrDefault(MAX_FILE_SIZE, String.valueOf(1024 * 1024 * 10)));
		archivalDirectory = new File(conf.getOrDefault(ARCHIVAL_DISK_DIRECTORY, "/tmp/sidewinder"));
		archivalDirectory.mkdirs();
		outputFile = new File(archivalDirectory, "archive." + System.currentTimeMillis() + ".bin");
		bos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
	}

	@Override
	public void archive(TimeSeriesArchivalObject object) throws ArchiveException {
		try {
			if (outputFile.length() > maxFileSize) {
				bos.close();
				outputFile = new File(archivalDirectory, "archive." + System.currentTimeMillis() + ".bin");
				bos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
			}
			serializeToStream(bos, new TimeSeriesArchivalObject(object.getDb(), object.getMeasurement(),
					object.getKey(), object.getBucket()));
		} catch (IOException e) {
			throw new ArchiveException(e);
		}
	}

	@Override
	public List<TimeSeriesArchivalObject> unarchive() throws ArchiveException {
		List<TimeSeriesArchivalObject> list = new ArrayList<>();
		try {
			for (File file : archivalDirectory.listFiles()) {
				DataInputStream bis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
				while (bis.available() > 0) {
					TimeSeriesArchivalObject object = deserializeFromStream(bis);
					list.add(object);
				}
				bis.close();
			}
		} catch (IOException e) {
			throw new ArchiveException(e);
		}
		return list;
	}

	public static void serializeToStream(DataOutputStream bos, TimeSeriesArchivalObject blob) throws IOException {
		bos.writeUTF(blob.getDb());
		bos.writeUTF(blob.getMeasurement());
		bos.writeUTF(blob.getKey());
		bos.writeLong(blob.getBucket().getHeaderTimestamp());
		bos.writeInt(blob.getBucket().getCount());
		Reader reader = blob.getBucket().getReader(null, null);
		byte[] buf = reader.toByteArray();
		bos.writeInt(buf.length);
		bos.write(buf);
		bos.flush();
	}

	public static TimeSeriesArchivalObject deserializeFromStream(DataInputStream bis) throws IOException {
		TimeSeriesArchivalObject bucketWraper = new TimeSeriesArchivalObject();
		bucketWraper.setDb(bis.readUTF());
		bucketWraper.setMeasurement(bis.readUTF());
		bucketWraper.setKey(bis.readUTF());
		long headerTs = bis.readLong();
		int count = bis.readInt();
		int bufSize = bis.readInt();
		ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);
		byte[] tempAry = new byte[bufSize];
		bis.read(tempAry);
		buf.put(tempAry);
		TimeSeriesBucket bucket = new TimeSeriesBucket(headerTs, count, new ByteBufferBitOutput(buf));
		bucketWraper.setBucket(bucket);
		return bucketWraper;
	}

}
