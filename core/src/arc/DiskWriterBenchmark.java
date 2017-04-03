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
package com.srotya.sidewinder.core.storage.compression.byzantine;

import java.io.File;
import java.util.HashMap;

import com.srotya.sidewinder.core.storage.Reader;
import com.srotya.sidewinder.core.storage.StorageEngine;

public class DiskWriterBenchmark {

	public static void main(String[] args) throws Throwable {
		ByzantineWriter writer = new ByzantineWriter();
		HashMap<String, String> conf = new HashMap<>();
		new File("/tmp/benchmark").mkdirs();
		conf.put(StorageEngine.PERSISTENCE_DISK, "false");
		conf.put("data.dir", "/tmp/benchmark");
		writer.setSeriesId("test_1M_writes" + 0);
		writer.configure(conf);
		long ts = System.currentTimeMillis();
		writer.setHeaderTimestamp(ts);
		int limit = 100_000_000;
		for (int i = 0; i < limit; i++) {
			writer.addValue(ts + i * 1000, i);
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Byzantine Write time:" + ts + " data size:" + writer.getBuf().position());
		ts = System.currentTimeMillis();
		Reader reader = writer.getReader();
		for (int i = 0; i < limit; i++) {
			reader.readPair();
		}
		ts = (System.currentTimeMillis() - ts);
		System.out.println("Byzantine Read time:" + ts);
	}

}
