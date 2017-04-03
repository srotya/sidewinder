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
package com.srotya.sidewinder.core.storage.compression.dod;

import java.io.EOFException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.srotya.sidewinder.core.storage.DataPoint;

/**
 */
public class DoD {

	private static final int COUNT = 10_000_000;

	public static void main(String[] args) throws InterruptedException {
		long ts = System.currentTimeMillis();

		final DodWriter gorilla = new DodWriter();
		ExecutorService pool = Executors.newFixedThreadPool(11);
		pool.submit(() -> {
			for (int i = 1; i < COUNT; i++) {
				int k = i;
				DataPoint dp = new DataPoint();
				dp.setTimestamp(ts + k);
				dp.setLongValue(i);
				gorilla.write(dp);
				if (i % 1000 == 0) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		});
		for (int i = 0; i < 10; i++) {
			final int k = i;
			pool.submit(() -> {
				while (true) {
					DoDReader reader = gorilla.getReader();
					long lastTs = reader.getLastTs();
					long tts = 0;
					int counter = 0;
					while (true) {
						try {
							tts = reader.readPair().getTimestamp();
							counter++;
							if (counter % 1000 == 0) {
								Thread.sleep(1);
							}
						} catch (EOFException e) {
							break;
						} catch (Exception e) {
							e.printStackTrace();
							System.err.println("Counter:" + reader.getCounter() + "\t" + reader.getBuf().position()
									+ "\t" + reader.getBuf().limit() + "\t" + reader.getBuf().capacity());
							break;
						}
					}
					System.out.println(k + " compare:" + counter + "\t" + (tts - lastTs));
				}
			});
		}
		System.out.println((((double) COUNT) / (System.currentTimeMillis() - ts)));

	}

}
