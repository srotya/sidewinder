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
package com.srotya.sidewinder.core.analytics;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

import org.nd4j.jita.conf.CudaEnvironment;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

public class Nd4jTest {

	public static void main(String[] args) {
		long ts = System.nanoTime();
		timeSeriesNormalizationTest();
		ts = System.nanoTime() - ts;
		System.out.println(ts / 1000 / 1000);
		// for (int i = 0; i < 100; i++) {
		// System.out.println(System.currentTimeMillis()/1000 + "\t" + i);
		// }
	}

	@SuppressWarnings("unused")
	private static void extracted() {
		ByteBuffer allocate = ByteBuffer.allocate(1024);
		BitSet set = BitSet.valueOf(allocate);
		for (int i = 0; i < 10000; i++) {
			set.set(i);
		}
		System.out.println(allocate.position());
	}

	public static void timeSeriesNormalizationTest() {
		int n = 10000;
		int windowInSeconds = 1000;
		long startTime = System.currentTimeMillis();
		double[][] oneSeries = new double[n][windowInSeconds];
		ThreadLocalRandom rand = ThreadLocalRandom.current();
		for (int i = 0; i < n; i++) {
			for (int k = 0; k < rand.nextInt(1000, 50000); k++) {
				long time = startTime + rand.nextInt(windowInSeconds);
				int index = (int) ((time - startTime) / 1000);
				oneSeries[i][index] = (oneSeries[i][index] + rand.nextInt(100)) / 2;
			}
		}

	}

	public static void nd4jTest() {
		System.out.println(
				"Device count:" + CudaEnvironment.getInstance().getConfiguration().getAvailableDevices().size());
		ThreadLocalRandom rand = ThreadLocalRandom.current();
		double[][] data = new double[100000][100];
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				data[i][j] = rand.nextDouble();
			}
		}

		long ts = System.currentTimeMillis();
		INDArray ary = Nd4j.create(data);
		for (int i = 0; i < 100; i++) {
			ary.transpose();
		}
		ts = System.currentTimeMillis() - ts;
		System.out.println(ts + " ms for 100 iterations");
	}

}
