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

import java.util.concurrent.ThreadLocalRandom;

import org.nd4j.jita.conf.CudaEnvironment;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

public class Nd4jTest {

	public static void main(String[] args) {
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
