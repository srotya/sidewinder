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
package com.srotya.sidewinder.core.functions;

import java.util.List;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Series;

public class SignalProcessingFunctions {

	@FunctionName(alias = "fft", description = "Takes the FFT of each series", type = "signal")
	public static class ForwardFFT implements Function {
		@Override
		public List<Series> apply(List<Series> t) {
			FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
			for (Series s : t) {
				List<DataPoint> dataPoints = s.getDataPoints();
				Complex[] transform = fft.transform(dpListToArray(dataPoints, s.isFp()), TransformType.FORWARD);
				for (int i = 0; i < dataPoints.size(); i++) {
					DataPoint dataPoint = dataPoints.get(i);
					if (s.isFp()) {
						dataPoint.setLongValue((long) transform[i].abs());
					} else {
						dataPoint.setValue(transform[i].abs());
					}
				}
			}
			return t;
		}

		@Override
		public void init(Object[] args) throws Exception {
		}

		@Override
		public int getNumberOfArgs() {
			return 0;
		}
	}

	public static double[] dpListToArray(List<DataPoint> dp, boolean isFp) {
		int len = nextPowerOf2(dp.size());
		double[] ary = new double[(int) Math.pow(2, len)];
		for (int i = 0; i < dp.size(); i++) {
			DataPoint dataPoint = dp.get(i);
			if (isFp) {
				ary[i] = dataPoint.getValue();
			} else {
				ary[i] = dataPoint.getLongValue();
			}
		}
		return ary;
	}

	public static int nextPowerOf2(int a) {
		int b = 0;
		if (a >= 65536) {
			a /= 65536;
			b += 16;
		}
		if (a >= 256) {
			a /= 256;
			b += 8;
		}
		if (a >= 16) {
			a /= 16;
			b += 4;
		}
		if (a >= 4) {
			a /= 4;
			b += 2;
		}
		if (a >= 2) {
			a /= 2;
			b += 1;
		}
		return b;
	}

}
