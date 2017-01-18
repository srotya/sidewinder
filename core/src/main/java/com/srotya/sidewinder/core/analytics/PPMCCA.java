/**
 * Copyright 2016 Ambud Sharma
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

/**
 * PPMCC i.e. Pearson Product Moment Correlation Coefficient
 * 
 * @author ambud
 */
public class PPMCCA {
	
	private PPMCCA() {
	}

	public static double[] compute(double[][] ary, int baseIndex) {
		double[] a = ary[baseIndex];
		double amean = average(a);
		double aStdDev = standardDeviation(a, amean);

		double[] result = new double[ary.length];

		for (int i = 0; i < ary.length; i++) {
			double[] b = ary[i];
			double bmean = average(b);
			double bStdDev = standardDeviation(b, bmean);

			result[i] = (covariance(a, aStdDev, b, bmean)) / (aStdDev * bStdDev);
		}
		return result;
	}

	public static double compute(double[] a, double[] b) {
		double amean = average(a);
		double aStdDev = standardDeviation(a, amean);

		double bmean = average(b);
		double bStdDev = standardDeviation(b, bmean);

		double cor = (covariance(a, aStdDev, b, bmean)) / (aStdDev * bStdDev);
		return cor;
	}

	public static double average(double[] a) {
		double avg = 0;
		for (int i = 0; i < a.length; i++) {
			avg += a[i];
		}
		avg = avg / a.length;
		return avg;
	}

	public static double covariance(double[] a, double amean, double[] b, double bmean) {
		double t = 0;
		for (int i = 0; i < a.length; i++) {
			t += (a[i] - amean) * (b[i] - bmean);
		}
		return t / a.length;
	}

	public static double standardDeviation(double[] a, double avg) {
		double aStdDev = 0;
		for (int i = 0; i < a.length; i++) {
			double tmp = (a[i] - avg);
			aStdDev += tmp * tmp;
		}
		aStdDev = Math.sqrt(aStdDev / (a.length));
		return aStdDev;
	}
}
