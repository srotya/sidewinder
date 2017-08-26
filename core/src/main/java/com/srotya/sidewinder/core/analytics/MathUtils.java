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
package com.srotya.sidewinder.core.analytics;

/**
 * Set of math utilities optimized for fast execution.
 * 
 * @author ambud
 */
public class MathUtils {

	/**
	 * Compute PPMCC between the supplied arrays
	 * 
	 * @param a first array to compute ppmcc
	 * @param b second array to compute ppmcc
	 * @return ppmcc
	 */
	public static double ppmcc(double[] a, double[] b) {
		double amean = MathUtils.mean(a);
		double aStdDev = MathUtils.standardDeviation(a, amean);

		double bmean = MathUtils.mean(b);
		double bStdDev = MathUtils.standardDeviation(b, bmean);

		double cor = (MathUtils.covariance(a, aStdDev, b, bmean)) / (aStdDev * bStdDev);
		return cor;
	}

	/**
	 * Compute mean or average of the supplied array
	 * 
	 * @param a array to compute mean for
	 * @return average / mean
	 */
	public static double mean(double[] a) {
		double avg = 0;
		for (int i = 0; i < a.length; i++) {
			avg += a[i];
		}
		avg = avg / a.length;
		return avg;
	}

	/**
	 * Compute mean or average of the supplied array
	 * 
	 * @param a array to compute mean for
	 * @return average / mean
	 */
	public static long mean(long[] a) {
		long avg = 0;
		for (int i = 0; i < a.length; i++) {
			avg += a[i];
		}
		avg = avg / a.length;
		return avg;
	}
	/**
	 * Compute co-variance of the supplied array pair
	 * 
	 * @param a array to compute covariance for
	 * @param amean mean of array a
	 * @param b array to compute covariance for
	 * @param bmean mean of array b
	 * @return co-variance
	 */
	public static double covariance(double[] a, double amean, double[] b, double bmean) {
		double t = 0;
		for (int i = 0; i < a.length; i++) {
			t += (a[i] - amean) * (b[i] - bmean);
		}
		return t / a.length;
	}
	
	/**
	 * Compute standard deviation for the supplied array
	 * 
	 * @param a array to compute stddev for
	 * @param avg
	 * @return standard deviation
	 */
	public static long standardDeviation(long[] a, long avg) {
		long aStdDev = 0;
		for (int i = 0; i < a.length; i++) {
			double tmp = (a[i] - avg);
			aStdDev += tmp * tmp;
		}
		aStdDev = (long) Math.sqrt(aStdDev / (a.length));
		return aStdDev;
	}

	/**
	 * Compute standard deviation for the supplied array
	 * 
	 * @param a
	 * @param avg
	 * @return standard deviation
	 */
	public static double standardDeviation(double[] a, double avg) {
		double aStdDev = 0;
		for (int i = 0; i < a.length; i++) {
			double tmp = (a[i] - avg);
			aStdDev += tmp * tmp;
		}
		aStdDev = Math.sqrt(aStdDev / (a.length));
		return aStdDev;
	}

	/**
	 * Compute PPMCC (Pearson Product Moment Correlation Coefficient) for
	 * supplied array of double arrays comparing it to the series at baseIndex
	 * in the array.
	 * 
	 * @param ary
	 * @param baseIndex
	 * @return array of ppmcc
	 */
	public static double[] ppmcc(double[][] ary, int baseIndex) {
		double[] a = ary[baseIndex];
		double amean = mean(a);
		double aStdDev = standardDeviation(a, amean);

		double[] result = new double[ary.length];

		for (int i = 0; i < ary.length; i++) {
			double[] b = ary[i];
			double bmean = mean(b);
			double bStdDev = standardDeviation(b, bmean);

			result[i] = (covariance(a, aStdDev, b, bmean)) / (aStdDev * bStdDev);
		}
		return result;
	}

}
