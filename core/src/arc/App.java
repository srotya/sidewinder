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
package com.test.zmisc;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.math.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) {
		double[] a = new double[32768];
		double[] b = new double[32768];

		ThreadLocalRandom rand = ThreadLocalRandom.current();

		for (int i = 0; i < a.length; i++) {
			a[i] = rand.nextDouble();
			b[i] = a[i];
		}

		long ts = System.currentTimeMillis();
		PearsonsCorrelation corr = new PearsonsCorrelation();
		corr.correlation(a, b);
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time elapsed:" + ts + "ms");

		// standard deviation is the sqrt(mean((X-avg)^2))

		ts = System.currentTimeMillis();
		double amean = average(a);
		double aStdDev = standardDeviation(a, amean);

		StandardDeviation sd = new StandardDeviation();
		sd.setBiasCorrected(false);

		double bmean = average(b);
		double bStdDev = standardDeviation(b, bmean);

		double cor = (covariance(a, aStdDev, b, bmean)) / (aStdDev * bStdDev);
		ts = System.currentTimeMillis() - ts;
		System.out.println("Time elapsed:" + ts + "ms");
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
