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

import static org.junit.Assert.*;

import org.apache.commons.math.stat.correlation.Covariance;
import org.apache.commons.math.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.junit.Test;

/**
 * Unit tests for {@link MathUtils}
 * 
 * @author ambud
 */
public class TestMathUtils {

	@Test
	public void testPPMCCMulti() {
		double[][] data = new double[][] { new double[] { 2, 3, 4, 5, 6 }, new double[] { 2.2, 33.2, 44.4, 55.5, 66.6 },
				new double[] { 2, 3, 4, 5, 6 } };
		PearsonsCorrelation corr = new PearsonsCorrelation();
		double[] ppmcc = MathUtils.ppmcc(data, 0);
		for (int i = 0; i < data.length; i++) {
			double correlation = corr.correlation(data[0], data[i]);
			assertEquals(ppmcc[i], correlation, 0.001);
		}
	}

	@Test
	public void testPPMCC() {
		double[] a = new double[] { 2, 3, 4, 5, 6 };
		double[] b = new double[] { 2.2, 33.2, 44.4, 55.5, 66.6 };
		PearsonsCorrelation corr = new PearsonsCorrelation();
		double ppmcc = MathUtils.ppmcc(a, b);
		double correlation = corr.correlation(a, b);
		assertEquals(correlation, ppmcc, 0.001);
	}

	@Test
	public void testMean() {
		double[] a = new double[] { 2, 3, 4, 5, 6 };
		double mean = MathUtils.mean(a);
		Mean cmean = new Mean();
		assertEquals(cmean.evaluate(a), mean, 0.0001);
	}

	@Test
	public void testCovariance() {
		double[] a = new double[] { 2, 3, 4, 5, 6 };
		double[] b = new double[] { 2.2, 33.2, 44.4, 55.5, 66.6 };
		Covariance cov = new Covariance();
		double covariance = cov.covariance(a, b, false);
		double amean = MathUtils.mean(a);
		double bmean = MathUtils.mean(b);
		assertEquals(covariance, MathUtils.covariance(a, amean, b, bmean), 0.001);
	}

}
