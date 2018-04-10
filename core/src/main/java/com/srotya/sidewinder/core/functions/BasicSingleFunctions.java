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

import java.util.Iterator;
import java.util.List;

import com.srotya.sidewinder.core.analytics.MathUtils;
import com.srotya.sidewinder.core.storage.DataPoint;

public class BasicSingleFunctions {

	@FunctionName(alias = "sfirst", description = "Returns the first value in the series", type="single")
	public static class FirstFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (isFp) {
				output.setValue(dataPoints.get(0).getValue());
			} else {
				output.setLongValue(dataPoints.get(0).getLongValue());
			}
		}

	}

	@FunctionName(alias = "slast", description = "Returns the last value in the series", type="single")
	public static class LastFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (isFp) {
				output.setValue(dataPoints.get(dataPoints.size() - 1).getValue());
			} else {
				output.setLongValue(dataPoints.get(dataPoints.size() - 1).getLongValue());
			}
		}

	}

	@FunctionName(alias = "smax", description = "Returns the largest value in the series", type="single")
	public static class MaxFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (isFp) {
				double max = 0;
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					if (dataPoint.getValue() > max) {
						max = dataPoint.getValue();
					}
				}
				output.setValue(max);
			} else {
				long max = 0;
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					if (dataPoint.getValue() > max) {
						max = dataPoint.getLongValue();
					}
				}
				output.setLongValue(max);
			}
		}

	}

	@FunctionName(alias = "smean", description = "Returns the average value of the series", type="single")
	public static class MeanFunction extends SumFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			super.aggregateToSingle(dataPoints, output, isFp);
			if (isFp) {
				output.setValue(output.getValue() / dataPoints.size());
			} else {
				output.setLongValue(output.getLongValue() / dataPoints.size());
			}
		}

	}

	@FunctionName(alias = "smin", description = "Returns the smallest value in the series", type="single")
	public static class MinFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (dataPoints.isEmpty()) {
				return;
			}
			if (isFp) {
				double min = dataPoints.get(0).getValue();
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					if (dataPoint.getValue() < min) {
						min = dataPoint.getValue();
					}
				}
				output.setValue(min);
			} else {
				long min = dataPoints.get(0).getLongValue();
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					if (dataPoint.getLongValue() < min) {
						min = dataPoint.getLongValue();
					}
				}
				output.setLongValue(min);
			}
		}

	}

	@FunctionName(alias = "sstddev", description = "Returns the standard deviation value of the series", type="single")
	public static class StdDeviationFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (!isFp) {
				long[] ary = new long[dataPoints.size()];
				for (int i = 0; i < dataPoints.size(); i++) {
					DataPoint dataPoint = dataPoints.get(i);
					ary[i] = dataPoint.getLongValue();
				}
				long avg = MathUtils.mean(ary);
				long standardDeviation = MathUtils.standardDeviation(ary, avg);
				output.setLongValue(standardDeviation);
			} else {
				double[] ary = new double[dataPoints.size()];
				for (int i = 0; i < dataPoints.size(); i++) {
					DataPoint dataPoint = dataPoints.get(i);
					ary[i] = dataPoint.getLongValue();
				}
				double avg = MathUtils.mean(ary);
				double standardDeviation = MathUtils.standardDeviation(ary, avg);
				output.setValue(standardDeviation);
			}
		}

	}

	@FunctionName(alias = "ssum", description = "Returns the sum of all value in the series", type="single")
	public static class SumFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (!isFp) {
				long sum = 0;
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					sum += dataPoint.getLongValue();
				}
				output.setLongValue(sum);
			} else {
				double sum = 0;
				for (Iterator<DataPoint> iterator = dataPoints.iterator(); iterator.hasNext();) {
					DataPoint dataPoint = iterator.next();
					sum += dataPoint.getValue();
				}
				output.setValue(sum);
			}
		}

	}

	@FunctionName(alias = "srms", description = "Returns the Root Mean Squared value of the series", type="single")
	public static class SRMSFunction extends ReduceFunction {

		@Override
		public void aggregateToSingle(List<DataPoint> dataPoints, DataPoint output, boolean isFp) {
			if (isFp) {
				double squaresSum = 0;
				for (DataPoint dp : dataPoints) {
					squaresSum += dp.getValue() * dp.getValue();
				}
				output.setValue(Math.sqrt(squaresSum));
			} else {
				long squaresSum = 0;
				for (DataPoint dp : dataPoints) {
					squaresSum += dp.getLongValue() * dp.getLongValue();
				}
				output.setValue((long) Math.sqrt(squaresSum));
			}
		}

	}

}
