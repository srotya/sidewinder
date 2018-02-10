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
package com.srotya.sidewinder.core.functions.multiseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.srotya.sidewinder.core.functions.Function;
import com.srotya.sidewinder.core.functions.FunctionName;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.Series;

/**
 * @author ambud
 */
public abstract class MultiSeriesFunction implements Function {

	@Override
	public List<Series> apply(List<Series> t) {
		List<Series> output = new ArrayList<>();
		boolean fp = t.get(0).isFp();
		List<List<DataPoint>> intermediate = new ArrayList<>();
		int size = t.get(0).getDataPoints().size();
		for (int i = 0; i < t.size(); i++) {
			Series ts = t.get(i);
			if (size != ts.getDataPoints().size()) {
				throw new IllegalArgumentException("Non-uniform series length");
			}
			intermediate.add(ts.getDataPoints());
		}
		List<DataPoint> compute = compute(intermediate, fp);
		Series series = new Series(compute);
		series.setFp(fp);
		series.setMeasurementName(t.get(0).getMeasurementName());
		series.setValueFieldName(name());
		series.setTags(Arrays.asList("multiseries"));
		output.add(series);
		return output;
	}

	public abstract List<DataPoint> compute(List<List<DataPoint>> list, boolean isFp);

	public abstract String name();

	@Override
	public void init(Object[] args) throws Exception {
	}

	@Override
	public int getNumberOfArgs() {
		return 0;
	}

	@FunctionName(alias = "ms-division", description = "Divides first series by the rest of the series", type = "multi-series")
	public static class Division extends MultiSeriesFunction {

		public List<DataPoint> compute(List<List<DataPoint>> dps, boolean isFp) {
			List<DataPoint> output = new ArrayList<>();
			int size = dps.get(0).size();
			for (int i = 0; i < size; i++) {
				if (isFp) {
					double division = Double.doubleToLongBits(dps.get(0).get(i).getLongValue());
					for (int j = 1; j < dps.size(); j++) {
						division /= Double.doubleToLongBits(dps.get(j).get(i).getLongValue());
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), Double.doubleToLongBits(division)));
				} else {
					long division = dps.get(0).get(i).getLongValue();
					for (int j = 1; j < dps.size(); j++) {
						division /= dps.get(j).get(i).getLongValue();
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), division));
				}
			}
			return output;
		}

		@Override
		public String name() {
			return "division";
		}

	}

	@FunctionName(alias = "ms-multiplication", description = "Multiplies values of all series", type = "multi-series")
	public static class Multiplication extends MultiSeriesFunction {

		public List<DataPoint> compute(List<List<DataPoint>> dps, boolean isFP) {
			List<DataPoint> output = new ArrayList<>();
			int size = dps.get(0).size();
			for (int i = 0; i < size; i++) {
				if (isFP) {
					double multiplication = Double.doubleToLongBits(dps.get(0).get(i).getLongValue());
					for (int j = 1; j < dps.size(); j++) {
						multiplication *= Double.doubleToLongBits(dps.get(j).get(i).getLongValue());
					}
					output.add(
							new DataPoint(dps.get(0).get(i).getTimestamp(), Double.doubleToLongBits(multiplication)));
				} else {
					long multiplication = dps.get(0).get(i).getLongValue();
					for (int j = 1; j < dps.size(); j++) {
						multiplication *= dps.get(j).get(i).getLongValue();
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), multiplication));
				}
			}
			return output;
		}

		@Override
		public String name() {
			return "multiplication";
		}

	}

	@FunctionName(alias = "ms-substraction", description = "Adds values of all series", type = "multi-series")
	public static class Substraction extends MultiSeriesFunction {

		public List<DataPoint> compute(List<List<DataPoint>> dps, boolean isFP) {
			List<DataPoint> output = new ArrayList<>();
			int size = dps.get(0).size();
			for (int i = 0; i < size; i++) {
				if (isFP) {
					double substraction = Double.doubleToLongBits(dps.get(0).get(i).getLongValue());
					for (int j = 1; j < dps.size(); j++) {
						substraction -= Double.doubleToLongBits(dps.get(j).get(i).getLongValue());
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), Double.doubleToLongBits(substraction)));
				} else {
					long substraction = dps.get(0).get(i).getLongValue();
					for (int j = 1; j < dps.size(); j++) {
						substraction -= dps.get(j).get(i).getLongValue();
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), substraction));
				}
			}
			return output;
		}

		@Override
		public String name() {
			return "substraction";
		}
	}

	@FunctionName(alias = "ms-addition", description = "Subtracts first series by the rest of the series", type = "multi-series")
	public static class Addition extends MultiSeriesFunction {

		public List<DataPoint> compute(List<List<DataPoint>> dps, boolean isFP) {
			List<DataPoint> output = new ArrayList<>();
			int size = dps.get(0).size();
			for (int i = 0; i < size; i++) {
				if (isFP) {
					double sum = 0;
					for (int j = 0; j < dps.size(); j++) {
						sum += Double.doubleToLongBits(dps.get(j).get(i).getLongValue());
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), Double.doubleToLongBits(sum)));
				} else {
					long sum = 0;
					for (int j = 0; j < dps.size(); j++) {
						sum += dps.get(j).get(i).getLongValue();
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), sum));
				}
			}
			return output;
		}

		@Override
		public String name() {
			return "addition";
		}

	}

	@FunctionName(alias = "ms-average", description = "Averages all series", type = "multi-series")
	public static class Average extends MultiSeriesFunction {

		public List<DataPoint> compute(List<List<DataPoint>> dps, boolean isFP) {
			List<DataPoint> output = new ArrayList<>();
			int size = dps.get(0).size();
			for (int i = 0; i < size; i++) {
				if (isFP) {
					double avg = 0;
					for (int j = 0; j < dps.size(); j++) {
						avg += Double.doubleToLongBits(dps.get(j).get(i).getLongValue());
					}
					output.add(
							new DataPoint(dps.get(0).get(i).getTimestamp(), Double.doubleToLongBits(avg / dps.size())));
				} else {
					long avg = 0;
					for (int j = 0; j < dps.size(); j++) {
						avg += dps.get(j).get(i).getLongValue();
					}
					output.add(new DataPoint(dps.get(0).get(i).getTimestamp(), avg / dps.size()));
				}
			}
			return output;
		}

		@Override
		public String name() {
			return "average";
		}

	}

}
