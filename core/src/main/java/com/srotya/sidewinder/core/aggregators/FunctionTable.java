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
package com.srotya.sidewinder.core.aggregators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.srotya.sidewinder.core.aggregators.single.FirstFunction;
import com.srotya.sidewinder.core.aggregators.single.LastFunction;
import com.srotya.sidewinder.core.aggregators.single.MaxFunction;
import com.srotya.sidewinder.core.aggregators.single.MeanFunction;
import com.srotya.sidewinder.core.aggregators.single.MinFunction;
import com.srotya.sidewinder.core.aggregators.single.StdDeviationFunction;
import com.srotya.sidewinder.core.aggregators.single.SumFunction;
import com.srotya.sidewinder.core.aggregators.windowed.DerivativeFunction;
import com.srotya.sidewinder.core.aggregators.windowed.IntegralFunction;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedFirst;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedLast;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMax;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMean;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedMin;
import com.srotya.sidewinder.core.aggregators.windowed.WindowedStdDev;

/**
 * @author ambud
 */
public class FunctionTable {

	public static final String NONE = "none";
	public static final String DVDT = "derivative";
	public static final String AVG = "average";
	public static final String SMEAN = "smean";
	public static final String INTEGRAL = "integral";
	public static final String MEAN = "mean";
	public static final String SSUM = "ssum";
	public static final String STDDEV = "stddev";
	public static final String SSTDDEV = "sstddev";
	public static final String SMIN = "smin";
	public static final String MIN = "min";
	public static final String SMAX = "smax";
	public static final String MAX = "max";
	public static final String SFIRST = "sfirst";
	public static final String FIRST = "first";
	public static final String SLAST = "slast";
	public static final String LAST = "last";
	public static final FunctionTable table = new FunctionTable();
	public Map<String, Class<? extends AggregationFunction>> functionMap;

	private FunctionTable() {
		functionMap = new HashMap<>();
	}

	static {
		FunctionTable.get().register(SSUM, SumFunction.class);
		FunctionTable.get().register(MEAN, WindowedMean.class);
		FunctionTable.get().register(INTEGRAL, IntegralFunction.class);
		FunctionTable.get().register(SMEAN, MeanFunction.class);
		FunctionTable.get().register(AVG, WindowedMean.class);
		FunctionTable.get().register(DVDT, DerivativeFunction.class);
		FunctionTable.get().register(SSTDDEV, StdDeviationFunction.class);
		FunctionTable.get().register(STDDEV, WindowedStdDev.class);
		FunctionTable.get().register(SMIN, MinFunction.class);
		FunctionTable.get().register(MIN, WindowedMin.class);
		FunctionTable.get().register(SMAX, MaxFunction.class);
		FunctionTable.get().register(MAX, WindowedMax.class);
		FunctionTable.get().register(SFIRST, FirstFunction.class);
		FunctionTable.get().register(FIRST, WindowedFirst.class);
		FunctionTable.get().register(SLAST, LastFunction.class);
		FunctionTable.get().register(LAST, WindowedLast.class);
		FunctionTable.get().register(NONE, null);
	}

	public static FunctionTable get() {
		return table;
	}

	public void register(String name, Class<? extends AggregationFunction> functionClass) {
		functionMap.put(name.toLowerCase(), functionClass);
	}

	public Class<? extends AggregationFunction> lookupFunction(String name) {
		return functionMap.get(name.toLowerCase());
	}

	public Set<String> listFunctions() {
		return functionMap.keySet();
	}
}
