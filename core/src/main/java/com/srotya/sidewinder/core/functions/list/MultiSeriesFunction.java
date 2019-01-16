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
package com.srotya.sidewinder.core.functions.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.SeriesOutput;

/**
 * @author ambud
 */
@FunctionName(alias = { "ms", "multiseries" }, description = "Multiseries aggregate function", type = "aggregate")
public class MultiSeriesFunction implements Function {

	private int timeBucketSeconds;
	private ReduceFunction aggregator;

	@Override
	public List<SeriesOutput> apply(List<SeriesOutput> t) {
		List<SeriesOutput> output = new ArrayList<>();
		if (t.size() == 0) {
			return output;
		}
		SortedMap<Long, List<DataPoint>> bucketMap = new TreeMap<>();
		boolean fp = t.get(0).isFp();
		for (int i = 0; i < t.size(); i++) {
			SeriesOutput ts = t.get(i);
			for (DataPoint dataPoint : ts.getDataPoints()) {
				long key = (dataPoint.getTimestamp() * timeBucketSeconds / 1000) / timeBucketSeconds;
				List<DataPoint> list = bucketMap.get(key);
				if (list == null) {
					list = new ArrayList<>();
					bucketMap.put(key, list);
				}
				list.add(dataPoint);
			}
		}
		List<DataPoint> compute = compute(bucketMap, fp);
		SeriesOutput series = new SeriesOutput(compute);
		series.setFp(fp);
		series.setMeasurementName(t.get(0).getMeasurementName());
		series.setValueFieldName(t.get(0).getValueFieldName() + "-" + aggregator.getClass().getSimpleName());
		series.setTags(Arrays.asList(Tag.newBuilder().setTagKey("multiseries").setTagValue("true").build()));
		output.add(series);
		return output;
	}

	public List<DataPoint> compute(SortedMap<Long, List<DataPoint>> bucketMap, boolean isFp) {
		List<DataPoint> dps = new ArrayList<>();
		for (Entry<Long, List<DataPoint>> entry : bucketMap.entrySet()) {
			DataPoint output = new DataPoint();
			aggregator.aggregateToSingle(entry.getValue(), output, isFp);
			output.setTimestamp(entry.getKey() * 1000);
			dps.add(output);
		}
		return dps;
	}

	@Override
	public void init(Object[] args) throws Exception {
		timeBucketSeconds = (Integer) args[0];
		String aggregateFunction = (String) args[1];
		@SuppressWarnings("unchecked")
		Class<ReduceFunction> lookupFunction = (Class<ReduceFunction>) FunctionTable.get()
				.lookupFunction(aggregateFunction);
		if (lookupFunction == null) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregateFunction);
		}
		try {
			ReduceFunction tmp = lookupFunction.newInstance();
			this.aggregator = (ReduceFunction) tmp;
		} catch (ClassCastException e) {
			throw new IllegalArgumentException("Invalid aggregation function:" + aggregateFunction);
		}
	}

	@Override
	public int getNumberOfArgs() {
		return 2;
	}

}
