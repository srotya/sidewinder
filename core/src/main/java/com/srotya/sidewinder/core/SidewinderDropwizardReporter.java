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
package com.srotya.sidewinder.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class SidewinderDropwizardReporter extends ScheduledReporter {

	private static final String _INTERNAL = "_internal";
	private StorageEngine engine;
	private String name;

	public SidewinderDropwizardReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
			TimeUnit durationUnit, StorageEngine engine) {
		super(registry, name, filter, rateUnit, durationUnit);
		this.name = name;
		this.engine = engine;
	}

	@Override
	public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
			SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		if (counters != null) {
			for (Entry<String, Counter> entry : counters.entrySet()) {
				DataPoint dp = MiscUtils.buildDataPoint(_INTERNAL, name, entry.getKey(), Arrays.asList("local"),
						System.currentTimeMillis(), entry.getValue().getCount());
				try {
					engine.writeDataPoint(dp);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		if (meters != null) {
			for (Entry<String, Meter> entry : meters.entrySet()) {
				DataPoint dp = MiscUtils.buildDataPoint(_INTERNAL, name, entry.getKey(), Arrays.asList("local"),
						System.currentTimeMillis(), entry.getValue().getCount());
				try {
					engine.writeDataPoint(dp);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
