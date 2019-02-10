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
package com.srotya.sidewinder.core.monitoring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.Database;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.disk.DiskMalloc;

/**
 * @author ambud
 */
public class SidewinderDropwizardReporter extends ScheduledReporter {

	private static final String T_k = "host";
	private static final String _INTERNAL = "_internal";
	private StorageEngine engine;
	private String name;

	public SidewinderDropwizardReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
			TimeUnit durationUnit, StorageEngine engine, ScheduledExecutorService es) throws IOException {
		super(registry, name, filter, rateUnit, durationUnit, es);
		this.name = name;
		this.engine = engine;
	}

	@Override
	public void report(@SuppressWarnings("rawtypes") SortedMap<String, Gauge> gauges,
			SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
			SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		try {
			if(!engine.checkIfExists(_INTERNAL)) {
				Map<String, String> conf = new HashMap<>();
				conf.put(Database.BUCKET_SIZE, "3600");
				conf.put(DiskMalloc.CONF_MEASUREMENT_FILE_INCREMENT, "10485760");
				conf.put(DiskMalloc.CONF_MALLOC_PTRFILE_INCREMENT, "1048576");
				conf.put(DiskMalloc.CONF_MEASUREMENT_BUF_INCREMENT_SIZE, "1024");
				this.engine.getOrCreateDatabase(_INTERNAL, 24, conf);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		long ts = System.currentTimeMillis();
		if (counters != null && !counters.isEmpty()) {
			Builder builder = Point.newBuilder();
			builder.setDbName(_INTERNAL).setMeasurementName(name)
					.addTags(Tag.newBuilder().setTagKey("type").setTagValue("counters"))
					.addTags(Tag.newBuilder().setTagKey(T_k).setTagValue("local").build()).setTimestamp(ts);
			for (Entry<String, Counter> entry : counters.entrySet()) {
				builder.addValueFieldName(entry.getKey()).addFp(false).addValue(entry.getValue().getCount());
			}
			try {
				engine.writeDataPointWithLock(builder.build(), true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (meters != null && !meters.isEmpty()) {
			Builder builder = Point.newBuilder();
			builder.setDbName(_INTERNAL).setMeasurementName(name)
					.addTags(Tag.newBuilder().setTagKey("type").setTagValue("meters"))
					.addTags(Tag.newBuilder().setTagKey(T_k).setTagValue("local").build()).setTimestamp(ts);
			for (Entry<String, Meter> entry : meters.entrySet()) {
				builder.addValueFieldName(entry.getKey()).addFp(false).addValue(entry.getValue().getCount());
			}
			try {
				engine.writeDataPointWithLock(builder.build(), true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (timers != null && !timers.isEmpty()) {
			Builder builder = Point.newBuilder();
			builder.setDbName(_INTERNAL).setMeasurementName(name)
					.addTags(Tag.newBuilder().setTagKey("type").setTagValue("timers"))
					.addTags(Tag.newBuilder().setTagKey(T_k).setTagValue("local").build()).setTimestamp(ts);
			for (Entry<String, Timer> entry : timers.entrySet()) {
				builder.addValueFieldName(entry.getKey()).addFp(false).addValue(entry.getValue().getCount());
			}
			try {
				engine.writeDataPointWithLock(builder.build(), true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}