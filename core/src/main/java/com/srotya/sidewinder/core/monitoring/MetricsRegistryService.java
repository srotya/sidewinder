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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class MetricsRegistryService {

	public static final String DISABLE_SELFMON = "selfmon.disabled";
	public static boolean DISABLE_SELF_MONITORING = Boolean.parseBoolean(System.getProperty(DISABLE_SELFMON, "false"));
	private static MetricsRegistryService instance;
	private Map<String, MetricRegistry> registry;
	private Map<String, SidewinderDropwizardReporter> reporter;
	private StorageEngine engine;
	private ScheduledExecutorService es;

	private MetricsRegistryService(StorageEngine engine, ScheduledExecutorService es) {
		this.engine = engine;
		this.es = es;
		registry = new HashMap<>();
		reporter = new HashMap<>();
	}

	public static MetricsRegistryService getInstance(StorageEngine engine, ScheduledExecutorService es) {
		if (instance == null) {
			instance = new MetricsRegistryService(engine, es);
		}
		return instance;
	}

	public static MetricsRegistryService getInstance() {
		return instance;
	}

	public MetricRegistry getInstance(String key) {
		MetricRegistry reg = registry.get(key);
		if (reg == null) {
			reg = new MetricRegistry();
			if (!DISABLE_SELF_MONITORING) {
				SidewinderDropwizardReporter reporter = new SidewinderDropwizardReporter(reg, key, new MetricFilter() {

					@Override
					public boolean matches(String name, Metric metric) {
						return true;
					}
					
				}, TimeUnit.SECONDS, TimeUnit.SECONDS, engine, es);
				reporter.start(1, TimeUnit.SECONDS);
				this.reporter.put(key, reporter);
			}
			registry.put(key, reg);
		}
		return reg;
	}

}
