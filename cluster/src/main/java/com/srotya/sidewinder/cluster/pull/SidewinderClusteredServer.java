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
package com.srotya.sidewinder.cluster.pull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.cluster.WALManagerImpl;
import com.srotya.minuteman.connectors.AtomixConnector;
import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.utils.FileUtils;
import com.srotya.sidewinder.cluster.ClusterConfiguration;
import com.srotya.sidewinder.cluster.pull.api.InfluxApi;
import com.srotya.sidewinder.cluster.pull.storage.SidewinderWALClient;
import com.srotya.sidewinder.core.SidewinderDropwizardReporter;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.monitoring.ResourceMonitor;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author ambud
 */
public class SidewinderClusteredServer extends Application<ClusterConfiguration> {

	public static void main(String[] args) throws Exception {
		new SidewinderClusteredServer().run(args);
	}

	@Override
	public void run(ClusterConfiguration configuration, Environment env) throws Exception {
		final MetricRegistry registry = new MetricRegistry();
		Map<String, String> conf = new HashMap<>();
		if (configuration.getConfigPath() != null) {
			loadConfiguration(configuration, conf);
		}
		ScheduledExecutorService bgTasks = Executors.newScheduledThreadPool(2, new BackgrounThreadFactory("bgtasks"));

		FileUtils.delete(new File(conf.get("wal.dir")));
		FileUtils.delete(new File(conf.get("data.dir")));
		FileUtils.delete(new File(conf.get("index.dir")));
		conf.put(WALManager.WAL_CLIENT_CLASS, SidewinderWALClient.class.getName());
		String storageEngineClass = conf.getOrDefault("storage.engine",
				"com.srotya.sidewinder.core.storage.mem.MemStorageEngine");
		StorageEngine storageEngine;
		try {
			storageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
			storageEngine.configure(conf, bgTasks);
			storageEngine.connect();
		} catch (Exception e) {
			throw new IOException(e);
		}
		
		ResourceMonitor rm = ResourceMonitor.getInstance();
		rm.init(storageEngine, bgTasks);
		
		registerMetrics(registry, storageEngine, bgTasks);
		WALManager walManager = buildClusterAndWALManager(conf, bgTasks, registry, storageEngine);
		System.out.println("WAL Manager:" + walManager.getAddress());

		env.jersey().register(new InfluxApi(walManager, storageEngine, conf));
		env.jersey().register(new GrafanaQueryApi(storageEngine));
	}

	private WALManager buildClusterAndWALManager(Map<String, String> conf, ScheduledExecutorService bgTasks,
			MetricRegistry registry, StorageEngine storageEngine) throws IOException, Exception {
		final ClusterConnector connector;
		try {
			connector = new AtomixConnector();
			connector.init(conf);
		} catch (Exception e) {
			throw new IOException(e);
		}
		final WALManager walManager = new WALManagerImpl();
		walManager.init(conf, connector, bgTasks, storageEngine);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					connector.stop();
					walManager.stop();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		return walManager;
	}

	private void loadConfiguration(ClusterConfiguration configuration, Map<String, String> conf)
			throws IOException, FileNotFoundException {
		String path = configuration.getConfigPath();
		if (path != null) {
			Properties props = new Properties();
			props.load(new FileInputStream(path));
			for (final String name : props.stringPropertyNames()) {
				conf.put(name, props.getProperty(name));
			}
		}
	}

	private void registerMetrics(final MetricRegistry registry, StorageEngine storageEngine, ScheduledExecutorService es) {
		@SuppressWarnings("resource")
		SidewinderDropwizardReporter requestReporter = new SidewinderDropwizardReporter(registry, "request",
				new MetricFilter() {

					@Override
					public boolean matches(String name, Metric metric) {
						return true;
					}
				}, TimeUnit.SECONDS, TimeUnit.SECONDS, storageEngine, es);
		requestReporter.start(1, TimeUnit.SECONDS);
	}

}
