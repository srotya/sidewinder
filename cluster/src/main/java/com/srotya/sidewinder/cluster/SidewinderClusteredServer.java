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
package com.srotya.sidewinder.cluster;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.cluster.api.InfluxApi;
import com.srotya.sidewinder.cluster.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.ClusteredWriteServiceImpl;
import com.srotya.sidewinder.core.ResourceMonitor;
import com.srotya.sidewinder.core.SidewinderDropwizardReporter;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.health.RestAPIHealthCheck;
import com.srotya.sidewinder.core.rpc.WriterServiceImpl;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;

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
		HashMap<String, String> conf = new HashMap<>();
		if (configuration.getConfigPath() != null) {
			loadConfiguration(configuration, conf);
		}
		int port = Integer.parseInt(conf.getOrDefault("cluster.grpc.port", "55021"));

		ScheduledExecutorService bgTasks = Executors.newScheduledThreadPool(2, new BackgrounThreadFactory("bgtasks"));

		String storageEngineClass = conf.getOrDefault("storage.engine",
				"com.srotya.sidewinder.core.storage.mem.MemStorageEngine");
		StorageEngine storageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
		storageEngine.configure(conf, bgTasks);
		storageEngine.connect();

		ClusterConnector connector = (ClusterConnector) Class.forName(conf.getOrDefault("cluster.connector",
				"com.srotya.sidewinder.cluster.connectors.ConfigConnector")).newInstance();
		connector.init(conf);

		RoutingEngine router = (RoutingEngine) Class.forName(conf.getOrDefault("cluster.routing.engine",
				"com.srotya.sidewinder.cluster.routing.impl.MasterSlaveRoutingEngine")).newInstance();
		router.init(conf, storageEngine, connector);

		final Server server = ServerBuilder.forPort(port)
				.decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new ClusteredWriteServiceImpl(router, conf))
				.addService(new WriterServiceImpl(storageEngine, conf)).build().start();

		registerMetrics(registry, storageEngine);

		Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
			@Override
			public void run() {
				server.shutdownNow();
			}
		});

		ResourceMonitor.getInstance().init(storageEngine, bgTasks);
		ClusterResourceMonitor.getInstance().init(storageEngine, connector, bgTasks);
		env.jersey().register(new GrafanaQueryApi(storageEngine, registry));
		env.jersey().register(new MeasurementOpsApi(storageEngine, registry));
		env.jersey().register(new DatabaseOpsApi(storageEngine, registry));
		if (connector.isBootstrap()) {
			env.jersey().register(new InfluxApi(router, registry, conf));
		}
		env.healthChecks().register("restapi", new RestAPIHealthCheck());
	}

	private void loadConfiguration(ClusterConfiguration configuration, HashMap<String, String> conf)
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

	private void registerMetrics(final MetricRegistry registry, StorageEngine storageEngine) {
		@SuppressWarnings("resource")
		SidewinderDropwizardReporter requestReporter = new SidewinderDropwizardReporter(registry, "request",
				new MetricFilter() {

					@Override
					public boolean matches(String name, Metric metric) {
						return true;
					}
				}, TimeUnit.SECONDS, TimeUnit.SECONDS, storageEngine);
		requestReporter.start(1, TimeUnit.SECONDS);
	}

}
