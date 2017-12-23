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
package com.srotya.sidewinder.cluster.push;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.cluster.ClusterConfiguration;
import com.srotya.sidewinder.cluster.ClusterResourceMonitor;
import com.srotya.sidewinder.cluster.pull.rpc.ClusterMetadataServiceImpl;
import com.srotya.sidewinder.cluster.push.api.InfluxApi;
import com.srotya.sidewinder.cluster.push.connectors.ClusterConnector;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.push.rpc.ClusteredWriteServiceImpl;
import com.srotya.sidewinder.core.ConfigConstants;
import com.srotya.sidewinder.core.SidewinderDropwizardReporter;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.monitoring.ResourceMonitor;
import com.srotya.sidewinder.core.monitoring.RestAPIHealthCheck;
import com.srotya.sidewinder.core.rpc.WriterServiceImpl;
import com.srotya.sidewinder.core.security.AllowAllAuthorizer;
import com.srotya.sidewinder.core.security.BasicAuthenticator;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.dropwizard.Application;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Environment;
import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class SidewinderClusteredServer extends Application<ClusterConfiguration> {

	private static final Logger logger = Logger.getLogger(SidewinderClusteredServer.class.getName());

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

		StorageEngine storageEngine;

		String storageEngineClass = conf.getOrDefault("storage.engine",
				"com.srotya.sidewinder.core.storage.mem.MemStorageEngine");
		try {
			storageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
			storageEngine.configure(conf, bgTasks);
			storageEngine.connect();
		} catch (Exception e) {
			throw new IOException(e);
		}

		ClusterConnector connector;
		try {
			connector = (ClusterConnector) Class.forName(conf.getOrDefault("cluster.connector",
					"com.srotya.sidewinder.cluster.push.connectors.ConfigConnector")).newInstance();
			connector.init(conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		RoutingEngine router;
		try {
			router = (RoutingEngine) Class.forName(conf.getOrDefault("cluster.routing.engine",
					"com.srotya.sidewinder.cluster.push.routing.impl.MasterSlaveRoutingEngine")).newInstance();
			router.init(conf, storageEngine, connector);
		} catch (Exception e) {
			throw new IOException(e);
		}

		System.out.println("Waiting for coordinator to be elected.");
		while (router.getLeader() == null) {
			Thread.sleep(1000);
			System.out.print(".");
		}
		logger.info("\nCoordinator elected, registering services");

		final Server server = ServerBuilder.forPort(port)
				.decompressorRegistry(DecompressorRegistry.getDefaultInstance())
				.addService(new ClusteredWriteServiceImpl(router, conf))
				.addService(new ClusterMetadataServiceImpl(router, conf))
				.addService(new WriterServiceImpl(storageEngine, conf)).build().start();

		registerMetrics(registry, storageEngine, bgTasks);

		Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
			@Override
			public void run() {
				server.shutdownNow();
			}
		});

		ResourceMonitor.getInstance().init(storageEngine, bgTasks);
		ClusterResourceMonitor.getInstance().init(storageEngine, connector, bgTasks);
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		env.jersey().register(new MeasurementOpsApi(storageEngine));
		env.jersey().register(new DatabaseOpsApi(storageEngine));
		env.jersey().register(new InfluxApi(router));
		env.healthChecks().register("restapi", new RestAPIHealthCheck());

		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.AUTH_BASIC_ENABLED, ConfigConstants.FALSE))) {
			AuthFilter<BasicCredentials, Principal> basicCredentialAuthFilter = new BasicCredentialAuthFilter.Builder<>()
					.setAuthenticator(new BasicAuthenticator(conf.get(ConfigConstants.AUTH_BASIC_USERS)))
					.setAuthorizer(new AllowAllAuthorizer()).setPrefix("Basic").buildAuthFilter();
			env.jersey().register(basicCredentialAuthFilter);
		}

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

	private void registerMetrics(final MetricRegistry registry, StorageEngine storageEngine,
			ScheduledExecutorService es) {
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
