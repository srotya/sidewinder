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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.Principal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.TimeoutException;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.InfluxApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.SqlApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.graphite.GraphiteServer;
import com.srotya.sidewinder.core.health.RestAPIHealthCheck;
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
 * Main driver class for single-node sidewinder. It will register the REST APIs,
 * initialize the storage engine as well as start the network receivers for HTTP
 * and binary protocols for Sidewinder.
 * 
 * @author ambud
 *
 */
public class SidewinderServer extends Application<SidewinderConfig> {

	private static final Logger logger = Logger.getLogger(SidewinderServer.class.getName());
	private StorageEngine storageEngine;
	private static SidewinderServer sidewinderServer;
	private List<Runnable> shutdownTasks;

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		shutdownTasks = new ArrayList<>();
		Map<String, String> conf = new HashMap<>();
		overloadProperties(config, conf);

		ScheduledExecutorService bgTasks = Executors.newScheduledThreadPool(
				Integer.parseInt(conf.getOrDefault(ConfigConstants.BG_THREAD_COUNT, "2")),
				new BackgrounThreadFactory("sidewinderbg-tasks"));
		initializeStorageEngine(conf, bgTasks);
		enableMonitoring(bgTasks);
		registerWebAPIs(env, conf, bgTasks);
		checkAndEnableGRPC(conf);
		checkAndEnableGraphite(conf);
		registerShutdownHook(conf);
	}

	private void registerShutdownHook(Map<String, String> conf) {
		Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
			@Override
			public void run() {
				for (Runnable task : shutdownTasks) {
					task.run();
				}
			}
		});
	}

	private void checkAndEnableGraphite(Map<String, String> conf) throws Exception {
		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.GRAPHITE_ENABLED, ConfigConstants.FALSE))) {
			final GraphiteServer server = new GraphiteServer(conf, storageEngine);
			server.start();
			shutdownTasks.add(new Runnable() {

				@Override
				public void run() {
					try {
						server.stop();
					} catch (Exception e) {
						logger.log(Level.SEVERE, "Failed to shutdown graphite server", e);
					}
				}
			});
		}
	}

	private void enableMonitoring(ScheduledExecutorService bgTasks) {
		ResourceMonitor.getInstance().init(storageEngine, bgTasks);
	}

	private void overloadProperties(SidewinderConfig config, Map<String, String> conf)
			throws IOException, FileNotFoundException {
		String path = config.getConfigPath();
		if (path != null) {
			Properties props = new Properties();
			props.load(new FileInputStream(path));
			for (final String name : props.stringPropertyNames()) {
				conf.put(name, props.getProperty(name));
			}
		}
	}

	private void initializeStorageEngine(Map<String, String> conf, ScheduledExecutorService bgTasks)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		String storageEngineClass = conf.getOrDefault(ConfigConstants.STORAGE_ENGINE,
				ConfigConstants.DEFAULT_STORAGE_ENGINE);
		logger.info("Using Storage Engine:" + storageEngineClass);
		storageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
		storageEngine.configure(conf, bgTasks);
		storageEngine.connect();
	}

	private void registerWebAPIs(Environment env, Map<String, String> conf, ScheduledExecutorService bgTasks)
			throws SQLException, ClassNotFoundException {
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		env.jersey().register(new MeasurementOpsApi(storageEngine));
		env.jersey().register(new DatabaseOpsApi(storageEngine));
		env.jersey().register(new InfluxApi(storageEngine));
		env.jersey().register(new SqlApi(storageEngine));
		env.healthChecks().register("restapi", new RestAPIHealthCheck());

		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.AUTH_BASIC_ENABLED, ConfigConstants.FALSE))) {
			logger.info("Enabling basic authentication");
			AuthFilter<BasicCredentials, Principal> basicCredentialAuthFilter = new BasicCredentialAuthFilter.Builder<>()
					.setAuthenticator(new BasicAuthenticator(conf.get(ConfigConstants.AUTH_BASIC_USERS)))
					.setAuthorizer(new AllowAllAuthorizer()).setPrefix("Basic").buildAuthFilter();
			env.jersey().register(basicCredentialAuthFilter);
		}
	}

	private void checkAndEnableGRPC(Map<String, String> conf) throws IOException {
		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.ENABLE_GRPC, ConfigConstants.FALSE))) {
			final ExecutorService es = Executors
					.newFixedThreadPool(
							Integer.parseInt(conf.getOrDefault(ConfigConstants.GRPC_EXECUTOR_COUNT,
									ConfigConstants.DEFAULT_GRPC_EXECUTOR_COUNT)),
							new BackgrounThreadFactory("grpc-threads"));

			final WriterServiceImpl writer = new WriterServiceImpl(storageEngine, conf);
			final Server server = ServerBuilder
					.forPort(Integer.parseInt(conf.getOrDefault(ConfigConstants.GRPC_PORT, "9928"))).executor(es)
					.decompressorRegistry(DecompressorRegistry.getDefaultInstance()).addService(writer).build().start();
			shutdownTasks.add(new Runnable() {

				@Override
				public void run() {
					server.shutdown();
					try {
						server.awaitTermination(10, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						logger.log(Level.SEVERE, "Failed to terminate GRPC server", e);
					}
					es.shutdownNow();
					try {
						writer.getDisruptor().shutdown(100, TimeUnit.SECONDS);
					} catch (TimeoutException e) {
						logger.log(Level.SEVERE, "Failed to terminate GRPC disruptor", e);
					}
					writer.getEs().shutdownNow();
				}
			});

		}
	}

	/**
	 * Main method to launch dropwizard app
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		sidewinderServer = new SidewinderServer();
		sidewinderServer.run(args);
	}

	/**
	 * @return
	 */
	public static SidewinderServer getSidewinderServer() {
		return sidewinderServer;
	}

	/**
	 * @return
	 */
	public StorageEngine getStorageEngine() {
		return storageEngine;
	}

}
