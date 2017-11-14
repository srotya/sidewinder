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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.aggregators.FunctionTable;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.InfluxApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.SqlApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.external.Ingester;
import com.srotya.sidewinder.core.monitoring.ResourceMonitor;
import com.srotya.sidewinder.core.monitoring.RestAPIHealthCheck;
import com.srotya.sidewinder.core.rpc.GRPCServer;
import com.srotya.sidewinder.core.security.AllowAllAuthorizer;
import com.srotya.sidewinder.core.security.BasicAuthenticator;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.basic.BasicCredentials;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

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

	@Override
	public void initialize(Bootstrap<SidewinderConfig> bootstrap) {
		if (!Boolean.parseBoolean(System.getProperty(ConfigConstants.UI_DISABLE, "true"))) {
			bootstrap.addBundle(new AssetsBundle("/web", "/ui", "index.html"));
		}
	}

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		Map<String, String> conf = new HashMap<>();
		overloadProperties(config, conf);

		ScheduledExecutorService bgTasks = Executors.newScheduledThreadPool(
				Integer.parseInt(conf.getOrDefault(ConfigConstants.BG_THREAD_COUNT, "2")),
				new BackgrounThreadFactory("sidewinderbg-tasks"));
		initializeStorageEngine(conf, bgTasks);
		enableMonitoring(bgTasks);
		registerWebAPIs(env, conf, bgTasks);
		checkAndEnableIngesters(conf, env);
		checkAndRegisterFunctions(conf);
	}

	private void checkAndRegisterFunctions(Map<String, String> conf) {
		String packages = conf.get(ConfigConstants.EXTERNAL_FUNCTIONS);
		if (packages != null) {
			String[] splits = packages.split(",");
			for (String packageName : splits) {
				FunctionTable.findAndRegisterFunctionsWithPackageName(packageName);
				logger.info("Registering functions from package name:" + packageName);
			}
		}
	}

	private void checkAndEnableIngesters(Map<String, String> conf, Environment env) throws Exception {
		String val = conf.get("ingesters");
		ArrayList<String> list = new ArrayList<>();
		if (val != null) {
			list.addAll(Arrays.asList(val.split(",")));
		}
		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.ENABLE_GRPC, ConfigConstants.FALSE))) {
			list.add(GRPCServer.class.getCanonicalName());
		}
		for (String split : list) {
			String trim = split.trim();
			if (trim.isEmpty()) {
				continue;
			}
			Class<?> cls = Class.forName(trim);
			if (Ingester.class.isAssignableFrom(cls)) {
				final Ingester server = (Ingester) cls.newInstance();
				server.init(conf, storageEngine);
				env.lifecycle().manage(server);
			} else {
				logger.warning("Ignoring invalid ingester type:" + cls + "");
			}
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
		env.jersey().register(new SqlApi(storageEngine));
		if (Boolean.parseBoolean(conf.getOrDefault("jersey.influx", "true"))) {
			env.jersey().register(new InfluxApi(storageEngine));
		}
		env.healthChecks().register("restapi", new RestAPIHealthCheck());

		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.AUTH_BASIC_ENABLED, ConfigConstants.FALSE))) {
			logger.info("Enabling basic authentication");
			AuthFilter<BasicCredentials, Principal> basicCredentialAuthFilter = new BasicCredentialAuthFilter.Builder<>()
					.setAuthenticator(new BasicAuthenticator(conf.get(ConfigConstants.AUTH_BASIC_USERS)))
					.setAuthorizer(new AllowAllAuthorizer()).setPrefix("Basic").buildAuthFilter();
			env.jersey().register(basicCredentialAuthFilter);
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
