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
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;

import com.codahale.metrics.health.HealthCheck;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.SqlApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.ingress.binary.NettyBinaryIngestionServer;
import com.srotya.sidewinder.core.ingress.http.NettyHTTPIngestionServer;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * Main driver class for single-node sidewinder. It will register the REST APIs,
 * initialize the storage engine as well as start the network receivers for HTTP and binary protocols
 * for Sidewinder.
 * 
 * @author ambud
 *
 */
public class SidewinderServer extends Application<SidewinderConfig> {

	private static final Logger logger = Logger.getLogger(SidewinderServer.class.getName());
	private StorageEngine storageEngine;
	private static SidewinderServer sidewinderServer;

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		HashMap<String, String> conf = new HashMap<>();
		String path = config.getConfigPath();
		if (path != null) {
			Properties props = new Properties();
			props.load(new FileInputStream(path));
			for (final String name : props.stringPropertyNames()) {
				conf.put(name, props.getProperty(name));
			}
		}

		String storageEngineClass = conf.getOrDefault("storage.engine",
				"com.srotya.sidewinder.core.storage.mem.MemStorageEngine");

		logger.info("Using Storage Engine:" + storageEngineClass);

		storageEngine = (StorageEngine) Class.forName(storageEngineClass).newInstance();
		storageEngine.configure(conf);
		storageEngine.connect();
		env.lifecycle().manage(storageEngine);
		ResourceMonitor.getInstance().init(storageEngine);
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		env.jersey().register(new MeasurementOpsApi(storageEngine));
		env.jersey().register(new DatabaseOpsApi(storageEngine));
		env.jersey().register(new SqlApi(storageEngine));
		env.healthChecks().register("server", new HealthCheck() {

			@Override
			protected Result check() throws Exception {
				return Result.healthy();
			}
		});
		NettyHTTPIngestionServer server = new NettyHTTPIngestionServer();
		server.init(storageEngine, conf);
		server.start();

		NettyBinaryIngestionServer binServer = new NettyBinaryIngestionServer();
		binServer.init(storageEngine, conf);
		binServer.start();
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
