/**
 * Copyright 2016 Ambud Sharma
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

import java.util.HashMap;

import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.InfluxAPI;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.ingress.http.NettyHTTPIngestionServer;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.gorilla.MemStorageEngine;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author ambud
 *
 */
public class SidewinderServer extends Application<SidewinderConfig> {

	private StorageEngine storageEngine;
	private static SidewinderServer sidewinderServer;

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		storageEngine = new MemStorageEngine();
		storageEngine.configure(new HashMap<>());
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		env.jersey().register(new MeasurementOpsApi(storageEngine));
		env.jersey().register(new DatabaseOpsApi(storageEngine));
		env.jersey().register(new InfluxAPI(storageEngine));
		NettyHTTPIngestionServer server = new NettyHTTPIngestionServer();
		server.init(storageEngine, new HashMap<>());
		server.start();
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
