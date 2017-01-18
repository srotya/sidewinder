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

import java.io.IOException;
import java.util.HashMap;

import com.srotya.sidewinder.cluster.storage.ClusteredMemStorageEngine;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.api.MeasurementOpsApi;
import com.srotya.sidewinder.core.api.grafana.GrafanaQueryApi;
import com.srotya.sidewinder.core.ingress.binary.NettyBinaryIngestionServer;
import com.srotya.sidewinder.core.ingress.http.NettyHTTPIngestionServer;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author ambud
 */
public class SidewinderClusteredServer extends Application<SidewinderConfig> {

	private StorageEngine storageEngine;
	private static SidewinderClusteredServer sidewinderServer;

	@Override
	public void run(SidewinderConfig config, Environment env) throws Exception {
		storageEngine = new ClusteredMemStorageEngine();
		NettyHTTPIngestionServer server = new NettyHTTPIngestionServer();
		server.init(storageEngine, new HashMap<>());
		server.start();
		
		NettyBinaryIngestionServer binServer = new NettyBinaryIngestionServer();
		binServer.init(storageEngine, new HashMap<>());
		binServer.start();
		
		storageEngine.configure(new HashMap<>());
		storageEngine.connect();
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				try {
					storageEngine.disconnect();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		env.jersey().register(new GrafanaQueryApi(storageEngine));
		env.jersey().register(new MeasurementOpsApi(storageEngine));
		env.jersey().register(new DatabaseOpsApi(storageEngine));
	}

	/**
	 * Main method to launch dropwizard app
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		sidewinderServer = new SidewinderClusteredServer();
		sidewinderServer.run(args);
	}

	/**
	 * @return
	 */
	public static SidewinderClusteredServer getSidewinderServer() {
		return sidewinderServer;
	}

	/**
	 * @return
	 */
	public StorageEngine getStorageEngine() {
		return storageEngine;
	}

}
