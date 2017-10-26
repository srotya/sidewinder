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
package com.srotya.sidewinder.core.rpc;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.TimeoutException;
import com.srotya.sidewinder.core.ConfigConstants;
import com.srotya.sidewinder.core.external.Ingester;
import com.srotya.sidewinder.core.security.BasicAuthenticator;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

import io.grpc.DecompressorRegistry;
import io.grpc.ServerInterceptors;
import io.grpc.internal.ServerImpl;
import io.grpc.netty.NettyServerBuilder;

public class GRPCServer extends Ingester {

	private static final Logger logger = Logger.getLogger(GRPCServer.class.getName());
	private NettyServerBuilder serverBuilder;
	private ServerImpl server;
	private int threadCount;
	private WriterServiceImpl writer;
	private ExecutorService es;

	@Override
	public void start() throws Exception {
		es = Executors.newFixedThreadPool(threadCount, new BackgrounThreadFactory("grpc-threads"));
		serverBuilder = serverBuilder.executor(es).maxMessageSize(10485760);
		server = serverBuilder.build().start();
		logger.info("Starting GRPC server");
	}

	@Override
	public void stop() throws Exception {
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

	@Override
	public void init(Map<String, String> conf, StorageEngine storageEngine) {
		threadCount = Integer.parseInt(
				conf.getOrDefault(ConfigConstants.GRPC_EXECUTOR_COUNT, ConfigConstants.DEFAULT_GRPC_EXECUTOR_COUNT));
		writer = new WriterServiceImpl(storageEngine, conf);
		serverBuilder = NettyServerBuilder
				.forPort(Integer.parseInt(conf.getOrDefault(ConfigConstants.GRPC_PORT, "9928")))
				.decompressorRegistry(DecompressorRegistry.getDefaultInstance());
		// enable GRPC authentication
		if (Boolean.parseBoolean(conf.getOrDefault(ConfigConstants.AUTH_BASIC_ENABLED, ConfigConstants.FALSE))) {
			serverBuilder.addService(ServerInterceptors.intercept(writer,
					new BasicAuthenticator(conf.get(ConfigConstants.AUTH_BASIC_USERS))));
		} else {
			serverBuilder.addService(writer);
		}
		logger.info("Configured GRPC server");
	}

}
