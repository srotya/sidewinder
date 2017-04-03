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
package com.srotya.sidewinder.cluster.rpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;

import io.grpc.DecompressorRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @author ambud
 */
public class SidewinderGRPCServer {

	private Server server;

	public void start() throws IOException {
		StorageEngine engine = new MemStorageEngine();
		ScheduledExecutorService bgTaskPool = Executors.newScheduledThreadPool(1);
		engine.configure(new HashMap<>(), bgTaskPool);
		/* The port on which the server should run */
		int port = 50051;
		server = ServerBuilder.forPort(port).decompressorRegistry(DecompressorRegistry.getDefaultInstance()).build()
				.start();
		System.out.println("Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// Use stderr here since the logger may have been reset by its
				// JVM shutdown hook.
				System.err.println("*** shutting down gRPC server since JVM is shutting down");
				SidewinderGRPCServer.this.stop();
				System.err.println("*** server shut down");
			}
		});
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		SidewinderGRPCServer rpc = new SidewinderGRPCServer();
		rpc.start();
		Thread.sleep(100000);
	}

	public void stop() {
		if (server != null) {
			server.shutdown();
		}
	}

}
