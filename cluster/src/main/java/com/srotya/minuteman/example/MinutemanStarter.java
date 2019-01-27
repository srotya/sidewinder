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
package com.srotya.minuteman.example;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.cluster.WALManagerImpl;
import com.srotya.minuteman.connectors.AtomixConnector;
import com.srotya.minuteman.connectors.ClusterConnector;
import com.srotya.minuteman.rpc.DataRequest;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.utils.FileUtils;
import com.srotya.minuteman.wal.LocalWALClient;
import com.srotya.minuteman.wal.WAL;
import com.srotya.minuteman.wal.WALClient;

public class MinutemanStarter {

	public static void main(String[] args) throws Exception {
		String string = "helodasewrwermfjfsf9s90fasfweknwqjnqwnerqwher8werjwnerknwerkwlejrklwjrjwlkjijoiwerwerwer343tdasd";
		for (int i = 0; i < 6; i++) {
			string += string;
		}
		System.out.println("Message size:" + string.length());
		// System.exit(0);
		FileUtils.delete(new File("target/node" + args[0]));
		ClusterConnector connector;
		Map<String, String> conf = new HashMap<>();
		conf.put("cluster.atomix.port", args[0]);
		conf.put("cluster.atomix.bootstrap", args[1]);
		conf.put("cluster.grpc.port", args[2]);
		conf.put(WAL.WAL_ISR_THRESHOLD, String.valueOf(1024 * 1024 * 64));
		conf.put(WALClient.MAX_FETCH_BYTES, String.valueOf(1024 * 1024 * 2));
		conf.put(LocalWALClient.WAL_LOCAL_READ_MODE, LocalWALClient.COMMITTED);
		conf.put("wal.dir", "target/node" + args[0]);
		try {
			connector = new AtomixConnector();
			connector.init(conf);
		} catch (Exception e) {
			throw new IOException(e);
		}

		ScheduledExecutorService es = Executors.newScheduledThreadPool(1);

		WALManager walManager = new WALManagerImpl();
		walManager.init(conf, connector, es, null);
		System.out.print("Please enter route key name:");
		Scanner sc = new Scanner(System.in);
		if (sc.hasNext()) {
			String nextLine = sc.nextLine();
			System.out.println("Add new wal to the system:" + nextLine);
			System.out.print("Enter replication factor:");
			String leaderId = connector.requestNewRoute(nextLine, sc.nextInt());
			Node node = walManager.getNodeMap().get(leaderId);
			ReplicationServiceBlockingStub ch = ReplicationServiceGrpc.newBlockingStub(node.getChannel());
			long ts = System.currentTimeMillis();

			for (long i = 0; i < 10_000_000; i++) {
				GenericResponse response = ch.writeData(DataRequest.newBuilder().setRouteKey(nextLine)
						.setData(ByteString.copyFrom((string + i).getBytes())).build());
				if (response.getResponseCode() != 200) {
					System.out.println(response.getResponseString() + "\t" + i);
				}
				if (i % 10000 == 0) {
					ts = System.currentTimeMillis() - ts;
					System.out.println("written 10k:" + string.length() * i + " bytes ts:" + "\t" + ts / 1000);
					ts = System.currentTimeMillis();
					Thread.sleep(500);
				}
			}
		}
		sc.close();
	}

}
