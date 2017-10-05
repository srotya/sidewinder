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
package com.srotya.sidewinder.cluster.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rpc.DataRequest;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.utils.HTTPDataPointDecoder;

/**
 * @author ambud
 *
 */
@Path("/influx")
public class InfluxApi {

	private Meter meter;
	private WALManager mgr;
	private static AtomicInteger counter = new AtomicInteger(0);

	public InfluxApi(WALManager mgr, MetricRegistry registry, Map<String, String> conf) {
		this.mgr = mgr;
		meter = registry.meter("writes");
	}

	@POST
	@Consumes({ MediaType.TEXT_PLAIN })
	public void insertData(@QueryParam("db") String dbName, String payload) {
		if (payload == null) {
			throw new BadRequestException("Empty request no acceptable");
		}
		List<Point> dps = HTTPDataPointDecoder.pointsFromString(dbName, payload);
		counter.addAndGet(dps.size());
		if (dps.isEmpty()) {
			throw new BadRequestException("Empty request no acceptable");
		}
		Map<String, List<Point>> measurementMap = new HashMap<>();
		for (Point dp : dps) {
			List<Point> list = measurementMap.get(pointToRouteKey(dp));
			if (list == null) {
				list = new ArrayList<>();
				measurementMap.put(pointToRouteKey(dp), list);
			}
			list.add(dp);
		}

		for (Entry<String, List<Point>> entry : measurementMap.entrySet()) {
			String replicaLeader = mgr.getReplicaLeader(entry.getKey());
			// add route key
			if (replicaLeader == null) {
				replicaLeader = requestRouteKey(mgr, entry.getKey());
				if (replicaLeader == null) {
					continue;
				}
			}
			Node node = mgr.getNodeMap().get(replicaLeader);
			ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(node.getChannel());
			byte[] ary = BatchData.newBuilder().setMessageId(System.nanoTime()).addAllPoints(entry.getValue()).build()
					.toByteArray();
			GenericResponse writeData = stub.writeData(
					DataRequest.newBuilder().setData(ByteString.copyFrom(ary)).setRouteKey(entry.getKey()).build());
			try {
				BatchData.parseFrom(ary);
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (writeData.getResponseCode() != 200) {
				System.out.println("Failed to write:" + writeData.getResponseString());
			}
			// else {
			// System.out.println("Wrote data:" + entry.getKey() + " to node:" +
			// node.getNodeKey());
			// }
		}
		// System.out.println("Requests received:" + counter.get());
		meter.mark(dps.size());
	}

	private String requestRouteKey(WALManager mgr, String key) {
		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(mgr.getCoordinator().getChannel());
		RouteResponse route = stub.addRoute(RouteRequest.newBuilder().setRouteKey(key).setReplicationFactor(2).build());
		if (route.getResponseCode() != 200) {
			System.out.println("Route add failed:" + route.getResponseString() + " for key:" + key);
			return null;
		} else {
			return route.getLeaderid();
		}
	}

	public static String pointToRouteKey(Point dp) {
		return dp.getDbName() + "#" + dp.getMeasurementName();
	}

}
