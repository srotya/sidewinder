/**
 * Copyright Ambud Sharma
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
import java.util.logging.Logger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.Node;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rpc.DataRequest;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceBlockingStub;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;
import com.srotya.sidewinder.cluster.Utils;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.InfluxDecoder;

/**
 * @author ambud
 *
 */
@Path("/influx")
public class InfluxApi {

	private static final Logger logger = Logger.getLogger(InfluxApi.class.getName());
	private Meter meter;
	private WALManager mgr;
	private static AtomicInteger counter = new AtomicInteger(0);

	public InfluxApi(WALManager mgr, StorageEngine engine, Map<String, String> conf) {
		this.mgr = mgr;
		MetricRegistry registry = MetricsRegistryService.getInstance().getInstance("requests");
		meter = registry.meter("writes");
	}

	@POST
	@Consumes({ MediaType.TEXT_PLAIN })
	public void insertData(@QueryParam("db") String dbName,
			@QueryParam("presorted") @DefaultValue("false") String presortedParam,
			@QueryParam("replica") @DefaultValue(value = "1") String replicaParam, String payload) {
		boolean presorted = Boolean.parseBoolean(presortedParam);
		int replica = Integer.parseInt(replicaParam);
		if (payload == null) {
			throw new BadRequestException("Empty request no acceptable");
		}
		List<Point> dps = InfluxDecoder.pointsFromString(dbName, payload);
		counter.addAndGet(dps.size());
		if (dps.isEmpty()) {
			throw new BadRequestException("Empty request no acceptable");
		}
		Map<Integer, List<Point>> shardMap = new HashMap<>();

		for (Point dp : dps) {
			Integer pointToRouteKey = Utils.pointToRouteKey(dp);
			List<Point> list = shardMap.get(pointToRouteKey);
			if (list == null) {
				list = new ArrayList<>();
				shardMap.put(pointToRouteKey, list);
			}
			list.add(dp);
		}

		for (Entry<Integer, List<Point>> entry : shardMap.entrySet()) {
			Integer replicaLeader = mgr.getReplicaLeader(entry.getKey());
			// add route key
			if (replicaLeader == null) {
				replicaLeader = requestRouteKey(mgr, entry.getKey(), replica);
				if (replicaLeader == null) {
					continue;
				}
			}
			byte[] ary = BatchData.newBuilder().addAllPoints(entry.getValue()).build().toByteArray();
			if (replicaLeader.equals(mgr.getThisNodeKey())) {
				try {
					mgr.getWAL(entry.getKey()).write(ary, false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				DataRequest dataRequest = DataRequest.newBuilder().setData(ByteString.copyFrom(ary))
						.setRouteKey(entry.getKey()).build();
				Node node = mgr.getStrategy().get(replicaLeader);
				ReplicationServiceBlockingStub rpc = ReplicationServiceGrpc.newBlockingStub(node.getChannel());
				GenericResponse response = rpc.writeData(dataRequest);
				if (response.getResponseCode() != 200) {
					logger.severe("Error writing data");
				}
			}
		}
		meter.mark(dps.size());
	}

	private Integer requestRouteKey(WALManager mgr, Integer key, int replicationFactor) {
		ReplicationServiceBlockingStub stub = ReplicationServiceGrpc.newBlockingStub(mgr.getCoordinator().getChannel());
		RouteResponse route = stub
				.addRoute(RouteRequest.newBuilder().setRouteKey(key).setReplicationFactor(replicationFactor).build());
		if (route.getResponseCode() != 200) {
			logger.severe("Route add failed:" + route.getResponseString() + " for key:" + key);
			return null;
		} else {
			return route.getLeaderid();
		}
	}
}