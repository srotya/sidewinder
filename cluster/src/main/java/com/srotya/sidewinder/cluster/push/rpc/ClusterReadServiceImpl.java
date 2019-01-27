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
package com.srotya.sidewinder.cluster.push.rpc;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc.ClusterReadServiceImplBase;
import com.srotya.sidewinder.cluster.rpc.Query;
import com.srotya.sidewinder.cluster.rpc.QueryResponses;

import io.grpc.stub.StreamObserver;

public class ClusterReadServiceImpl extends ClusterReadServiceImplBase {

	private RoutingEngine engine;
	private boolean repairOnRead;

	public ClusterReadServiceImpl(Map<String, String> conf, RoutingEngine engine) {
		this.engine = engine;
		this.repairOnRead = Boolean.parseBoolean(conf.getOrDefault("cluster.push.repairOnRead", "true"));
	}

	@Override
	public void queryData(Query request, StreamObserver<QueryResponses> responseObserver) {
		try {
			List<Node> nodes = engine.routeQuery(request);
			if (repairOnRead) {
				Map<Node, QueryResponses> queryResponses = nodes.stream().parallel().map(n -> {
					Query query = Query.newBuilder(request).setDirect(true).build();
					try {
						QueryResponses response = n.getEndpointService().readData(query);
						return new AbstractMap.SimpleEntry<Node, QueryResponses>(n, response);
					} catch (Exception e) {
						return null;
					}
				}).filter(v->v!=null).collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
				Node node = nodes.get(0);
				QueryResponses masterData = queryResponses.get(node);
				for (Entry<Node, QueryResponses> entry : queryResponses.entrySet()) {
				}
			} else {
				// if read repair is disabled, simply query the current leader
				Node node = nodes.get(0);
				QueryResponses response = node.getEndpointService().readData(request);
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}

		} catch (IOException e) {
			responseObserver.onError(e);
		}
	}

}
