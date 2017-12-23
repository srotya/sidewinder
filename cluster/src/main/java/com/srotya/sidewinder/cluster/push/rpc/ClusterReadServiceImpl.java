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
package com.srotya.sidewinder.cluster.push.rpc;

import java.io.IOException;
import java.util.List;

import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.Query;
import com.srotya.sidewinder.cluster.rpc.QueryResponse;
import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc.ClusterReadServiceImplBase;

import io.grpc.stub.StreamObserver;

public class ClusterReadServiceImpl extends ClusterReadServiceImplBase {
	
	private RoutingEngine engine;

	public ClusterReadServiceImpl(RoutingEngine engine) {
		this.engine = engine;
	}
	
	@Override
	public void queryData(Query request, StreamObserver<QueryResponse> responseObserver) {
		try {
			List<Node> routeQuery = engine.routeQuery(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
