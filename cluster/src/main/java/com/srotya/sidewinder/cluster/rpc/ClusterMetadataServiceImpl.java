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

import java.util.Map;

import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.ClusterMetadataServiceGrpc.ClusterMetadataServiceImplBase;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.SingleData;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class ClusterMetadataServiceImpl extends ClusterMetadataServiceImplBase {

	private RoutingEngine router;

	public ClusterMetadataServiceImpl(RoutingEngine router, Map<String, String> conf) {
		this.router = router;
	}

	@Override
	public void requestRouteTableEntry(SingleData request, StreamObserver<Ack> responseObserver) {
		router.addRoutableKey(request.getPoint(), 3);
		responseObserver.onNext(Ack.newBuilder().setMessageId(request.getMessageId()).setResponseCode(200).build());
		responseObserver.onCompleted();
	}

}
