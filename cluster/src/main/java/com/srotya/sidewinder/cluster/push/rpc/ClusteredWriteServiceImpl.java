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
import java.util.Map;
import java.util.logging.Logger;

import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.cluster.rpc.ClusteredWriteServiceGrpc.ClusteredWriteServiceImplBase;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.BatchData;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.SingleData;
import com.srotya.sidewinder.core.storage.RejectException;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class ClusteredWriteServiceImpl extends ClusteredWriteServiceImplBase {

	private static final Logger logger = Logger.getLogger(ClusteredWriteServiceImpl.class.getName());
	private RoutingEngine router;

	public ClusteredWriteServiceImpl(RoutingEngine router, Map<String, String> conf) {
		this.router = router;
	}

	@Override
	public void writeBatchDataPoint(BatchData request, StreamObserver<Ack> responseObserver) {
		List<Point> points = request.getPointsList();
		for (Point point : points) {
			try {
				routeWriteAndReplicate(point);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		responseObserver.onNext(Ack.newBuilder().setMessageId(request.getMessageId()).build());
		responseObserver.onCompleted();
	}

	@Override
	public void writeSingleDataPoint(SingleData request, StreamObserver<Ack> responseObserver) {
		Point point = request.getPoint();
		try {
			routeWriteAndReplicate(point);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		responseObserver.onNext(Ack.newBuilder().setMessageId(request.getMessageId()).build());
		responseObserver.onCompleted();
	}

	private void routeWriteAndReplicate(Point point) throws IOException, InterruptedException {
		List<Node> nodes = router.routeData(point);
		for (Node node : nodes) {
			try {
				node.getEndpointService().write(point);
			} catch (IOException e) {
				if (!(e instanceof RejectException)) {
					logger.severe("Data point rejected:" + point.getDbName() + ":" + point.getMeasurementName() + " "
							+ node.getAddress() + ":" + node.getPort());
				} else {
					break;
				}
			}
		}
	}

}
