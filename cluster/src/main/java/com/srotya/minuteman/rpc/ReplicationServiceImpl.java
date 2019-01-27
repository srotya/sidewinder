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
package com.srotya.minuteman.rpc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.srotya.minuteman.cluster.Replica;
import com.srotya.minuteman.cluster.WALManager;
import com.srotya.minuteman.rpc.BatchDataRequest;
import com.srotya.minuteman.rpc.BatchDataResponse;
import com.srotya.minuteman.rpc.DataRequest;
import com.srotya.minuteman.rpc.GenericResponse;
import com.srotya.minuteman.rpc.ReplicaRequest;
import com.srotya.minuteman.rpc.RouteRequest;
import com.srotya.minuteman.rpc.RouteResponse;
import com.srotya.minuteman.rpc.BatchDataResponse.Builder;
import com.srotya.minuteman.rpc.ReplicationServiceGrpc.ReplicationServiceImplBase;
import com.srotya.minuteman.wal.WAL;
import com.srotya.minuteman.wal.WALRead;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class ReplicationServiceImpl extends ReplicationServiceImplBase {

	private static final Logger logger = Logger.getLogger(ReplicationServiceImpl.class.getName());
	private WALManager mgr;

	public ReplicationServiceImpl(WALManager mgr) {
		this.mgr = mgr;
	}

	@Override
	public void requestBatchReplication(BatchDataRequest request, StreamObserver<BatchDataResponse> responseObserver) {
		Builder builder = BatchDataResponse.newBuilder();
		try {
			WAL wal = mgr.getWAL(request.getRouteKey());
			if (wal != null) {
				WALRead read = wal.read(request.getNodeId(), request.getOffset(), request.getMaxBytes(), false);
				builder.setNextOffset(read.getNextOffset()).setCommitOffset(read.getCommitOffset());
				if (read.getData() != null) {
					for (byte[] data : read.getData()) {
						builder.addData(ByteString.copyFrom(data));
					}
				}
				responseObserver.onNext(builder.build());
			} else {
				builder.setNextOffset(-1).setResponseCode(404);
				responseObserver.onNext(builder.build());
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Failed request to add new route key:" + request.getRouteKey(), e);
			responseObserver.onNext(builder.setResponseCode(500).build());
		}
		responseObserver.onCompleted();
	}

	@Override
	public void addRoute(RouteRequest request, StreamObserver<RouteResponse> responseObserver) {
		com.srotya.minuteman.rpc.RouteResponse.Builder builder = RouteResponse.newBuilder();
		try {
			logger.info("Request to add new route key and compute routes for it:" + request.getRouteKey());
			List<Replica> replicas = mgr.addRoutableKey(request.getRouteKey(), request.getReplicationFactor());
			List<String> collect = replicas.stream().map(r -> r.getReplicaNodeKey()).collect(Collectors.toList());
			responseObserver.onNext(builder.addAllReplicaids(collect).setLeaderid(replicas.get(0).getLeaderNodeKey())
					.setResponseCode(200).setResponseString("Successful").build());
		} catch (Exception e) {
			logger.log(Level.SEVERE,
					"Failed request to add new route key:" + request.getRouteKey() + " reason:" + e.getMessage());
			logger.log(Level.FINE, "Route request failure reason for key:" + request.getRouteKey(), e);
			responseObserver.onNext(builder.setResponseCode(500).setResponseString(e.getMessage()).build());
		}
		responseObserver.onCompleted();
	}

	@Override
	public void addReplica(ReplicaRequest request, StreamObserver<GenericResponse> responseObserver) {
		try {
			Replica replica = new Replica();
			replica.setLeaderAddress(request.getLeaderAddress());
			replica.setLeaderPort(request.getLeaderPort());
			replica.setReplicaAddress(request.getReplicaAddress());
			replica.setReplicaPort(request.getReplicaPort());
			replica.setRouteKey(request.getRouteKey());
			logger.info("Request to add replica:" + request.getRouteKey());
			mgr.replicaUpdated(replica);
			responseObserver
					.onNext(GenericResponse.newBuilder().setResponseCode(200).setResponseString("Success!").build());
		} catch (Exception e) {
			e.printStackTrace();
			responseObserver.onNext(GenericResponse.newBuilder().setResponseCode(500)
					.setResponseString("Failed to add replica:" + e.getMessage()).build());
		}
		logger.info("Request to add replica COMPLETED:" + request.getRouteKey());
		responseObserver.onCompleted();
	}

	@Override
	public void writeData(DataRequest request, StreamObserver<GenericResponse> responseObserver) {
		try {
			WAL wal = mgr.getWAL(request.getRouteKey());
			if (wal != null) {
				wal.write(request.getData().toByteArray(), false);
				responseObserver.onNext(
						GenericResponse.newBuilder().setResponseCode(200).setResponseString("Success!").build());
			} else {
				responseObserver
						.onNext(GenericResponse.newBuilder().setResponseCode(400)
								.setResponseString(
										"Wal not found on node:" + request.getRouteKey() + " " + mgr.getThisNodeKey())
								.build());
			}
		} catch (Exception e) {
			e.printStackTrace();
			responseObserver.onNext(GenericResponse.newBuilder().setResponseCode(500)
					.setResponseString("Failed to write data:" + e.getMessage()).build());
		}
		responseObserver.onCompleted();
	}

	@Override
	public void updateIsr(IsrUpdateRequest request, StreamObserver<GenericResponse> responseObserver) {
		String routeKey = request.getRouteKey();
		Map<String, Boolean> isrUpdateMap = request.getIsrMapMap();
		com.srotya.minuteman.rpc.GenericResponse.Builder builder = GenericResponse.newBuilder();
		try {
			mgr.updateReplicaIsrStatus(routeKey, isrUpdateMap);
			builder.setResponseCode(200);
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, "Failed to update ISR status for coordinator", e);
			builder.setResponseCode(500);
			builder.setResponseString(e.getMessage());
		}
		responseObserver.onNext(builder.build());
		responseObserver.onCompleted();
	}
}
