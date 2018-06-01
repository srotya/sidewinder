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
package com.srotya.sidewinder.core.rpc;

import java.io.IOException;
import java.util.Set;

import com.srotya.sidewinder.core.rpc.MetaServiceGrpc.MetaServiceImplBase;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class MetadataServiceImpl extends MetaServiceImplBase {

	private StorageEngine engine;

	public MetadataServiceImpl(StorageEngine engine) {
		this.engine = engine;
	}

	@Override
	public void createDatabase(DBRequest request, StreamObserver<Ack> responseObserver) {
		try {
			engine.getOrCreateDatabase(request.getDbName());
			responseObserver.onNext(Ack.newBuilder().setMessageId(request.getMessageId()).build());
			responseObserver.onCompleted();
		} catch (IOException e) {
			responseObserver.onError(e);
		}
	}

	@Override
	public void createMeasurement(MeasurementRequest request, StreamObserver<Ack> responseObserver) {
		try {
			engine.getOrCreateMeasurement(request.getDbName(), request.getMeasurementName());
			responseObserver.onNext(Ack.newBuilder().setMessageId(request.getMessageId()).build());
			responseObserver.onCompleted();
		} catch (IOException e) {
			responseObserver.onError(e);
		}
	}

	@Override
	public void listDatabases(Null request, StreamObserver<DatabaseList> responseObserver) {
		try {
			Set<String> databases = engine.getDatabases();
			responseObserver.onNext(
					DatabaseList.newBuilder().addAllDatabases(databases).setMessageId(request.getMessageId()).build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

	@Override
	public void listMeasurements(DatabaseName request, StreamObserver<Database> responseObserver) {
		try {
			Set<String> measurements = engine.getAllMeasurementsForDb(request.getDatabaseName());
			responseObserver.onNext(Database.newBuilder().setDatabase(request.getDatabaseName())
					.addAllMeasurement(measurements).build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

}
