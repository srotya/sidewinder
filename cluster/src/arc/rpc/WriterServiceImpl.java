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
package com.srotya.sidewinder.cluster.rpc;

import java.io.IOException;
import java.util.ArrayList;

import com.srotya.sidewinder.cluster.rpc.WriterServiceGrpc.WriterServiceImplBase;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class WriterServiceImpl extends WriterServiceImplBase {

	private StorageEngine engine;

	public WriterServiceImpl(StorageEngine engine) {
		this.engine = engine;
	}

	@Override
	public void writeSingleDataPoint(SingleData request, StreamObserver<Ack> responseObserver) {
		Point point = request.getPoint();
		try {
			DataPoint dp = PointUtils.pointToDataPoint(point);
			// System.out.println("Logging point:" + dp);
			engine.writeDataPoint(dp);
			Ack ack = Ack.newBuilder().setMessageId(request.getMessageId()).build();
			responseObserver.onNext(ack);
			responseObserver.onCompleted();
		} catch (IOException e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
	}

	@Override
	public void writeBatchDataPoint(BatchData request, StreamObserver<Ack> responseObserver) {
		try {
			for (Point point : request.getPointsList()) {
				DataPoint dp = PointUtils.pointToDataPoint(point);
				engine.writeDataPoint(dp);
			}
			Ack ack = Ack.newBuilder().setMessageId(request.getMessageId()).build();
			responseObserver.onNext(ack);
			responseObserver.onCompleted();
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

	public static class PointUtils {

		private PointUtils() {
		}

		public static DataPoint pointToDataPoint(Point point) {
			DataPoint dp = new DataPoint();
			dp.setDbName(point.getDbName());
			dp.setFp(point.getFp());
			dp.setLongValue(point.getValue());
			dp.setMeasurementName(point.getMeasurementName());
			dp.setTags(new ArrayList<>(point.getTagsList()));
			dp.setTimestamp(point.getTimestamp());
			dp.setValueFieldName(point.getValueFieldName());
			return dp;
		}

	}

}
