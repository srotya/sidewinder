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
package com.srotya.sidewinder.core.rpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceImplBase;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.Writer;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;
import com.srotya.sidewinder.core.utils.MiscUtils;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class WriterServiceImpl extends WriterServiceImplBase {

	private StorageEngine engine;
	private Map<String, String> conf;

	public WriterServiceImpl(StorageEngine engine, Map<String, String> conf) {
		this.engine = engine;
		this.conf = conf;
	}

	@Override
	public void writeSingleDataPoint(SingleData request, StreamObserver<Ack> responseObserver) {
		Point point = request.getPoint();
		try {
			DataPoint dp = MiscUtils.pointToDataPoint(point);
			engine.writeDataPoint(dp);
			Ack ack = Ack.newBuilder().setMessageId(request.getMessageId()).build();
			responseObserver.onNext(ack);
			responseObserver.onCompleted();
		} catch (IOException e) {
			responseObserver.onError(e);
		}
	}

	@Override
	public void writeBatchDataPoint(BatchData request, StreamObserver<Ack> responseObserver) {
		try {
			for (Point point : request.getPointsList()) {
				DataPoint dp = MiscUtils.pointToDataPoint(point);
				engine.writeDataPoint(dp);
			}
			Ack ack = Ack.newBuilder().setMessageId(request.getMessageId()).build();
			responseObserver.onNext(ack);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
	}

	@Override
	public void writeSeriesPoint(RawTimeSeriesBucket request, StreamObserver<Ack> responseObserver) {
		try {
			TimeSeries series = engine.getOrCreateTimeSeries(request.getDbName(), request.getMeasurementName(),
					request.getValueFieldName(), new ArrayList<>(request.getTagsList()), request.getBucketSize(),
					request.getFp());
			for (Bucket bucket : request.getBucketsList()) {
				TimeSeriesBucket tsb = series.getOrCreateSeriesBucket(TimeUnit.MILLISECONDS,
						bucket.getHeaderTimestamp());
				Writer writer = tsb.getWriter();
				writer.configure(conf);
				writer.setCounter(bucket.getCount());
				writer.bootstrap(bucket.getData().asReadOnlyByteBuffer());
			}
			Ack ack = Ack.newBuilder().setMessageId(request.getMessageId()).build();
			responseObserver.onNext(ack);
			responseObserver.onCompleted();
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

}