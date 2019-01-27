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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.srotya.sidewinder.cluster.rpc.RawTimeSeriesBucketResponse.Builder;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc.ReplicationServiceImplBase;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;
import com.srotya.sidewinder.core.storage.mem.TimeSeries;

import io.grpc.stub.StreamObserver;

/**
 * @author ambud
 */
public class ReplicationServiceImpl extends ReplicationServiceImplBase {

	private StorageEngine engine;

	public ReplicationServiceImpl(StorageEngine engine) {
		this.engine = engine;
	}

	@Override
	public void fetchTimeseriesDataAtOffset(RawTimeSeriesOffsetRequest request,
			StreamObserver<RawTimeSeriesOffsetResponse> responseObserver) {
		try {
			if (engine.checkTimeSeriesExists(request.getDbName(), request.getMeasurementName(),
					request.getValueFieldName(), new ArrayList<>(request.getTagsList()))) {
				TimeSeries timeSeries = engine.getTimeSeries(request.getDbName(), request.getMeasurementName(),
						request.getValueFieldName(), new ArrayList<>(request.getTagsList()));
				Iterator<Entry<String, TimeSeriesBucket>> itr = timeSeries
						.getSeriesBuckets(TimeUnit.MILLISECONDS, request.getBlockTimestamp()).entrySet().iterator();
				ByteBuffer rawBytes = itr.next().getValue().getWriter().getRawBytes();
				RawTimeSeriesOffsetResponse.Builder response = RawTimeSeriesOffsetResponse.newBuilder()
						.setDbName(request.getDbName()).setMeasurementName(request.getMeasurementName())
						.setValueFieldName(request.getValueFieldName()).addAllTags(request.getTagsList())
						.setOffset(request.getOffset());
				if (request.getOffset() < rawBytes.limit()) {
					rawBytes.position(request.getOffset());
					ByteString str = bufToByteString(rawBytes);
					response.setData(str);
				}
				if (itr.hasNext()) {
					response.setNextTimestamp(itr.next().getValue().getHeaderTimestamp());
				}
			}
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

	private ByteString bufToByteString(ByteBuffer rawBytes) {
		byte[] output = new byte[rawBytes.limit()];
		rawBytes.get(output);
		ByteString str = ByteString.copyFrom(output);
		return str;
	}

	@Override
	public void readTimeseriesBucket(RawTimeSeriesBucketRequest request,
			StreamObserver<RawTimeSeriesBucketResponse> responseObserver) {
		try {
			// System.out.println("Received replication request:" + request);
			if (engine.checkTimeSeriesExists(request.getDbName(), request.getMeasurementName(),
					request.getValueFieldName(), new ArrayList<>(request.getTagsList()))) {
				TimeSeries timeSeries = engine.getTimeSeries(request.getDbName(), request.getMeasurementName(),
						request.getValueFieldName(), new ArrayList<>(request.getTagsList()));
				SortedMap<String, TimeSeriesBucket> seriesBuckets = timeSeries.getSeriesBuckets(TimeUnit.MILLISECONDS,
						request.getBlockTimestamp());

				// System.out.println("Fetched buckets for timeseries:" +
				// seriesBuckets + "\t" + timeSeries);

				Builder builder = RawTimeSeriesBucketResponse.newBuilder().setDbName(request.getDbName())
						.setMeasurementName(request.getMeasurementName()).setValueFieldName(request.getValueFieldName())
						.addAllTags(request.getTagsList());
				for (Entry<String, TimeSeriesBucket> entry : seriesBuckets.entrySet()) {
					ByteBuffer rawBytes = entry.getValue().getWriter().getRawBytes();
					ByteString str = bufToByteString(rawBytes);
					builder.addBuckets(
							Bucket.newBuilder().setBucketId(entry.getKey()).setCount(entry.getValue().getCount())
									.setHeaderTimestamp(entry.getValue().getHeaderTimestamp()).setData(str).build());
				}
				responseObserver.onNext(builder.build());
				responseObserver.onCompleted();
			} else {
				// System.out.println("Series not found!");
				responseObserver.onCompleted();
			}
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

}
