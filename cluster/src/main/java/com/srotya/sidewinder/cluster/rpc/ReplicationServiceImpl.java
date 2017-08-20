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
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import com.srotya.sidewinder.cluster.rpc.ListTimeseriesOffsetResponse.OffsetEntry;
import com.srotya.sidewinder.cluster.rpc.ListTimeseriesOffsetResponse.OffsetEntry.Bucket;
import com.srotya.sidewinder.cluster.rpc.ReplicationServiceGrpc.ReplicationServiceImplBase;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.TimeSeries;
import com.srotya.sidewinder.core.storage.TimeSeriesBucket;

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
	public void listBatchTimeseriesOffsetList(ListTimeseriesOffsetRequest request,
			StreamObserver<ListTimeseriesOffsetResponse> responseObserver) {
		String dbName = request.getDbName();
		String measurementName = request.getMeasurementName();
		try {
			Measurement measurement = engine.getOrCreateMeasurement(dbName, measurementName);

			ListTimeseriesOffsetResponse.Builder builder = ListTimeseriesOffsetResponse.newBuilder();
			for (TimeSeries timeSeries : measurement.getTimeSeries()) {
				OffsetEntry.Builder offsetBuilder = OffsetEntry.newBuilder();

				String seriesId = timeSeries.getSeriesId();
				String[] keys = seriesId.split(Measurement.FIELD_TAG_SEPARATOR);
				String valueFieldName = keys[0];

				List<String> tags = Measurement.decodeStringToTags(measurement.getTagIndex(), keys[1]);
				offsetBuilder.setValueFieldName(valueFieldName);
				offsetBuilder.addAllTags(tags);

				SortedMap<String, List<TimeSeriesBucket>> bucketRawMap = timeSeries.getBucketRawMap();
				for (Entry<String, List<TimeSeriesBucket>> entry : bucketRawMap.entrySet()) {
					for (int i = 0; i < entry.getValue().size(); i++) {
						TimeSeriesBucket timeSeriesBucket = entry.getValue().get(i);
						OffsetEntry.Bucket.Builder bucket = Bucket.newBuilder();
						bucket.setIndex(i);
						bucket.setOffset(timeSeriesBucket.getWriter().currentOffset());
						bucket.setBucketTs(entry.getKey());
						offsetBuilder.addBuckets(bucket.build());
					}
				}
				builder.addEntries(offsetBuilder.build());
			}
			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			responseObserver.onError(e);
		}
	}

	@Override
	public void batchFetchTimeseriesDataAtOffset(BatchRawTimeSeriesOffsetRequest request,
			StreamObserver<BatchRawTimeSeriesOffsetResponse> responseObserver) {
		BatchRawTimeSeriesOffsetResponse.Builder rawBuilder = BatchRawTimeSeriesOffsetResponse.newBuilder();
		List<RawTimeSeriesOffsetRequest> requestsList = request.getRequestsList();
		for (RawTimeSeriesOffsetRequest rawRequest : requestsList) {
			RawTimeSeriesOffsetResponse.Builder builder = RawTimeSeriesOffsetResponse.newBuilder();

			String dbName = rawRequest.getDbName();
			String measurementName = rawRequest.getMeasurementName();
			String valueFieldName = rawRequest.getValueFieldName();
			ProtocolStringList tags = rawRequest.getTagsList();
			long blockTimestamp = rawRequest.getBlockTimestamp();
			int index = rawRequest.getIndex();
			int offset = rawRequest.getOffset();
			int bucketSize = rawRequest.getBucketSize();
			try {
				TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, valueFieldName,
						new ArrayList<>(tags));
				String timeBucket = TimeSeries.getTimeBucket(TimeUnit.MILLISECONDS, blockTimestamp, bucketSize);
				SortedMap<String, List<TimeSeriesBucket>> bucketRawMap = timeSeries.getBucketRawMap();
				List<TimeSeriesBucket> list = bucketRawMap.get(timeBucket);
				TimeSeriesBucket timeSeriesBucket = list.get(index);
				builder.setBlockTimestamp(blockTimestamp);
				builder.setDbName(dbName);
				builder.setMeasurementName(measurementName);
				builder.setBucketSize(bucketSize);
				builder.setMessageId(request.getMessageId());
				builder.setFp(timeSeries.isFp());
				builder.setValueFieldName(valueFieldName);
				builder.addAllTags(tags);
				builder.setIndex(index);
				
				builder.setCount(timeSeriesBucket.getCount());
				ByteBuffer buf = timeSeriesBucket.getWriter().getRawBytes();
				buf.position(offset);
				builder.setData(ByteString.copyFrom(buf));
				rawBuilder.addResponses(builder.build());
			} catch (Exception e) {
				e.printStackTrace();
				responseObserver.onError(e);
			}
		}
		responseObserver.onNext(rawBuilder.build());
		responseObserver.onCompleted();
	}

	@Override
	public void fetchTimeseriesDataAtOffset(RawTimeSeriesOffsetRequest request,
			StreamObserver<RawTimeSeriesOffsetResponse> responseObserver) {
		RawTimeSeriesOffsetResponse.Builder builder = RawTimeSeriesOffsetResponse.newBuilder();

		String dbName = request.getDbName();
		String measurementName = request.getMeasurementName();
		String valueFieldName = request.getValueFieldName();
		ProtocolStringList tags = request.getTagsList();
		long blockTimestamp = request.getBlockTimestamp();
		int index = request.getIndex();
		int offset = request.getOffset();
		int bucketSize = request.getBucketSize();
		try {
			TimeSeries timeSeries = engine.getTimeSeries(dbName, measurementName, valueFieldName,
					new ArrayList<>(tags));
			String timeBucket = TimeSeries.getTimeBucket(TimeUnit.MILLISECONDS, blockTimestamp, bucketSize);
			SortedMap<String, List<TimeSeriesBucket>> bucketRawMap = timeSeries.getBucketRawMap();
			List<TimeSeriesBucket> list = bucketRawMap.get(timeBucket);
			TimeSeriesBucket timeSeriesBucket = list.get(index);

			builder.setBlockTimestamp(blockTimestamp);
			builder.setDbName(dbName);
			builder.setMeasurementName(measurementName);
			builder.setBucketSize(bucketSize);
			builder.setMessageId(request.getMessageId());
			builder.setFp(timeSeries.isFp());
			builder.setValueFieldName(valueFieldName);
			builder.addAllTags(tags);
			builder.setIndex(index);
			builder.setCount(timeSeriesBucket.getCount());
			ByteBuffer buf = timeSeriesBucket.getWriter().getRawBytes();
			buf.position(offset);
			builder.setData(ByteString.copyFrom(buf));

			responseObserver.onNext(builder.build());
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
			responseObserver.onError(e);
		}
	}

}
