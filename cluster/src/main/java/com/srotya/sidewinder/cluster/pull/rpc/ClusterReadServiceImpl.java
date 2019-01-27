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
package com.srotya.sidewinder.cluster.pull.rpc;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.NotFoundException;

import com.srotya.sidewinder.cluster.rpc.ClusterReadServiceGrpc.ClusterReadServiceImplBase;
import com.srotya.sidewinder.cluster.rpc.DataObject;
import com.srotya.sidewinder.cluster.rpc.DeltaRequest;
import com.srotya.sidewinder.cluster.rpc.DeltaResponse;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.StorageEngine;

import io.grpc.stub.StreamObserver;

public class ClusterReadServiceImpl extends ClusterReadServiceImplBase {

	private static final Logger logger = Logger.getLogger(ClusterReadServiceImpl.class.getName());
	private StorageEngine engine;
	private Map<String, String> conf;

	public ClusterReadServiceImpl(Map<String, String> conf, StorageEngine engine) {
		this.conf = conf;
		this.engine = engine;
	}

	@Override
	public void getData(DataObject object, StreamObserver<DataObject> responseObserver) {
		try {
//			TimeSeries timeSeries = null;
			// engine.getTimeSeries(object.getDbName(), object.getMeasurementName(),
			// object.getValueFieldName(), new ArrayList<>(object.getTagsList()));
//			List<Writer> list = timeSeries.getBucketRawMap().get(object.getBucket());
//			List<String> collect = list.stream().filter(v -> v.isFull() | v.isReadOnly())
//					.map(v -> new SimpleEntry<Long, ByteBuffer>(v.getHeaderTimestamp(), v.getRawBytes())).map(p -> {
//						ByteBuffer b = p.getValue();
//						ByteBuffer temp = ByteBuffer.allocate(b.capacity());
//						b.rewind();
//						temp.put(b);
//						byte[] payload = temp.array();
//						try {
//							String string = Base64.encodeBase64String(payload);
//							return p.getKey() + "_" + string;
//						} catch (Exception e) {
//							e.printStackTrace();
//							return null;
//						}
//					}).collect(Collectors.toList());
//			DataObject.Builder builder = DataObject.newBuilder(object);
//			builder.addAllBuf(collect);
//			object = builder.build();
//			responseObserver.onNext(object);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Fetch data for cluster read failed", e);
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	@Override
	public void getDeltas(DeltaRequest request, StreamObserver<DeltaResponse> responseObserver) {
		try {
			engine.checkIfExists(request.getDbName(), request.getMeasurementName());
		} catch (IOException e) {
			throw new NotFoundException(e.getMessage());
		}
		try {
			DeltaResponse.Builder deltas = DeltaResponse.newBuilder();
			Measurement measurement = engine.getOrCreateMeasurement(request.getDbName(), request.getMeasurementName());
			if (request.getCompact()) {
				measurement.compact();
			}
//			for (ByteString key : measurement.getSeriesKeys()) {
//				SeriesFieldMap seriesFromKey = measurement.getSeriesFromKey(key);
//				List<Tag> tags = Measurement.decodeStringToTags(measurement.getTagIndex(), key);
//				TimeSeries timeSeries = seriesFromKey.get(request.getValueFieldName());
//				SortedMap<Integer, List<Writer>> bucketRawMap = timeSeries.getBucketRawMap();
//				bucketRawMap = Utils.checkAndScopeTimeRange(request.getStartTime(), request.getEndTime(), timeSeries,
//						bucketRawMap);
//				for (Entry<Integer, List<Writer>> entry : bucketRawMap.entrySet()) {
//					DeltaObject object = DeltaUtils.buildAndAddDeltaObject(tags, request.getValueFieldName(), entry,
//							timeSeries);
//					deltas.addObject(object);
//				}
//			}
			responseObserver.onNext(deltas.build());
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Fetch DELTA for cluster failed db:" + request.getDbName() + " m:"
					+ request.getMeasurementName() + " v:" + request.getValueFieldName(), e);
			responseObserver.onError(e);
		}
		responseObserver.onCompleted();
	}

	public Map<String, String> getConf() {
		return conf;
	}

}
