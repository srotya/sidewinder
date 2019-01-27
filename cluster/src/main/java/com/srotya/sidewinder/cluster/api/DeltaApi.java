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
package com.srotya.sidewinder.cluster.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.codec.binary.Base64;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.srotya.sidewinder.cluster.Utils;
import com.srotya.sidewinder.cluster.api.MerkleTreeUtils.MerkleTree;
import com.srotya.sidewinder.cluster.rpc.DataObject;
import com.srotya.sidewinder.cluster.rpc.DeltaObject;
import com.srotya.sidewinder.cluster.rpc.DeltaResponse;
import com.srotya.sidewinder.cluster.rpc.DeltaUtils;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.compression.Writer;

@Path("/rpc")
public class DeltaApi {

//	private static final Logger logger = Logger.getLogger(DeltaApi.class.getName());
//	private StorageEngine engine;
//	private Timer timer;
//
//	public DeltaApi(StorageEngine engine) {
//		this.engine = engine;
//		MetricRegistry registry = MetricsRegistryService.getInstance().getInstance("requests");
//		timer = registry.timer("delta");
//	}
//
//	@Path("/data")
//	@POST
//	@Consumes({ MediaType.TEXT_PLAIN })
//	@Produces({ MediaType.TEXT_PLAIN })
//	public String getData(String input) throws IOException {
//		DataObject.Builder builder = DataObject.newBuilder();
//		JsonFormat.parser().merge(input, builder);
//		DataObject object = builder.build();
//		Series timeSeries = null;
////		 engine.getTimeSeries(object.getDbName(), object.getMeasurementName(),
////					object.getValueFieldName(), new ArrayList<>(object.getTagsList()));
//		if (timeSeries == null) {
//			throw new NotFoundException();
//		}
//		try {
//			List<Writer> list = timeSeries.getBucketMap().get(object.getBucket());
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
//			builder.addAllBuf(collect);
//			return JsonFormat.printer().print(builder.build());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			throw e;
//		}
//
//	}
//
//	@Path("/gc/{dbName}/{measurement}")
//	@POST
//	@Produces("application/json")
//	public void gc(@PathParam("dbName") String dbName, @PathParam("measurement") String measurementName) {
//		try {
//			engine.checkIfExists(dbName, measurementName);
//		} catch (IOException e) {
//			throw new NotFoundException(e.getMessage());
//		}
//		try {
//			for (int i = 0; i < 1000; i++) {
//				Measurement measurement = engine.getOrCreateMeasurement(dbName, measurementName);
//				measurement.collectGarbage(null);
//			}
//		} catch (IOException e) {
//			throw new InternalServerErrorException(e);
//		}
//	}
//
//	@Path("/compact/{dbName}/{measurement}")
//	@POST
//	@Produces("application/json")
//	public int compact(@PathParam("dbName") String dbName, @PathParam("measurement") String measurementName) {
//		try {
//			engine.checkIfExists(dbName, measurementName);
//		} catch (IOException e) {
//			throw new NotFoundException(e.getMessage());
//		}
//		try {
//			Measurement measurement = engine.getOrCreateMeasurement(dbName, measurementName);
//			return measurement.compact().size();
//		} catch (IOException e) {
//			throw new InternalServerErrorException(e);
//		}
//	}
//
//	@Path("/merkle/{dbName}/{measurement}")
//	@GET
//	@Produces("application/json")
//	public List<MerkleTree> merkleTree(@PathParam("dbName") String dbName,
//			@PathParam("measurement") String measurementName, @QueryParam("compact") boolean compact,
//			@QueryParam("startTime") long startTime, @QueryParam("endTime") long endTime) {
//		List<MerkleTree> trees = new ArrayList<>();
//		try {
//			engine.checkIfExists(dbName, measurementName);
//		} catch (IOException e) {
//			throw new NotFoundException(e.getMessage());
//		}
//		try {
//			Measurement measurement = engine.getOrCreateMeasurement(dbName, measurementName);
//			if (compact) {
//				measurement.compact();
//			}
//			for (ByteString key : measurement.getSeriesKeys()) {
//				SeriesFieldMap seriesFromKey = measurement.getSeriesFromKey(key);
//				List<Tag> tags = Measurement.decodeStringToTags(measurement.getTagIndex(), key);
//				for (String field : seriesFromKey.getFields()) {
//					MerkleTreeUtils.buildMerkleTreeForSeries(startTime, endTime, trees, seriesFromKey, tags, field);
//				}
//			}
//		} catch (IOException | NoSuchAlgorithmException e) {
//			throw new InternalServerErrorException(e);
//		}
//		return trees;
//	}
//
//	@Path("/delta/{dbName}/{measurement}")
//	@GET
//	@Produces("text/plain")
//	public String getDeltas(@PathParam("dbName") String dbName, @PathParam("measurement") String measurementName,
//			@QueryParam("compact") boolean compact, @QueryParam("startTime") long startTime,
//			@QueryParam("endTime") long endTime) throws InvalidProtocolBufferException {
//		Context time = timer.time();
//		logger.fine("Received delta request for:" + dbName + ":" + measurementName);
//		DeltaResponse.Builder deltaResponse = DeltaResponse.newBuilder();
//		try {
//			engine.checkIfExists(dbName, measurementName);
//		} catch (IOException e) {
//			throw new NotFoundException(e.getMessage());
//		}
//		try {
//			Measurement measurement = engine.getOrCreateMeasurement(dbName, measurementName);
//			if (compact) {
//				measurement.compact();
//			}
//			for (ByteString key : measurement.getSeriesKeys()) {
//				SeriesFieldMap seriesFromKey = measurement.getSeriesFromKey(key);
//				List<Tag> tags = Measurement.decodeStringToTags(measurement.getTagIndex(), key);
//				for (String field : seriesFromKey.getFields()) {
//					TimeSeries timeSeries = seriesFromKey.get(field);
//					SortedMap<Integer, List<Writer>> bucketRawMap = timeSeries.getBucketRawMap();
//					bucketRawMap = Utils.checkAndScopeTimeRange(startTime, endTime, timeSeries, bucketRawMap);
//					for (Entry<Integer, List<Writer>> entry : bucketRawMap.entrySet()) {
//						DeltaObject object = DeltaUtils.buildAndAddDeltaObject(tags, field, entry, timeSeries);
//						if (object != null) {
//							deltaResponse.addObject(object);
//						}
//					}
//				}
//			}
//		} catch (IOException e) {
//			throw new InternalServerErrorException(e);
//		}
//		time.stop();
//		return JsonFormat.printer().print(deltaResponse);
//	}

}
