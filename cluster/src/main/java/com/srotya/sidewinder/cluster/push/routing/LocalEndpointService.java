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
package com.srotya.sidewinder.cluster.push.routing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.srotya.sidewinder.cluster.rpc.Query;
import com.srotya.sidewinder.cluster.rpc.QueryResponse;
import com.srotya.sidewinder.cluster.rpc.QueryResponses;
import com.srotya.sidewinder.cluster.rpc.QueryResponses.Builder;
import com.srotya.sidewinder.core.filters.TagFilter;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.InvalidFilterException;
import com.srotya.sidewinder.core.utils.MiscUtils;

/**
 * @author ambud
 */
public class LocalEndpointService implements EndpointService {

	private StorageEngine storageEngine;
	private RoutingEngine engine;

	public LocalEndpointService(StorageEngine storageEngine, RoutingEngine engine) {
		this.storageEngine = storageEngine;
		this.engine = engine;
	}

	@Override
	public void write(Point point) throws IOException {
		storageEngine.writeDataPointWithLock(point, false);
	}

	@Override
	public void close() throws IOException {
		storageEngine.shutdown();
	}

	@Override
	public void requestRouteEntry(Point point) throws IOException {
		engine.addRoutableKey(point, 3);
	}

	@Override
	public ListenableFuture<Ack> writeAsync(Point point) throws InterruptedException, IOException {
		storageEngine.writeDataPointWithLock(point, false);
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(List<Point> points) throws InterruptedException, IOException {
		for (Point point : points) {
			storageEngine.writeDataPointWithLock(point, false);
		}
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, List<Point> points)
			throws InterruptedException, IOException {
		for (Point point : points) {
			storageEngine.writeDataPointWithLock(point, false);
		}
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, Point point) throws InterruptedException, IOException {
		storageEngine.writeDataPointWithLock(point, false);
		return null;
	}

	@Override
	public QueryResponses readData(Query query) throws IOException {
		Builder queryResponseBuilder = QueryResponses.newBuilder();
		List<Series> resultMap = new ArrayList<>();
		TagFilter filter = null;
		try {
			filter = MiscUtils.buildTagFilter(query.getTagFilter());
		} catch (InvalidFilterException e) {
			throw new IOException("Invalid query", e);
		}
		storageEngine.getDatabaseMap().get(query.getDbName()).get(query.getMeasurementName());
		// TODO fix
//		.queryDataPoints(
//				query.getValueFieldName(), query.getStartTs(), query.getEndTs(), filter, null, resultMap);
//		for (Series series : resultMap) {
//			QueryResponse response = QueryResponse.newBuilder().setDbName(query.getDbName())
//					.setMeasurementName(series.getMeasurementName()).setValueFieldName(series.getValueFieldName())
//					.setFp(series.isFp()).addAllTags(series.getTags()).setMessageId(query.getMessageId()).build();
//			queryResponseBuilder.addResponses(response);
//		}
		return queryResponseBuilder.build();
	}

}
