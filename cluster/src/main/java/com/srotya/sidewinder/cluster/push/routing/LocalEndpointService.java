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
package com.srotya.sidewinder.cluster.push.routing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.StorageEngine;

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
		if (point.getFp()) {
			storageEngine.writeDataPoint(point.getDbName(), point.getMeasurementName(), point.getValueFieldName(),
					new ArrayList<>(point.getTagsList()), point.getTimestamp(), Double.longBitsToDouble(point.getValue()));
		} else {
			storageEngine.writeDataPoint(point.getDbName(), point.getMeasurementName(), point.getValueFieldName(),
					new ArrayList<>(point.getTagsList()), point.getTimestamp(), point.getValue());
		}
	}

	@Override
	public void close() throws IOException {
		storageEngine.disconnect();
	}

	@Override
	public void requestRouteEntry(Point point) throws IOException {
		engine.addRoutableKey(point, 3);
	}

	@Override
	public ListenableFuture<Ack> writeAsync(Point point) throws InterruptedException, IOException {
		storageEngine.writeDataPoint(point);
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(List<Point> points) throws InterruptedException, IOException {
		for (Point point : points) {
			storageEngine.writeDataPoint(point);
		}
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, List<Point> points)
			throws InterruptedException, IOException {
		for (Point point : points) {
			storageEngine.writeDataPoint(point);
		}
		return null;
	}

	@Override
	public ListenableFuture<Ack> writeAsync(long messageId, Point point) throws InterruptedException, IOException {
		storageEngine.writeDataPoint(point);
		return null;
	}

}
