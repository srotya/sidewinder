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
package com.srotya.sidewinder.cluster.routing;

import java.io.IOException;
import java.util.ArrayList;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.DataPoint;
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
		DataPoint dp = new DataPoint();
		dp.setDbName(point.getDbName());
		dp.setFp(point.getFp());
		dp.setLongValue(point.getValue());
		dp.setMeasurementName(point.getMeasurementName());
		dp.setTags(new ArrayList<>(point.getTagsList()));
		dp.setTimestamp(point.getTimestamp());
		dp.setValueFieldName(point.getValueFieldName());
//		System.err.println("Writing dp:" + dp);
		storageEngine.writeDataPoint(dp);
	}

	@Override
	public void close() throws IOException {
		storageEngine.disconnect();
	}

	@Override
	public void requestRouteEntry(Point point) throws IOException {
		engine.addRoutableKey(point, 3);
	}

}
