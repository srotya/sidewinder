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
package com.srotya.sidewinder.core.api;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.InfluxDecoder;

/**
 * @author ambud
 */
@Path("/influx")
public class InfluxApi {

	private StorageEngine storageEngine;
	private Meter meter;

	public InfluxApi(StorageEngine storageEngine) {
		this.storageEngine = storageEngine;
		MetricRegistry registry = MetricsRegistryService.getInstance().getInstance("requests");
		meter = registry.meter("influx-writes");
	}

	@POST
	@Consumes({ MediaType.TEXT_PLAIN })
	public void insertData(@QueryParam("db") String dbName, String payload) {
		if (payload == null) {
			throw new BadRequestException("Empty request no acceptable");
		}
		List<DataPoint> dps = InfluxDecoder.dataPointsFromString(dbName, payload);
		if (dps.isEmpty()) {
			throw new BadRequestException("Empty request no acceptable");
		}
		meter.mark(dps.size());
		for (DataPoint dp : dps) {
			try {
				storageEngine.writeDataPoint(dp);
			} catch (IOException e) {
				throw new BadRequestException(e);
			}
		}
	}

}