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
package com.srotya.sidewinder.cluster.push.api;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.codahale.metrics.MetricRegistry;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.core.rpc.Point;

@Path("/cluster/databases")
public class DatabaseOpsApi {

	public static final String DB_NAME = "dbName";
	private RoutingEngine engine;

	public DatabaseOpsApi(RoutingEngine engine, MetricRegistry registry) {
		this.engine = engine;
		if (registry != null) {
			// register things
		}
	}

	@Path("/{DB_NAME}/measurements/{MEASUREMENT_NAME}")
	@POST
	public void createMeasurement(@PathParam("DB_NAME") String dbName,
			@PathParam("MEASUREMENT_NAME") String measurementName) {
		Point point = Point.newBuilder().setDbName(dbName).setMeasurementName(measurementName).build();
		try {
			engine.addRoutableKey(point, 3);
		} catch (UnsupportedOperationException e) {
			throw new BadRequestException(e);
		}
	}

}
