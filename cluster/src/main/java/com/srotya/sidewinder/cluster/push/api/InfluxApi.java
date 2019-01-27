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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.srotya.sidewinder.cluster.push.routing.Node;
import com.srotya.sidewinder.cluster.push.routing.RoutingEngine;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.utils.InfluxDecoder;

/**
 * @author ambud
 */
@Path("/influx")
public class InfluxApi {
	
	private static AtomicInteger counter = new AtomicInteger(0);
	private RoutingEngine engine;
	
	public InfluxApi(RoutingEngine engine) {
		this.engine = engine;
	}

	@POST
	@Consumes({ MediaType.TEXT_PLAIN })
	public void insertData(@QueryParam("db") String dbName, String payload) {
		if (payload == null) {
			throw new BadRequestException("Empty request no acceptable");
		}
		List<Point> dps = InfluxDecoder.pointsFromString(dbName, payload);
		counter.addAndGet(dps.size());
		if (dps.isEmpty()) {
			throw new BadRequestException("Empty request no acceptable");
		}
		for (Point dp : dps) {
			try {
				List<Node> routeData = engine.routeData(dp);
				for (Node node : routeData) {
					node.getEndpointService().write(dp);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
