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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.srotya.sidewinder.core.ingress.http.HTTPDataPointDecoder;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
@Path("/http")
public class InfluxApi {

	private StorageEngine storageEngine;

	public InfluxApi(StorageEngine storageEngine) {
		this.storageEngine = storageEngine;
	}
	
	@POST
	@Consumes({ MediaType.TEXT_PLAIN })
	public void insertData(@QueryParam("db") String dbName, String payload) {
		List<DataPoint> dps = HTTPDataPointDecoder.dataPointsFromString(dbName, payload);
		for (DataPoint dp : dps) {
			try {
				storageEngine.writeDataPoint(dbName, dp);
			} catch (IOException e) {
			}
		}
	}

}
