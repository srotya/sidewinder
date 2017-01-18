/**
 * Copyright 2016 Ambud Sharma
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

import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud.sharma
 */
@Path("/database/{" + DatabaseOpsApi.DB_NAME + "}")
public class SqlOpsApi {

//	private StorageEngine storageEngine;

	public SqlOpsApi(StorageEngine storageEngine) {
//		this.storageEngine = storageEngine;
	}

	@Consumes({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	@Produces({ MediaType.APPLICATION_JSON })
	@GET
	public List<Map<Long, Double>> getData(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String sql) {
		
		return null;
	}

}
