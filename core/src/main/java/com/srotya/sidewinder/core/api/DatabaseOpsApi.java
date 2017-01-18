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

import java.util.Set;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.SecurityUtils;

@Path("/database")
public class DatabaseOpsApi {

	public static final String DB_NAME = "dbName";
	private StorageEngine storageEngine;

	public DatabaseOpsApi(StorageEngine storageEngine) {
		this.storageEngine = storageEngine;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Set<String> showDatabases() throws Exception {
		return storageEngine.getDatabases();
	}

	@Path("/{" + DB_NAME + "}")
	@PUT
	public void createDatabase(@PathParam(DB_NAME) String dbName,
			@DefaultValue("28") @QueryParam("retentionPolicy") String retentionPolicy) {
		storageEngine.getOrCreateDatabase(dbName, Integer.parseInt(retentionPolicy));
	}

	@Path("/{" + DB_NAME + "}")
	@DELETE
	public void dropDatabase(@PathParam(DB_NAME) String dbName) {
		try {
			storageEngine.dropDatabase(dbName);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/{" + DB_NAME + "}")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Set<String> listMeasurements(@PathParam(DB_NAME) String dbName) {
		try {
			Set<String> measurements = storageEngine.getAllMeasurementsForDb(dbName);
			if (measurements.size() == 0) {
				throw new NotFoundException("No such database:" + dbName);
			} else {
				return measurements;
			}
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/{" + DB_NAME + "}/check")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public String checkIfExists(@PathParam(DB_NAME) String dbName) {
		try {
			if (!storageEngine.checkIfExists(dbName)) {
				throw new NotFoundException("Database:" + dbName + " not found");
			} else {
				return "true";
			}
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/scookie")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public String getSCookie() {
		return SecurityUtils.createSCookie();
	}

	@DELETE
	public void dropDatabases(@QueryParam("scookie") String cookie) {
		if (SecurityUtils.isValidSCookie(cookie)) {
			try {
				storageEngine.deleteAllData();
			} catch (Exception e) {
				throw new InternalServerErrorException(e);
			}
		} else {
			throw new BadRequestException("Invalid SCookie");
		}
	}

}
