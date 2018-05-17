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
package com.srotya.sidewinder.core.api;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;
import com.srotya.sidewinder.core.api.grafana.TargetSeries;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.Measurement;
import com.srotya.sidewinder.core.storage.SeriesOutput;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

@Path("/databases")
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
		try {
			storageEngine.getOrCreateDatabase(dbName);
		} catch (NumberFormatException | IOException e) {
			throw new InternalServerErrorException(e);
		}
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
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e);
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

	@DELETE
	public void dropDatabases() {
		try {
			storageEngine.deleteAllData();
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/{" + DB_NAME + "}/query")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.TEXT_PLAIN })
	public String querySeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String query) {
		try {
			String[] queryParts = query.split("<=?");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			long endTs = System.currentTimeMillis();
			long startTs = endTs;
			String startTime = queryParts[0];
			String endTime = queryParts[2];
			if (startTime.contains("now")) {
				String[] split = startTime.split("-");
				int offset = Integer.parseInt(split[1].charAt(0) + "");
				startTs = startTs - (offset * 3600 * 1000);
			} else if (startTime.matches("\\d+")) {
				startTs = Long.parseLong(startTime);
				endTs = Long.parseLong(endTime);
			} else {
				startTs = sdf.parse(startTime).getTime();
				endTs = sdf.parse(endTime).getTime();
			}
			// cpu.load.host=v1.domain=test\.com=>derivative,10,mean

			TargetSeries tagSeries = MiscUtils.extractTargetFromQuery(query);

			List<SeriesOutput> points = storageEngine.queryDataPoints(dbName, tagSeries.getMeasurementName(),
					tagSeries.getFieldName(), startTs, endTs, tagSeries.getTagFilter(), null,
					tagSeries.getAggregationFunction());
			return new Gson().toJson(points);
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e);
		} catch (BadRequestException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/{" + DB_NAME + "}/gc")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	public int collectGarbage(@PathParam(DatabaseOpsApi.DB_NAME) String dbName) {
		Map<String, Measurement> map = storageEngine.getDatabaseMap().get(dbName);
		if (map == null) {
			throw new NotFoundException("Database not found:" + dbName);
		}
		int counter = 0;
		for (Entry<String, Measurement> entry : map.entrySet()) {
			try {
				Set<String> collectGarbage = entry.getValue().collectGarbage(null);
				if (collectGarbage != null) {
					counter += collectGarbage.size();
				}
			} catch (IOException e) {
				throw new InternalServerErrorException("Failed to collect garbage for:" + dbName + "." + entry.getKey(),
						e);
			}
		}
		return counter;
	}
	
	@Path("/{" + DB_NAME + "}/compact")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	public int compact(@PathParam(DatabaseOpsApi.DB_NAME) String dbName) {
		Map<String, Measurement> map = storageEngine.getDatabaseMap().get(dbName);
		if (map == null) {
			throw new NotFoundException("Database not found:" + dbName);
		}
		int counter = 0;
		for (Entry<String, Measurement> entry : map.entrySet()) {
			try {
				Set<String> compactedBuffers = entry.getValue().compact();
				if (compactedBuffers != null) {
					counter += compactedBuffers.size();
				}
			} catch (IOException e) {
				throw new InternalServerErrorException("Failed to compact:" + dbName + "." + entry.getKey(),
						e);
			}
		}
		return counter;
	}

}
