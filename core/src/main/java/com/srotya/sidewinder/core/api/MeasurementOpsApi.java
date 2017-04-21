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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
@Path("/database/{dbName}/measurement/{" + MeasurementOpsApi.MEASUREMENT + "}")
public class MeasurementOpsApi {

	private static final String END_TIME = "endTime";
	private static final String START_TIME = "startTime";
	public static final String MEASUREMENT = "measurementName";
	private StorageEngine engine;

	public MeasurementOpsApi(StorageEngine storageEngine) {
		this.engine = storageEngine;
	}

	@PUT
	public void createMeasurement(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName) {
		try {
			engine.getOrCreateMeasurement(dbName, measurementName);
		} catch (IOException e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/series/{seriesName}")
	@PUT
	@Consumes({ MediaType.APPLICATION_JSON })
	public void createSeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName, String seriesConfig) {
		Gson gson = new Gson();
		JsonObject series = gson.fromJson(seriesConfig, JsonObject.class);
		List<String> tags = new ArrayList<>();
		for (JsonElement jsonElement : series.get("tags").getAsJsonArray()) {
			tags.add(jsonElement.getAsString());
		}
		try {
			engine.getOrCreateTimeSeries(dbName, measurementName, series.get("valueField").getAsString(), tags,
					series.get("timeBucket").getAsInt(), series.get("floatingPoint").getAsBoolean());
		} catch (IOException e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/series/{seriesName}/retention/{retentionPolicy}")
	@PUT
	public void updateRetentionPolicy(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName, @PathParam("retentionPolicy") int retentionPolicy) {
		engine.updateDefaultTimeSeriesRetentionPolicy(dbName, retentionPolicy);
		System.out.println("Updated retention policy for:" + dbName + "\t" + retentionPolicy + " hours");
	}

	@DELETE
	public void dropMeasurement(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName) {
		try {
			engine.dropMeasurement(dbName, measurementName);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

	@Path("/check")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public String checkMeasurement(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName) throws Exception {
		try {
			if (engine.checkIfExists(dbName, measurementName)) {
				return "true";
			} else {
				throw new NotFoundException("Measurement / database not found:" + dbName + "/" + measurementName);
			}
		} catch (Exception e) {
			if (e instanceof NotFoundException) {
				throw e;
			} else {
				throw new InternalServerErrorException(e);
			}
		}
	}

	@Path("/all")
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Map<String, List<DataPoint>> getAllOfMeasurement(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName) {
		try {
			return engine.queryDataPoints(dbName, measurementName, "value", 0, Long.MAX_VALUE, Arrays.asList(""), null);
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		} catch (IOException e) {
			throw new InternalServerErrorException(e);
		}
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public List<Number[]> getSeries(@PathParam(DatabaseOpsApi.DB_NAME) String dbName,
			@PathParam(MEASUREMENT) String measurementName,
			@DefaultValue("now-1h") @QueryParam(START_TIME) String startTime, @QueryParam(END_TIME) String endTime) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			long endTs = System.currentTimeMillis();
			long startTs = endTs;

			if (startTime.contains("now")) {
				String[] split = startTime.split("-");
				int offset = Integer.parseInt(split[1].charAt(0) + "");
				startTs = startTs - (offset * 3600 * 1000);
			} else {
				startTs = sdf.parse(startTime).getTime();
				endTs = sdf.parse(endTime).getTime();
			}
			Map<String, List<DataPoint>> points = engine.queryDataPoints(dbName, measurementName, "value", startTs,
					endTs, Arrays.asList(""), null);
			List<Number[]> response = new ArrayList<>();
			for (Entry<String, List<DataPoint>> entry : points.entrySet()) {
				for (DataPoint dataPoint : entry.getValue()) {
					if (!dataPoint.isFp()) {
						response.add(new Number[] { dataPoint.getLongValue(), dataPoint.getTimestamp() });
					} else {
						response.add(new Number[] { dataPoint.getValue(), dataPoint.getTimestamp() });
					}
				}
			}
			return response;
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}
	}

}
