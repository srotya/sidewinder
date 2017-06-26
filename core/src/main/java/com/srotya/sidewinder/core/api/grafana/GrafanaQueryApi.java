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
package com.srotya.sidewinder.core.api.grafana;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.aggregators.FunctionTable;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * API specifically for designed for Grafana Sidewinder Datasource. This API is
 * currently NOT REST compliant and is designed to be purely functional.
 * 
 * @author ambud
 */
@Path("/{" + DatabaseOpsApi.DB_NAME + "}")
public class GrafanaQueryApi {

	private static final Logger logger = Logger.getLogger(GrafanaQueryApi.class.getName());
	private StorageEngine engine;
	private TimeZone tz;
	private Meter grafanaQueryCounter;

	public GrafanaQueryApi(StorageEngine engine, MetricRegistry registry) throws SQLException {
		this.engine = engine;
		tz = TimeZone.getDefault();
		grafanaQueryCounter = registry.meter("queries");
	}

	@Path("/hc")
	@GET
	public String getHealth(@PathParam(DatabaseOpsApi.DB_NAME) String dbName) throws Exception {
		logger.fine("Checking db name:" + dbName);
		if (engine.checkIfExists(dbName)) {
			return "true";
		} else {
			throw new NotFoundException("Database:" + dbName + " doesn't exist");
		}
	}

	@Path("/query")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public String query(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String query) throws ParseException {
		grafanaQueryCounter.mark();
		Gson gson = new GsonBuilder().create();
		JsonObject json = gson.fromJson(query, JsonObject.class);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		JsonObject range = json.get("range").getAsJsonObject();
		long startTs = sdf.parse(range.get("from").getAsString()).getTime();
		long endTs = sdf.parse(range.get("to").getAsString()).getTime();

		startTs = tz.getOffset(startTs) + startTs;
		endTs = tz.getOffset(endTs) + endTs;

		List<TargetSeries> targetSeries = new ArrayList<>();
		GrafanaUtils.extractTargetsFromJson(json, targetSeries);

		List<Target> output = new ArrayList<>();
		for (TargetSeries targetSeriesEntry : targetSeries) {
			try {
				GrafanaUtils.queryAndGetData(engine, dbName, startTs, endTs, output, targetSeriesEntry);
			} catch (IOException e) {
				throw new InternalServerErrorException(e);
			}
		}
		logger.log(Level.FINE, "Response:" + dbName + "\t" + gson.toJson(json) + "\t" + gson.toJson(output));
		return gson.toJson(output);
	}

	@Path("/query/measurements")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryMeasurementNames(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		logger.log(Level.FINE, "Query measurements for db:" + dbName);
		try {
			return engine.getMeasurementsLike(dbName, "");
		} catch (RejectException e) {
			throw new BadRequestException(e);
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		}
	}

	@Path("/query/tags")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryTags(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		logger.log(Level.FINE, "Query tags for db:" + dbName + "\t" + queryString);
		if (queryString == null || queryString.trim().isEmpty()) {
			throw new BadRequestException();
		}
		try {
			Gson gson = new Gson();
			JsonObject measurement = gson.fromJson(queryString, JsonObject.class);
			if (measurement.has("target")) {
				return engine.getTagsForMeasurement(dbName, measurement.get("target").getAsString());
			} else {
				throw new ItemNotFoundException("Bad request");
			}
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e.getMessage());
		}
	}

	@Path("/query/fields")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryFields(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		try {
			Gson gson = new Gson();
			JsonObject measurement = gson.fromJson(queryString, JsonObject.class);
			if (measurement.has("target")) {
				Set<String> response = engine.getFieldsForMeasurement(dbName, measurement.get("target").getAsString());
				logger.log(Level.INFO, "Query fields for db:" + dbName + "\t" + response + "\t" + queryString);
				return response;
			} else {
				throw new ItemNotFoundException("Bad request");
			}
		} catch (ItemNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		}
	}

	@Path("/query/ctypes")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryConditionTypes() {
		return new HashSet<>(Arrays.asList("AND", "OR"));
	}

	@Path("/query/aggregators")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryAggregators() {
		return FunctionTable.get().listFunctions();
	}

	@Path("/query/units")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryTimeUnits() {
		return new HashSet<>(Arrays.asList("secs", "mins", "hours", "days", "weeks", "years"));
	}

}
