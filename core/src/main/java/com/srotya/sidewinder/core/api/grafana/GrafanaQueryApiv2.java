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
package com.srotya.sidewinder.core.api.grafana;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
import javax.ws.rs.core.Response;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.api.DatabaseOpsApi;
import com.srotya.sidewinder.core.functions.iterative.FunctionIteratorTable;
import com.srotya.sidewinder.core.monitoring.MetricsRegistryService;
import com.srotya.sidewinder.core.storage.ItemNotFoundException;
import com.srotya.sidewinder.core.storage.RejectException;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.utils.GrafanaStreamingOutput;
import com.srotya.sidewinder.core.utils.InvalidFilterException;

/**
 * API specifically for designed for Grafana Sidewinder Datasource. This API is
 * currently NOT REST compliant and is designed to be purely functional.
 * 
 * @author ambud
 */
@Path("/grafana/v2/{" + DatabaseOpsApi.DB_NAME + "}")
public class GrafanaQueryApiv2 {

	private static final Logger logger = Logger.getLogger(GrafanaQueryApiv2.class.getName());
	private StorageEngine engine;
	private TimeZone tz;
	private Meter grafanaQueryCounter;
	private Timer grafanaQueryLatency;

	public GrafanaQueryApiv2(StorageEngine engine) throws SQLException {
		this.engine = engine;
		tz = TimeZone.getDefault();
		MetricRegistry registry = MetricsRegistryService.getInstance().getInstance("grafana");
		grafanaQueryCounter = registry.meter("queries");
		grafanaQueryLatency = registry.timer("latency");
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
	@Consumes({ MediaType.APPLICATION_JSON })
	public Response queryData(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString)
			throws ParseException {
		grafanaQueryCounter.mark();
		Context time = grafanaQueryLatency.time();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		logger.log(Level.FINE,
				() -> "Grafana query:" + dbName + "\t" + gson.toJson(gson.fromJson(queryString, JsonObject.class)));
		JsonObject json = gson.fromJson(queryString, JsonObject.class);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		JsonObject range = json.get("range").getAsJsonObject();
		long startTs = sdf.parse(range.get("from").getAsString()).getTime();
		long endTs = sdf.parse(range.get("to").getAsString()).getTime();

		startTs = tz.getOffset(startTs) + startTs;
		endTs = tz.getOffset(endTs) + endTs;

		List<TargetSeries> targetSeries = new ArrayList<>();
		try {
			GrafanaUtils.extractTargetsFromJson(json, targetSeries);
		} catch (InvalidFilterException e) {
			throw new BadRequestException(e.getMessage());
		}

		List<Iterator<GrafanaOutputv2>> output = new ArrayList<>();
		logger.log(Level.FINE,
				"Extracted targets from query json, target count:" + targetSeries.size() + " " + new Date(startTs));
		for (TargetSeries targetSeriesEntry : targetSeries) {
			logger.log(Level.FINE, () -> "Running grafana query fetch for:" + targetSeriesEntry);
			try {
				Iterator<GrafanaOutputv2> outputIterator = GrafanaUtils.queryAndGetDatav2(engine, dbName, startTs,
						endTs, targetSeriesEntry);
				if (outputIterator != null) {
					output.add(outputIterator);
				}
			} catch (IOException e) {
				throw new InternalServerErrorException(e);
			}
		}
		time.stop();

		// Adding sorted output so series colors do not change in grafana
		logger.log(Level.FINER, () -> "Grafana query result size:" + output.size());
		return Response.ok(new GrafanaStreamingOutput(output)).type("application/json").build();
	}

	@Path("/query/measurements")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryMeasurementNames(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		logger.log(Level.FINE, () -> "Query measurements for db:" + dbName + "\t" + queryString);
		try {
			if (queryString != null && !queryString.isEmpty()) {
				JsonObject query = new Gson().fromJson(queryString, JsonObject.class);
				if (query.has("target")) {
					String target = query.get("target").getAsString();
					if (target.startsWith("measurement:")) {
						return engine.getTagKeysForMeasurement(dbName, target.replace("measurement:", ""));
					} else if (target.contains("field:")) {
						return engine.getFieldsForMeasurement(dbName, target.replace("field:", ""));
					} else {
						return engine.getMeasurementsLike(dbName, target);
					}
				}
			}
			return engine.getMeasurementsLike(dbName, "");
		} catch (RejectException e) {
			throw new BadRequestException(e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new InternalServerErrorException(e.getMessage());
		}
	}

	@Path("/query/tags")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryTagKeys(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		logger.log(Level.FINE, () -> "Query tags for db:" + dbName + "\t" + queryString);
		if (queryString == null || queryString.trim().isEmpty()) {
			throw new BadRequestException();
		}
		try {
			Gson gson = new Gson();
			JsonObject measurement = gson.fromJson(queryString, JsonObject.class);
			if (measurement.has("target")) {
				return engine.getTagKeysForMeasurement(dbName, measurement.get("target").getAsString());
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

	@Path("/query/tagvs")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryTagValues(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String queryString) {
		logger.log(Level.FINE, () -> "Query tag values for db:" + dbName + "\t" + queryString);
		if (queryString == null || queryString.trim().isEmpty()) {
			throw new BadRequestException();
		}
		try {
			Gson gson = new Gson();
			JsonObject measurement = gson.fromJson(queryString, JsonObject.class);
			if (measurement.has("target")) {
				return engine.getTagValuesForMeasurement(dbName, measurement.get("target").getAsString(),
						measurement.get("tag").getAsString());
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
				logger.log(Level.FINE, () -> "Query fields for db:" + dbName + "\t" + response + "\t" + queryString);
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

	@Path("/query/otypes")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryOperatorTypes() {
		return new HashSet<>(Arrays.asList("=", ">", "<", ">=", "<=", "~"));
	}

	@Path("/query/aggregators")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryAggregators() {
		return FunctionIteratorTable.get().listFunctions();
	}

	@Path("/query/units")
	@POST
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_JSON })
	public Set<String> queryTimeUnits() {
		return new HashSet<>(Arrays.asList("secs", "mins", "hours", "days", "weeks", "months", "years"));
	}
}