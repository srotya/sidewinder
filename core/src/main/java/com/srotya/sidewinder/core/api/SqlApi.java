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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.srotya.sidewinder.core.sql.calcite.SidewinderDatabaseSchema;
import com.srotya.sidewinder.core.sql.calcite.functions.DateDiffFunction;
import com.srotya.sidewinder.core.sql.calcite.functions.NowFunction;
import com.srotya.sidewinder.core.sql.calcite.functions.ToMilliseconds;
import com.srotya.sidewinder.core.sql.calcite.functions.ToTimestamp;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * REST API to service SQL queries
 * 
 * @author ambud
 */
@Path("/sql")
public class SqlApi {

	private StorageEngine engine;
	private Connection connection;

	public SqlApi(StorageEngine engine) throws SQLException, ClassNotFoundException {
		this.engine = engine;
		initCalcite();
	}

	public void initCalcite() throws SQLException, ClassNotFoundException {
		Class.forName("org.apache.calcite.jdbc.Driver");
		connection = DriverManager.getConnection("jdbc:calcite:");
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
		ModelHandler.create(calciteConnection.getRootSchema(), "now", Arrays.asList(""), NowFunction.class.getName(),
				"apply");
		ModelHandler.create(calciteConnection.getRootSchema(), "tomilli", Arrays.asList(""),
				ToMilliseconds.class.getName(), "apply");
		ModelHandler.create(calciteConnection.getRootSchema(), "totimestamp", Arrays.asList(""),
				ToTimestamp.class.getName(), "apply");
		ModelHandler.create(calciteConnection.getRootSchema(), "datediff", Arrays.asList(""),
				DateDiffFunction.class.getName(), "apply");
	}

	public boolean checkAndAddSchema(String dbName) throws Exception {
		synchronized (connection) {
			if (!engine.checkIfExists(dbName)) {
				return false;
			}
			CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
			String tdbName = dbName.toUpperCase();
			if (calciteConnection.getRootSchema().getSubSchema(tdbName) == null) {
				System.err.println("Adding DB to connection:" + dbName);
				calciteConnection.getRootSchema().add(tdbName, new SidewinderDatabaseSchema(engine, dbName));
			}
			return true;
		}
	}

	@Path("/database/{" + DatabaseOpsApi.DB_NAME + "}")
	@Produces({ MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.TEXT_PLAIN })
	@POST
	public String queryResults(@PathParam(DatabaseOpsApi.DB_NAME) String dbName, String sql) {
		try {
			if (!checkAndAddSchema(dbName)) {
				throw new NotFoundException("Database " + dbName + " not found");
			}
			Statement st = connection.createStatement();
			ResultSet resultSet = st.executeQuery(sql);
			JsonArray convert = convert(resultSet);
			Gson gson = new Gson();
			resultSet.close();
			st.close();
			return gson.toJson(convert);
		} catch (NotFoundException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new BadRequestException(e.getMessage());
		}
	}

	public static JsonArray convert(ResultSet rs) throws SQLException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		JsonArray json = new JsonArray();
		ResultSetMetaData rsmd = rs.getMetaData();

		while (rs.next()) {
			int numColumns = rsmd.getColumnCount();
			JsonObject obj = new JsonObject();

			for (int i = 1; i < numColumns + 1; i++) {
				String column_name = rsmd.getColumnName(i);
				switch (rsmd.getColumnType(i)) {
				case java.sql.Types.ARRAY:
					break;
				case java.sql.Types.BIGINT:
					obj.addProperty(column_name, rs.getInt(column_name));
					break;
				case java.sql.Types.DOUBLE:
					obj.addProperty(column_name, rs.getDouble(column_name));
					break;
				case java.sql.Types.TIMESTAMP:
					obj.addProperty(column_name, rs.getTimestamp(column_name).getTime());
					break;
				case java.sql.Types.DATE:
					obj.addProperty(column_name, format.format(rs.getDate(column_name)));
					break;
				case java.sql.Types.VARCHAR:
				case java.sql.Types.NVARCHAR:
					break;
				default:
					break;
				}
			}
			json.add(obj);
		}

		return json;
	}

}
