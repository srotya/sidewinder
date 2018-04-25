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
package com.srotya.sidewinder.core.sql.calcite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;

import com.google.gson.JsonArray;
import com.srotya.sidewinder.core.api.SqlApi;
import com.srotya.sidewinder.core.utils.BackgrounThreadFactory;

public class CalciteTest {

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, SQLException, InterruptedException {
		ScheduledExecutorService bgt = Executors.newScheduledThreadPool(1,
				new BackgrounThreadFactory("sidewinderbg-tasks"));
		// SqlApi api = new SqlApi(null);

		JdbcMeta meta = new JdbcMeta(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory;");
		Service service = new LocalService(meta);
		HttpServer server = new HttpServer(1099, new AvaticaJsonHandler(service));
		server.start();
		//

//		Class.forName("org.apache.calcite.avatica.remote.Driver");

//		Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:1099");
//		Statement st = connection.createStatement();
//		ResultSet resultSet = st.executeQuery("select * from db1.m1");
//		JsonArray convert = SqlApi.convert(resultSet);
//		System.err.println(convert);
//		st.close();
//		connection.close();

		while(true) {
			Thread.sleep(10000);
		}
		
		// long ts = System.currentTimeMillis();
		// TimeSeries s = engine.getOrCreateTimeSeries("db1", "m1", "v1",
		// Arrays.asList(Tag.newBuilder().setTagKey("t").setTagValue("1").build(),
		// Tag.newBuilder().setTagKey("t").setTagValue("2").build()),
		// 1024, false);
		// for (int i = 0; i < 100; i++) {
		// s.addDataPoint(TimeUnit.MILLISECONDS, ts + i * 1000, i);
		// }
		//
		// while (true) {
		// System.out.print("Enter query:");
		// Scanner sc = new Scanner(System.in);
		// String nextLine = sc.nextLine();
		// try {
		// String queryResults = api.queryResults("db1", nextLine);
		// System.out.println(queryResults);
		// } catch (Exception e) {
		// }
		// }
	}
}
