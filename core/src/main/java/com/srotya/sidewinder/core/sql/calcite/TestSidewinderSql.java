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
package com.srotya.sidewinder.core.sql.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author ambud
 */
public class TestSidewinderSql {

	public static void main(String[] args) throws ClassNotFoundException {
		Class.forName("org.apache.calcite.jdbc.Driver");
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		// info.setProperty("operand.db", "test");
		try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/model.json",
				info)) {
			Statement st = connection.createStatement();
			ResultSet results = st.executeQuery(
					"select cpu_value,time_stamp from test.cpu_value where cpu_value>11.0 and tags like '%host%' and time_stamp>totimestamp(1484646984784)"
			// "select time_stamp,cpu_value from test.cpu_value where
			// cpu_value>11.0 and datediff(time_stamp, now()) < tomilli(10,
			// 'HOURS')"
			);//
				// "select cpu_value,time_stamp from test.cpu_value where
				// cpu_value>11.0 and time_stamp<now()"
				// "select cpu_value,time_stamp from test.cpu_value where
				// cpu_value>11.0 and datediff(time_stamp, now()) < tomilli(10,
				// 'HOURS')"
			ResultSetMetaData rsmd = results.getMetaData();
			System.out.println("\n\n:");
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				System.out.print("Column:" + rsmd.getColumnName(i) + ",");
			}
			System.out.println("\n\n");
			while (results.next()) {
				System.out.println("Measurements:" + results.getLong(2) + "\t" + results.getDouble(1));
			}
			st.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
