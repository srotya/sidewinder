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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.srotya.sidewinder.core.SidewinderServer;
import com.srotya.sidewinder.core.rpc.Tag;
import com.srotya.sidewinder.core.sql.calcite.SidewinderDatabaseSchema;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;
import com.srotya.sidewinder.core.utils.MiscUtils;

@RunWith(MockitoJUnitRunner.class)
public class TestSchemaAndBasicQueries {

	@Mock
	private SchemaPlus parent;

	@Test
	public void testFunctions() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("db12", "m12", "v1",
							Arrays.asList(Tag.newBuilder().setTagKey("k1").setTagValue("v1").build()), ts + i * 100, i),
					true);
		}
		SidewinderServer.setStorageEngine(engine);
		Connection connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory;");
		Statement st = connection.createStatement();

		ResultSet rset = st.executeQuery("select totime(ts) from db12.m12");
		int c = 0;
		while (rset.next()) {
			assertEquals(Timestamp.from(Instant.ofEpochMilli(ts + c * 100)), rset.getTimestamp(1));
			c++;
		}
		rset.close();

		rset = st.executeQuery("select tolong(totime(ts)) from db12.m12");
		c = 0;
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(1));
			c++;
		}
		rset.close();

		st.close();
		connection.close();
	}

	@Test
	public void testCalciteLocalParameters() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("db12", "m12", "v1",
							Arrays.asList(Tag.newBuilder().setTagKey("k1").setTagValue("v1").build()), ts + i * 100, i),
					true);
		}
		SidewinderServer.setStorageEngine(engine);
		Connection connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory;schema.maxResult=500");
		Statement st = connection.createStatement();

		ResultSet rset = st.executeQuery("select * from db12.m12");
		int c = 0;
		assertEquals(3, rset.getMetaData().getColumnCount());
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(1));
			assertEquals(c, rset.getLong(2));
			c++;
		}
		assertEquals(500, c);

		st.close();
		connection.close();

		connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory");
		st = connection.createStatement();

		rset = st.executeQuery("select * from db12.m12");
		c = 0;
		assertEquals(3, rset.getMetaData().getColumnCount());
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(1));
			assertEquals(c, rset.getLong(2));
			c++;
		}
		assertEquals(1000, c);

		st.close();
		connection.close();
	}

	@Test
	public void testCalciteLocalSelect() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		long ts = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			engine.writeDataPointLocked(
					MiscUtils.buildDataPoint("db12", "m12", "v1",
							Arrays.asList(Tag.newBuilder().setTagKey("k1").setTagValue("v1").build()), ts + i * 100, i),
					true);
		}
		SidewinderServer.setStorageEngine(engine);
		Connection connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory");
		Statement st = connection.createStatement();
		ResultSet rset = st.executeQuery("select * from db12.m12");
		int c = 0;
		assertEquals(3, rset.getMetaData().getColumnCount());
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(1));
			assertEquals(c, rset.getLong(2));
			c++;
		}
		assertEquals(100, c);
		rset.close();

		rset = st.executeQuery("select count(*) from db12.m12");
		assertTrue(rset.next());
		assertEquals(100, rset.getLong(1));
		rset.close();

		rset = st.executeQuery("select v1, ts from db12.m12");
		assertEquals(2, rset.getMetaData().getColumnCount());
		c = 0;
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(2));
			assertEquals(c, rset.getLong(1));
			c++;
		}
		rset.close();

		rset = st.executeQuery("select ts,v1,k1 from db12.m12");
		assertEquals(3, rset.getMetaData().getColumnCount());
		c = 0;
		while (rset.next()) {
			assertEquals(ts + c * 100, rset.getLong(1));
			assertEquals(c, rset.getLong(2));
			assertEquals("v1", rset.getString(3));
			c++;
		}
		rset.close();

		st.close();
		connection.close();
	}

	@Test
	public void testCalciteLocalConnection() throws SQLException, IOException, InterruptedException {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		engine.getOrCreateMeasurement("db1", "m1");
		SidewinderServer.setStorageEngine(engine);
		Connection connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory");
		Statement st = connection.createStatement();
		st.executeQuery("select * from DB1.M1");
		// query must succeed without any failures
		st.close();

		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet rs = metaData.getTables(null, null, "%", null);
		int c = 0;
		while (rs.next()) {
			if (!rs.getString(2).equalsIgnoreCase("metadata") && !rs.getString(2).equalsIgnoreCase("_internal")) {
				assertEquals("DB1", rs.getString(2));
				assertEquals("M1", rs.getString(3));
				c++;
			}
		}
		assertEquals(1, c);
		connection.close();

		engine.getOrCreateMeasurement("db1", "m2");

		connection = DriverManager.getConnection(
				"jdbc:calcite:schemaFactory=com.srotya.sidewinder.core.sql.calcite.SidewinderSchemaFactory");
		metaData = connection.getMetaData();
		rs = metaData.getTables(null, null, "%", null);
		c = 0;
		while (rs.next()) {
			if (!rs.getString(2).equalsIgnoreCase("metadata") && !rs.getString(2).equalsIgnoreCase("_internal")) {
				c++;
			}
		}
		assertEquals(2, c);
		connection.close();

	}

	@Test
	public void testDatabaseSchema() throws Exception {
		Map<String, Schema> map = new HashMap<>();
		when(parent.add(Matchers.anyString(), Matchers.any(Schema.class))).then(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				map.put(invocation.getArgument(0), invocation.getArgument(1));
				return null;
			}
		});

		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		engine.getOrCreateMeasurement("db1", "m1");
		SidewinderDatabaseSchema schema = new SidewinderDatabaseSchema(engine, parent, 100);
		assertEquals(0, schema.getTableNames().size());
		assertEquals(1, map.size());
		assertEquals("DB1", map.keySet().iterator().next());

		Schema next = map.values().iterator().next();
		Set<String> tableNames = next.getTableNames();
		assertEquals(1, tableNames.size());
		assertEquals("M1", tableNames.iterator().next());
	}

	@Test
	public void testDatabaseSchemaFail() throws Exception {
		StorageEngine engine = new MemStorageEngine();
		engine.configure(new HashMap<>(), null);
		engine.getOrCreateMeasurement("db1", "m1");
		try {
			new SidewinderDatabaseSchema(engine, null, 100);
			fail("Must throw exception");
		} catch (Exception e) {
		}
	}

}
