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
package com.srotya.sidewinder.core.sql.calcite.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
import com.srotya.sidewinder.core.sql.calcite.SidewinderDatabaseSchema;
import com.srotya.sidewinder.core.storage.StorageEngine;
import com.srotya.sidewinder.core.storage.mem.MemStorageEngine;

@RunWith(MockitoJUnitRunner.class)
public class TestSchema {

	@Mock
	private SchemaPlus parent;

	@Test
	public void testCalciteLocal() throws SQLException, IOException {
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
		SidewinderDatabaseSchema schema = new SidewinderDatabaseSchema(engine, parent);
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
			new SidewinderDatabaseSchema(engine, null);
			fail("Must throw exception");
		} catch (Exception e) {
		}
	}

}
