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
package com.srotya.sidewinder.core.sql.parser;

import static org.junit.Assert.*;

import org.junit.Test;

import com.srotya.sidewinder.core.sql.SQLDriver;
import com.srotya.sidewinder.core.sql.SQLParserBaseListener;
import com.srotya.sidewinder.core.sql.operators.Operator;

/**
 * @author ambud
 */
public class TestSQLDriver {

	@Test
	public void testSimpleStatement() {
		SQLParserBaseListener parseSQL = SQLDriver.parseSQL("select * from test");
		Operator filterTree = parseSQL.getFilterTree();
		assertNull(filterTree);
		assertEquals("test", parseSQL.getMeasurementName());
	}

	@Test
	public void testSimpleConditionStatement() {
		System.out.println("Timestamp:" + System.currentTimeMillis());
		SQLParserBaseListener parseSQL = SQLDriver.parseSQL("select * from test where datediff(timestamp, now())<5");
		Operator filterTree = parseSQL.getFilterTree();
		assertNotNull(filterTree);
		assertEquals("test", parseSQL.getMeasurementName());
		System.out.println(filterTree);
	}

	@Test
	public void testSimpleInConditionStatement() {
		System.out.println("Timestamp:" + System.currentTimeMillis());
		SQLParserBaseListener parseSQL = SQLDriver
				.parseSQL("select * from test where datediff(timestamp, now())<5 and tags in (host, cpu)");
		Operator filterTree = parseSQL.getFilterTree();
		assertNotNull(filterTree);
		assertEquals("test", parseSQL.getMeasurementName());
		System.out.println(filterTree);
	}

	@Test
	public void testSimpleInValueConditionStatement() {
		System.out.println("Timestamp:" + System.currentTimeMillis());
		SQLParserBaseListener parseSQL = SQLDriver
				.parseSQL("select * from test where value>5 and datediff(timestamp, now())<5 and tags in (host, cpu)");
		Operator filterTree = parseSQL.getFilterTree();
		assertNotNull(filterTree);
		assertEquals("test", parseSQL.getMeasurementName());
		System.out.println(filterTree);
	}

}
