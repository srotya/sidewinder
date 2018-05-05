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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.srotya.sidewinder.core.sql.calcite.SqlFunctions.*;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class SidewinderDatabaseSchema extends AbstractSchema {

	private static final Logger logger = Logger.getLogger(SidewinderDatabaseSchema.class.getName());

	public SidewinderDatabaseSchema(StorageEngine engine, SchemaPlus parentSchema, int maxResultCount)
			throws Exception {
		for (String dbName : engine.getDatabases()) {
			logger.fine(() -> "Adding db:" + dbName);
			parentSchema.add(dbName.toUpperCase(),
					new SidewinderTableSchema(engine, parentSchema, dbName, maxResultCount));
		}
	}

	@Override
	protected Multimap<String, Function> getFunctionMultimap() {
		Map<String, Function> functionMap = new HashMap<>();
		functionMap.put("TOTIME", ScalarFunctionImpl.create(ToTimestamp.class, "eval"));
		functionMap.put("TOTS", ScalarFunctionImpl.create(ToTimestamp.class, "eval"));
		functionMap.put("TOMILLI", ScalarFunctionImpl.create(ToMilli.class, "eval"));
		functionMap.put("DATEDIFF", ScalarFunctionImpl.create(DateDiff.class, "eval"));
		functionMap.put("NOW", ScalarFunctionImpl.create(Now.class, "eval"));
		functionMap.put("TOLONG", ScalarFunctionImpl.create(ToLong.class, "eval"));
		Multimap<String, Function> map = ImmutableListMultimap.copyOf(functionMap.entrySet());
		return map;
	}

}