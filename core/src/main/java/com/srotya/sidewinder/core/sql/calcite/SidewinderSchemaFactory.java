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

import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.srotya.sidewinder.core.SidewinderServer;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class SidewinderSchemaFactory implements SchemaFactory {

	public SidewinderSchemaFactory() {
	}

	@Override
	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
		StorageEngine storageEngine = SidewinderServer.getStorageEngine();
		Object dbName = operand.get("dbName");
		if (dbName == null) {
			try {
				return new SidewinderDatabaseSchema(storageEngine, parentSchema);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			return new SidewinderTableSchema(storageEngine, parentSchema, dbName.toString());
		}
	}

}
