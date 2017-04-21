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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class SidewinderDatabaseSchema extends AbstractSchema {

	private String dbName;
	private StorageEngine engine;

	public SidewinderDatabaseSchema(StorageEngine engine, String dbName) {
		this.engine = engine;
		this.dbName = dbName;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		Map<String, Table> tableMap = new HashMap<>();
		try {
			for (String measurementName : engine.getAllMeasurementsForDb(dbName)) {
				for (String fieldName : engine.getFieldsForMeasurement(dbName, measurementName)) {
					boolean isFp = engine.isMeasurementFieldFP(dbName, measurementName, fieldName);
					tableMap.put((measurementName + "_" + fieldName).toUpperCase(),
							new MeasurementTable(engine, dbName, measurementName, fieldName, isFp));
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.println(tableMap);
		return tableMap;
	}

}