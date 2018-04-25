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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.storage.ByteString;
import com.srotya.sidewinder.core.storage.Series;
import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
public class SidewinderDatabaseSchema extends AbstractSchema {

	private static final Logger logger = Logger.getLogger(SidewinderDatabaseSchema.class.getName());
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
			MockMeasurement measurement = new MockMeasurement(1024, 100);
			measurement.setTimebucket(4096);
			Series series = new Series(measurement, new ByteString("series"), 0);
			long ts = System.currentTimeMillis();
			for (int i = 0; i < 1000; i++) {
				Point dp = Point.newBuilder().setTimestamp(ts + i * 1000).addValueFieldName("F").addFp(false)
						.addValue(i).addValueFieldName("L").addFp(true).addValue(Double.doubleToLongBits(i * 1.1))
						.build();
				series.addPoint(dp, measurement);
			}
			tableMap.put("M1", new MeasurementTable(measurement, series, Arrays.asList("F", "L")));
			// for (String measurementName : engine.getAllMeasurementsForDb(dbName)) {
			// tableMap.put(measurementName.toUpperCase(), new MeasurementTable(engine,
			// dbName, measurementName,
			// engine.getFieldsForMeasurement(dbName, measurementName),
			// engine.getTagKeysForMeasurement(dbName, measurementName)));
			// }
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to get table map for query", e);
		}
		return tableMap;
	}

}