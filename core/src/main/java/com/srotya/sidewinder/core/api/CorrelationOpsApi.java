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
package com.srotya.sidewinder.core.api;

import java.util.List;

import javax.ws.rs.Path;

import com.srotya.sidewinder.core.storage.StorageEngine;

/**
 * @author ambud
 */
@Path("/correlate/database/{dbName}/series/{" + CorrelationOpsApi.SERIES_NAME + "}")
public class CorrelationOpsApi {

	public static final String SERIES_NAME = "seriesName";

	public CorrelationOpsApi(StorageEngine storageEngine) {
	}

	
	public List<String> correlateSeries(String seriesBlock) {
//		correlation.correlation(xArray, yArray)
		
		return null;
	}
	
}
