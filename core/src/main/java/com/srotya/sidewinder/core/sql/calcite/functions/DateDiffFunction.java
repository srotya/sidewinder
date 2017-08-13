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
package com.srotya.sidewinder.core.sql.calcite.functions;

import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Function2;

public class DateDiffFunction implements Function2<Timestamp, Timestamp, Long> {

	@Override
	public Long apply(Timestamp v0, Timestamp v1) {
		return Math.abs(v0.getTime() - v1.getTime());
	}

}
