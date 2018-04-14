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

import java.sql.Timestamp;
import java.time.Instant;

import org.apache.calcite.linq4j.function.Function1;

/**
 * @author ambud
 */
public class ToTimestamp implements Function1<Long, Timestamp> {

	@Override
	public Timestamp apply(Long a0) {
		return Timestamp.from(Instant.ofEpochMilli(a0));
	}
	
	public static Timestamp eval(long a0) {
		return Timestamp.from(Instant.ofEpochMilli(a0));
	}

}
