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

import java.util.concurrent.TimeUnit;

import org.apache.calcite.linq4j.function.Function2;

import com.srotya.sidewinder.core.utils.TimeUtils;

/**
 * @author ambud
 */
public class ToMilliseconds implements Function2<Integer, String, Long> {

	@Override
	public Long apply(Integer v0, String v1) {
		return (long) TimeUtils.timeToSeconds(TimeUnit.valueOf(v1), v0) * 1000;
	}

}
