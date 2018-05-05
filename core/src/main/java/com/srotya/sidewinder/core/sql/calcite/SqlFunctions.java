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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.srotya.sidewinder.core.utils.TimeUtils;

public class SqlFunctions {

	public static class ToTimestamp {

		public Timestamp eval(Long arg) {
			return Timestamp.from(Instant.ofEpochMilli(arg));
		}

	}

	public static class ToMilli {

		public Long eval(Integer v0, String v1) {
			return (long) TimeUtils.timeToSeconds(TimeUnit.valueOf(v1), v0) * 1000;
		}

	}

	public static class ToLong {

		public Long eval(Timestamp ts) {
			return ts.getTime();
		}

	}

	public static class Now {

		public Timestamp eval() {
			return Timestamp.from(Instant.now());
		}

	}

	public static class DateDiff {

		public Long eval(Timestamp v0, Timestamp v1) {
			return Math.abs(v0.getTime() - v1.getTime());
		}

	}

}
