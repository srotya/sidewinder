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
package com.srotya.sidewinder.core.utils;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Unit tests for {@link TimeUtils}
 * 
 * @author ambud
 */
public class TestTimeUtils {

	@Test
	public void testGetTimeBucket() {
		long timestamp = System.currentTimeMillis();
		int timeBucket = TimeUtils.getTimeBucket(TimeUnit.MILLISECONDS, timestamp, 3600);
		assertEquals(((timestamp / 1000) / 3600) * 3600, timeBucket);
	}

	@Test
	public void testTimeToSeconds() {
		long time = System.currentTimeMillis() / 1000 / 3600;
		int timeToSeconds = TimeUtils.timeToSeconds(TimeUnit.HOURS, time);
		assertEquals(time * 3600, timeToSeconds);

		time = System.currentTimeMillis() * 1000 * 1000;
		timeToSeconds = TimeUtils.timeToSeconds(TimeUnit.NANOSECONDS, time);
		assertEquals(time / 1000 / 1000 / 1000, timeToSeconds);
	}

	@Test
	public void testFlooredNaturalTime() {
		int time = (int) (System.currentTimeMillis() / 1000);
		int windowFlooredNaturalTime = TimeUtils.getWindowFlooredNaturalTime(time, 3600);
		assertEquals((time / 3600) * 3600, windowFlooredNaturalTime);
	}
}
