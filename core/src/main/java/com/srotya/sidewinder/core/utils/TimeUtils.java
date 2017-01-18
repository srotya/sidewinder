/**
 * Copyright 2016 Ambud Sharma
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

import java.util.concurrent.TimeUnit;

/**
 * Utility class offering time functions
 * 
 * @author ambudsharma
 */
public class TimeUtils {

	private static final int MAX_BITS_TO_SHIFT = 63; // 32 bits to shift to
														// convert nanoseconds
														// and 31; to be
														// subtracted (log base
														// 2 (window))
	private static final IllegalArgumentException ARGUMENT_EXCEPTION = new IllegalArgumentException();

	private TimeUtils() {
	}

	/**
	 * Floor long time to supplied Window
	 * 
	 * @param unit
	 * @param timestamp
	 * @param bucketSizeInSeconds
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static int getTimeBucket(TimeUnit unit, long timestamp, int bucketSizeInSeconds)
			throws IllegalArgumentException {
		int ts = timeToSeconds(unit, timestamp);
		return getWindowFlooredNaturalTime(ts, bucketSizeInSeconds);
	}

	/**
	 * @param unit
	 * @param timestamp
	 * @param windowSizeInSeconds (must be a power of 2)
	 * @return
	 */
	public static int getWindowFlooredBinaryTime(TimeUnit unit, long timestamp, int windowSizeInSeconds) {
		timestamp = timeToNanoSeconds(unit, timestamp);
		System.err.println("shift:" + (MAX_BITS_TO_SHIFT - Integer.numberOfLeadingZeros(windowSizeInSeconds)));
		int ts = (int) (timestamp >> (MAX_BITS_TO_SHIFT - Integer.numberOfLeadingZeros(windowSizeInSeconds)));
		return ts;
	}

	/**
	 * Convert supplied timestamp to seconds;
	 * 
	 * @param time
	 * @param unit
	 * @return
	 */
	public static int timeToSeconds(TimeUnit unit, long time) {
		int ts;
		switch (unit) {
		case NANOSECONDS:
			ts = (int) (time / (1000 * 1000 * 1000));
			break;
		case MICROSECONDS:
			ts = (int) (time / (1000 * 1000));
			break;
		case MILLISECONDS:
			ts = (int) (time / 1000);
			break;
		case SECONDS:
			ts = (int) time;
			break;
		case MINUTES:
			ts = (int) (time * 60);
			break;
		case HOURS:
			ts = (int) (time * 3600);
			break;
		case DAYS:
			ts = (int) (time * 3600 * 24);
			break;
		default:
			throw ARGUMENT_EXCEPTION;
		}
		return ts;
	}

	/**
	 * @param unit
	 * @param time
	 * @return
	 */
	public static long timeToNanoSeconds(TimeUnit unit, long time) {
		long ts;
		switch (unit) {
		case NANOSECONDS:
			ts = time;
			break;
		case MICROSECONDS:
			ts = ((long) time * (1000));
			break;
		case MILLISECONDS:
			ts = (((long) time) * 1000 * 1000);
			break;
		case SECONDS:
			ts = (((long) time) * (1000 * 1000 * 1000));
			break;
		default:
			throw ARGUMENT_EXCEPTION;
		}
		return ts;
	}

	/**
	 * Floor integer time to supplied Window
	 * 
	 * @param timeInSeconds
	 * @param bucketInSeconds
	 * @return windowedTime
	 */
	public static int getWindowFlooredNaturalTime(int timeInSeconds, int bucketInSeconds) {
		/**
		 * bit shifting division and multiplication doesn't outperform the JIT
		 * compiler
		 */
		// return (timeInSeconds >> bucketInSeconds) << bucketInSeconds;
		return ((timeInSeconds / bucketInSeconds) * bucketInSeconds);
	}

}
