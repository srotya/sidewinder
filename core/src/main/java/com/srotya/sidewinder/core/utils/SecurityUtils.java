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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ambud
 *
 */
public class SecurityUtils {

	private static final String SCOOKIE_DATE_FORMAT = "yyyy-dd-MM't'HH:mm:ss";

	private SecurityUtils() {
	}

	public static String createSCookie() {
		SimpleDateFormat formatter = new SimpleDateFormat(SCOOKIE_DATE_FORMAT);
		String format = formatter.format(new Date());
		// add encryption
		return format;
	}

	/**
	 * @param cookie
	 * @return
	 */
	public static boolean isValidSCookie(String cookie) {
		SimpleDateFormat formatter = new SimpleDateFormat(SCOOKIE_DATE_FORMAT);
		try {
			// decrypt then parse
			formatter.parse(cookie);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

}
