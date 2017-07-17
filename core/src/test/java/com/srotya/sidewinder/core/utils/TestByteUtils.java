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

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.google.gson.JsonArray;

/**
 * Unit tests for {@link ByteUtils}
 * 
 * @author ambud
 */
public class TestByteUtils {

	@Test
	public void testIntToByteAndBack() {
		for (int i = 0; i < 100; i++) {
			byte[] intToByteMSB = ByteUtils.intToByteMSB(i);
			int bytesToIntMSB = ByteUtils.bytesToIntMSB(intToByteMSB);
			assertEquals(i, bytesToIntMSB);
		}
	}

	@Test
	public void testLongBytes() {
		byte[] doubleToBytes = ByteUtils.doubleToBytes(2.2);
		double bytesToDouble = ByteUtils.bytesToDouble(doubleToBytes);
		assertEquals(2.2, bytesToDouble, 0.01);
	}

	@Test
	public void testJsonArrayToList() {
		JsonArray ary = new JsonArray();
		ary.add("test");
		ary.add("test1");
		ary.add("test2");
		List<String> list = ByteUtils.jsonArrayToStringList(ary);
		assertEquals("test", list.get(0));
		assertEquals("test1", list.get(1));
		assertEquals("test2", list.get(2));
	}

	@Test
	public void testByteAryToAscii() {
		int time = 1380327876;
		System.out.println("time:" + new Date((long) time * 1000));
		time = Integer.MAX_VALUE - time;
		System.out.println("inverted time:" + time);
		System.out.println("inverted time:" + new Date((long) time * 1000));
		System.out.println("inverted time:" + ByteUtils.byteAryToAscii(ByteUtils.intToByteMSB(time)));
		System.out.println("max time:" + new Date((long) Integer.MAX_VALUE * 1000));
		System.out.println("max time:" + ByteUtils.byteAryToAscii(ByteUtils.intToByteMSB(Integer.MAX_VALUE)));
	}

}
