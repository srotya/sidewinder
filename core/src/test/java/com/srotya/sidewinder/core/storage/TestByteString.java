/**
 * Copyright 2018 Ambud Sharma
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
package com.srotya.sidewinder.core.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestByteString {

	@Test
	public void testSplits() {
		ByteString str = new ByteString("abcdefg");
		ByteString[] split = str.split("e");
		assertEquals(2, split.length);
		assertEquals("abcd", split[0].toString());
		assertEquals("fg", split[1].toString());

		str = new ByteString("ab=cd=fg=asaa");
		split = str.split("=");
		assertEquals(4, split.length);
		assertEquals("ab", split[0].toString());
		assertEquals("cd", split[1].toString());
		assertEquals("fg", split[2].toString());
		assertEquals("asaa", split[3].toString());
	}

	@Test
	public void testToBytes() {
		ByteString str = new ByteString("abcdefg");
		assertEquals(7, str.getBytes().length);
		assertEquals(7, str.length());
	}

	@Test
	public void testToBytesLinkedByteString() {
		LinkedByteString bs = new LinkedByteString(ByteString.get("abcd"), ByteString.get("efgh"),
				ByteString.get("ijkl"));
		assertEquals(12, bs.getBytes().length);
	}
}
