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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

public class ByteString implements Comparable<ByteString> {

	private byte[] data;

	public static ByteString get(String str) {
		return new ByteString(str);
	}

	public ByteString(String str) {
		data = new byte[str.length()];
		for (int i = 0; i < str.length(); i++) {
			data[i] = (byte) str.charAt(i);
		}
	}
	
	public ByteString(ByteString str) {
		data = new byte[str.length()];
		for (int i = 0; i < str.length(); i++) {
			data[i] = str.charAt(i);
		}
	}

	public byte charAt(int i) {
		return data[i];
	}

	public String toString() {
		try {
			return new String(data, "ascii");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public ByteString(byte[] str) {
		data = str;
	}

	public boolean isEmpty() {
		return data == null || data.length == 0;
	}

	@Override
	public int hashCode() {
		int h = 0;
		if (data != null) {
			for (int i = 0; i < data.length; i++) {
				h = 31 * h + data[i];
			}
		}
		return h;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof ByteString) {
			ByteString str = ((ByteString) obj);
			if (str.data.length == data.length) {
				for (int i = 0; i < data.length; i++) {
					if (str.data[i] != data[i]) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}

	public int indexOf(byte ch, int startIndex) {
		for (int i = startIndex; i < data.length; i++) {
			byte b = data[i];
			if (b == ch) {
				return i;
			}
		}
		return -1;
	}

	public ByteString subString(int start, int length) {
		if (start + length > data.length) {
			throw new IllegalArgumentException("Invalid substring offsets:" + start + " length:" + length);
		}
		byte[] ary = new byte[length];
		System.arraycopy(data, start, ary, 0, length);
		return new ByteString(ary);
	}

	public ByteString[] split(String separator) {
		byte tagSeparator = (byte) separator.charAt(0);
		List<ByteString> str = new ArrayList<>();
		int curr = 0;
		int indexOf = indexOf(tagSeparator, curr);
		while (curr < data.length) {
			if (indexOf == -1) {
				str.add(subString(curr, data.length - curr));
				break;
			}
			str.add(subString(curr, indexOf - curr));
			curr = indexOf + 1;
			indexOf = indexOf(tagSeparator, curr);
		}
		return str.toArray(new ByteString[1]);
	}

	public int length() {
		return data.length;
	}

	@Override
	public int compareTo(ByteString o) {
		final byte[] v1 = data;
		final byte[] v2 = o.data;
		final int n = Math.min(v1.length, v2.length);
		for (int i = 0; i < n; i++) {
			int c1 = v1[i] & 0xff;
			int c2 = v2[i] & 0xff;
			if (c1 != c2) {
				return c1 - c2;
			}
		}
		return v1.length - v2.length;
	}

	@NotThreadSafe
	public static class StringCache {

		private Map<ByteString, ByteString> cache = new HashMap<>();

		private StringCache() {
		}

		public static StringCache instance() {
			return new StringCache();
		}

		public ByteString get(ByteString entry) {
			ByteString e = cache.get(entry);
			if (e == null) {
				e = entry;
				cache.put(entry, entry);
			}
			return e;
		}

	}

	public byte[] getBytes() {
		return data;
	}

}
